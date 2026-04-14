use futures::{Sink, SinkExt, Stream};

#[cfg(feature = "prometheus")]
use crate::metrics::{
    dec_inflight_slot_download,
    inc_offset_commitment_count, inc_skip_offset_commitment_count, inc_slot_download_count,
    inc_slot_status_offset_processed_count, inc_total_event_downloaded,
    observe_slot_download_duration, set_max_slot_detected,
    set_processed_slot_status_offset_queue_len, set_slot_status_update_queue_len,
};
use {
    super::state_machine::{FumaroleSM, FumeDownloadRequest, FumeOffset, FumeShardIdx},
    crate::{
        FumaroleClient, FumaroleGrpcConnector, FumaroleSubscribeError, GrpcFumaroleClient,
        proto::{
            self, BlockFilters, CommitOffset, ControlCommand, DataCommand, DataResponse,
            DownloadBlockShard, GetChainTipResponse, JoinControlPlane, PollBlockchainHistory,
            control_response::Response, data_command::Command, data_response,
        },
    },
    futures::StreamExt,
    solana_clock::Slot,
    std::{
        collections::{HashMap, VecDeque},
        error::Error as StdError,
        future::Future,
        pin::Pin,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc::{self, error::TrySendError},
        task::{self, Id, JoinSet},
    },
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::PollSender,
    tonic::Code,
    yellowstone_grpc_proto::geyser::{
        self, CommitmentLevel, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
        subscribe_update::UpdateOneof,
    },
};

pub const DEFAULT_GC_INTERVAL: usize = 100;

///
/// Mimics Dragonsmouth subscribe request bidirectional stream.
///
pub struct DragonsmouthSubscribeRequestBidi {
    #[allow(dead_code)]
    pub tx: mpsc::Sender<SubscribeRequest>,
    pub rx: mpsc::Receiver<SubscribeRequest>,
}

impl DataPlaneConn {
    pub const fn new(client: GrpcFumaroleClient) -> Self {
        Self {
            client,
        }
    }
}

pub enum DownloadTaskResult {
    Ok(CompletedDownloadBlockTask),
    Err { slot: Slot, err: DownloadBlockError },
}

pub enum BackgroundJobResult {
    #[allow(dead_code)]
    UpdateTip(GetChainTipResponse),
}

///
/// Fumarole runtime based on Tokio outputting Dragonsmouth only events.
///
/// Drives the Fumarole State-Machine ([`FumaroleSM`]) using Async I/O.
///
pub(crate) struct TokioFumeDragonsmouthRuntime<C>
where
    C: ControlPlaneConnector,
{
    pub sm: FumaroleSM,
    #[allow(dead_code)]
    pub blockchain_id: Vec<u8>,
    pub fumarole_client: FumaroleClient,
    pub download_task_runner_chans: DownloadTaskRunnerChannels,
    pub dragonsmouth_bidi: DragonsmouthSubscribeRequestBidi,
    pub subscribe_request: Arc<SubscribeRequest>,
    pub persistent_subscriber_name: String,
    pub control_plane_connector: C,
    pub control_plane_tx: C::ControlPlaneSink,
    pub control_plane_rx: C::ControlPlaneStream,
    pub dragonsmouth_outlet:
        mpsc::Sender<Result<geyser::SubscribeUpdate, FumaroleSubscribeError>>,
    pub commit_interval: Duration,
    pub get_tip_interval: Duration,
    pub last_commit: Instant,
    pub last_tip: Instant,
    pub last_history_poll: Option<Instant>,
    pub gc_interval: usize, // in ticks
    pub non_critical_background_jobs: JoinSet<BackgroundJobResult>,
    pub no_commit: bool,
    pub stop: bool,
}

const DEFAULT_HISTORY_POLL_SIZE: i64 = 100;

const fn build_poll_history_cmd(from: Option<FumeOffset>) -> ControlCommand {
    ControlCommand {
        command: Some(proto::control_command::Command::PollHist(
            // from None means poll the entire history from wherever we left off since last commit.
            PollBlockchainHistory {
                shard_id: 0, /*ALWAYS 0-FOR FIRST VERSION OF FUMAROLE */
                from,
                limit: Some(DEFAULT_HISTORY_POLL_SIZE),
            },
        )),
    }
}

const fn build_commit_offset_cmd(offset: FumeOffset) -> ControlCommand {
    ControlCommand {
        command: Some(proto::control_command::Command::CommitOffset(
            CommitOffset {
                offset,
                shard_id: 0, /*ALWAYS 0-FOR FIRST VERSION OF FUMAROLE */
            },
        )),
    }
}

impl From<SubscribeRequest> for BlockFilters {
    fn from(val: SubscribeRequest) -> Self {
        BlockFilters {
            accounts: val.accounts,
            transactions: val.transactions,
            entries: val.entry,
            blocks_meta: val.blocks_meta,
        }
    }
}

enum LoopInstruction {
    Continue,
    ErrorStop,
}

pub type BoxedProtocolError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub enum ControlPlaneStreamError {
    Disconnected(BoxedProtocolError),
    ApplicationError(BoxedProtocolError),
}

impl std::fmt::Display for ControlPlaneStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected(err) => write!(f, "control plane disconnected: {err}"),
            Self::ApplicationError(err) => write!(f, "control plane application error: {err}"),
        }
    }
}

impl StdError for ControlPlaneStreamError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Disconnected(err) | Self::ApplicationError(err) => Some(err.as_ref()),
        }
    }
}


pub trait ControlPlaneConnector {
    type SubscribeError: StdError + Send + Sync + 'static;

    type ControlPlaneSink: Sink<proto::ControlCommand> + Send + Unpin;
    type ControlPlaneStream: Stream<Item = Result<proto::ControlResponse, ControlPlaneStreamError>>
        + Unpin;

    type SubscribeFut: Future<
            Output = Result<(Self::ControlPlaneSink, Self::ControlPlaneStream), Self::SubscribeError>,
        > + Send;

    fn subscribe(&self, initial_join: JoinControlPlane) -> Self::SubscribeFut;
}

impl ControlPlaneConnector for FumaroleClient {
    type SubscribeError = tonic::Status;
    type ControlPlaneSink = PollSender<proto::ControlCommand>;
    type ControlPlaneStream = ReceiverStream<Result<proto::ControlResponse, ControlPlaneStreamError>>;
    type SubscribeFut = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (Self::ControlPlaneSink, Self::ControlPlaneStream),
                        Self::SubscribeError,
                    >,
                > + Send,
        >,
    >;

    fn subscribe(&self, initial_join: JoinControlPlane) -> Self::SubscribeFut {
        let mut client = self.inner.clone();
        Box::pin(async move {
            let (control_plane_tx, control_plane_rx) = mpsc::channel(100);
            let initial_join_command = ControlCommand {
                command: Some(proto::control_command::Command::InitialJoin(initial_join)),
            };
            control_plane_tx
                .send(initial_join_command)
                .await
                .expect("failed to send initial join");

            let resp = client.subscribe_v2(ReceiverStream::new(control_plane_rx)).await?;
            let mut streaming = resp.into_inner();

            let (bounded_tx, bounded_rx) = mpsc::channel(100);
            tokio::spawn(async move {
                while let Some(result) = streaming.message().await.transpose() {
                    let mapped = result.map_err(|status| match status.code() {
                        Code::Unavailable | Code::DataLoss | Code::Internal => {
                            ControlPlaneStreamError::Disconnected(Box::new(status))
                        }
                        _ => ControlPlaneStreamError::ApplicationError(Box::new(status)),
                    });
                    if bounded_tx.send(mapped).await.is_err() {
                        break;
                    }
                }
            });

            Ok((PollSender::new(control_plane_tx), ReceiverStream::new(bounded_rx)))
        })
    }
}

impl<C> TokioFumeDragonsmouthRuntime<C>
where
    C: ControlPlaneConnector,
{
    fn handle_control_response(&mut self, control_response: proto::ControlResponse) {
        let Some(response) = control_response.response else {
            return;
        };
        match response {
            proto::control_response::Response::CommitOffset(commit_offset_result) => {
                tracing::debug!("received commit offset : {commit_offset_result:?}");
                self.sm.update_committed_offset(commit_offset_result.offset);
            }
            proto::control_response::Response::PollHist(blockchain_history) => {
                self.last_history_poll = None;
                if !blockchain_history.events.is_empty() {
                    tracing::debug!(
                        "polled blockchain history : {} events",
                        blockchain_history.events.len()
                    );
                }

                self.sm.queue_blockchain_event(blockchain_history.events);
                #[cfg(feature = "prometheus")]
                {
                    set_max_slot_detected(self.sm.max_slot_detected);
                }
            }
            proto::control_response::Response::Pong(_pong) => {
                tracing::debug!("pong");
            }
            proto::control_response::Response::Init(_init) => {
                unreachable!("init should not be received here");
            }
        }
    }

    async fn poll_history_if_needed(&mut self) {
        if self.last_history_poll.is_none() && self.sm.need_new_blockchain_events() {
            #[cfg(feature = "prometheus")]
            {
                use crate::metrics::inc_poll_history_call_count;
                inc_poll_history_call_count();
            }
            tracing::trace!(
                "polling blockchain history from offset {}",
                self.sm.unprocessed_blockchain_event.len()
            );
            let cmd = build_poll_history_cmd(Some(self.sm.committable_offset));
            if self.control_plane_tx.send(cmd).await.is_err() {
                panic!("control plane disconnected");
            }
            self.last_history_poll = Some(Instant::now());
        }
    }

    fn commitment_level(&self) -> Option<geyser::CommitmentLevel> {
        self.subscribe_request
            .commitment
            .map(|cl| CommitmentLevel::try_from(cl).expect("invalid commitment level"))
    }

    fn schedule_download_task_if_any(&mut self) {
        // This loop drains as many download slot request as possible,
        // limited to available [`DataPlaneBidi`].
        loop {
            let result = self
                .download_task_runner_chans
                .download_task_queue_tx
                .try_reserve();
            let permit = match result {
                Ok(permit) => permit,
                Err(TrySendError::Full(_)) => {
                    #[cfg(feature = "prometheus")]
                    {
                        use crate::metrics::incr_download_queue_full_detection_count;
                        incr_download_queue_full_detection_count();
                    }
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    panic!("download task runner closed unexpectedly")
                }
            };

            let Some(download_request) = self.sm.pop_slot_to_download(self.commitment_level())
            else {
                break;
            };
            let download_task_args = DownloadTaskArgs { download_request };
            permit.send(download_task_args);
        }
    }

    async fn handle_download_result(&mut self, download_result: DownloadTaskResult) {
        match download_result {
            DownloadTaskResult::Ok(completed) => {
                let CompletedDownloadBlockTask {
                    slot,
                    block_uid: _,
                    shard_idx_vec,
                } = completed;
                for shard_idx in shard_idx_vec {
                    self.sm.make_slot_download_progress(slot, Some(shard_idx));
                }
            }
            DownloadTaskResult::Err { slot, err } => {
                // TODO add option to let user decide what to do, by default let it crash
                match err {
                    DownloadBlockError::OutletDisconnected => {
                        // Will be handled in the main loop.
                        self.stop = true;
                    }
                    DownloadBlockError::FailedDownload(dataplane_err) => {
                        if matches!(dataplane_err.kind(), DataplaneErrorKind::SlotNotFound) {
                            self.sm.make_slot_download_progress(slot, None);
                            tracing::error!("slot {slot} not found, skipping...");
                        } else {
                            self.stop = true;
                            let _ = self.dragonsmouth_outlet.send(Err(dataplane_err.into())).await;
                        }
                    }
                    DownloadBlockError::IncompleteDownload => {
                        self.stop = true;
                        // This should not happen, so we panic here.
                        panic!("Incomplete download for slot {slot}");
                    }
                }
            }
        }
    }

    async unsafe fn force_commit_offset(&mut self) {
        if self.no_commit {
            tracing::debug!("no_commit is set, skipping offset commitment");
            self.sm.update_committed_offset(self.sm.committable_offset);
            return;
        }
        self.control_plane_tx
            .send(build_commit_offset_cmd(self.sm.committable_offset))
            .await
            .unwrap_or_else(|_| panic!("failed to commit offset"));
        #[cfg(feature = "prometheus")]
        {
            use crate::metrics::set_max_offset_committed;

            inc_offset_commitment_count();
            set_max_offset_committed(self.sm.committable_offset);
        }
    }

    async fn commit_offset(&mut self) {
        if self.sm.last_committed_offset < self.sm.committable_offset {
            unsafe {
                self.force_commit_offset().await;
            }
        } else {
            #[cfg(feature = "prometheus")]
            {
                inc_skip_offset_commitment_count();
            }
        }

        self.last_commit = Instant::now();
    }

    async fn drain_slot_status(&mut self) {
        let commitment = self.subscribe_request.commitment();
        let mut slot_status_vec = VecDeque::with_capacity(10);

        while let Some(slot_status) = self.sm.pop_next_slot_status() {
            slot_status_vec.push_back(slot_status);
        }

        if slot_status_vec.is_empty() {
            return;
        }

        for slot_status in slot_status_vec {
            let mut matched_filters = vec![];
            for (filter_name, filter) in &self.subscribe_request.slots {
                if let Some(true) = filter.filter_by_commitment {
                    if slot_status.commitment_level == commitment {
                        matched_filters.push(filter_name.clone());
                    }
                } else {
                    matched_filters.push(filter_name.clone());
                }
            }

            if !matched_filters.is_empty() {
                let update = SubscribeUpdate {
                    filters: matched_filters,
                    created_at: None,
                    update_oneof: Some(geyser::subscribe_update::UpdateOneof::Slot(
                        SubscribeUpdateSlot {
                            slot: slot_status.slot,
                            parent: slot_status.parent_slot,
                            status: slot_status.commitment_level.into(),
                            dead_error: slot_status.dead_error,
                        },
                    )),
                };
                if self.dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                    return;
                }
            }

            self.sm
                .mark_event_as_processed(slot_status.session_sequence);
            #[cfg(feature = "prometheus")]
            {
                inc_slot_status_offset_processed_count();
            }
        }

        #[cfg(feature = "prometheus")]
        {
            set_processed_slot_status_offset_queue_len(self.sm.processed_offset_queue_len());
        }
    }

    async fn rejoin_controle_plane(&mut self) -> Result<(), C::SubscribeError> {
        self.last_history_poll = None;
        let initial_join = JoinControlPlane {
            consumer_group_name: Some(self.persistent_subscriber_name.clone()),
        };
        let (control_plane_tx, mut control_plane_rx) =
            self.control_plane_connector.subscribe(initial_join).await?;
        let initial_response = control_plane_rx
            .next()
            .await
            .expect("control plane closed before init")
            .expect("control plane init error");
        let response = initial_response.response.expect("none");
        let Response::Init(initial_state) = response else {
            panic!("unexpected initial response: {response:?}")
        };
        self.control_plane_tx = control_plane_tx;
        self.control_plane_rx = control_plane_rx;
        tracing::info!("rejoined control plane with initial state: {initial_state:?}");
        Ok(())
    }

    async fn handle_control_plane_resp(
        &mut self,
        result: Result<proto::ControlResponse, ControlPlaneStreamError>,
    ) -> LoopInstruction {
        match result {
            Ok(control_response) => {
                self.handle_control_response(control_response);
                LoopInstruction::Continue
            }
            Err(ControlPlaneStreamError::Disconnected(e)) => {
                tracing::warn!(
                    "control plane connection lost with error: {e:?}, attempting to rejoin..."
                );
                match self.rejoin_controle_plane().await {
                    Ok(_) => LoopInstruction::Continue,
                    Err(e) => {
                        tracing::error!("failed to rejoin control plane with error: {e:?}");
                        let _ = self
                            .dragonsmouth_outlet
                            .send(Err(FumaroleSubscribeError::ControlPlaneDisconnected))
                            .await;
                        LoopInstruction::ErrorStop
                    }
                }
            }
            Err(ControlPlaneStreamError::ApplicationError(e)) => {
                tracing::error!("control plane application error: {e:?}");
                let _ = self
                    .dragonsmouth_outlet
                    .send(Err(FumaroleSubscribeError::ControlPlaneDisconnected))
                    .await;
                LoopInstruction::ErrorStop
            }
        }
    }

    async fn handle_new_subscribe_request(&mut self, subscribe_request: SubscribeRequest) {
        self.subscribe_request = Arc::new(subscribe_request);
        self.download_task_runner_chans
            .cnc_tx
            .send(DownloadTaskRunnerCommand::UpdateSubscribeRequest(
                Arc::clone(&self.subscribe_request),
            ))
            .await
            .expect("failed to send subscribe request");
    }

    async fn update_tip(&mut self) {
        #[cfg(feature = "prometheus")]
        {
            use crate::proto::GetChainTipRequest;

            let mut fumarole_client = self.fumarole_client.clone();
            let blockchain_id = self.blockchain_id.clone();
            let job = async move {
                let result = fumarole_client
                    .get_chain_tip(GetChainTipRequest { blockchain_id })
                    .await
                    .expect("failed to get chain tip")
                    .into_inner();
                BackgroundJobResult::UpdateTip(result)
            };

            self.non_critical_background_jobs.spawn(job);
        }
        self.last_tip = Instant::now();
    }

    fn handle_non_critical_job_result(&mut self, result: BackgroundJobResult) {
        match result {
            BackgroundJobResult::UpdateTip(get_tip_response) => {
                tracing::debug!("received get tip response: {get_tip_response:?}");
                let GetChainTipResponse {
                    shard_to_max_offset_map,
                    ..
                } = get_tip_response;
                if shard_to_max_offset_map.is_empty() {
                    tracing::warn!("get tip response is empty, no shard to max offset map");
                    return;
                }
                if let Some(tip) = shard_to_max_offset_map.values().max() {
                    tracing::trace!("tip is {tip}");
                    #[cfg(feature = "prometheus")]
                    {
                        use crate::metrics::set_fumarole_blockchain_offset_tip;
                        set_fumarole_blockchain_offset_tip(*tip);
                    }
                }
            }
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.poll_history_if_needed().await;

        // Always start to commit offset, to make sure not another instance is committing to the same offset.
        unsafe {
            self.force_commit_offset().await;
        }
        let mut ticks = 0;
        while !self.stop {
            ticks += 1;
            if ticks % self.gc_interval == 0 {
                self.sm.gc();
                ticks = 0;
            }
            if self.dragonsmouth_outlet.is_closed() {
                tracing::debug!("Detected dragonsmouth outlet closed");
                break;
            }

            #[cfg(feature = "prometheus")]
            {
                let slot_status_update_queue_len = self.sm.slot_status_update_queue_len();
                set_slot_status_update_queue_len(slot_status_update_queue_len);
            }

            let get_tip_deadline = self.last_tip + self.get_tip_interval;
            let commit_deadline = self.last_commit + self.commit_interval;

            self.poll_history_if_needed().await;
            self.schedule_download_task_if_any();
            tokio::select! {
                Some(subscribe_request) = self.dragonsmouth_bidi.rx.recv() => {
                    tracing::debug!("dragonsmouth subscribe request received");
                    // self.subscribe_request = subscribe_request
                    self.handle_new_subscribe_request(subscribe_request).await;
                }
                control_response = self.control_plane_rx.next() => {
                    match control_response {
                        Some(result) => {
                            match self.handle_control_plane_resp(result).await {
                                LoopInstruction::Continue => {
                                    // continue
                                }
                                LoopInstruction::ErrorStop => {
                                    tracing::debug!("control plane error");
                                    break;
                                }
                            }
                        }
                        None => {
                            tracing::debug!("control plane disconnected");
                            break;
                        }
                    }
                }
                Some(result) = self.non_critical_background_jobs.join_next() => {
                    match result {
                        Ok(result) => {
                            self.handle_non_critical_job_result(result);
                        }
                        Err(e) => {
                            tracing::warn!("non critical background job error with: {e:?}");
                        }
                    }
                }
                maybe = self.download_task_runner_chans.download_result_rx.recv() => {
                    match maybe {
                        Some(result) => {
                            self.handle_download_result(result).await;
                        },
                        None => {
                            tracing::info!("download task runner channel closed");
                            break;
                        }
                    }
                }

                _ = tokio::time::sleep_until(commit_deadline.into()) => {
                    tracing::trace!("commit deadline reached");
                    self.commit_offset().await;
                }
                _ = tokio::time::sleep_until(get_tip_deadline.into()) => {
                    self.update_tip().await;
                }
            }
            self.drain_slot_status().await;
        }
        self.stop = true;
        tracing::debug!("fumarole runtime exiting");
        Ok(())
    }
}

///
/// Channels to interact with a "download task runner".
///
/// Instead of using Trait which does not work very well for Asynchronous Specs,
/// we use channels to create polymorphic behaviour (indirection-through-channels),
/// similar to how actor-based programming works.
///
pub struct DownloadTaskRunnerChannels {
    ///
    /// Where you send download task request to.
    ///
    pub download_task_queue_tx: mpsc::Sender<DownloadTaskArgs>,

    ///
    /// Sends command the download task runner.
    ///
    pub cnc_tx: mpsc::Sender<DownloadTaskRunnerCommand>,

    ///
    /// Where you get back feedback from download task result.
    pub download_result_rx: mpsc::Receiver<DownloadTaskResult>,
}

pub enum DownloadTaskRunnerCommand {
    UpdateSubscribeRequest(Arc<SubscribeRequest>),
}

///
/// The download task specification to use by the runner.
///
#[derive(Debug, Clone)]
pub struct DownloadTaskArgs {
    pub download_request: FumeDownloadRequest,
}

pub(crate) struct DataPlaneConn {
    client: GrpcFumaroleClient,
}

#[derive(Clone, Debug)]
struct ScheduleShardDownload {
    blockchain_id: Vec<u8>,
    shard_idx: FumeShardIdx,
    block_uid: Vec<u8>,
    attempt: usize,
    slot: Slot,
}

pub(crate) struct PipelinedShardDownloader<Outlet> {
    cnc_rx: mpsc::Receiver<Arc<SubscribeRequest>>,
    download_completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: Outlet,
    shard_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    shared_shard_download_queue: flume::Receiver<QueuedShardDownload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataplaneErrorKind {
    RecoverableTransport,
    SlotNotFound,
    InvalidSubscribeFilter,
    NonRecoverable,
}

#[derive(Debug, thiserror::Error)]
#[error("dataplane stream error ({kind:?}): {message}")]
pub struct DataplaneStreamError {
    kind: DataplaneErrorKind,
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DataplaneStreamError {
    fn new(
        kind: DataplaneErrorKind,
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            message,
            source,
        }
    }

    pub const fn kind(&self) -> DataplaneErrorKind {
        self.kind
    }

    pub const fn is_recoverable(&self) -> bool {
        matches!(self.kind, DataplaneErrorKind::RecoverableTransport)
    }
}

impl From<tonic::Status> for DataplaneStreamError {
    fn from(status: tonic::Status) -> Self {
        let message = status.message().to_ascii_lowercase();
        let kind = match status.code() {
            Code::Unavailable
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::DataLoss
            | Code::Unknown
            | Code::Cancelled
            | Code::DeadlineExceeded => DataplaneErrorKind::RecoverableTransport,
            Code::NotFound => DataplaneErrorKind::SlotNotFound,
            Code::InvalidArgument if message.contains("filter") => {
                DataplaneErrorKind::InvalidSubscribeFilter
            }
            _ => DataplaneErrorKind::NonRecoverable,
        };

        Self::new(kind, status.to_string(), Some(Box::new(status)))
    }
}

type DataplaneSinkSendError = mpsc::error::SendError<DataCommand>;

fn create_dataplane_sink(
    tx: mpsc::Sender<DataCommand>,
) -> impl Sink<DataCommand, Error = DataplaneSinkSendError> + Send {
    futures::sink::unfold(tx, |tx, cmd| async move {
        tx.send(cmd).await?;
        Ok::<_, DataplaneSinkSendError>(tx)
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DownloadBlockError {
    #[error("dragonsmouth outlet disconnected")]
    OutletDisconnected,
    #[error(transparent)]
    FailedDownload(#[from] DataplaneStreamError),
    #[error("download finished too early")]
    IncompleteDownload,
}

pub struct CompletedDownloadBlockTask {
    slot: u64,
    #[allow(dead_code)]
    block_uid: [u8; 16],
    shard_idx_vec: Vec<FumeShardIdx>,
}

#[derive(Debug)]
pub struct CompletedDownloadBlockShardTask {
    shard_idx: FumeShardIdx,
    block_uid: [u8; 16],
    block_meta: Option<SubscribeUpdate>,
    slot: Slot,
}

#[derive(Debug, thiserror::Error)]
enum ShardDownloaderErrorKind {
    #[error(transparent)]
    DataplaneError(#[from] DataplaneStreamError),
    #[error("outlet disconnected")]
    SinkClosed,
    #[error("subscribe data sending-half disconnected")]
    SubscribeDataTxDisconnected,
}

#[derive(Debug, thiserror::Error)]
#[error("shard downloader error: {kind}")]
pub struct ShardDownloaderError {
    slot_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    kind: ShardDownloaderErrorKind,
}

impl<Outlet> PipelinedShardDownloader<Outlet>
where
    Outlet: Sink<Result<SubscribeUpdate, FumaroleSubscribeError>> + Send + 'static,
{

    async fn pipelined_downloader<Source>(
        mut rx: Source,
        outlet: Outlet,
        completed_slot_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    ) -> Result<Option<CompletedDownloadBlockShardTask>, ShardDownloaderErrorKind>
    where
        Source: Stream<Item = Result<DataResponse, DataplaneStreamError>> + Unpin,
    {
        let mut outlet = Box::pin(outlet);
        let mut total_event_downloaded = 0;
        let mut block_meta: Option<SubscribeUpdate> = None;
        let mut t = Instant::now();
        tracing::trace!("starting continuous shard downloader loop");
        while let Some(data) = rx.next().await {
            let resp = data?.response.expect("response");

            match resp {
                data_response::Response::Update(update) => {
                    total_event_downloaded += 1;
                    #[cfg(feature = "prometheus")]
                    {
                        inc_total_event_downloaded(1);
                    }
                    #[allow(clippy::collapsible_else_if)]
                    if matches!(update.update_oneof, Some(UpdateOneof::BlockMeta(_))) {
                        block_meta = Some(update);
                    } else {
                        if outlet.send(Ok(update)).await.is_err() {
                            tracing::error!("failed to send update to outlet: outlet disconnected");
                            return Err(ShardDownloaderErrorKind::SinkClosed);
                        }
                    }
                }
                data_response::Response::BlockShardDownloadFinish(footer) => {
                    let download_time = t.elapsed();
                    #[cfg(feature = "prometheus")]
                    {
                        use crate::metrics::observe_download_shard_download_time;

                        observe_download_shard_download_time(download_time);
                    }
                    assert!(footer.shard_indices.len() == 1);
                    tracing::trace!(
                        "shard {} download finished with {} events in {:?}",
                        footer.shard_indices[0],
                        total_event_downloaded,
                        download_time,
                    );
                    total_event_downloaded = 0;
                    let completed = CompletedDownloadBlockShardTask {
                        shard_idx: footer.shard_indices[0],
                        block_meta: block_meta.clone(),
                        block_uid: footer
                            .block_uid
                            .try_into()
                            .expect("block uid size mismatch"),
                        slot: footer.slot,
                    };
                    if let Err(e) = completed_slot_tx.send(completed).await {
                        let completed = e.0;
                        return Ok(Some(completed));
                    }
                    t = Instant::now();
                }
            }
        }
        tracing::trace!("exiting continuous shard downloader loop");
        Ok(None)
    }

    async fn downloader_loop<DataplaneSink, DataplaneStream>(
        mut self,
        mut dataplane_sink: DataplaneSink,
        dataplane_stream: DataplaneStream,
    ) -> Result<VecDeque<ScheduleShardDownload>, ShardDownloaderError>
    where
        DataplaneSink: Sink<DataCommand> + Send + Unpin + 'static,
        DataplaneStream: Stream<Item = Result<DataResponse, DataplaneStreamError>>
            + Send
            + Unpin
            + 'static,
    {

        dataplane_sink.send(DataCommand {
            command: Some(Command::FilterUpdate(
                (*self.subscribe_request).clone().into(),
            )),
        })
        .await
        .map_err(|_| ShardDownloaderError {
            kind: ShardDownloaderErrorKind::SubscribeDataTxDisconnected,
            slot_scheduled_for_download: self.shard_scheduled_for_download.clone(),
        })?;
        let mut joinset = JoinSet::new();

        let dragonsmouth_outlet = self.dragonsmouth_outlet;
        let (completed_slot_tx, mut completed_slot_rx) = mpsc::channel(10);
        joinset.spawn(Self::pipelined_downloader(
            dataplane_stream,
            dragonsmouth_outlet,
            completed_slot_tx,
        ));

        let err = loop {
            let poll_shared_queue = self.shard_scheduled_for_download.len() < 2;
            tokio::select! {
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(subscribe_request) => {
                            self.subscribe_request = subscribe_request;
                            tracing::info!("updated subscribe request in continuous shard downloader");
                            let cmd = crate::proto::data_command::Command::FilterUpdate((*self.subscribe_request).clone().into());
                            let result = dataplane_sink.send(DataCommand {
                                command: Some(cmd)
                            }).await;
                            if result.is_err() {
                                break Some(ShardDownloaderErrorKind::SubscribeDataTxDisconnected);
                            }
                        },
                        None => {
                            break None;
                        }
                    }
                }
                maybe = completed_slot_rx.recv() => {
                    match maybe {
                        Some(completed) => {
                            let expected_shard_download = self.shard_scheduled_for_download.pop_front().expect("no scheduled shard download");
                            assert!(expected_shard_download.shard_idx == completed.shard_idx, "shard idx mismatch");
                            assert!(expected_shard_download.block_uid == completed.block_uid, "block uid mismatch");
                            // Notify the caller about completed shard download
                            if self.download_completed_tx.send(completed).await.is_err() {
                                break None;
                            }
                        },
                        None => {
                            break None;
                        }
                    }
                }
                Some(result) = joinset.join_next() => {
                    let result2 = result.expect("join error");
                    match result2 {
                        Ok(_) => break None,
                        Err(e) => break Some(e),
                    }

                }
                result = self.shared_shard_download_queue.recv_async(), if poll_shared_queue => {
                    let Ok(queued_shard_download) = result else {
                        break None;
                    };
                    let scheduled_shard_download = ScheduleShardDownload {
                        blockchain_id: queued_shard_download.request.blockchain_id.clone(),
                        shard_idx: queued_shard_download.request.shard_idx as u32,
                        block_uid: queued_shard_download.request.block_uid.clone(),
                        slot: queued_shard_download.slot,
                        attempt: queued_shard_download.attempt,
                    };
                    self.shard_scheduled_for_download.push_back(scheduled_shard_download);
                    let cmd = crate::proto::data_command::Command::DownloadBlockShard(queued_shard_download.request);
                    let result = dataplane_sink.send(DataCommand {
                        command: Some(cmd)
                    }).await;


                    if result.is_err() {
                        break Some(ShardDownloaderErrorKind::SubscribeDataTxDisconnected);
                    }

                }
            }
        };
        drop(completed_slot_rx);

        if let Some(Ok(Ok(Some(completed)))) = joinset.join_next().await {
            let expected_shard_download = self
                .shard_scheduled_for_download
                .pop_front()
                .expect("no scheduled shard download");
            assert!(
                expected_shard_download.shard_idx == completed.shard_idx,
                "shard idx mismatch"
            );
            assert!(
                expected_shard_download.block_uid == completed.block_uid,
                "block uid mismatch"
            );
            // Notify the caller about completed shard download
            let _ = self.download_completed_tx.send(completed).await;
        };

        if let Some(err_kind) = err {
            Err(ShardDownloaderError {
                kind: err_kind,
                slot_scheduled_for_download: self.shard_scheduled_for_download,
            })
        } else {
            Ok(self.shard_scheduled_for_download)
        }
    }
}

struct ShardedSlotDownloadProgress {
    started_at: Instant,
    block_meta: Option<SubscribeUpdate>,
    remaining_shard_idx: Vec<FumeShardIdx>,
}

struct QueuedShardDownload {
    slot: Slot,
    request: DownloadBlockShard,
    attempt: usize,
}


/// A trait to abstract the connection and interaction with fumarole data plane.
pub(crate) trait FumaroleConnector {
    /// The error type for subscribing to data plane.
    type DataplaneSubscribeError: std::error::Error + Send + Sync + 'static;
    /// The error type for sending commands to data plane.
    type DataplaneSinkError: std::error::Error + Send + Sync + 'static;
    /// The sink type for sending commands to data plane.
    type DataplaneSink: Sink<DataCommand, Error = Self::DataplaneSinkError>
        + Send
        + Unpin
        + 'static;

    /// The stream type for receiving data responses from fumarole.
    type DataplaneStream: Stream<Item = Result<DataResponse, DataplaneStreamError>>
        + Send
        + Unpin
        + 'static;

    /// Subscribe to data plane with the given subscribe request, and get back a stream for receiving data responses and a sink for sending data commands.
    type DataplaneSubscribeFut: Future<
            Output = Result<
                (Self::DataplaneSink, Self::DataplaneStream),
                Self::DataplaneSubscribeError,
            >,
        > + Send;

    fn subscribe_data(&self) -> Self::DataplaneSubscribeFut;
}

impl FumaroleConnector for FumaroleGrpcConnector {
    type DataplaneSubscribeError = tonic::Status;
    type DataplaneSinkError = DataplaneSinkSendError;
    type DataplaneSink = Pin<Box<dyn Sink<DataCommand, Error = Self::DataplaneSinkError> + Send>>;
    type DataplaneStream = Pin<Box<dyn Stream<Item = Result<DataResponse, DataplaneStreamError>> + Send>>;
    type DataplaneSubscribeFut = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (Self::DataplaneSink, Self::DataplaneStream),
                        Self::DataplaneSubscribeError,
                    >,
                > + Send,
        >,
    >;

    fn subscribe_data(&self) -> Self::DataplaneSubscribeFut {
        let connector = self.clone();
        Box::pin(async move {
            let mut client = connector.connect().await.map_err(|e| {
                tonic::Status::unavailable(format!("failed to connect data plane: {e}"))
            })?;
            let (tx, rx) = mpsc::channel(100);
            let response = client.subscribe_data(ReceiverStream::new(rx)).await?;
            let sink: Self::DataplaneSink = Box::pin(create_dataplane_sink(tx));
            let stream: Self::DataplaneStream = Box::pin(response
                .into_inner()
                .map(|result| result.map_err(DataplaneStreamError::from)));
            Ok((sink, stream))
        })
    }

}

pub(crate) struct ShardedDownloadOrchestrator<C> {
    shared_shard_download_queue_tx: flume::Sender<QueuedShardDownload>,
    shared_shard_download_queue_rx: flume::Receiver<QueuedShardDownload>,
    slot_download_progression_map: HashMap<Slot, ShardedSlotDownloadProgress>,
    completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    completed_rx: mpsc::Receiver<CompletedDownloadBlockShardTask>,
    connector: C,
    shard_downloader_js: JoinSet<Result<VecDeque<ScheduleShardDownload>, ShardDownloaderError>>,
    shard_downloader_handles: HashMap<task::Id, PipelinedShardDownloaderHandle>,
    cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
    download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
    outlet: mpsc::Sender<DownloadTaskResult>,
    max_download_attempt_per_slot: usize,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, FumaroleSubscribeError>>,
    total_shard_downloaders: usize,
}

///
/// Holds information about on-going data plane task.
///
#[derive(Debug, Clone)]
pub(crate) struct PipelinedShardDownloaderHandle {
    cnc_tx: mpsc::Sender<Arc<SubscribeRequest>>,
}

impl<C> ShardedDownloadOrchestrator<C>
where
    C: FumaroleConnector + Send + 'static,
    DataplaneStreamError: From<C::DataplaneSubscribeError>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connector: C,
        cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
        download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
        outlet: mpsc::Sender<DownloadTaskResult>,
        max_download_attempt_by_slot: usize,
        subscribe_request: Arc<SubscribeRequest>,
        total_shard_downloaders: usize,
        dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, FumaroleSubscribeError>>,
    ) -> Self {
        let (completed_tx, completed_rx) = mpsc::channel(1000);
        let (shared_shard_download_queue_tx, shared_shard_download_queue_rx) = flume::unbounded();
        Self {
            connector,
            shard_downloader_js: JoinSet::new(),
            shard_downloader_handles: HashMap::new(),
            cnc_rx,
            download_task_queue,
            outlet,
            max_download_attempt_per_slot: max_download_attempt_by_slot,
            subscribe_request,
            shared_shard_download_queue_tx,
            shared_shard_download_queue_rx,
            slot_download_progression_map: HashMap::new(),
            dragonsmouth_outlet,
            total_shard_downloaders,
            completed_tx,
            completed_rx,
        }
    }

    fn recycle_scheduled_shard_downloads(
        &mut self,
        shard_downloads: VecDeque<ScheduleShardDownload>,
    ) {
        for scheduled_shard_download in shard_downloads {
            let queued = QueuedShardDownload {
                slot: scheduled_shard_download.slot,
                request: DownloadBlockShard {
                    blockchain_id: scheduled_shard_download.blockchain_id,
                    block_uid: scheduled_shard_download.block_uid,
                    shard_idx: scheduled_shard_download.shard_idx as i32,
                    block_filters: None,
                    slot: Some(scheduled_shard_download.slot),
                },
                attempt: scheduled_shard_download.attempt,
            };
            self.shared_shard_download_queue_tx.send(queued).unwrap();
        }
    }

    async fn spawn_shard_downloader(&mut self) -> Result<(), DownloadBlockError> {
        let (dataplane_tx, dataplane_stream) = self
            .connector
            .subscribe_data()
            .await
            .map_err(|e| DownloadBlockError::FailedDownload(DataplaneStreamError::from(e)))?;

        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let dragonsmouth_sink = PollSender::new(self.dragonsmouth_outlet.clone());
        let shard_downloader = PipelinedShardDownloader {
            cnc_rx,
            download_completed_tx: self.completed_tx.clone(),
            subscribe_request: Arc::clone(&self.subscribe_request),
            dragonsmouth_outlet: dragonsmouth_sink,
            shard_scheduled_for_download: Default::default(),
            shared_shard_download_queue: self.shared_shard_download_queue_rx.clone(),
        };
        let ah = self.shard_downloader_js.spawn(async move {
            shard_downloader
                .downloader_loop(dataplane_tx, dataplane_stream)
                .await
        });
        let handle = PipelinedShardDownloaderHandle { cnc_tx };
        self.shard_downloader_handles.insert(ah.id(), handle);
        Ok(())
    }

    async fn handle_shard_download_completed(
        &mut self,
        mut completed: CompletedDownloadBlockShardTask,
    ) {
        #[cfg(feature = "prometheus")]
        {
            dec_inflight_slot_download();
        }

        tracing::trace!(
            "slot:short {}:{} download completed",
            completed.slot,
            completed.shard_idx
        );

        let slot = completed.slot;
        let is_slot_complete = {
            let slot_progression = self
                .slot_download_progression_map
                .get_mut(&slot)
                .expect("should track slot progression");
            slot_progression
                .remaining_shard_idx
                .retain(|x| x != &completed.shard_idx);
            if let Some(block_meta) = completed.block_meta.take() {
                slot_progression.block_meta = Some(block_meta);
            }

            slot_progression.remaining_shard_idx.is_empty()
        };

        // Handle any completed shards
        {
            // let elapsed = task_meta.scheduled_at.elapsed();
            #[cfg(feature = "prometheus")]
            {
                if is_slot_complete {
                    inc_slot_download_count();
                }
            }
            let _ = self
                .outlet
                .send(DownloadTaskResult::Ok(CompletedDownloadBlockTask {
                    slot,
                    block_uid: completed.block_uid,
                    shard_idx_vec: vec![completed.shard_idx],
                }))
                .await;
            if is_slot_complete {
                let completed = self.slot_download_progression_map.remove(&slot).unwrap();
                let elapsed = completed.started_at.elapsed();
                #[cfg(feature = "prometheus")]
                {
                    observe_slot_download_duration(elapsed);
                }
                let block_meta = completed.block_meta;
                tracing::debug!("slot {slot} download completed");
                let _ = self
                    .dragonsmouth_outlet
                    .send(Ok(block_meta.expect("missing block meta")))
                    .await;

                #[cfg(feature = "prometheus")]
                {
                    inc_slot_download_count();
                }
            }
        }
    }

    fn schedule_slot_download_task(&mut self, task_spec: DownloadTaskArgs) {
        if self
            .slot_download_progression_map
            .contains_key(&task_spec.download_request.slot)
        {
            // Already scheduled
            tracing::warn!(
                "slot {} already scheduled for download",
                task_spec.download_request.slot
            );
            return;
        }
        let slot = task_spec.download_request.slot;
        let num_shards = task_spec.download_request.num_shards;
        let shard_idx_vec = (0..num_shards).collect::<Vec<_>>();
        for shard_idx in &shard_idx_vec {
            let download_shard_task = DownloadBlockShard {
                blockchain_id: task_spec.download_request.blockchain_id.clone().to_vec(),
                block_uid: task_spec.download_request.block_uid.clone().to_vec(),
                shard_idx: *shard_idx as i32,
                block_filters: None,
                slot: Some(slot),
            };
            let queued_download = QueuedShardDownload {
                slot,
                request: download_shard_task,
                attempt: 1,
            };
            self.shared_shard_download_queue_tx
                .send(queued_download)
                .unwrap();
        }
        let slot_progress = ShardedSlotDownloadProgress {
            started_at: Instant::now(),
            remaining_shard_idx: shard_idx_vec,
            block_meta: None,
        };
        self.slot_download_progression_map
            .insert(slot, slot_progress);
    }

    async fn handle_shard_downloader_result(
        &mut self,
        task_id: Id,
        result: Result<VecDeque<ScheduleShardDownload>, ShardDownloaderError>,
    ) -> Result<(), DownloadBlockError> {
        if self.shard_downloader_handles.remove(&task_id).is_none() {
            return Ok(());
        }

        match result {
            Ok(shard_download_to_recycle) => {
                self.recycle_scheduled_shard_downloads(shard_download_to_recycle);
            }
            Err(e) => {
                let ShardDownloaderError {
                    mut slot_scheduled_for_download,
                    kind,
                } = e;

                match kind {
                    ShardDownloaderErrorKind::DataplaneError(dataplane_err) => {
                        if let Some(schedule_shard_download) = slot_scheduled_for_download.pop_front() {
                            let attempt = schedule_shard_download.attempt;
                            if dataplane_err.is_recoverable()
                                && attempt < self.max_download_attempt_per_slot
                            {
                                let queued = QueuedShardDownload {
                                    slot: schedule_shard_download.slot,
                                    request: DownloadBlockShard {
                                        blockchain_id: schedule_shard_download.blockchain_id,
                                        block_uid: schedule_shard_download.block_uid,
                                        shard_idx: schedule_shard_download.shard_idx as i32,
                                        block_filters: None,
                                        slot: Some(schedule_shard_download.slot),
                                    },
                                    attempt: attempt + 1,
                                };
                                self.shared_shard_download_queue_tx.send(queued).unwrap();
                            } else {
                                self.recycle_scheduled_shard_downloads(slot_scheduled_for_download);
                                return Err(DownloadBlockError::FailedDownload(dataplane_err));
                            }
                        }
                        self.recycle_scheduled_shard_downloads(slot_scheduled_for_download);
                    }
                    ShardDownloaderErrorKind::SinkClosed => {
                        return Err(DownloadBlockError::OutletDisconnected);
                    }
                    ShardDownloaderErrorKind::SubscribeDataTxDisconnected => {
                        self.recycle_scheduled_shard_downloads(slot_scheduled_for_download);
                    }
                }
            }
        }

        self.spawn_shard_downloader().await
    }

    async fn handle_control_command(&mut self, cmd: DownloadTaskRunnerCommand) {
        match cmd {
            DownloadTaskRunnerCommand::UpdateSubscribeRequest(subscribe_request) => {
                self.subscribe_request = subscribe_request;
                for handle in self.shard_downloader_handles.values() {
                    let _ = handle
                        .cnc_tx
                        .send(Arc::clone(&self.subscribe_request))
                        .await;
                }
            }
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), DownloadBlockError> {
        for _ in 0..self.total_shard_downloaders {
            self.spawn_shard_downloader().await?;
        }

        while !self.outlet.is_closed() {
            let can_poll_new_download_task = self.shared_shard_download_queue_tx.len() < 50;
            tokio::select! {
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(cmd) => {
                            self.handle_control_command(cmd).await;
                        },
                        None => {
                            tracing::debug!("command channel disconnected");
                            break;
                        }
                    }
                }
                maybe_download_task = self.download_task_queue.recv(), if can_poll_new_download_task => {
                    match maybe_download_task {
                        Some(download_task) => {
                            self.schedule_slot_download_task(download_task);
                        }
                        None => {
                            tracing::debug!("download task queue disconnected");
                            break;
                        }
                    }
                }
                maybe = self.completed_rx.recv() => {
                    match maybe {
                        Some(completed) => {
                            self.handle_shard_download_completed(completed).await;
                        },
                        None => {
                            tracing::debug!("completed download channel disconnected");
                            break;
                        }
                    }
                }
                Some(result) = self.shard_downloader_js.join_next_with_id() => {
                    if result.is_err() && (self.outlet.is_closed() || self.cnc_rx.is_closed()) {
                        // When we do Ctrl+C or shutdown the runtime,
                        // the task runner will be closed and we will receive an error.
                        // We can safely ignore this error.
                        tracing::debug!("task runner closed");
                        break;
                    }
                    let (task_id, result) = result.expect("download task result");
                    self.handle_shard_downloader_result(task_id, result).await?;
                }
            }
        }
        tracing::debug!("Closing GrpcDownloadTaskRunner loop");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc as futures_mpsc;

    fn mk_update(update_oneof: UpdateOneof) -> DataResponse {
        DataResponse {
            response: Some(data_response::Response::Update(SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(update_oneof),
            })),
        }
    }

    #[tokio::test]
    async fn pipelined_downloader_forwards_updates_and_reports_completed_shard() {
        let (outlet_tx, mut outlet_rx) =
            futures_mpsc::unbounded::<Result<SubscribeUpdate, FumaroleSubscribeError>>();
        let (completed_tx, mut completed_rx) = mpsc::channel(2);

        let footer = proto::BlockShardDownloadFinish {
            block_uid: vec![7u8; 16],
            slot: 42,
            shard_indices: vec![3],
        };

        let stream = tokio_stream::iter(vec![
            Ok(mk_update(UpdateOneof::Slot(SubscribeUpdateSlot::default()))),
            Ok(mk_update(UpdateOneof::BlockMeta(
                yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta::default(),
            ))),
            Ok(DataResponse {
                response: Some(data_response::Response::BlockShardDownloadFinish(footer)),
            }),
        ]);

        let result = PipelinedShardDownloader::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
        )
        .await;

        assert!(matches!(result, Ok(None)));

        let forwarded = outlet_rx
            .next()
            .await
            .expect("expected forwarded update")
            .expect("expected ok update");
        assert!(matches!(forwarded.update_oneof, Some(UpdateOneof::Slot(_))));
        assert!(matches!(outlet_rx.try_next(), Ok(None)));

        let completed = completed_rx.recv().await.expect("expected completion footer");
        assert_eq!(completed.shard_idx, 3);
        assert_eq!(completed.slot, 42);
        assert_eq!(completed.block_uid, [7u8; 16]);
        assert!(matches!(
            completed.block_meta.and_then(|u| u.update_oneof),
            Some(UpdateOneof::BlockMeta(_))
        ));
    }

    #[tokio::test]
    async fn pipelined_downloader_returns_sink_closed_when_outlet_is_disconnected() {
        let (outlet_tx, outlet_rx) =
            futures_mpsc::unbounded::<Result<SubscribeUpdate, FumaroleSubscribeError>>();
        drop(outlet_rx);
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Ok(mk_update(UpdateOneof::Slot(
            SubscribeUpdateSlot::default(),
        )))]);

        let result = PipelinedShardDownloader::<futures_mpsc::UnboundedSender<Result<SubscribeUpdate, FumaroleSubscribeError>>>::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
        )
        .await;

        assert!(matches!(result, Err(ShardDownloaderErrorKind::SinkClosed)));
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_recoverable_transport_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<SubscribeUpdate, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::unavailable("connection dropped"),
        ))]);

        let result = PipelinedShardDownloader::<futures_mpsc::UnboundedSender<Result<SubscribeUpdate, FumaroleSubscribeError>>>::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
        )
        .await;

        match result {
            Err(ShardDownloaderErrorKind::DataplaneError(err)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::RecoverableTransport);
                assert!(err.is_recoverable());
            }
            _ => panic!("expected dataplane recoverable transport error"),
        }
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_slot_not_found_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<SubscribeUpdate, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::not_found("slot not found"),
        ))]);

        let result = PipelinedShardDownloader::<futures_mpsc::UnboundedSender<Result<SubscribeUpdate, FumaroleSubscribeError>>>::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
        )
        .await;

        match result {
            Err(ShardDownloaderErrorKind::DataplaneError(err)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::SlotNotFound);
                assert!(!err.is_recoverable());
            }
            _ => panic!("expected dataplane slot-not-found error"),
        }
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_invalid_subscribe_filter_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<SubscribeUpdate, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::invalid_argument("invalid filter expression"),
        ))]);

        let result = PipelinedShardDownloader::<futures_mpsc::UnboundedSender<Result<SubscribeUpdate, FumaroleSubscribeError>>>::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
        )
        .await;

        match result {
            Err(ShardDownloaderErrorKind::DataplaneError(err)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::InvalidSubscribeFilter);
                assert!(!err.is_recoverable());
            }
            _ => panic!("expected dataplane invalid-filter error"),
        }
    }
}
