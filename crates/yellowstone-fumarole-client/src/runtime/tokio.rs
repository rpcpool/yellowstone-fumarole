#[cfg(feature = "prometheus")]
use crate::metrics::{
    dec_inflight_slot_download, inc_failed_slot_download_attempt, inc_inflight_slot_download,
    inc_offset_commitment_count, inc_skip_offset_commitment_count, inc_slot_download_count,
    inc_slot_status_offset_processed_count, inc_total_event_downloaded,
    observe_slot_download_duration, set_max_slot_detected,
    set_processed_slot_status_offset_queue_len, set_slot_status_update_queue_len,
};
use {
    super::state_machine::{FumaroleSM, FumeDownloadRequest, FumeOffset, FumeShardIdx},
    crate::{
        FumaroleClient, FumaroleGrpcConnector, GrpcFumaroleClient,
        proto::{
            self, BlockFilters, CommitOffset, ControlCommand, DataCommand, DataResponse,
            DownloadBlockShard, GetChainTipResponse, JoinControlPlane, PollBlockchainHistory,
            control_response::Response, data_command::Command, data_response,
        },
        runtime::state_machine::FumeNumShards,
        util::grpc::into_bounded_mpsc_rx,
    },
    futures::StreamExt,
    solana_clock::Slot,
    std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc::{self, error::TrySendError},
        task::{self, Id, JoinSet},
    },
    tokio_stream::wrappers::ReceiverStream,
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
            used_cnt: 0,
            client,
            rev: 0,
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
pub(crate) struct TokioFumeDragonsmouthRuntime {
    pub sm: FumaroleSM,
    #[allow(dead_code)]
    pub blockchain_id: Vec<u8>,
    pub fumarole_client: FumaroleClient,
    pub download_task_runner_chans: DownloadTaskRunnerChannels,
    pub dragonsmouth_bidi: DragonsmouthSubscribeRequestBidi,
    pub subscribe_request: Arc<SubscribeRequest>,
    pub persistent_subscriber_name: String,
    pub control_plane_tx: mpsc::Sender<proto::ControlCommand>,
    pub control_plane_rx: mpsc::Receiver<Result<proto::ControlResponse, tonic::Status>>,
    pub dragonsmouth_outlet: mpsc::Sender<Result<geyser::SubscribeUpdate, tonic::Status>>,
    pub commit_interval: Duration,
    pub get_tip_interval: Duration,
    pub last_commit: Instant,
    pub last_tip: Instant,
    pub last_history_poll: Option<Instant>,
    pub gc_interval: usize, // in ticks
    pub non_critical_background_jobs: JoinSet<BackgroundJobResult>,
    pub no_commit: bool,
    pub stop: bool,
    pub experimental_enable_sharded_block_download: bool,
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

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),
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

impl TokioFumeDragonsmouthRuntime {
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
            self.control_plane_tx.send(cmd).await.expect("disconnected");
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
                    DownloadBlockError::FailedDownload(h2_err) => {
                        if h2_err.code() == Code::NotFound {
                            self.sm.make_slot_download_progress(slot, None);
                            tracing::error!("slot {slot} not found, skipping...");
                        } else {
                            self.stop = true;
                            let _ = self.dragonsmouth_outlet.send(Err(h2_err)).await;
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
            .expect("failed to commit offset");
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

    async fn rejoin_controle_plane(&mut self) -> Result<(), tonic::Status> {
        self.last_history_poll = None;
        let (fume_control_plane_tx, fume_control_plane_rx) = mpsc::channel(100);

        let initial_join = JoinControlPlane {
            consumer_group_name: Some(self.persistent_subscriber_name.clone()),
        };
        let initial_join_command = ControlCommand {
            command: Some(proto::control_command::Command::InitialJoin(initial_join)),
        };

        // IMPORTANT: Make sure we send the request here before we subscribe to the stream
        // Otherwise this will block until timeout by remote server.
        fume_control_plane_tx
            .send(initial_join_command)
            .await
            .expect("failed to send initial join");

        let resp = if self.experimental_enable_sharded_block_download {
            self.fumarole_client
                .inner
                .subscribe_v2(ReceiverStream::new(fume_control_plane_rx))
                .await?
        } else {
            self.fumarole_client
                .inner
                .subscribe(ReceiverStream::new(fume_control_plane_rx))
                .await?
        };
        self.control_plane_tx = fume_control_plane_tx;
        let mut streaming_rx = resp.into_inner();
        let initial_reponse = streaming_rx.message().await?.expect("init");
        self.control_plane_rx = into_bounded_mpsc_rx(100, streaming_rx);

        let response = initial_reponse.response.expect("none");
        let Response::Init(initial_state) = response else {
            panic!("unexpected initial response: {response:?}")
        };
        tracing::info!("rejoined control plane with initial state: {initial_state:?}");
        Ok(())
    }

    async fn handle_control_plane_resp(
        &mut self,
        result: Result<proto::ControlResponse, tonic::Status>,
    ) -> LoopInstruction {
        match result {
            Ok(control_response) => {
                self.handle_control_response(control_response);
                LoopInstruction::Continue
            }
            Err(e) => {
                if matches!(
                    e.code(),
                    Code::Unavailable | Code::DataLoss | Code::Internal
                ) {
                    tracing::warn!(
                        "control plane connection lost with error: {e:?}, attempting to rejoin..."
                    );
                    match self.rejoin_controle_plane().await {
                        Ok(_) => LoopInstruction::Continue,
                        Err(e) => {
                            tracing::error!("failed to rejoin control plane with error: {e:?}");
                            let _ = self.dragonsmouth_outlet.send(Err(e)).await;
                            LoopInstruction::ErrorStop
                        }
                    }
                } else {
                    let _ = self.dragonsmouth_outlet.send(Err(e)).await;
                    LoopInstruction::ErrorStop
                }
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
                control_response = self.control_plane_rx.recv() => {
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
/// Holds information about on-going data plane task.
///
#[derive(Debug, Clone)]
pub(crate) struct DataPlaneTaskMeta {
    client_idx: usize,
    request: FumeDownloadRequest,
    scheduled_at: Instant,
    client_rev: u64,
}

///
/// Download task runner that use gRPC protocol to download slot content.
///
/// It manages concurrent [`GrpcDownloadBlockTaskRun`] instance and route back
/// download result to the requestor.
///
pub struct LegacyGrpcDownloadTaskRunner {
    ///
    /// Pool of gRPC channels
    ///
    data_plane_channel_vec: Vec<DataPlaneConn>,

    ///
    /// gRPC channel connector
    ///
    connector: FumaroleGrpcConnector,

    ///
    /// Sets of inflight download tasks
    ///
    tasks: JoinSet<Result<CompletedDownloadBlockTask, GrpcDownloadTaskError>>,
    ///
    /// Inflight download task metadata index
    ///
    task_meta: HashMap<task::Id, DataPlaneTaskMeta>,

    ///
    /// Command-and-Control channel to send command to the runner
    ///
    cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,

    ///
    /// Download task queue
    ///
    download_task_queue: mpsc::Receiver<DownloadTaskArgs>,

    ///
    /// Current inflight slow download attempt
    ///
    download_attempts: HashMap<Slot, usize>,

    ///
    /// The sink to send download task result to.
    ///
    outlet: mpsc::Sender<DownloadTaskResult>,

    ///
    /// The maximum download attempt per slot (how many download failure do we allow)
    ///
    max_download_attempt_per_slot: usize,

    /// The subscribe request to use for the download task
    subscribe_request: Arc<SubscribeRequest>,

    /// The maximum concurrent downloads allowed
    max_concurrent_downloads: usize,

    dragonsmouth_outlet: mpsc::Sender<Result<geyser::SubscribeUpdate, tonic::Status>>,
}

///
/// The download task specification to use by the runner.
///
#[derive(Debug, Clone)]
pub struct DownloadTaskArgs {
    pub download_request: FumeDownloadRequest,
}

pub(crate) struct DataPlaneConn {
    used_cnt: usize,
    client: GrpcFumaroleClient,
    rev: u64,
}

enum RetryDecision {
    DontRetry(DownloadBlockError),
    Retry,
}

impl LegacyGrpcDownloadTaskRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_plane_channel_vec: Vec<DataPlaneConn>,
        connector: FumaroleGrpcConnector,
        cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
        download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
        outlet: mpsc::Sender<DownloadTaskResult>,
        max_download_attempt_by_slot: usize,
        subscribe_request: Arc<SubscribeRequest>,
        max_concurrent_downloads: usize,
        dragonsmouth_outlet: mpsc::Sender<Result<geyser::SubscribeUpdate, tonic::Status>>,
    ) -> Self {
        Self {
            data_plane_channel_vec,
            connector,
            tasks: JoinSet::new(),
            task_meta: HashMap::new(),
            cnc_rx,
            download_task_queue,
            download_attempts: HashMap::new(),
            outlet,
            max_download_attempt_per_slot: max_download_attempt_by_slot,
            subscribe_request,
            max_concurrent_downloads,
            dragonsmouth_outlet,
        }
    }

    ///
    /// Always pick the client with the highest permit limit (least used)
    ///
    fn find_least_use_client(&self) -> Option<usize> {
        self.data_plane_channel_vec
            .iter()
            .enumerate()
            .min_by_key(|(_, conn)| conn.used_cnt)
            .map(|(idx, _)| idx)
    }

    fn retry_decision(
        &self,
        e: GrpcDownloadTaskError,
        slot: Slot,
        remaining_attempt: usize,
        download_attempt: usize,
    ) -> RetryDecision {
        match e {
            GrpcDownloadTaskError::OutletDisconnected => {
                // Will naturally stop the runtime loop.
                RetryDecision::DontRetry(DownloadBlockError::OutletDisconnected)
            }
            GrpcDownloadTaskError::IncompleteDownload => {
                if remaining_attempt == 0 {
                    tracing::error!(
                        "download slot {slot} failed: IncompleteDownload, max attempts reached"
                    );
                    RetryDecision::DontRetry(DownloadBlockError::IncompleteDownload)
                } else {
                    RetryDecision::Retry
                }
            }
            GrpcDownloadTaskError::TonicError(h2_err) => {
                if matches!(
                    h2_err.code(),
                    Code::Unavailable
                        | Code::Internal
                        | Code::Aborted
                        | Code::ResourceExhausted
                        | Code::DataLoss
                        | Code::Unknown
                        | Code::Cancelled
                        | Code::DeadlineExceeded
                ) {
                    if download_attempt >= self.max_download_attempt_per_slot {
                        tracing::error!(
                            "download slot {slot} failed with tonic error: {h2_err:?}, max attempts reached"
                        );
                        RetryDecision::DontRetry(DownloadBlockError::FailedDownload(h2_err))
                    } else {
                        tracing::warn!(
                            "download slot {slot} failed with tonic error: {h2_err:?}, retrying..."
                        );
                        RetryDecision::Retry
                    }
                } else {
                    RetryDecision::DontRetry(DownloadBlockError::FailedDownload(h2_err))
                }
            }
        }
    }

    async fn handle_legacy_data_plane_task_result(
        &mut self,
        task_id: task::Id,
        result: Result<CompletedDownloadBlockTask, GrpcDownloadTaskError>,
    ) {
        let Some(task_meta) = self.task_meta.remove(&task_id) else {
            panic!("missing task meta")
        };

        #[cfg(feature = "prometheus")]
        {
            dec_inflight_slot_download();
        }

        let slot = task_meta.request.slot;

        let state = self
            .data_plane_channel_vec
            .get_mut(task_meta.client_idx)
            .expect("should not be none");
        state.used_cnt = state.used_cnt.checked_sub(1).expect("underflow");

        match result {
            Ok(completed) => {
                let CompletedDownloadBlockTask {
                    slot,
                    block_uid: _,
                    shard_idx_vec: _,
                } = completed;

                #[cfg(feature = "prometheus")]
                {
                    let elapsed = task_meta.scheduled_at.elapsed();
                    observe_slot_download_duration(elapsed);
                    inc_slot_download_count();
                }

                let _ = self.download_attempts.remove(&slot);
                let _ = self.outlet.send(DownloadTaskResult::Ok(completed)).await;
            }
            Err(e) => {
                #[cfg(feature = "prometheus")]
                {
                    inc_failed_slot_download_attempt();
                }
                let download_attempt = self
                    .download_attempts
                    .get(&slot)
                    .expect("should track download attempt");

                let remaining_attempt = self
                    .max_download_attempt_per_slot
                    .saturating_sub(*download_attempt);

                let retry_decision =
                    self.retry_decision(e, slot, remaining_attempt, *download_attempt);

                match retry_decision {
                    RetryDecision::Retry => {
                        // We need to retry it

                        tracing::debug!(
                            "download slot {slot} failed, remaining attempts: {remaining_attempt}"
                        );
                        // Recreate the data plane bidi
                        let t = Instant::now();

                        tracing::debug!("data plane bidi rebuilt in {:?}", t.elapsed());
                        let conn = self
                            .data_plane_channel_vec
                            .get_mut(task_meta.client_idx)
                            .expect("should not be none");

                        if task_meta.client_rev == conn.rev {
                            let new_client = self
                                .connector
                                .connect()
                                .await
                                .expect("failed to reconnect data plane client");
                            conn.client = new_client;
                            conn.rev += 1;
                        }

                        tracing::debug!("Download slot {slot} failed, rescheduling for retry...");
                        let task_spec = DownloadTaskArgs {
                            download_request: task_meta.request,
                            // dragonsmouth_outlet: task_meta.dragonsmouth_outlet,
                        };
                        // Reschedule download immediately
                        self.spawn_grpc_download_task_legacy(task_spec);
                    }
                    RetryDecision::DontRetry(err) => {
                        self.download_attempts.remove(&slot);
                        let _ = self
                            .outlet
                            .send(DownloadTaskResult::Err { slot, err })
                            .await;
                    }
                }
            }
        }
    }

    fn spawn_grpc_download_task_legacy(&mut self, task_spec: DownloadTaskArgs) {
        let client_idx = self
            .find_least_use_client()
            .expect("no available data plane client");
        let conn = self
            .data_plane_channel_vec
            .get_mut(client_idx)
            .expect("should not be none");

        let client = conn.client.clone();
        let client_rev = conn.rev;

        let DownloadTaskArgs {
            download_request,
            // filters,
            // dragonsmouth_outlet,
        } = task_spec;
        let slot = download_request.slot;
        let task = LegacyGrpcDownloadBlockTaskRun {
            download_request: download_request.clone(),
            num_shards: download_request.num_shards,
            client,
            filters: Some((*self.subscribe_request).clone().into()),
            dragonsmouth_outlet: self.dragonsmouth_outlet.clone(),
        };
        let ah = self
            .tasks
            .spawn(async move { task.run_legacy_download_block().await });
        let task_meta = DataPlaneTaskMeta {
            client_idx,
            request: download_request.clone(),
            scheduled_at: Instant::now(),
            client_rev,
        };
        self.download_attempts
            .entry(slot)
            .and_modify(|e| *e += 1)
            .or_insert(1);
        conn.used_cnt += 1;

        #[cfg(feature = "prometheus")]
        {
            inc_inflight_slot_download();
        }

        self.task_meta.insert(ah.id(), task_meta);
    }

    fn handle_control_command(&mut self, cmd: DownloadTaskRunnerCommand) {
        match cmd {
            DownloadTaskRunnerCommand::UpdateSubscribeRequest(subscribe_request) => {
                self.subscribe_request = subscribe_request;
            }
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), DownloadBlockError> {
        while !self.outlet.is_closed() {
            let has_avail_download_permit = self.tasks.len() < self.max_concurrent_downloads;
            tracing::debug!(
                "GrpcDownloadTaskRunner loop: has_avail_download_permit={}, inflight_tasks={}",
                has_avail_download_permit,
                self.tasks.len()
            );
            tokio::select! {
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(cmd) => {
                            self.handle_control_command(cmd);
                        },
                        None => {
                            tracing::debug!("command channel disconnected");
                            break;
                        }
                    }
                }
                maybe_download_task = self.download_task_queue.recv(), if has_avail_download_permit => {
                    match maybe_download_task {
                        Some(download_task) => {
                            tracing::debug!("spawning legacy download task for slot {}", download_task.download_request.slot);
                            self.spawn_grpc_download_task_legacy(download_task);
                        }
                        None => {
                            tracing::debug!("download task queue disconnected");
                            break;
                        }
                    }
                }
                Some(result) = self.tasks.join_next_with_id() => {
                    if result.is_err() && (self.outlet.is_closed() || self.cnc_rx.is_closed()) {
                        // When we do Ctrl+C or shutdown the runtime,
                        // the task runner will be closed and we will receive an error.
                        // We can safely ignore this error.
                        tracing::debug!("task runner closed");
                        break;
                    }
                    let (task_id, result) = result.expect("download task result");
                    self.handle_legacy_data_plane_task_result(task_id, result).await;
                }
            }
        }
        tracing::debug!("Closing GrpcDownloadTaskRunner loop");
        Ok(())
    }
}

pub(crate) struct LegacyGrpcDownloadBlockTaskRun {
    download_request: FumeDownloadRequest,
    num_shards: FumeNumShards,
    client: GrpcFumaroleClient,
    filters: Option<BlockFilters>,
    dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
}

#[derive(Clone, Debug)]
struct ScheduleShardDownload {
    blockchain_id: Vec<u8>,
    shard_idx: FumeShardIdx,
    block_uid: Vec<u8>,
    attempt: usize,
    slot: Slot,
}

pub(crate) struct GrpcContinuousShardDownloader {
    cnc_rx: mpsc::Receiver<Arc<SubscribeRequest>>,
    download_completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    client: GrpcFumaroleClient,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    shard_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    shared_shard_download_queue: flume::Receiver<QueuedShardDownload>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DownloadBlockError {
    #[error("dragonsmouth outlet disconnected")]
    OutletDisconnected,
    #[error(transparent)]
    FailedDownload(#[from] tonic::Status),
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
enum GrpcDownloadTaskError {
    #[error("outlet disconnected")]
    OutletDisconnected,
    #[error(transparent)]
    TonicError(#[from] tonic::Status),
    #[error("incomplete download")]
    IncompleteDownload,
}

impl LegacyGrpcDownloadBlockTaskRun {
    async fn run_legacy_download_block(
        mut self,
    ) -> Result<CompletedDownloadBlockTask, GrpcDownloadTaskError> {
        let request = DownloadBlockShard {
            blockchain_id: self.download_request.blockchain_id.to_vec(),
            block_uid: self.download_request.block_uid.to_vec(),
            shard_idx: 0,
            block_filters: self.filters,
            slot: Some(self.download_request.slot),
        };
        let resp = self.client.download_block(request).await;

        let mut rx = match resp {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                return Err(e.into());
            }
        };
        let mut total_event_downloaded = 0;
        while let Some(data) = rx.next().await {
            let resp = data?
                // .map_err(|e| {
                // let code = e.code();
                // tracing::error!("download block error: {code:?}");
                // map_tonic_error_code_to_download_block_error(code)
                // })?
                .response
                .expect("missing response");

            match resp {
                data_response::Response::Update(update) => {
                    total_event_downloaded += 1;

                    #[cfg(feature = "prometheus")]
                    {
                        inc_total_event_downloaded(1);
                    }

                    if self.dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                        return Err(GrpcDownloadTaskError::OutletDisconnected);
                    }
                }
                data_response::Response::BlockShardDownloadFinish(_) => {
                    tracing::debug!(
                        "block download finished with {} events",
                        total_event_downloaded
                    );

                    return Ok(CompletedDownloadBlockTask {
                        slot: self.download_request.slot,
                        block_uid: self.download_request.block_uid,
                        shard_idx_vec: (0..self.num_shards).collect(),
                    });
                }
            }
        }

        Err(GrpcDownloadTaskError::IncompleteDownload)
    }
}

#[derive(Debug, thiserror::Error)]
enum ShardDownloaderErrorKind {
    #[error(transparent)]
    Tonic(#[from] tonic::Status),
    #[error("outlet disconnected")]
    OutletDisconnected,
    #[error("subscribe data sending-half disconnected")]
    SubscribeDataTxDisconnected,
}

#[derive(Debug, thiserror::Error)]
#[error("shard downloader error: {kind}")]
pub struct ShardDownloaderError {
    slot_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    kind: ShardDownloaderErrorKind,
}

impl GrpcContinuousShardDownloader {
    async fn continuous_downloader(
        mut rx: tonic::Streaming<DataResponse>,
        dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
        completed_slot_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    ) -> Result<Option<CompletedDownloadBlockShardTask>, ShardDownloaderErrorKind> {
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
                        if dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                            return Err(ShardDownloaderErrorKind::OutletDisconnected);
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

    async fn downloader_loop(
        mut self,
    ) -> Result<VecDeque<ScheduleShardDownload>, ShardDownloaderError> {
        let (tx, rx) = tokio::sync::mpsc::channel(10_000);
        let data_response_rx = self
            .client
            .subscribe_data(ReceiverStream::new(rx))
            .await
            .map_err(|e| ShardDownloaderError {
                kind: ShardDownloaderErrorKind::from(e),
                slot_scheduled_for_download: self.shard_scheduled_for_download.clone(),
            })?
            .into_inner();
        tx.send(DataCommand {
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

        let dragonsmouth_outlet = self.dragonsmouth_outlet.clone();
        let (completed_slot_tx, mut completed_slot_rx) = mpsc::channel(10);
        joinset.spawn(Self::continuous_downloader(
            data_response_rx,
            dragonsmouth_outlet,
            completed_slot_tx,
        ));

        let err = loop {
            let poll_shared_queue = self.shard_scheduled_for_download.len() <= 1;
            tokio::select! {
                maybe = self.cnc_rx.recv() => {
                    match maybe {
                        Some(subscribe_request) => {
                            self.subscribe_request = subscribe_request;
                            tracing::info!("updated subscribe request in continuous shard downloader");
                            let cmd = crate::proto::data_command::Command::FilterUpdate((*self.subscribe_request).clone().into());
                            let result = tx.send(DataCommand {
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
                    let result = tx.send(DataCommand {
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

pub(crate) struct GrpcShardedDownloadOrchestrator {
    data_plane_channel_vec: Vec<DataPlaneConn>,
    shared_shard_download_queue_tx: flume::Sender<QueuedShardDownload>,
    shared_shard_download_queue_rx: flume::Receiver<QueuedShardDownload>,

    slot_download_progression_map: HashMap<Slot, ShardedSlotDownloadProgress>,
    completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    completed_rx: mpsc::Receiver<CompletedDownloadBlockShardTask>,
    // TODO: supports auto-reconnect on ConnectionReset.
    #[allow(dead_code)]
    connector: FumaroleGrpcConnector,
    shard_downloader_js: JoinSet<Result<VecDeque<ScheduleShardDownload>, ShardDownloaderError>>,
    shard_downloader_handles: HashMap<task::Id, ContinuousShardDownloaderHandle>,
    cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
    download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
    outlet: mpsc::Sender<DownloadTaskResult>,
    max_download_attempt_per_slot: usize,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
}

///
/// Holds information about on-going data plane task.
///
#[derive(Debug, Clone)]
pub(crate) struct ContinuousShardDownloaderHandle {
    client_idx: usize,
    cnc_tx: mpsc::Sender<Arc<SubscribeRequest>>,
}

impl GrpcShardedDownloadOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_plane_channel_vec: Vec<DataPlaneConn>,
        connector: FumaroleGrpcConnector,
        cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
        download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
        outlet: mpsc::Sender<DownloadTaskResult>,
        max_download_attempt_by_slot: usize,
        subscribe_request: Arc<SubscribeRequest>,
        dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    ) -> Self {
        let num_data_plane_clients = data_plane_channel_vec.len();
        assert!(num_data_plane_clients > 0, "no data plane client provided");
        let (completed_tx, completed_rx) = mpsc::channel(1000);
        let (shared_shard_download_queue_tx, shared_shard_download_queue_rx) = flume::unbounded();
        Self {
            data_plane_channel_vec,
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
            completed_tx,
            completed_rx,
        }
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
        let Some(handle) = self.shard_downloader_handles.remove(&task_id) else {
            return Ok(());
        };
        match result {
            Ok(shard_download_to_recycle) => {
                for scheduled_shard_download in shard_download_to_recycle {
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
            Err(e) => {
                let ShardDownloaderError {
                    mut slot_scheduled_for_download,
                    kind,
                } = e;

                match kind {
                    ShardDownloaderErrorKind::Tonic(status) => {
                        if matches!(
                            status.code(),
                            Code::Unavailable
                                | Code::Internal
                                | Code::Aborted
                                | Code::ResourceExhausted
                                | Code::DataLoss
                                | Code::Unknown
                                | Code::Cancelled
                                | Code::DeadlineExceeded
                        ) {
                            // Assume the first slot in slot_schedule_for_download caused the error
                            if let Some(schedule_shard_download) =
                                slot_scheduled_for_download.pop_front()
                            {
                                let attempt = schedule_shard_download.attempt;
                                if attempt >= self.max_download_attempt_per_slot {
                                    return Err(DownloadBlockError::FailedDownload(status));
                                } else {
                                    if status.code() == Code::Internal {
                                        // reconnect the data plane client
                                        let conn = self
                                            .data_plane_channel_vec
                                            .get_mut(handle.client_idx)
                                            .expect("should not be none");
                                        let new_client = self
                                            .connector
                                            .connect()
                                            .await
                                            .expect("failed to reconnect data plane client");
                                        conn.client = new_client;
                                        conn.rev += 1;
                                        tracing::debug!(
                                            "reconnected data plane client for client idx {}",
                                            handle.client_idx
                                        );
                                    }
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
                                }
                            }
                        }
                    }
                    ShardDownloaderErrorKind::OutletDisconnected => {
                        return Err(DownloadBlockError::OutletDisconnected);
                    }
                    ShardDownloaderErrorKind::SubscribeDataTxDisconnected => {}
                }
            }
        }

        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let client_idx = handle.client_idx;
        let client = self
            .data_plane_channel_vec
            .get(client_idx)
            .expect("should not be none");
        let shard_downloader = GrpcContinuousShardDownloader {
            cnc_rx,
            download_completed_tx: self.completed_tx.clone(),
            client: client.client.clone(),
            subscribe_request: Arc::clone(&self.subscribe_request),
            dragonsmouth_outlet: self.dragonsmouth_outlet.clone(),
            shard_scheduled_for_download: Default::default(),
            shared_shard_download_queue: self.shared_shard_download_queue_rx.clone(),
        };
        let ah = self
            .shard_downloader_js
            .spawn(async move { shard_downloader.downloader_loop().await });
        let handle = ContinuousShardDownloaderHandle { client_idx, cnc_tx };
        self.shard_downloader_handles.insert(ah.id(), handle);
        Ok(())
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
        for (client_idx, client) in self.data_plane_channel_vec.iter().enumerate() {
            let (cnc_tx, cnc_rx) = mpsc::channel(10);
            let shard_downloader = GrpcContinuousShardDownloader {
                cnc_rx,
                download_completed_tx: self.completed_tx.clone(),
                client: client.client.clone(),
                subscribe_request: Arc::clone(&self.subscribe_request),
                dragonsmouth_outlet: self.dragonsmouth_outlet.clone(),
                shard_scheduled_for_download: Default::default(),
                shared_shard_download_queue: self.shared_shard_download_queue_rx.clone(),
            };
            let ah = self
                .shard_downloader_js
                .spawn(async move { shard_downloader.downloader_loop().await });
            let handle = ContinuousShardDownloaderHandle { client_idx, cnc_tx };
            self.shard_downloader_handles.insert(ah.id(), handle);
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
