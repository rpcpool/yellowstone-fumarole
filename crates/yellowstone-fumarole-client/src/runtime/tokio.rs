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
            self, BlockFilters, CommitOffset, ControlCommand, DownloadBlockShard,
            GetChainTipResponse, PollBlockchainHistory, data_response,
        },
    },
    futures::StreamExt,
    solana_clock::Slot,
    std::{
        collections::{HashMap, VecDeque},
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc::{self, error::TrySendError},
        task::{self, JoinSet},
    },
    tonic::Code,
    yellowstone_grpc_proto::geyser::{
        self, CommitmentLevel, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
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
    pub const fn new(client: GrpcFumaroleClient, concurrency_limit: usize) -> Self {
        Self {
            permits: concurrency_limit,
            client,
            rev: 0,
        }
    }

    const fn has_permit(&self) -> bool {
        self.permits > 0
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
    #[allow(dead_code)]
    pub fumarole_client: FumaroleClient,
    pub download_task_runner_chans: DownloadTaskRunnerChannels,
    pub dragonsmouth_bidi: DragonsmouthSubscribeRequestBidi,
    pub subscribe_request: SubscribeRequest,
    #[allow(dead_code)]
    pub consumer_group_name: String,
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
    pub(crate) stop: bool,
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
    #[allow(dead_code)]
    const RUNTIME_NAME: &'static str = "tokio";

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
                    set_max_slot_detected(Self::RUNTIME_NAME, self.sm.max_slot_detected);
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
                inc_poll_history_call_count(Self::RUNTIME_NAME);
            }
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
                        incr_download_queue_full_detection_count(Self::RUNTIME_NAME);
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
            let download_task_args = DownloadTaskArgs {
                download_request,
                dragonsmouth_outlet: self.dragonsmouth_outlet.clone(),
            };
            permit.send(download_task_args);
        }
    }

    async fn handle_download_result(&mut self, download_result: DownloadTaskResult) {
        match download_result {
            DownloadTaskResult::Ok(completed) => {
                let CompletedDownloadBlockTask {
                    slot,
                    block_uid: _,
                    shard_idx,
                    total_event_downloaded: _,
                } = completed;
                self.sm.make_slot_download_progress(slot, shard_idx);
            }
            DownloadTaskResult::Err { slot, err } => {
                // TODO add option to let user decide what to do, by default let it crash
                self.stop = true;
                match err {
                    DownloadBlockError::OutletDisconnected => {
                        // Will be handled in the main loop.
                    }
                    DownloadBlockError::FailedDownload(h2_err) => {
                        let _ = self.dragonsmouth_outlet.send(Err(h2_err)).await;
                    }
                    DownloadBlockError::IncompleteDownload => {
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

            inc_offset_commitment_count(Self::RUNTIME_NAME);
            set_max_offset_committed(Self::RUNTIME_NAME, self.sm.committable_offset);
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
                inc_skip_offset_commitment_count(Self::RUNTIME_NAME);
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

        tracing::debug!("draining {} slot status", slot_status_vec.len());
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
                tracing::trace!("sending dragonsmouth update: {:?}", update);
                if self.dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                    return;
                }
            }

            self.sm
                .mark_event_as_processed(slot_status.session_sequence);
            #[cfg(feature = "prometheus")]
            {
                inc_slot_status_offset_processed_count(Self::RUNTIME_NAME);
            }
        }

        #[cfg(feature = "prometheus")]
        {
            set_processed_slot_status_offset_queue_len(
                Self::RUNTIME_NAME,
                self.sm.processed_offset_queue_len(),
            );
        }
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
                // TODO implement auto-reconnect on Unavailable
                let _ = self.dragonsmouth_outlet.send(Err(e)).await;
                LoopInstruction::ErrorStop
            }
        }
    }

    async fn handle_new_subscribe_request(&mut self, subscribe_request: SubscribeRequest) {
        self.subscribe_request = subscribe_request;
        self.download_task_runner_chans
            .cnc_tx
            .send(DownloadTaskRunnerCommand::UpdateSubscribeRequest(
                self.subscribe_request.clone(),
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
                        set_fumarole_blockchain_offset_tip(Self::RUNTIME_NAME, *tip);
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
                set_slot_status_update_queue_len(Self::RUNTIME_NAME, slot_status_update_queue_len);
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
    UpdateSubscribeRequest(SubscribeRequest),
}

///
/// Holds information about on-going data plane task.
///
#[derive(Debug, Clone)]
pub(crate) struct DataPlaneTaskMeta {
    client_idx: usize,
    request: FumeDownloadRequest,
    dragonsmouth_outlet: mpsc::Sender<Result<geyser::SubscribeUpdate, tonic::Status>>,
    scheduled_at: Instant,
    client_rev: u64,
}

///
/// Download task runner that use gRPC protocol to download slot content.
///
/// It manages concurrent [`GrpcDownloadBlockTaskRun`] instance and route back
/// download result to the requestor.
///
pub struct GrpcDownloadTaskRunner {
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
    subscribe_request: SubscribeRequest,
}

///
/// The download task specification to use by the runner.
///
#[derive(Debug, Clone)]
pub struct DownloadTaskArgs {
    pub download_request: FumeDownloadRequest,
    pub dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
}

pub(crate) struct DataPlaneConn {
    permits: usize,
    client: GrpcFumaroleClient,
    rev: u64,
}

impl GrpcDownloadTaskRunner {
    #[allow(dead_code)]
    const RUNTIME_NAME: &'static str = "tokio_grpc_task_runner";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_plane_channel_vec: Vec<DataPlaneConn>,
        connector: FumaroleGrpcConnector,
        cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
        download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
        outlet: mpsc::Sender<DownloadTaskResult>,
        max_download_attempt_by_slot: usize,
        subscribe_request: SubscribeRequest,
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
        }
    }

    ///
    /// Always pick the client with the highest permit limit (least used)
    ///
    fn find_least_use_client(&self) -> Option<usize> {
        self.data_plane_channel_vec
            .iter()
            .enumerate()
            .max_by_key(|(_, conn)| conn.permits)
            .filter(|(_, conn)| conn.has_permit())
            .map(|(idx, _)| idx)
    }

    async fn handle_data_plane_task_result(
        &mut self,
        task_id: task::Id,
        result: Result<CompletedDownloadBlockTask, GrpcDownloadTaskError>,
    ) -> Result<(), DownloadBlockError> {
        let Some(task_meta) = self.task_meta.remove(&task_id) else {
            panic!("missing task meta")
        };

        #[cfg(feature = "prometheus")]
        {
            dec_inflight_slot_download(Self::RUNTIME_NAME);
        }

        let slot = task_meta.request.slot;

        let state = self
            .data_plane_channel_vec
            .get_mut(task_meta.client_idx)
            .expect("should not be none");
        state.permits += 1;

        match result {
            Ok(completed) => {
                let CompletedDownloadBlockTask {
                    total_event_downloaded,
                    slot,
                    block_uid: _,
                    shard_idx: _,
                } = completed;
                let elapsed = task_meta.scheduled_at.elapsed();

                #[cfg(feature = "prometheus")]
                {
                    observe_slot_download_duration(Self::RUNTIME_NAME, elapsed);
                    inc_slot_download_count(Self::RUNTIME_NAME);
                }

                tracing::debug!(
                    "downloaded slot {slot} in {elapsed:?}, total events: {total_event_downloaded}"
                );
                let _ = self.download_attempts.remove(&slot);
                let _ = self.outlet.send(DownloadTaskResult::Ok(completed)).await;
            }
            Err(e) => {
                #[cfg(feature = "prometheus")]
                {
                    inc_failed_slot_download_attempt(Self::RUNTIME_NAME);
                }
                let download_attempt = self
                    .download_attempts
                    .get(&slot)
                    .expect("should track download attempt");

                let remaining_attempt = self
                    .max_download_attempt_per_slot
                    .saturating_sub(*download_attempt);

                enum RetryDecision {
                    DontRetry(DownloadBlockError),
                    Retry,
                }

                let retry_decision = match e {
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
                            if download_attempt >= &self.max_download_attempt_per_slot {
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
                };

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
                            dragonsmouth_outlet: task_meta.dragonsmouth_outlet,
                        };
                        // Reschedule download immediately
                        self.spawn_grpc_download_task(task_meta.client_idx, task_spec);
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
        Ok(())
    }

    fn spawn_grpc_download_task(&mut self, client_idx: usize, task_spec: DownloadTaskArgs) {
        let conn = self
            .data_plane_channel_vec
            .get_mut(client_idx)
            .expect("should not be none");

        let client = conn.client.clone();
        let client_rev = conn.rev;

        let DownloadTaskArgs {
            download_request,
            // filters,
            dragonsmouth_outlet,
        } = task_spec;
        let slot = download_request.slot;
        let task = GrpcDownloadBlockTaskRun {
            download_request: download_request.clone(),
            client,
            filters: Some(self.subscribe_request.clone().into()),
            dragonsmouth_outlet: dragonsmouth_outlet.clone(),
        };
        let ah = self.tasks.spawn(task.run());
        let task_meta = DataPlaneTaskMeta {
            client_idx,
            request: download_request.clone(),
            dragonsmouth_outlet,
            scheduled_at: Instant::now(),
            client_rev,
        };
        self.download_attempts
            .entry(slot)
            .and_modify(|e| *e += 1)
            .or_insert(1);
        conn.permits = conn.permits.checked_sub(1).expect("underflow");

        #[cfg(feature = "prometheus")]
        {
            inc_inflight_slot_download(Self::RUNTIME_NAME);
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

    #[allow(dead_code)]
    fn available_download_permit(&self) -> usize {
        self.data_plane_channel_vec
            .iter()
            .map(|conn| conn.permits)
            .sum()
    }

    pub(crate) async fn run(mut self) -> Result<(), DownloadBlockError> {
        while !self.outlet.is_closed() {
            let maybe_available_client_idx = self.find_least_use_client();

            #[cfg(feature = "prometheus")]
            {
                use crate::metrics::set_available_download_permit;
                set_available_download_permit(
                    Self::RUNTIME_NAME,
                    self.available_download_permit() as i64,
                );
            }

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
                maybe_download_task = self.download_task_queue.recv(), if maybe_available_client_idx.is_some() => {
                    match maybe_download_task {
                        Some(download_task) => {
                            let Some(client_idx) = maybe_available_client_idx else {
                                unreachable!("client idx is some")
                            };
                            self.spawn_grpc_download_task(client_idx, download_task);
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
                    self.handle_data_plane_task_result(task_id, result).await?;
                }
            }
        }
        tracing::debug!("Closing GrpcDownloadTaskRunner loop");
        Ok(())
    }
}

pub(crate) struct GrpcDownloadBlockTaskRun {
    download_request: FumeDownloadRequest,
    client: GrpcFumaroleClient,
    filters: Option<BlockFilters>,
    dragonsmouth_outlet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
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

// fn map_tonic_error_code_to_download_block_error(code: Code) -> DownloadBlockError {
//     match code {
//         Code::NotFound => DownloadBlockError::BlockShardNotFound,
//         Code::Unavailable => DownloadBlockError::Disconnected,
//         Code::Internal
//         | Code::Aborted
//         | Code::DataLoss
//         | Code::ResourceExhausted
//         | Code::Unknown
//         | Code::Cancelled => DownloadBlockError::FailedDownload,
//         Code::Ok => {
//             unreachable!("ok")
//         }
//         Code::InvalidArgument => {
//             panic!("invalid argument");
//         }
//         Code::DeadlineExceeded => DownloadBlockError::FailedDownload,
//         rest => DownloadBlockError::Unknown(tonic::Status::new(rest, "unknown error")),
//     }
// }

pub(crate) struct CompletedDownloadBlockTask {
    slot: u64,
    #[allow(dead_code)]
    block_uid: [u8; 16],
    shard_idx: FumeShardIdx,
    total_event_downloaded: usize,
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

impl GrpcDownloadBlockTaskRun {
    #[allow(dead_code)]
    const RUNTIME_NAME: &'static str = "tokio_grpc_task_run";

    async fn run(mut self) -> Result<CompletedDownloadBlockTask, GrpcDownloadTaskError> {
        let request = DownloadBlockShard {
            blockchain_id: self.download_request.blockchain_id.to_vec(),
            block_uid: self.download_request.block_uid.to_vec(),
            shard_idx: 0,
            block_filters: self.filters,
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
                        inc_total_event_downloaded(Self::RUNTIME_NAME, 1);
                    }

                    if self.dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                        return Err(GrpcDownloadTaskError::OutletDisconnected);
                    }
                }
                data_response::Response::BlockShardDownloadFinish(_) => {
                    return Ok(CompletedDownloadBlockTask {
                        slot: self.download_request.slot,
                        block_uid: self.download_request.block_uid,
                        shard_idx: 0,
                        total_event_downloaded,
                    });
                }
            }
        }

        Err(GrpcDownloadTaskError::IncompleteDownload)
    }
}
