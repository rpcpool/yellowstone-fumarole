#[cfg(feature = "prometheus")]
use crate::metrics::{
    dec_inflight_slot_download, inc_offset_commitment_count, inc_skip_offset_commitment_count,
    inc_slot_download_count, inc_slot_status_offset_processed_count, inc_total_event_downloaded,
    observe_slot_download_duration, set_max_slot_detected,
    set_processed_slot_status_offset_queue_len, set_slot_status_update_queue_len,
};
use {
    super::{
        ports::{ControlPlaneConnector, ControlPlaneStreamError, FumaroleDataplaneConnector},
        state_machine::{FumaroleSM, FumeDownloadRequest, FumeOffset, FumeShardIdx},
    },
    crate::{
        FumaroleClient,
        error::FumaroleSubscribeError,
        proto::{
            self, BlockFilters, CommitOffset, ControlCommand, DataCommand, DataResponse,
            DownloadBlockShard, GetChainTipResponse, JoinControlPlane, PollBlockchainHistory,
            control_response::Response, data_command::Command, data_response,
        },
    },
    futures::{Sink, SinkExt, Stream, StreamExt},
    rustc_hash::FxHashSet,
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
    tokio_util::sync::PollSender,
    yellowstone_grpc_proto::geyser::{
        self, CommitmentLevel, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
        subscribe_update::UpdateOneof,
    },
};

pub const DEFAULT_GC_INTERVAL: usize = 100;


pub struct FumaroleRuntimeDataEvent {
    pub slot: Slot,
    pub update: SubscribeUpdate,
}

pub enum FumaroleRuntimeEvent {
    Data(FumaroleRuntimeDataEvent),
    SlotEnded(u64),
}

///
/// Mimics Dragonsmouth subscribe request bidirectional stream.
///
pub struct DragonsmouthSubscribeRequestBidi {
    #[allow(dead_code)]
    pub tx: mpsc::Sender<SubscribeRequest>,
    pub rx: mpsc::Receiver<SubscribeRequest>,
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
pub(crate) struct FumaroleAsyncRuntime<C>
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
    pub dragonsmouth_outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
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
const CONTROL_PLANE_REJOIN_MAX_ATTEMPTS: usize = 3;
const CONTROL_PLANE_REJOIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_PLANE_REJOIN_BACKOFF: Duration = Duration::from_secs(2);



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

impl<C> FumaroleAsyncRuntime<C>
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

    async fn schedule_download_task_if_any(&mut self) {
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
            tracing::debug!(slot = download_request.slot, "scheduling download task");
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
                            let _ = self
                                .dragonsmouth_outlet
                                .send(Err(dataplane_err.into()))
                                .await;
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
                if self
                    .dragonsmouth_outlet
                    .send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
                        slot: slot_status.slot,
                        update,
                    })))
                    .await
                    .is_err()
                {
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
                for attempt in 1..=CONTROL_PLANE_REJOIN_MAX_ATTEMPTS {
                    tracing::warn!(
                        "control plane rejoin attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS}"
                    );

                    match tokio::time::timeout(
                        CONTROL_PLANE_REJOIN_ATTEMPT_TIMEOUT,
                        self.rejoin_controle_plane(),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            tracing::info!(
                                "control plane rejoin succeeded on attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS}"
                            );
                            return LoopInstruction::Continue;
                        }
                        Ok(Err(rejoin_err)) => {
                            tracing::warn!(
                                "control plane rejoin attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS} failed: {rejoin_err:?}"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                "control plane rejoin attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS} timed out after {:?}",
                                CONTROL_PLANE_REJOIN_ATTEMPT_TIMEOUT
                            );
                        }
                    }

                    if attempt < CONTROL_PLANE_REJOIN_MAX_ATTEMPTS {
                        tokio::time::sleep(CONTROL_PLANE_REJOIN_BACKOFF).await;
                    }
                }

                tracing::error!(
                    "exhausted control plane rejoin attempts ({CONTROL_PLANE_REJOIN_MAX_ATTEMPTS}) after disconnect: {e:?}"
                );
                let _ = self
                    .dragonsmouth_outlet
                    .send(Err(FumaroleSubscribeError::ControlPlaneRejoinFailed {
                        details: Some(Box::new(std::io::Error::other(format!(
                            "exhausted control plane rejoin attempts ({CONTROL_PLANE_REJOIN_MAX_ATTEMPTS})"
                        )))),
                    }))
                    .await;
                LoopInstruction::ErrorStop
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
                tracing::debug!(
                    last_committed_offset = self.sm.last_committed_offset,
                    committable_offset = self.sm.committable_offset,
                    "received get tip response: {:?}",
                    get_tip_response.shard_to_max_offset_map
                );
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
            self.schedule_download_task_if_any().await;
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
                    tracing::debug!(
                        download_queue_capacity = self.download_task_runner_chans.download_task_queue_tx.capacity(),
                        "commit deadline reached"
                    );
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

#[derive(Clone, Debug)]
struct ScheduleShardDownload {
    blockchain_id: Vec<u8>,
    shard_idx: FumeShardIdx,
    block_uid: Vec<u8>,
    attempt: usize,
    slot: Slot,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DedupKey {
    Account {
        pubkey: Vec<u8>,
        txn_signature: Option<Vec<u8>>,
    },
    Transaction {
        index: u64,
    },
    Entry {
        index: u64,
    },
}

const DEDUP_WINDOW_SIZE: usize = 100_000;
const ORCHESTRATOR_DOWNLOADER_QUEUE_CAPACITY: usize = 2;
const PENDING_SHARD_DOWNLOAD_RETRY_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Default, Debug, Clone)]
struct DedupState {
    seen: HashMap<(u64, FumeShardIdx), FxHashSet<DedupKey>>,
    completed_shards: FxHashSet<(u64, FumeShardIdx)>,
    completed_order: VecDeque<(u64, FumeShardIdx)>,
}

impl DedupState {
    /// Returns true when the event should be skipped as duplicate.
    fn dedup(&mut self, slot: u64, shard_idx: FumeShardIdx, ev: &UpdateOneof) -> bool {
        if self.is_shard_done(slot, shard_idx) {
            return true;
        }

        let Some(key) = Self::key_for_update(ev) else {
            // Non-deduped event kinds are skipped by default.
            return true;
        };
        let shard_key = (slot, shard_idx);
        let shard_seen = self.seen.entry(shard_key).or_default();
        !shard_seen.insert(key)
    }

    fn mark_shard_done(&mut self, slot: u64, shard_idx: FumeShardIdx) {
        let shard_key = (slot, shard_idx);
        if !self.completed_shards.insert(shard_key) {
            return;
        }
        self.completed_order.push_back(shard_key);

        self.seen.remove(&shard_key);

        if self.completed_order.len() > DEDUP_WINDOW_SIZE {
            if let Some(oldest) = self.completed_order.pop_front() {
                self.completed_shards.remove(&oldest);
            }
        }
    }

    fn is_shard_done(&self, slot: u64, shard_idx: FumeShardIdx) -> bool {
        self.completed_shards.contains(&(slot, shard_idx))
    }

    fn shrink_seen_if_needed(&mut self) {
        // `seen` is keyed by shard (slot, shard_idx) and is aggressively removed
        // in `mark_shard_done`, so no additional key-level compaction is required.
        while self.completed_order.len() > DEDUP_WINDOW_SIZE {
            if let Some(oldest) = self.completed_order.pop_front() {
                self.completed_shards.remove(&oldest);
            }
        }
    }

    fn key_for_update(ev: &UpdateOneof) -> Option<DedupKey> {
        match ev {
            UpdateOneof::Account(msg) => {
                if let Some(account_info) = msg.account.as_ref() {
                    Some(DedupKey::Account {
                        pubkey: account_info.pubkey.clone(),
                        txn_signature: account_info.txn_signature.clone(),
                    })
                } else {
                    None
                }
            }
            UpdateOneof::Slot(_) => None,
            UpdateOneof::Transaction(msg) => {
                if let Some(tx_info) = msg.transaction.as_ref() {
                    Some(DedupKey::Transaction {
                        index: tx_info.index,
                    })
                } else {
                    None
                }
            }
            UpdateOneof::TransactionStatus(msg) => Some(DedupKey::Transaction { index: msg.index }),
            UpdateOneof::BlockMeta(_)
            | UpdateOneof::Block(_)
            | UpdateOneof::Ping(_)
            | UpdateOneof::Pong(_) => None,
            UpdateOneof::Entry(msg) => Some(DedupKey::Entry { index: msg.index }),
        }
    }
}

pub(crate) struct PipelinedShardDownloader<Outlet> {
    cnc_rx: mpsc::Receiver<Arc<SubscribeRequest>>,
    download_completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: Outlet,
    shard_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    shard_download_queue: mpsc::Receiver<QueuedShardDownload>,
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
    pub(crate) fn new(
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
struct ShardDownloaderError {
    slot_scheduled_for_download: VecDeque<ScheduleShardDownload>,
    shard_download_queue_rx: mpsc::Receiver<QueuedShardDownload>,
    recyclable_dedup_state: Option<DedupState>,
    kind: ShardDownloaderErrorKind,
}

pub struct ShardDownloaderRecycleState {
    scheduled_shard_downloads: VecDeque<ScheduleShardDownload>,
    shard_download_queue_rx: mpsc::Receiver<QueuedShardDownload>,
    recyclable_dedup_state: DedupState,
}

type PipelinedDownloaderOutcome = Result<
    (Option<CompletedDownloadBlockShardTask>, DedupState),
    (ShardDownloaderErrorKind, DedupState),
>;

impl<Outlet> PipelinedShardDownloader<Outlet>
where
    Outlet: Sink<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>> + Unpin + Send + 'static,
{
    async fn pipelined_downloader<Source>(
        mut rx: Source,
        mut outlet: Outlet,
        completed_slot_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
        mut dedup_state: DedupState,
        mut scheduled_shard_rx: mpsc::UnboundedReceiver<(u64, FumeShardIdx)>,
    ) -> PipelinedDownloaderOutcome
    where
        Source: Stream<Item = Result<DataResponse, DataplaneStreamError>> + Unpin,
    {
        let mut total_event_downloaded = 0;
        let mut block_meta: Option<SubscribeUpdate> = None;
        let mut scheduled_shards = VecDeque::new();
        let mut t = Instant::now();
        tracing::trace!("starting continuous shard downloader loop");
        while let Some(data) = rx.next().await {
            while let Ok(shard_ctx) = scheduled_shard_rx.try_recv() {
                scheduled_shards.push_back(shard_ctx);
            }

            let resp = match data {
                Ok(data) => data.response.expect("response"),
                Err(err) => {
                    return Err((ShardDownloaderErrorKind::DataplaneError(err), dedup_state));
                }
            };

            match resp {
                data_response::Response::Update(update) => {
                    if update.update_oneof.as_ref().is_none() {
                        continue;
                    }

                    total_event_downloaded += 1;
                    #[cfg(feature = "prometheus")]
                    {
                        inc_total_event_downloaded(1);
                    }
                    #[allow(clippy::collapsible_else_if)]
                    if matches!(update.update_oneof, Some(UpdateOneof::BlockMeta(_))) {
                        block_meta = Some(update);
                    } else {
                        let event_slot = if let Some((expected_slot, expected_shard_idx)) =
                            scheduled_shards
                                .front()
                                .map(|(slot, shard_idx)| (*slot, *shard_idx))
                        {
                            if dedup_state.dedup(
                                expected_slot,
                                expected_shard_idx,
                                update
                                    .update_oneof
                                    .as_ref()
                                    .expect("update oneof must be set"),
                            ) {
                                continue;
                            }
                            expected_slot
                        } else {
                            // In tests and during startup races we might receive updates before
                            // scheduled shard context is visible here. Skip dedup in that case.
                            match update.update_oneof.as_ref() {
                                Some(UpdateOneof::Account(msg)) => msg.slot,
                                Some(UpdateOneof::Slot(msg)) => msg.slot,
                                Some(UpdateOneof::Transaction(msg)) => msg.slot,
                                Some(UpdateOneof::TransactionStatus(msg)) => msg.slot,
                                Some(UpdateOneof::Block(msg)) => msg.slot,
                                Some(UpdateOneof::BlockMeta(msg)) => msg.slot,
                                Some(UpdateOneof::Entry(msg)) => msg.slot,
                                Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) | None => {
                                    continue;
                                }
                            }
                        };

                        if futures::future::poll_fn(|cx| outlet.poll_ready_unpin(cx))
                            .await
                            .is_err()
                        {
                            return Err((ShardDownloaderErrorKind::SinkClosed, dedup_state));
                        }
                        if outlet
                            .start_send_unpin(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
                                slot: event_slot,
                                update,
                            })))
                            .is_err()
                        {
                            return Err((ShardDownloaderErrorKind::SinkClosed, dedup_state));
                        }
                    }
                }
                data_response::Response::BlockShardDownloadFinish(footer) => {
                    dedup_state.mark_shard_done(footer.slot, footer.shard_indices[0]);
                    dedup_state.shrink_seen_if_needed();
                    tracing::trace!(
                        "shard {} for slot {} download finished, dedup state updated",
                        footer.shard_indices[0],
                        footer.slot
                    );
                    if let Some((scheduled_slot, scheduled_shard_idx)) =
                        scheduled_shards.pop_front()
                    {
                        debug_assert_eq!(
                            scheduled_slot, footer.slot,
                            "slot mismatch for scheduled shard completion"
                        );
                        debug_assert_eq!(
                            scheduled_shard_idx, footer.shard_indices[0],
                            "shard idx mismatch for scheduled shard completion"
                        );
                    }

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
                        return Ok((Some(completed), dedup_state));
                    }
                    t = Instant::now();
                }
            }
        }
        tracing::trace!("exiting continuous shard downloader loop");
        Ok((None, dedup_state))
    }

    async fn downloader_loop<DataplaneSink, DataplaneStream>(
        mut self,
        mut dataplane_sink: DataplaneSink,
        dataplane_stream: DataplaneStream,
        dedup_state: DedupState,
    ) -> Result<ShardDownloaderRecycleState, ShardDownloaderError>
    where
        DataplaneSink: Sink<DataCommand> + Send + Unpin + 'static,
        DataplaneStream:
            Stream<Item = Result<DataResponse, DataplaneStreamError>> + Send + Unpin + 'static,
    {
        let initial_filter_update_result = dataplane_sink
            .send(DataCommand {
                command: Some(Command::FilterUpdate(
                    (*self.subscribe_request).clone().into(),
                )),
            })
            .await;
        if initial_filter_update_result.is_err() {
            return Err(ShardDownloaderError {
                kind: ShardDownloaderErrorKind::SubscribeDataTxDisconnected,
                slot_scheduled_for_download: self.shard_scheduled_for_download,
                shard_download_queue_rx: self.shard_download_queue,
                recyclable_dedup_state: Some(dedup_state),
            });
        }
        let mut joinset = JoinSet::new();
        let (scheduled_shard_tx, scheduled_shard_rx) = mpsc::unbounded_channel();

        let dragonsmouth_outlet = self.dragonsmouth_outlet;
        let (completed_slot_tx, mut completed_slot_rx) = mpsc::channel(10);
        joinset.spawn(Self::pipelined_downloader(
            dataplane_stream,
            dragonsmouth_outlet,
            completed_slot_tx,
            dedup_state,
            scheduled_shard_rx,
        ));

        let mut recyclable_dedup_state = None;
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
                        Ok((maybe_completed, dedup_state)) => {
                            recyclable_dedup_state = Some(dedup_state);
                            if let Some(completed) = maybe_completed {
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
                                let _ = self.download_completed_tx.send(completed).await;
                            }
                            break None
                        }
                        Err((e, dedup_state)) => {
                            recyclable_dedup_state = Some(dedup_state);
                            break Some(e)
                        },
                    }

                }
                result = self.shard_download_queue.recv(), if poll_shared_queue => {
                    let Some(queued_shard_download) = result else {
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
                    let _ = scheduled_shard_tx.send((queued_shard_download.slot, queued_shard_download.request.shard_idx as u32));
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

        if recyclable_dedup_state.is_none() {
            if let Some(result) = joinset.join_next().await {
                let result2 = result.expect("join error");
                match result2 {
                    Ok((maybe_completed, dedup_state)) => {
                        recyclable_dedup_state = Some(dedup_state);
                        if let Some(completed) = maybe_completed {
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
                            let _ = self.download_completed_tx.send(completed).await;
                        }
                    }
                    Err((err_kind, dedup_state)) => {
                        recyclable_dedup_state = Some(dedup_state);
                        if err.is_none() {
                            return Err(ShardDownloaderError {
                                kind: err_kind,
                                slot_scheduled_for_download: self.shard_scheduled_for_download,
                                shard_download_queue_rx: self.shard_download_queue,
                                recyclable_dedup_state,
                            });
                        }
                    }
                }
            }
        }

        let recyclable_dedup_state = recyclable_dedup_state.unwrap_or_default();

        if let Some(err_kind) = err {
            Err(ShardDownloaderError {
                kind: err_kind,
                slot_scheduled_for_download: self.shard_scheduled_for_download,
                shard_download_queue_rx: self.shard_download_queue,
                recyclable_dedup_state: Some(recyclable_dedup_state),
            })
        } else {
            Ok(ShardDownloaderRecycleState {
                scheduled_shard_downloads: self.shard_scheduled_for_download,
                shard_download_queue_rx: self.shard_download_queue,
                recyclable_dedup_state,
            })
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

struct PendingShardDownload {
    downloader_idx: usize,
    queued: QueuedShardDownload,
}

/// Orchestrates shard downloads across `N` pinned downloader workers.
///
/// Affinity model:
/// 1. Each downloader index `i` owns exactly one queue `queue[i]`.
/// 2. A shard `j` is always routed to `queue[j % N]`.
/// 3. If downloader `i` fails, recovered work is recycled back to `queue[i]`
///    before respawn, so no shard is redirected to another downloader index.
///
/// This design keeps ordering and ownership stable per shard lane while still
/// allowing independent downloader restarts.
///
/// Additional benefits:
/// 1. Downloaders can run parallel, non-shared dedup logic at lane level,
///    because each lane owns a stable subset of shards.
/// 2. The backend can perform better shard download prediction thanks to
///    deterministic shard-to-downloader affinity over time.
pub(crate) struct ShardedDownloadOrchestrator<C> {
    shard_download_queue_txs: Vec<mpsc::Sender<QueuedShardDownload>>,
    shard_download_queue_rxs: Vec<Option<mpsc::Receiver<QueuedShardDownload>>>,
    slot_download_progression_map: HashMap<Slot, ShardedSlotDownloadProgress>,
    completed_tx: mpsc::Sender<CompletedDownloadBlockShardTask>,
    completed_rx: mpsc::Receiver<CompletedDownloadBlockShardTask>,
    connector: C,
    shard_downloader_js: JoinSet<Result<ShardDownloaderRecycleState, ShardDownloaderError>>,
    shard_downloader_handles: HashMap<task::Id, PipelinedShardDownloaderHandle>,
    cnc_rx: mpsc::Receiver<DownloadTaskRunnerCommand>,
    download_task_queue: mpsc::Receiver<DownloadTaskArgs>,
    outlet: mpsc::Sender<DownloadTaskResult>,
    max_download_attempt_per_slot: usize,
    subscribe_request: Arc<SubscribeRequest>,
    dragonsmouth_outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    total_shard_downloaders: usize,
    pending_shard_downloads: VecDeque<PendingShardDownload>,
}

///
/// Holds information about on-going data plane task.
///
#[derive(Debug, Clone)]
pub(crate) struct PipelinedShardDownloaderHandle {
    downloader_idx: usize,
    cnc_tx: mpsc::Sender<Arc<SubscribeRequest>>,
}

impl<C> ShardedDownloadOrchestrator<C>
where
    C: FumaroleDataplaneConnector + Send + 'static,
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
        dragonsmouth_outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    ) -> Self {
        let (completed_tx, completed_rx) = mpsc::channel(1000);
        let mut shard_download_queue_txs = Vec::with_capacity(total_shard_downloaders);
        let mut shard_download_queue_rxs = Vec::with_capacity(total_shard_downloaders);
        for _ in 0..total_shard_downloaders {
            let (tx, rx) = mpsc::channel(ORCHESTRATOR_DOWNLOADER_QUEUE_CAPACITY);
            shard_download_queue_txs.push(tx);
            shard_download_queue_rxs.push(Some(rx));
        }
        Self {
            connector,
            shard_downloader_js: JoinSet::new(),
            shard_downloader_handles: HashMap::new(),
            cnc_rx,
            download_task_queue,
            outlet,
            max_download_attempt_per_slot: max_download_attempt_by_slot,
            subscribe_request,
            shard_download_queue_txs,
            shard_download_queue_rxs,
            slot_download_progression_map: HashMap::new(),
            dragonsmouth_outlet,
            total_shard_downloaders,
            completed_tx,
            completed_rx,
            pending_shard_downloads: VecDeque::new(),
        }
    }

    fn queue_idx_for_shard(&self, shard_idx: FumeShardIdx) -> usize {
        shard_idx as usize % self.total_shard_downloaders
    }

    /// Replaces downloader `i` queue endpoints with a fresh channel pair.
    ///
    /// This must happen before recycling, so recovered work is enqueued into
    /// the new queue instance that the next downloader task will consume.
    fn reset_downloader_queue(&mut self, downloader_idx: usize) {
        let (tx, rx) = mpsc::channel(ORCHESTRATOR_DOWNLOADER_QUEUE_CAPACITY);
        self.shard_download_queue_txs[downloader_idx] = tx;
        self.shard_download_queue_rxs[downloader_idx] = Some(rx);
    }

    /// Routes a queued download using shard affinity (`shard_idx % N`).
    ///
    /// Normal scheduling and retry paths should call this helper when they
    /// want default affinity routing.
    fn route_queued_download(&mut self, queued: QueuedShardDownload) {
        let shard_idx = queued.request.shard_idx as u32;
        let downloader_idx = self.queue_idx_for_shard(shard_idx);
        self.route_queued_download_to_downloader(downloader_idx, queued);
    }

    /// Routes a queued download to an explicit downloader queue index.
    ///
    /// Recovery paths use this to preserve lane ownership when recycling work
    /// from a failed downloader.
    fn route_queued_download_to_downloader(
        &mut self,
        downloader_idx: usize,
        queued: QueuedShardDownload,
    ) {
        match self.shard_download_queue_txs[downloader_idx].try_send(queued) {
            Ok(()) => {}
            Err(TrySendError::Full(queued)) => {
                self.pending_shard_downloads
                    .push_back(PendingShardDownload {
                        downloader_idx,
                        queued,
                    });
            }
            Err(TrySendError::Closed(_)) => {
                panic!("shard downloader queue unexpectedly closed");
            }
        }
    }

    fn drain_pending_shard_downloads(&mut self) {
        while let Some(pending) = self.pending_shard_downloads.pop_front() {
            match self.shard_download_queue_txs[pending.downloader_idx].try_send(pending.queued) {
                Ok(()) => {}
                Err(TrySendError::Full(queued)) => {
                    self.pending_shard_downloads
                        .push_front(PendingShardDownload {
                            downloader_idx: pending.downloader_idx,
                            queued,
                        });
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    panic!("shard downloader queue unexpectedly closed");
                }
            }
        }
    }

    /// Re-enqueues scheduled (already-issued) shard downloads back to the
    /// pinned queue of downloader `i`.
    ///
    /// Affinity invariant: for every recycled item, `shard_idx % N == i`.
    async fn recycle_scheduled_shard_downloads(
        &mut self,
        downloader_idx: usize,
        shard_downloads: VecDeque<ScheduleShardDownload>,
    ) {
        for scheduled_shard_download in shard_downloads {
            let expected_downloader_idx =
                self.queue_idx_for_shard(scheduled_shard_download.shard_idx);
            debug_assert_eq!(
                expected_downloader_idx, downloader_idx,
                "scheduled shard affinity mismatch during recycle"
            );
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
            self.route_queued_download_to_downloader(downloader_idx, queued);
        }
    }

    /// Drains all currently buffered queued downloads from the old receiver and
    /// re-enqueues them into downloader `i` new queue.
    ///
    /// `try_recv` is used intentionally to perform a non-blocking snapshot drain:
    /// read what is already buffered, then stop when empty.
    async fn recycle_queued_shard_downloads(
        &mut self,
        downloader_idx: usize,
        mut shard_download_queue_rx: mpsc::Receiver<QueuedShardDownload>,
    ) {
        while let Ok(queued_download) = shard_download_queue_rx.try_recv() {
            let expected_downloader_idx =
                self.queue_idx_for_shard(queued_download.request.shard_idx as u32);
            debug_assert_eq!(
                expected_downloader_idx, downloader_idx,
                "queued shard affinity mismatch during recycle"
            );
            self.route_queued_download_to_downloader(downloader_idx, queued_download);
        }
    }

    async fn spawn_shard_downloader(
        &mut self,
        downloader_idx: usize,
        recycled_dedup_state: Option<DedupState>,
    ) -> Result<(), DownloadBlockError> {
        let (dataplane_tx, dataplane_stream) = self
            .connector
            .subscribe_data()
            .await
            .map_err(|e| DownloadBlockError::FailedDownload(DataplaneStreamError::from(e)))?;

        let shard_download_queue = self.shard_download_queue_rxs[downloader_idx]
            .take()
            .expect("missing shard download queue receiver");

        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        let dragonsmouth_sink = PollSender::new(self.dragonsmouth_outlet.clone());
        let shard_downloader = PipelinedShardDownloader {
            cnc_rx,
            download_completed_tx: self.completed_tx.clone(),
            subscribe_request: Arc::clone(&self.subscribe_request),
            dragonsmouth_outlet: dragonsmouth_sink,
            shard_scheduled_for_download: Default::default(),
            shard_download_queue,
        };
        let ah = self.shard_downloader_js.spawn(async move {
            shard_downloader
                .downloader_loop(
                    dataplane_tx,
                    dataplane_stream,
                    recycled_dedup_state.unwrap_or_default(),
                )
                .await
        });
        let handle = PipelinedShardDownloaderHandle {
            downloader_idx,
            cnc_tx,
        };
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
                #[cfg(feature = "prometheus")]
                {
                    let elapsed = completed.started_at.elapsed();
                    observe_slot_download_duration(elapsed);
                }
                let block_meta = completed.block_meta;
                tracing::trace!("slot {slot} download completed");
                if let Some(block_meta) = block_meta {
                    let _ = self
                        .dragonsmouth_outlet
                        .send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
                            slot,
                            update: block_meta,
                        })))
                        .await;
                } else {
                    tracing::debug!(
                        slot,
                        "slot completed without block meta; emitting SlotEnded only"
                    );
                }
                let _ = self
                    .dragonsmouth_outlet
                    .send(Ok(FumaroleRuntimeEvent::SlotEnded(slot)))
                    .await;

                #[cfg(feature = "prometheus")]
                {
                    inc_slot_download_count();
                }
            }
        }
    }

    /// Splits one slot into per-shard downloads and enqueues each shard using
    /// the affinity rule `shard_idx % N`.
    async fn schedule_slot_download_task(&mut self, task_spec: DownloadTaskArgs) {
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
            self.route_queued_download(queued_download);
        }
        let slot_progress = ShardedSlotDownloadProgress {
            started_at: Instant::now(),
            remaining_shard_idx: shard_idx_vec,
            block_meta: None,
        };
        self.slot_download_progression_map
            .insert(slot, slot_progress);
    }

    /// Handles one downloader completion/failure and keeps shard-lane affinity.
    ///
    /// Recovery sequence:
    /// 1. Reset queue endpoints for failed downloader `i`.
    /// 2. Recycle all recoverable work back to queue `i`.
    /// 3. Spawn a new downloader bound to queue `i`.
    ///
    /// This prevents work from lane `i` being consumed by any other lane.
    async fn handle_shard_downloader_result(
        &mut self,
        task_id: Id,
        result: Result<ShardDownloaderRecycleState, ShardDownloaderError>,
    ) -> Result<(), DownloadBlockError> {
        let Some(handle) = self.shard_downloader_handles.remove(&task_id) else {
            return Ok(());
        };
        let downloader_idx = handle.downloader_idx;

        // Recovery order for strict pinned architecture:
        // 1) Reset queue endpoints for downloader `i` (new tx/rx pair).
        // 2) Recycle scheduled + buffered queued work back into queue `i`.
        // 3) Spawn a fresh downloader `i` bound to that new receiver.
        //
        // This preserves shard affinity (`shard_idx % N == i`) across crashes and
        // avoids rerouting work to other downloader indices.
        self.reset_downloader_queue(downloader_idx);

        let recycled_dedup_state = match result {
            Ok(shard_downloader_recycle_state) => {
                self.recycle_scheduled_shard_downloads(
                    downloader_idx,
                    shard_downloader_recycle_state.scheduled_shard_downloads,
                )
                .await;
                self.recycle_queued_shard_downloads(
                    downloader_idx,
                    shard_downloader_recycle_state.shard_download_queue_rx,
                )
                .await;
                Some(shard_downloader_recycle_state.recyclable_dedup_state)
            }
            Err(e) => {
                let ShardDownloaderError {
                    mut slot_scheduled_for_download,
                    shard_download_queue_rx,
                    recyclable_dedup_state,
                    kind,
                } = e;
                self.recycle_queued_shard_downloads(downloader_idx, shard_download_queue_rx)
                    .await;

                match kind {
                    ShardDownloaderErrorKind::DataplaneError(dataplane_err) => {
                        if let Some(schedule_shard_download) =
                            slot_scheduled_for_download.pop_front()
                        {
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
                                self.route_queued_download_to_downloader(downloader_idx, queued);
                            } else {
                                self.recycle_scheduled_shard_downloads(
                                    downloader_idx,
                                    slot_scheduled_for_download,
                                )
                                .await;
                                return Err(DownloadBlockError::FailedDownload(dataplane_err));
                            }
                        }
                        self.recycle_scheduled_shard_downloads(
                            downloader_idx,
                            slot_scheduled_for_download,
                        )
                        .await;
                    }
                    ShardDownloaderErrorKind::SinkClosed => {
                        return Err(DownloadBlockError::OutletDisconnected);
                    }
                    ShardDownloaderErrorKind::SubscribeDataTxDisconnected => {
                        self.recycle_scheduled_shard_downloads(
                            downloader_idx,
                            slot_scheduled_for_download,
                        )
                        .await;
                    }
                }
                recyclable_dedup_state
            }
        };

        self.spawn_shard_downloader(downloader_idx, recycled_dedup_state)
            .await
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

    fn can_poll_download_task_queue(&self) -> bool {
        self.pending_shard_downloads.is_empty()
    }

    pub(crate) async fn run(mut self) -> Result<(), DownloadBlockError> {
        for downloader_idx in 0..self.total_shard_downloaders {
            self.spawn_shard_downloader(downloader_idx, None).await?;
        }

        while !self.outlet.is_closed() {
            self.drain_pending_shard_downloads();
            let can_poll_download_task = self.can_poll_download_task_queue();
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
                maybe_download_task = self.download_task_queue.recv(), if can_poll_download_task => {
                    match maybe_download_task {
                        Some(download_task) => {
                            self.schedule_slot_download_task(download_task).await;
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
                _ = tokio::time::sleep(PENDING_SHARD_DOWNLOAD_RETRY_INTERVAL), if !self.pending_shard_downloads.is_empty() => {}
            }
        }
        tracing::debug!("Closing GrpcDownloadTaskRunner loop");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {super::*, futures::channel::mpsc as futures_mpsc, std::pin::Pin};

    #[derive(Clone, Default)]
    struct TestConnector;

    impl FumaroleDataplaneConnector for TestConnector {
        type DataplaneSubscribeError = tonic::Status;
        type DataplaneSinkError = mpsc::error::SendError<DataCommand>;
        type DataplaneSink =
            Pin<Box<dyn Sink<DataCommand, Error = Self::DataplaneSinkError> + Send>>;
        type DataplaneStream =
            Pin<Box<dyn Stream<Item = Result<DataResponse, DataplaneStreamError>> + Send>>;
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
            Box::pin(async { Err(tonic::Status::unimplemented("test connector")) })
        }
    }

    fn make_test_orchestrator() -> (
        ShardedDownloadOrchestrator<TestConnector>,
        mpsc::Receiver<DownloadTaskResult>,
        mpsc::Receiver<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    ) {
        let (completed_tx, completed_rx) = mpsc::channel(8);
        let (cnc_tx, cnc_rx) = mpsc::channel(1);
        let (download_task_queue_tx, download_task_queue_rx) = mpsc::channel(1);
        let (outlet_tx, outlet_rx) = mpsc::channel(8);
        let (dragonsmouth_outlet_tx, dragonsmouth_outlet_rx) = mpsc::channel(8);
        let (shard_download_queue_tx, shard_download_queue_rx) =
            mpsc::channel(ORCHESTRATOR_DOWNLOADER_QUEUE_CAPACITY);

        let orchestrator = ShardedDownloadOrchestrator {
            shard_download_queue_txs: vec![shard_download_queue_tx],
            shard_download_queue_rxs: vec![Some(shard_download_queue_rx)],
            slot_download_progression_map: HashMap::new(),
            completed_tx,
            completed_rx,
            connector: TestConnector,
            shard_downloader_js: JoinSet::new(),
            shard_downloader_handles: HashMap::new(),
            cnc_rx,
            download_task_queue: download_task_queue_rx,
            outlet: outlet_tx,
            max_download_attempt_per_slot: 3,
            subscribe_request: Arc::new(SubscribeRequest::default()),
            dragonsmouth_outlet: dragonsmouth_outlet_tx,
            total_shard_downloaders: 1,
            pending_shard_downloads: VecDeque::new(),
        };

        drop(cnc_tx);
        drop(download_task_queue_tx);

        (orchestrator, outlet_rx, dragonsmouth_outlet_rx)
    }

    fn make_test_orchestrator_with_downloaders(
        total_shard_downloaders: usize,
    ) -> (
        ShardedDownloadOrchestrator<TestConnector>,
        mpsc::Receiver<DownloadTaskResult>,
        mpsc::Receiver<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    ) {
        let (completed_tx, completed_rx) = mpsc::channel(8);
        let (cnc_tx, cnc_rx) = mpsc::channel(1);
        let (download_task_queue_tx, download_task_queue_rx) = mpsc::channel(1);
        let (outlet_tx, outlet_rx) = mpsc::channel(8);
        let (dragonsmouth_outlet_tx, dragonsmouth_outlet_rx) = mpsc::channel(8);

        let mut shard_download_queue_txs = Vec::with_capacity(total_shard_downloaders);
        let mut shard_download_queue_rxs = Vec::with_capacity(total_shard_downloaders);
        for _ in 0..total_shard_downloaders {
            let (tx, rx) = mpsc::channel(ORCHESTRATOR_DOWNLOADER_QUEUE_CAPACITY);
            shard_download_queue_txs.push(tx);
            shard_download_queue_rxs.push(Some(rx));
        }

        let orchestrator = ShardedDownloadOrchestrator {
            shard_download_queue_txs,
            shard_download_queue_rxs,
            slot_download_progression_map: HashMap::new(),
            completed_tx,
            completed_rx,
            connector: TestConnector,
            shard_downloader_js: JoinSet::new(),
            shard_downloader_handles: HashMap::new(),
            cnc_rx,
            download_task_queue: download_task_queue_rx,
            outlet: outlet_tx,
            max_download_attempt_per_slot: 3,
            subscribe_request: Arc::new(SubscribeRequest::default()),
            dragonsmouth_outlet: dragonsmouth_outlet_tx,
            total_shard_downloaders,
            pending_shard_downloads: VecDeque::new(),
        };

        drop(cnc_tx);
        drop(download_task_queue_tx);

        (orchestrator, outlet_rx, dragonsmouth_outlet_rx)
    }

    fn mk_update(update_oneof: UpdateOneof) -> DataResponse {
        DataResponse {
            response: Some(data_response::Response::Update(SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(update_oneof),
            })),
        }
    }

    fn empty_scheduled_shard_rx() -> mpsc::UnboundedReceiver<(u64, FumeShardIdx)> {
        let (_tx, rx) = mpsc::unbounded_channel();
        rx
    }

    #[tokio::test]
    async fn sharded_orchestrator_schedules_all_shards_and_dedups_slot() {
        let (mut orchestrator, _outlet_rx, _dragonsmouth_outlet_rx) = make_test_orchestrator();

        let request = FumeDownloadRequest {
            slot: 77,
            blockchain_id: [11u8; 16],
            block_uid: [22u8; 16],
            num_shards: 3,
            commitment_level: CommitmentLevel::Processed,
        };

        let mut queue_rx = orchestrator.shard_download_queue_rxs[0]
            .take()
            .expect("missing test queue receiver");

        let drain_jh = tokio::spawn(async move {
            let mut drained = Vec::new();
            for _ in 0..3 {
                drained.push(queue_rx.recv().await.expect("expected queued shard"));
            }
            (drained, queue_rx)
        });

        orchestrator
            .schedule_slot_download_task(DownloadTaskArgs {
                download_request: request.clone(),
            })
            .await;

        // With bounded queue + pending queue fallback, actively drain pending items
        // so the spawned receiver can observe all 3 enqueued shards deterministically.
        for _ in 0..16 {
            if orchestrator.pending_shard_downloads.is_empty() {
                break;
            }
            orchestrator.drain_pending_shard_downloads();
            tokio::task::yield_now().await;
        }

        let (drained, queue_rx) = drain_jh.await.expect("drain task should complete");
        orchestrator.shard_download_queue_rxs[0] = Some(queue_rx);

        assert!(orchestrator.slot_download_progression_map.contains_key(&77));
        {
            for (shard_idx, queued) in drained.into_iter().enumerate() {
                assert_eq!(queued.slot, 77);
                assert_eq!(queued.attempt, 1);
                assert_eq!(queued.request.shard_idx, shard_idx as i32);
                assert_eq!(queued.request.slot, Some(77));
                assert_eq!(queued.request.blockchain_id, request.blockchain_id.to_vec());
                assert_eq!(queued.request.block_uid, request.block_uid.to_vec());
            }
        }

        // Scheduling the same slot again should be ignored.
        orchestrator
            .schedule_slot_download_task(DownloadTaskArgs {
                download_request: request,
            })
            .await;
        {
            let queue_rx = orchestrator.shard_download_queue_rxs[0]
                .as_mut()
                .expect("missing test queue receiver");
            assert!(queue_rx.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn sharded_orchestrator_emits_completion_and_block_meta_when_slot_finishes() {
        let (mut orchestrator, mut outlet_rx, mut dragonsmouth_outlet_rx) =
            make_test_orchestrator();

        orchestrator.slot_download_progression_map.insert(
            42,
            ShardedSlotDownloadProgress {
                started_at: Instant::now(),
                block_meta: None,
                remaining_shard_idx: vec![0],
            },
        );

        let completed = CompletedDownloadBlockShardTask {
            shard_idx: 0,
            block_uid: [7u8; 16],
            block_meta: Some(SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(UpdateOneof::BlockMeta(
                    yellowstone_grpc_proto::geyser::SubscribeUpdateBlockMeta::default(),
                )),
            }),
            slot: 42,
        };

        orchestrator
            .handle_shard_download_completed(completed)
            .await;

        let task_result = outlet_rx
            .recv()
            .await
            .expect("expected completed task result");
        match task_result {
            DownloadTaskResult::Ok(task) => {
                assert_eq!(task.slot, 42);
                assert_eq!(task.block_uid, [7u8; 16]);
                assert_eq!(task.shard_idx_vec, vec![0]);
            }
            DownloadTaskResult::Err { .. } => panic!("expected DownloadTaskResult::Ok"),
        }

        let dm_msg = dragonsmouth_outlet_rx
            .recv()
            .await
            .expect("expected dragonsmouth block meta");
        let Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 42,
            update: dm_update,
        })) = dm_msg
        else {
            panic!("expected dragonsmouth data event")
        };
        assert!(matches!(
            dm_update.update_oneof,
            Some(UpdateOneof::BlockMeta(_))
        ));
        let slot_ended = dragonsmouth_outlet_rx
            .recv()
            .await
            .expect("expected slot ended event");
        assert!(matches!(slot_ended, Ok(FumaroleRuntimeEvent::SlotEnded(42))));
        assert!(!orchestrator.slot_download_progression_map.contains_key(&42));
    }

    #[tokio::test]
    async fn sharded_orchestrator_emits_slot_ended_without_block_meta_when_slot_finishes() {
        let (mut orchestrator, mut outlet_rx, mut dragonsmouth_outlet_rx) =
            make_test_orchestrator();

        orchestrator.slot_download_progression_map.insert(
            43,
            ShardedSlotDownloadProgress {
                started_at: Instant::now(),
                block_meta: None,
                remaining_shard_idx: vec![0],
            },
        );

        let completed = CompletedDownloadBlockShardTask {
            shard_idx: 0,
            block_uid: [8u8; 16],
            block_meta: None,
            slot: 43,
        };

        orchestrator
            .handle_shard_download_completed(completed)
            .await;

        let task_result = outlet_rx
            .recv()
            .await
            .expect("expected completed task result");
        match task_result {
            DownloadTaskResult::Ok(task) => {
                assert_eq!(task.slot, 43);
                assert_eq!(task.block_uid, [8u8; 16]);
                assert_eq!(task.shard_idx_vec, vec![0]);
            }
            DownloadTaskResult::Err { .. } => panic!("expected DownloadTaskResult::Ok"),
        }

        let slot_ended = dragonsmouth_outlet_rx
            .recv()
            .await
            .expect("expected slot ended event");
        assert!(matches!(slot_ended, Ok(FumaroleRuntimeEvent::SlotEnded(43))));
        assert!(dragonsmouth_outlet_rx.try_recv().is_err());
        assert!(!orchestrator.slot_download_progression_map.contains_key(&43));
    }

    #[tokio::test]
    async fn sharded_orchestrator_recycles_failed_downloader_back_to_same_queue() {
        let (mut orchestrator, _outlet_rx, _dragonsmouth_outlet_rx) =
            make_test_orchestrator_with_downloaders(2);

        let failed_downloader_idx = 1usize;

        let ah = orchestrator.shard_downloader_js.spawn(async {
            std::future::pending::<Result<ShardDownloaderRecycleState, ShardDownloaderError>>()
                .await
        });
        let task_id = ah.id();

        let (cnc_tx, _cnc_rx) = mpsc::channel(1);
        orchestrator.shard_downloader_handles.insert(
            task_id,
            PipelinedShardDownloaderHandle {
                downloader_idx: failed_downloader_idx,
                cnc_tx,
            },
        );

        let mut scheduled_shard_downloads = VecDeque::new();
        scheduled_shard_downloads.push_back(ScheduleShardDownload {
            blockchain_id: vec![1; 16],
            shard_idx: 3,
            block_uid: vec![2; 16],
            attempt: 1,
            slot: 55,
        });

        let (recycled_tx, recycled_rx) = mpsc::channel(2);
        recycled_tx
            .send(QueuedShardDownload {
                slot: 56,
                request: DownloadBlockShard {
                    blockchain_id: vec![3; 16],
                    block_uid: vec![4; 16],
                    shard_idx: 5,
                    block_filters: None,
                    slot: Some(56),
                },
                attempt: 2,
            })
            .await
            .expect("recycle queue send should succeed");
        drop(recycled_tx);

        let err = ShardDownloaderError {
            slot_scheduled_for_download: scheduled_shard_downloads,
            shard_download_queue_rx: recycled_rx,
            recyclable_dedup_state: Some(DedupState::default()),
            kind: ShardDownloaderErrorKind::SubscribeDataTxDisconnected,
        };

        let result = orchestrator
            .handle_shard_downloader_result(task_id, Err(err))
            .await;
        assert!(result.is_err());

        // Both recycled tasks (shard 3 and 5 => 1 mod 2) should go to downloader 1.
        let queue_1 = orchestrator.shard_download_queue_rxs[1]
            .as_mut()
            .expect("downloader 1 queue receiver should exist after reset");
        let first = queue_1
            .try_recv()
            .expect("expected first recycled task on downloader 1");
        let second = queue_1
            .try_recv()
            .expect("expected second recycled task on downloader 1");
        assert!(matches!(first.request.shard_idx, 3 | 5));
        assert!(matches!(second.request.shard_idx, 3 | 5));
        assert_ne!(first.request.shard_idx, second.request.shard_idx);

        let queue_0 = orchestrator.shard_download_queue_rxs[0]
            .as_mut()
            .expect("downloader 0 queue receiver should exist");
        assert!(queue_0.try_recv().is_err());
    }

    #[tokio::test]
    async fn pipelined_downloader_forwards_updates_and_reports_completed_shard() {
        let (outlet_tx, mut outlet_rx) =
            futures_mpsc::unbounded::<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>();
        let (completed_tx, mut completed_rx) = mpsc::channel(2);

        let footer = proto::BlockShardDownloadFinish {
            block_uid: vec![7u8; 16],
            slot: 42,
            shard_indices: vec![3],
        };

        let stream = tokio_stream::iter(vec![
            Ok(mk_update(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                ..SubscribeUpdateSlot::default()
            }))),
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
            DedupState::default(),
            empty_scheduled_shard_rx(),
        )
        .await;

        assert!(matches!(result, Ok((None, _))));

        let forwarded = outlet_rx.next().await.expect("expected forwarded update");
        let Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 42,
            update: forwarded,
        })) = forwarded
        else {
            panic!("expected data event")
        };
        assert!(matches!(forwarded.update_oneof, Some(UpdateOneof::Slot(_))));
        assert!(matches!(outlet_rx.try_next(), Ok(None)));

        let completed = completed_rx
            .recv()
            .await
            .expect("expected completion footer");
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
            futures_mpsc::unbounded::<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>();
        drop(outlet_rx);
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Ok(mk_update(UpdateOneof::Slot(
            SubscribeUpdateSlot::default(),
        )))]);

        let result = PipelinedShardDownloader::<
            futures_mpsc::UnboundedSender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
        >::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
            DedupState::default(),
            empty_scheduled_shard_rx(),
        )
        .await;

        assert!(matches!(
            result,
            Err((ShardDownloaderErrorKind::SinkClosed, _))
        ));
    }

    #[tokio::test]
    async fn dedup_state_drops_duplicate_updates() {
        let mut dedup_state = DedupState::default();
        let update = UpdateOneof::Entry(yellowstone_grpc_proto::geyser::SubscribeUpdateEntry {
            slot: 42,
            index: 7,
            num_hashes: 0,
            hash: Vec::new(),
            executed_transaction_count: 0,
            starting_transaction_index: 0,
        });

        assert!(!dedup_state.dedup(42, 0, &update));
        assert!(dedup_state.dedup(42, 0, &update));

        dedup_state.mark_shard_done(42, 0);
        assert!(dedup_state.is_shard_done(42, 0));

        // Once shard is marked done, associated keys are released.
        assert!(!dedup_state.dedup(42, 1, &update));
    }

    #[tokio::test]
    async fn dedup_state_skips_non_keyed_updates_by_default() {
        let mut dedup_state = DedupState::default();
        let update = UpdateOneof::Slot(SubscribeUpdateSlot {
            slot: 42,
            parent: Some(41),
            status: 1,
            dead_error: Some(String::new()),
        });

        assert!(dedup_state.dedup(42, 0, &update));
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_recoverable_transport_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::unavailable("connection dropped"),
        ))]);

        let result = PipelinedShardDownloader::<
            futures_mpsc::UnboundedSender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
        >::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
            DedupState::default(),
            empty_scheduled_shard_rx(),
        )
        .await;

        match result {
            Err((ShardDownloaderErrorKind::DataplaneError(err), _)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::RecoverableTransport);
                assert!(err.is_recoverable());
            }
            _ => panic!("expected dataplane recoverable transport error"),
        }
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_slot_not_found_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::not_found("slot not found"),
        ))]);

        let result = PipelinedShardDownloader::<
            futures_mpsc::UnboundedSender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
        >::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
            DedupState::default(),
            empty_scheduled_shard_rx(),
        )
        .await;

        match result {
            Err((ShardDownloaderErrorKind::DataplaneError(err), _)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::SlotNotFound);
                assert!(!err.is_recoverable());
            }
            _ => panic!("expected dataplane slot-not-found error"),
        }
    }

    #[tokio::test]
    async fn pipelined_downloader_propagates_invalid_subscribe_filter_error() {
        let (outlet_tx, _outlet_rx) =
            futures_mpsc::unbounded::<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>();
        let (completed_tx, _completed_rx) = mpsc::channel(1);

        let stream = tokio_stream::iter(vec![Err(DataplaneStreamError::from(
            tonic::Status::invalid_argument("invalid filter expression"),
        ))]);

        let result = PipelinedShardDownloader::<
            futures_mpsc::UnboundedSender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
        >::pipelined_downloader(
            stream,
            outlet_tx,
            completed_tx,
            DedupState::default(),
            empty_scheduled_shard_rx(),
        )
        .await;

        match result {
            Err((ShardDownloaderErrorKind::DataplaneError(err), _)) => {
                assert_eq!(err.kind(), DataplaneErrorKind::InvalidSubscribeFilter);
                assert!(!err.is_recoverable());
            }
            _ => panic!("expected dataplane invalid-filter error"),
        }
    }
}
