#[cfg(feature = "prometheus")]
use crate::metrics::{
    dec_inflight_slot_download, inc_failed_slot_download_attempt, inc_inflight_slot_download,
    inc_offset_commitment_count, inc_slot_download_count, observe_slot_download_duration,
    set_max_slot_detected, set_slot_download_queue_size,
};
use {
    super::{FumaroleSM, FumeDownloadRequest, FumeOffset},
    crate::{
        metrics::inc_total_event_downloaded,
        proto::{
            self, data_response, BlockFilters, CommitOffset, ControlCommand, DownloadBlockShard,
            PollBlockchainHistory,
        },
        FumaroleGrpcConnector, GrpcFumaroleClient,
    },
    futures::StreamExt,
    solana_sdk::clock::Slot,
    std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{mpsc, Semaphore},
        task::{self, JoinSet},
    },
    tonic::Code,
    yellowstone_grpc_proto::geyser::{
        self, CommitmentLevel, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
    },
};

///
/// Holds information about on-going data plane task.
///
#[derive(Clone, Debug)]
pub(crate) struct DataPlaneTaskMeta {
    download_request: FumeDownloadRequest,
    scheduled_at: Instant,
    download_attempt: u8,
    client_rev: u64,
    client_idx: usize,
}

///
/// Mimics Dragonsmouth subscribe request bidirectional stream.
///
pub struct DragonsmouthSubscribeRequestBidi {
    #[allow(dead_code)]
    pub tx: mpsc::Sender<SubscribeRequest>,
    pub rx: mpsc::Receiver<SubscribeRequest>,
}

pub(crate) struct DataPlaneConn {
    sem: Arc<Semaphore>,
    client: GrpcFumaroleClient,
    rev: u64,
}

struct ProtectedGrpcFumaroleClient {
    client: GrpcFumaroleClient,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl DataPlaneConn {
    pub fn new(client: GrpcFumaroleClient, concurrency_limit: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(concurrency_limit)),
            client,
            rev: 0,
        }
    }

    fn has_permit(&self) -> bool {
        self.sem.available_permits() > 0
    }

    fn acquire(&mut self) -> ProtectedGrpcFumaroleClient {
        let permit = Arc::clone(&self.sem)
            .try_acquire_owned()
            .expect("failed to acquire semaphore permit");
        ProtectedGrpcFumaroleClient {
            client: self.client.clone(),
            _permit: permit,
        }
    }
}

///
/// Fumarole runtime based on Tokio outputting Dragonsmouth only events.
///
pub(crate) struct TokioFumeDragonsmouthRuntime {
    pub rt: tokio::runtime::Handle,
    pub sm: FumaroleSM,
    pub dragonsmouth_bidi: DragonsmouthSubscribeRequestBidi,
    pub subscribe_request: SubscribeRequest,
    pub fumarole_connector: FumaroleGrpcConnector,
    #[allow(dead_code)]
    pub consumer_group_name: String,
    pub control_plane_tx: mpsc::Sender<proto::ControlCommand>,
    pub control_plane_rx: mpsc::Receiver<Result<proto::ControlResponse, tonic::Status>>,
    pub data_plane_channel_vec: Vec<DataPlaneConn>,
    pub data_plane_tasks: JoinSet<Result<CompletedDownloadBlockTask, DownloadBlockError>>,
    pub data_plane_task_meta: HashMap<tokio::task::Id, DataPlaneTaskMeta>,
    pub dragonsmouth_outlet: mpsc::Sender<Result<geyser::SubscribeUpdate, tonic::Status>>,
    pub download_to_retry: VecDeque<FumeDownloadRequest>,
    pub download_attempts: HashMap<Slot, u8>,
    pub max_slot_download_attempt: u8,
    pub commit_interval: Duration,
    pub last_commit: Instant,
}

const fn build_poll_history_cmd(from: Option<FumeOffset>) -> ControlCommand {
    ControlCommand {
        command: Some(proto::control_command::Command::PollHist(
            // from None means poll the entire history from wherever we left off since last commit.
            PollBlockchainHistory {
                shard_id: 0, /*ALWAYS 0-FOR FIRST VERSION OF FUMAROLE */
                from,
                limit: None,
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

pub(crate) struct DownloadBlockTask {
    download_request: FumeDownloadRequest,
    protected: ProtectedGrpcFumaroleClient,
    filters: Option<BlockFilters>,
    dragonsmouth_oulet: mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DownloadBlockError {
    #[error("download block task disconnected")]
    Disconnected,
    #[error("dragonsmouth outlet disconnected")]
    OutletDisconnected,
    #[error("block shard not found")]
    BlockShardNotFound,
    #[error("error during transportation or processing")]
    FailedDownload,
    #[error("unknown error: {0}")]
    Fatal(#[from] tonic::Status),
}

fn map_tonic_error_code_to_download_block_error(code: Code) -> DownloadBlockError {
    match code {
        Code::NotFound => DownloadBlockError::BlockShardNotFound,
        Code::Unavailable => DownloadBlockError::Disconnected,
        Code::Internal
        | Code::Aborted
        | Code::DataLoss
        | Code::ResourceExhausted
        | Code::Unknown
        | Code::Cancelled => DownloadBlockError::FailedDownload,
        Code::Ok => {
            unreachable!("ok")
        }
        Code::InvalidArgument => {
            panic!("invalid argument");
        }
        Code::DeadlineExceeded => DownloadBlockError::FailedDownload,
        rest => DownloadBlockError::Fatal(tonic::Status::new(rest, "unknown error")),
    }
}

pub(crate) struct CompletedDownloadBlockTask {
    total_event_downloaded: usize,
}

impl DownloadBlockTask {
    async fn run(mut self) -> Result<CompletedDownloadBlockTask, DownloadBlockError> {
        let request = DownloadBlockShard {
            blockchain_id: self.download_request.blockchain_id.to_vec(),
            block_uid: self.download_request.block_uid.to_vec(),
            shard_idx: 0,
            block_filters: self.filters,
        };
        let resp = self.protected.client.download_block(request).await;

        let mut rx = match resp {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                return Err(map_tonic_error_code_to_download_block_error(e.code()));
            }
        };
        let mut total_event_downloaded = 0;
        while let Some(data) = rx.next().await {
            let resp = data
                .map_err(|e| {
                    let code = e.code();
                    tracing::error!("download block error: {code:?}");
                    map_tonic_error_code_to_download_block_error(code)
                })?
                .response
                .expect("missing response");

            match resp {
                data_response::Response::Update(update) => {
                    total_event_downloaded += 1;
                    if self.dragonsmouth_oulet.send(Ok(update)).await.is_err() {
                        return Err(DownloadBlockError::OutletDisconnected);
                    }
                }
                data_response::Response::BlockShardDownloadFinish(_) => {
                    return Ok(CompletedDownloadBlockTask {
                        total_event_downloaded,
                    });
                }
            }
        }
        Err(DownloadBlockError::FailedDownload)
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

impl TokioFumeDragonsmouthRuntime {
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
                tracing::debug!(
                    "polled blockchain history : {} events",
                    blockchain_history.events.len()
                );
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
        if self.sm.need_new_blockchain_events() {
            let cmd = build_poll_history_cmd(Some(self.sm.committable_offset));
            tracing::debug!("polling history...");
            self.control_plane_tx.send(cmd).await.expect("disconnected");
        }
    }

    fn find_most_under_utilized_data_plane_client(&self) -> Option<usize> {
        self.data_plane_channel_vec
            .iter()
            .enumerate()
            .filter(|(_, conn)| conn.has_permit())
            .max_by_key(|(_, conn)| conn.sem.available_permits())
            .map(|(idx, _)| idx)
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
            let Some(client_idx) = self.find_most_under_utilized_data_plane_client() else {
                break;
            };

            let maybe_download_request = self
                .download_to_retry
                .pop_front()
                .or_else(|| self.sm.pop_slot_to_download(self.commitment_level()));

            let Some(download_request) = maybe_download_request else {
                break;
            };

            assert!(download_request.num_shards == 1, "this client is incompatible with remote server since it does not support sharded block download");
            let client = self
                .data_plane_channel_vec
                .get_mut(client_idx)
                .expect("should not be none");
            let permit = client.acquire();
            let client_rev = client.rev;
            let download_task = DownloadBlockTask {
                download_request: download_request.clone(),
                protected: permit,
                filters: Some(self.subscribe_request.clone().into()),
                dragonsmouth_oulet: self.dragonsmouth_outlet.clone(),
            };

            let download_attempts = self
                .download_attempts
                .entry(download_request.slot)
                .or_default();

            *download_attempts += 1;

            let ah = self
                .data_plane_tasks
                .spawn_on(download_task.run(), &self.rt);
            tracing::debug!("download task scheduled for slot {}", download_request.slot);
            self.data_plane_task_meta.insert(
                ah.id(),
                DataPlaneTaskMeta {
                    download_request,
                    scheduled_at: Instant::now(),
                    download_attempt: *download_attempts,
                    client_rev,
                    client_idx,
                },
            );

            #[cfg(feature = "prometheus")]
            {
                inc_inflight_slot_download(Self::RUNTIME_NAME);
            }
        }
    }

    async fn handle_data_plane_task_result(
        &mut self,
        task_id: task::Id,
        result: Result<CompletedDownloadBlockTask, DownloadBlockError>,
    ) -> Result<(), DownloadBlockError> {
        let Some(task_meta) = self.data_plane_task_meta.remove(&task_id) else {
            panic!("missing task meta")
        };

        #[cfg(feature = "prometheus")]
        {
            dec_inflight_slot_download(Self::RUNTIME_NAME);
        }

        let slot = task_meta.download_request.slot;
        tracing::debug!("download task result received for slot {}", slot);
        match result {
            Ok(completed) => {
                let CompletedDownloadBlockTask {
                    total_event_downloaded,
                } = completed;
                let elapsed = task_meta.scheduled_at.elapsed();

                #[cfg(feature = "prometheus")]
                {
                    observe_slot_download_duration(Self::RUNTIME_NAME, elapsed);
                    inc_slot_download_count(Self::RUNTIME_NAME);
                    inc_total_event_downloaded(Self::RUNTIME_NAME, total_event_downloaded);
                }

                tracing::debug!(
                    "downloaded slot {slot} in {elapsed:?}, total events: {total_event_downloaded}"
                );
                let _ = self.download_attempts.remove(&slot);
                // TODO: Add support for sharded progress
                self.sm.make_slot_download_progress(slot, 0);
            }
            Err(e) => {
                #[cfg(feature = "prometheus")]
                {
                    inc_failed_slot_download_attempt(Self::RUNTIME_NAME);
                }

                match e {
                    x @ (DownloadBlockError::Disconnected | DownloadBlockError::FailedDownload) => {
                        // We need to retry it
                        if task_meta.download_attempt >= self.max_slot_download_attempt {
                            tracing::error!(
                                "download slot {slot} failed: {x:?}, max attempts reached"
                            );
                            return Err(x);
                        }
                        let remaining_attempt = self
                            .max_slot_download_attempt
                            .saturating_sub(task_meta.download_attempt);
                        tracing::debug!(
                            "download slot {slot} failed: {x:?}, remaining attempts: {remaining_attempt}"
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
                                .fumarole_connector
                                .connect()
                                .await
                                .expect("failed to reconnect data plane client");
                            conn.client = new_client;
                        }

                        tracing::debug!("Download slot {slot} failed, rescheduling for retry...");
                        self.download_to_retry.push_back(task_meta.download_request);
                    }
                    DownloadBlockError::OutletDisconnected => {
                        // Will automatically be handled in the `run` main loop.
                        // so nothing to do.
                        tracing::debug!("dragonsmouth outlet disconnected");
                    }
                    DownloadBlockError::BlockShardNotFound => {
                        // TODO: I don't think it should ever happen, but lets panic first so we get notified by client if it ever happens.
                        tracing::error!("Slot {slot} not found");
                        panic!("slot {slot} not found");
                    }
                    DownloadBlockError::Fatal(e) => {
                        panic!("fatal error: {e}");
                    }
                }
            }
        }
        Ok(())
    }

    async fn commit_offset(&mut self) {
        if self.sm.last_committed_offset < self.sm.committable_offset {
            tracing::debug!("committing offset {}", self.sm.committable_offset);
            self.control_plane_tx
                .send(build_commit_offset_cmd(self.sm.committable_offset))
                .await
                .expect("failed to commit offset");
            #[cfg(feature = "prometheus")]
            {
                inc_offset_commitment_count(Self::RUNTIME_NAME);
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

        tracing::debug!("draining slot status: {} events", slot_status_vec.len());

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
                            // TODO: support dead slot
                            dead_error: slot_status.dead_error,
                        },
                    )),
                };
                if self.dragonsmouth_outlet.send(Ok(update)).await.is_err() {
                    return;
                }
            }

            self.sm.mark_offset_as_processed(slot_status.offset);
        }
    }

    async fn unsafe_cancel_all_tasks(&mut self) {
        tracing::debug!("aborting all data plane tasks");
        self.data_plane_tasks.abort_all();
        self.data_plane_task_meta.clear();
        self.download_attempts.clear();

        while (self.data_plane_tasks.join_next().await).is_some() {
            // Drain all tasks
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let inital_load_history_cmd = build_poll_history_cmd(None);
        self.control_plane_tx
            .send(inital_load_history_cmd)
            .await
            .expect("disconnected");

        loop {
            if self.dragonsmouth_outlet.is_closed() {
                tracing::debug!("Detected dragonsmouth outlet closed");
                break;
            }

            let commit_deadline = self.last_commit + self.commit_interval;

            self.poll_history_if_needed().await;
            self.schedule_download_task_if_any();
            tokio::select! {
                Some(subscribe_request) = self.dragonsmouth_bidi.rx.recv() => {
                    tracing::debug!("dragonsmouth subscribe request received");
                    self.subscribe_request = subscribe_request
                }
                control_response = self.control_plane_rx.recv() => {
                    match control_response {
                        Some(Ok(control_response)) => {
                            self.handle_control_response(control_response);
                        }
                        Some(Err(e)) => {
                            tracing::error!("control plane error: {e}");
                            return Err(Box::new(RuntimeError::GrpcError(e)));
                        }
                        None => {
                            tracing::debug!("control plane disconnected");
                            break;
                        }
                    }
                }
                Some(result) = self.data_plane_tasks.join_next_with_id() => {
                    let (task_id, download_result) = result.expect("data plane task set");
                    let result = self.handle_data_plane_task_result(task_id, download_result).await;
                    if let Err(e) = result {
                        self.unsafe_cancel_all_tasks().await;
                        if let DownloadBlockError::Fatal(e) = e {
                            let _ = self.dragonsmouth_outlet.send(Err(e)).await;
                        }
                        break;
                    }
                }

                _ = tokio::time::sleep_until(commit_deadline.into()) => {
                    tracing::debug!("commit deadline reached");
                    self.commit_offset().await;
                }
            }
            self.drain_slot_status().await;
        }
        tracing::debug!("fumarole runtime exiting");
        Ok(())
    }
}
