use {
    super::{FumaroleSM, FumeDownloadRequest, FumeOffset, FumeSlotStatus},
    crate::{
        proto::{
            self, data_command, BlockFilters, CommitOffset, ControlCommand, DataCommand,
            DownloadBlockShard, PollBlockchainHistory,
        },
        util::collections::KeyedVecDeque,
    },
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
        f32::consts::E,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc,
        task::{self, JoinError, JoinSet},
    },
    yellowstone_grpc_proto::geyser::{
        self, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
    },
};

///
/// Data-Plane bidirectional stream
struct DataPlaneBidi {
    tx: mpsc::Sender<proto::DataCommand>,
    rx: mpsc::Receiver<proto::DataResponse>,
}

///
/// Holds information about on-going data plane task.
///
#[derive(Clone, Debug)]
struct DataPlaneTaskMeta {
    download_request: FumeDownloadRequest,
    scheduled_at: Instant,
    download_attempt: u8,
}

///
/// Base trait for Data-plane bidirectional stream factories.
///
#[async_trait::async_trait]
pub trait DataPlaneBidiFactory {
    ///
    /// Builds a [`DataPlaneBidi`]
    ///
    async fn build(&self) -> DataPlaneBidi;
}

///
/// Fumarole runtime based on Tokio outputting Dragonsmouth only events.
///
pub(crate) struct TokioFumeDragonsmouthRuntime {
    rt: tokio::runtime::Handle,
    sm: FumaroleSM,
    block_filters: BlockFilters,
    data_plane_bidi_factory: Arc<dyn DataPlaneBidiFactory + Send + 'static>,
    subscribe_request: SubscribeRequest,
    consumer_group_name: String,
    control_plane_tx: mpsc::Sender<proto::ControlCommand>,
    control_plane_rx: mpsc::Receiver<proto::ControlResponse>,
    data_plane_bidi_vec: VecDeque<DataPlaneBidi>,
    data_plane_tasks: JoinSet<Result<DownloadBlockCompleted, DownloadBlockError>>,
    data_plane_task_meta: HashMap<tokio::task::Id, DataPlaneTaskMeta>,
    dragonsmouth_outlet: mpsc::Sender<geyser::SubscribeUpdate>,
    download_to_retry: VecDeque<FumeDownloadRequest>,
    download_attempts: HashMap<Slot, u8>,
    max_slot_download_attempt: u8,
    commit_interval: Duration,
    last_commit: Instant,
}

fn build_poll_history_cmd(from: Option<FumeOffset>) -> ControlCommand {
    ControlCommand {
        command: Some(proto::control_command::Command::PollHist(
            // from None means poll the entire history from wherever we left off since last commit.
            PollBlockchainHistory { from },
        )),
    }
}

fn build_commit_offset_cmd(offset: FumeOffset) -> ControlCommand {
    ControlCommand {
        command: Some(proto::control_command::Command::CommitOffset(
            CommitOffset { offset },
        )),
    }
}

struct DownloadBlockTask {
    download_request: FumeDownloadRequest,
    block_filter: BlockFilters,
    bidi: DataPlaneBidi,
    dragonsmouth_oulet: mpsc::Sender<SubscribeUpdate>,
}

struct DownloadBlockCompleted {
    bidi: DataPlaneBidi,
}

enum DownloadBlockError {
    Disconnected,
    OutletDisconnected,
    BlockShardNotFound,
}

impl DownloadBlockTask {
    async fn run(mut self) -> Result<DownloadBlockCompleted, DownloadBlockError> {
        let DataPlaneBidi { tx, mut rx } = self.bidi;

        // Make sure the stream is empty
        loop {
            match rx.try_recv() {
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(DownloadBlockError::Disconnected)
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Ok(_) => {}
            }
        }
        let data_cmd = data_command::Command::DownloadBlockShard(DownloadBlockShard {
            blockchain_id: self.download_request.blockchain_id.to_vec(),
            block_uid: self.download_request.block_uid.to_vec(),
            shard_idx: 0, // ONLY SUPPORTS 1 shard in V1.
        });
        let data_cmd = DataCommand {
            command: Some(data_cmd),
        };
        tx.send(data_cmd)
            .await
            .map_err(|_| DownloadBlockError::Disconnected)?;

        loop {
            let Some(data) = rx.recv().await else {
                return Err(DownloadBlockError::Disconnected);
            };

            let Some(resp) = data.response else { continue };

            match resp {
                proto::data_response::Response::Update(subscribe_update) => {
                    if self
                        .dragonsmouth_oulet
                        .send(subscribe_update)
                        .await
                        .is_err()
                    {
                        return Err(DownloadBlockError::OutletDisconnected);
                    }
                }
                proto::data_response::Response::BlockShardDownloadFinish(
                    _block_shard_download_finish,
                ) => {
                    break;
                }
                proto::data_response::Response::Error(data_error) => {
                    let Some(e) = data_error.error else { continue };
                    match e {
                        proto::data_error::Error::NotFound(block_not_found) => {
                            if block_not_found.block_uid.as_slice()
                                == self.download_request.block_uid.as_slice()
                            {
                                return Err(DownloadBlockError::BlockShardNotFound);
                            } else {
                                panic!("unexpected block uid")
                            }
                        }
                    }
                }
            }
        }

        let bidi = DataPlaneBidi { tx, rx };
        Ok(DownloadBlockCompleted { bidi })
    }
}

impl TokioFumeDragonsmouthRuntime {
    fn handle_control_response(&mut self, control_response: proto::ControlResponse) {
        let Some(response) = control_response.response else {
            return;
        };
        match response {
            proto::control_response::Response::CommitOffset(commit_offset_result) => {
                tracing::trace!("received commit offset : {commit_offset_result:?}");
                self.sm.update_committed_offset(commit_offset_result.offset);
            }
            proto::control_response::Response::PollNext(blockchain_history) => {
                tracing::trace!(
                    "polled blockchain history : {} events",
                    blockchain_history.events.len()
                );
                self.sm.queue_blockchain_event(blockchain_history.events);
            }
            proto::control_response::Response::Pong(_pong) => {
                tracing::trace!("pong");
            }
        }
    }

    async fn poll_history_if_needed(&mut self) {
        let cmd = build_poll_history_cmd(Some(self.sm.committable_offset));
        if self.sm.need_new_blockchain_events() {
            self.control_plane_tx.send(cmd).await.expect("disconnected");
        }
    }

    fn schedule_download_task_if_any(&mut self) {
        // This loop drains as many download slot request as possible,
        // limited to available [`DataPlaneBidi`].
        loop {
            if self.data_plane_bidi_vec.is_empty() {
                break;
            }

            let maybe_download_request = self
                .download_to_retry
                .pop_front()
                .or_else(|| self.sm.pop_slot_to_download());

            let Some(download_request) = maybe_download_request else {
                break;
            };

            assert!(download_request.num_shards == 1, "this client is incompatible with remote server since it does not support sharded block download");

            let data_plane_bidi = self
                .data_plane_bidi_vec
                .pop_back()
                .expect("should not be none");

            let download_task = DownloadBlockTask {
                download_request: download_request.clone(),
                block_filter: self.block_filters.clone(),
                bidi: data_plane_bidi,
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
            self.data_plane_task_meta.insert(
                ah.id(),
                DataPlaneTaskMeta {
                    download_request,
                    scheduled_at: Instant::now(),
                    download_attempt: *download_attempts,
                },
            );
        }
    }

    async fn handle_data_plane_task_result(
        &mut self,
        task_id: task::Id,
        result: Result<DownloadBlockCompleted, DownloadBlockError>,
    ) {
        let Some(task_meta) = self.data_plane_task_meta.remove(&task_id) else {
            panic!("missing task meta")
        };
        let slot = task_meta.download_request.slot;
        tracing::trace!("download task result received for slot {}", slot);
        match result {
            Ok(completed) => {
                let elapsed = task_meta.scheduled_at.elapsed();
                tracing::debug!("downloaded slot {slot} in {elapsed:?}");
                let _ = self.download_attempts.remove(&slot);
                self.data_plane_bidi_vec.push_back(completed.bidi);
                // TODO: Add support for sharded progress
                self.sm.make_slot_download_progress(slot, 0);
            }
            Err(e) => {
                match e {
                    DownloadBlockError::Disconnected => {
                        // We need to retry it
                        if task_meta.download_attempt >= self.max_slot_download_attempt {
                            panic!("Failed to download slot {slot}")
                        }

                        let data_plane_bidi = self.data_plane_bidi_factory.build().await;
                        self.data_plane_bidi_vec.push_back(data_plane_bidi);

                        tracing::debug!("Download slot {slot} failed, rescheduling for retry...");
                        self.download_to_retry.push_back(task_meta.download_request);
                    }
                    DownloadBlockError::OutletDisconnected => {
                        // Will automatically be handled in the `run` main loop.
                        // so nothing to do.
                    }
                    DownloadBlockError::BlockShardNotFound => {
                        // TODO: I don't think it should ever happen, but lets panic first so we get notified by client if it ever happens.
                        panic!("Slot {slot} not found");
                    }
                }
            }
        }
    }

    async fn commit_offset(&mut self) {
        if self.sm.last_committed_offset < self.sm.committable_offset {
            self.control_plane_tx
                .send(build_commit_offset_cmd(self.sm.committable_offset))
                .await
                .expect("failed to commit offset");
        }

        self.last_commit = Instant::now();
    }

    async fn drain_slot_status(&mut self) {
        let commitment = self.subscribe_request.commitment();
        let mut slot_status_vec = VecDeque::with_capacity(10);

        while let Some(slot_status) = self.sm.pop_next_slot_status() {
            slot_status_vec.push_back(slot_status);
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
                            // TODO: support dead slot
                            dead_error: None,
                        },
                    )),
                };

                if self.dragonsmouth_outlet.send(update).await.is_err() {
                    return;
                }
                self.sm.mark_offset_as_processed(slot_status.offset);
            }
        }
    }

    async fn run(mut self) {
        let inital_load_history_cmd = build_poll_history_cmd(None);
        self.control_plane_tx
            .send(inital_load_history_cmd)
            .await
            .expect("disconnected");

        loop {
            if self.dragonsmouth_outlet.is_closed() {
                tracing::trace!("Detected dragonsmouth outlet closed");
                break;
            }

            let commit_deadline = self.last_commit + self.commit_interval;

            self.poll_history_if_needed().await;
            self.schedule_download_task_if_any();
            tokio::select! {
                control_response = self.control_plane_rx.recv() => {
                    if let Some(control_response) = control_response {
                        self.handle_control_response(control_response);
                    } else {
                        break;
                    }
                }
                Some(result) = self.data_plane_tasks.join_next_with_id() => {
                    let (task_id, download_result) = result.expect("data plane task set");

                    self.handle_data_plane_task_result(task_id, download_result);
                }

                _ = tokio::time::sleep_until(commit_deadline.into()) => {
                    self.commit_offset().await;
                }
            }
            self.drain_slot_status().await;
        }
    }
}

pub struct TokioFumarolHandle {}
