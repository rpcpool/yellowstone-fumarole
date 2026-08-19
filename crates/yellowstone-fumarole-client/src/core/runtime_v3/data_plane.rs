//!
//! Data-plane side of the V3 push-based runtime: one [`run_lane`] task per lane.
//!
//! Each lane owns exactly one `SubscribeDataV3` stream for the lifetime of the subscription. It
//! joins, opens the start gate, and then just consumes whatever the server pushes -- there is no
//! outbound "give me this shard" request on this path, unlike the V1/V2 pull runtime in
//! `core::runtime`.
//!
use {
    super::KnownSlots,
    crate::{
        core::{
            ports::FumaroleDataplaneConnectorV3,
            runtime::{
                DataplaneStreamError, DedupState, FumaroleRuntimeDataEvent, FumaroleRuntimeEvent,
            },
            state_machine::{FumeBlockUID, FumeOffset, FumeShardIdx},
        },
        error::FumaroleSubscribeError,
        proto::{
            BlockFilters, DataCommandV3, JoinDataPlane, ReportHistoryProgress, StartDataPlane,
            data_command_v3::Command as DataCommandV3Kind,
            data_response_v3::Response as DataResponseV3Kind,
        },
    },
    futures::{Sink, SinkExt, StreamExt},
    solana_clock::Slot,
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
    yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof,
    },
};

/// Commands the driver can push down into a running lane.
pub(crate) enum LaneCommand {
    UpdateFilters(Arc<SubscribeRequest>),
    ReportHistoryProgress(i64),
}

/// Reported to the driver once a shard's `shard_start...shard_finish` span is fully consumed.
pub(crate) struct CompletedShard {
    pub slot: Slot,
    pub shard_idx: FumeShardIdx,
    #[allow(dead_code)]
    pub block_uid: FumeBlockUID,
    pub block_meta: Option<SubscribeUpdate>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LaneFatalError {
    #[error("dragonsmouth outlet disconnected")]
    OutletDisconnected,
    #[error("data plane command sink disconnected")]
    SinkDisconnected,
    #[error(transparent)]
    Dataplane(#[from] DataplaneStreamError),
}

const LANE_RECONNECT_MAX_ATTEMPTS: usize = 3;
const LANE_RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

pub(crate) struct PushShardLaneArgs<DP> {
    pub connector: DP,
    pub lane_idx: u32,
    pub num_lanes: u32,
    pub subscribe_request: Arc<SubscribeRequest>,
    pub cnc_rx: mpsc::Receiver<LaneCommand>,
    pub completed_tx: mpsc::Sender<CompletedShard>,
    pub dragonsmouth_outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    pub known_slots: Arc<KnownSlots>,
    /// Starting point for this lane's very first `JoinDataPlane`, in the control plane's offset
    /// space (i.e. the same value used for `JoinControlPlaneV3.initial_offset`). The server
    /// never infers or defaults this -- see the proto doc comment on `JoinDataPlane.starting_offset`.
    /// On reconnect, the lane instead resumes from the most recent value it's learned via
    /// `LaneCommand::ReportHistoryProgress` (the driver's own control-plane offset watermark) --
    /// *not* from anything the data plane itself has sent back. `BlockShardDownloadStart/Finish`'s
    /// `log_offset` is purely connection-local bookkeeping server-side and plays no role in
    /// resumption, so this lane never reads it for that purpose.
    pub initial_offset: FumeOffset,
}

///
/// Drives one data-plane lane for the lifetime of the subscription: connect, join, open the
/// start gate, consume, and on any recoverable transport error, reconnect and resume from the
/// last offset this lane observed. Only returns an error after exhausting reconnect attempts --
/// the caller should treat that as fatal for the whole subscription, mirroring how the V1
/// runtime treats a dead download-task-runner.
///
pub(crate) async fn run_lane<DP>(mut args: PushShardLaneArgs<DP>) -> Result<(), LaneFatalError>
where
    DP: FumaroleDataplaneConnectorV3 + Clone,
    DataplaneStreamError: From<DP::DataplaneSubscribeError>,
{
    let mut current_offset: FumeOffset = args.initial_offset;
    let mut dedup_state = DedupState::default();
    let mut attempt = 0usize;

    loop {
        let join = JoinDataPlane {
            lane_idx: args.lane_idx,
            num_lanes: args.num_lanes,
            block_filters: Some(BlockFilters::from((*args.subscribe_request).clone())),
            starting_offset: Some(current_offset),
        };
        match args.connector.subscribe_data_v3(join).await {
            Ok((sink, stream)) => {
                attempt = 0;
                match drive_one_connection(
                    &mut args,
                    sink,
                    stream,
                    &mut current_offset,
                    &mut dedup_state,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(LaneFatalError::Dataplane(e)) if e.is_recoverable() => {
                        tracing::warn!(
                            lane_idx = args.lane_idx,
                            "data plane lane disconnected, reconnecting from offset {current_offset}: {e:?}"
                        );
                        tokio::time::sleep(LANE_RECONNECT_BACKOFF).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => {
                attempt += 1;
                let err = DataplaneStreamError::from(e);
                if attempt >= LANE_RECONNECT_MAX_ATTEMPTS || !err.is_recoverable() {
                    return Err(LaneFatalError::Dataplane(err));
                }
                tracing::warn!(
                    lane_idx = args.lane_idx,
                    "failed to open data plane lane, attempt {attempt}/{LANE_RECONNECT_MAX_ATTEMPTS}: {err:?}"
                );
                tokio::time::sleep(LANE_RECONNECT_BACKOFF).await;
            }
        }
    }
}

async fn drive_one_connection<DP>(
    args: &mut PushShardLaneArgs<DP>,
    mut sink: DP::DataplaneSink,
    mut stream: DP::DataplaneStream,
    // The driver's own control-plane offset watermark, as last reported via
    // `LaneCommand::ReportHistoryProgress` -- this, not anything the data plane sends back, is
    // what a reconnect resumes from. See the doc comment on `PushShardLaneArgs::initial_offset`.
    current_offset: &mut FumeOffset,
    dedup_state: &mut DedupState,
) -> Result<(), LaneFatalError>
where
    DP: FumaroleDataplaneConnectorV3,
    DataplaneStreamError: From<DP::DataplaneSubscribeError>,
{
    // `Join` was already sent by `FumaroleDataplaneConnectorV3::subscribe_data_v3` itself,
    // before the connection call resolved (see that trait's doc comment for why it has to be
    // sent that early). `Start` has no such constraint, so it's sent here as usual.
    send_command(&mut sink, DataCommandV3Kind::Start(StartDataPlane {})).await?;

    // Sequencing contract: at most one shard in flight per lane, so this is enough state to
    // track the currently-open `shard_start...shard_finish` span.
    let mut current_shard: Option<(Slot, FumeShardIdx)> = None;
    let mut block_meta: Option<SubscribeUpdate> = None;

    loop {
        tokio::select! {
            maybe_cmd = args.cnc_rx.recv() => {
                match maybe_cmd {
                    Some(LaneCommand::UpdateFilters(request)) => {
                        args.subscribe_request = Arc::clone(&request);
                        let cmd = DataCommandV3Kind::FilterUpdate((*request).clone().into());
                        send_command(&mut sink, cmd).await?;
                    }
                    Some(LaneCommand::ReportHistoryProgress(highest_known_offset)) => {
                        // This is also this lane's resume point on the next reconnect -- see
                        // the doc comment on `PushShardLaneArgs::initial_offset`.
                        *current_offset = highest_known_offset;
                        let cmd = DataCommandV3Kind::ReportHistoryProgress(ReportHistoryProgress {
                            highest_known_offset,
                        });
                        send_command(&mut sink, cmd).await?;
                    }
                    None => return Ok(()),
                }
            }
            maybe_resp = stream.next() => {
                let Some(resp) = maybe_resp else { return Ok(()); };
                let resp = resp?;
                let Some(response) = resp.response else { continue; };
                match response {
                    DataResponseV3Kind::ShardStart(start) => {
                        // Handling the shard-before-history race: `ReportHistoryProgress` is
                        // only a pacing hint (see FUMAROLE_V3_PLAN.md), so this slot can
                        // legitimately be unknown to the driver's state machine yet. Stop
                        // reading further from this stream (this is what actually produces
                        // backpressure, via HTTP2) until the driver has caught up.
                        if !args.known_slots.contains(start.slot) {
                            let wait_started = std::time::Instant::now();
                            let mut still_waiting_logged_at = Duration::ZERO;
                            while !args.known_slots.contains(start.slot) {
                                args.known_slots.wait_for_change().await;
                                // Log periodically *while still stuck*, not just after resolving --
                                // an indefinite stall never reaches the post-loop log below at all.
                                let waited = wait_started.elapsed();
                                if waited - still_waiting_logged_at >= Duration::from_secs(1) {
                                    still_waiting_logged_at = waited;
                                    tracing::warn!(
                                        lane_idx = args.lane_idx,
                                        slot = start.slot,
                                        ?waited,
                                        "lane STILL stalled on shard-before-history race, has not resolved yet"
                                    );
                                }
                            }
                            let waited = wait_started.elapsed();
                            if waited > Duration::from_millis(500) {
                                tracing::warn!(
                                    lane_idx = args.lane_idx,
                                    slot = start.slot,
                                    ?waited,
                                    "lane stalled on shard-before-history race longer than expected"
                                );
                            }
                        }
                        current_shard = Some((start.slot, start.shard_idx));
                        block_meta = None;
                    }
                    DataResponseV3Kind::Update(update) => {
                        let Some((slot, shard_idx)) = current_shard else {
                            // A well-behaved server never sends an update outside a
                            // shard_start/shard_finish span; skip defensively rather than panic.
                            continue;
                        };
                        let Some(update_oneof) = update.update_oneof.as_ref() else {
                            continue;
                        };
                        if matches!(update_oneof, UpdateOneof::BlockMeta(_)) {
                            block_meta = Some(update);
                        } else {
                            if dedup_state.dedup(slot, shard_idx, update_oneof) {
                                continue;
                            }
                            if args
                                .dragonsmouth_outlet
                                .send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
                                    slot,
                                    update,
                                })))
                                .await
                                .is_err()
                            {
                                return Err(LaneFatalError::OutletDisconnected);
                            }
                        }
                    }
                    DataResponseV3Kind::ShardFinish(finish) => {
                        let Some((slot, shard_idx)) = current_shard.take() else {
                            continue;
                        };
                        let is_first_completion = dedup_state.mark_shard_done(slot, shard_idx);
                        dedup_state.shrink_seen_if_needed();
                        if !is_first_completion {
                            // `starting_offset`/resume semantics are "roughly here," not exact --
                            // a reconnect (or the server's own pacing) can legitimately re-deliver
                            // a shard already completed earlier in this lane's lifetime. The
                            // driver's slot-download state machine isn't idempotent to a second
                            // completion for the same shard (it may have already moved the slot
                            // out of "inflight" entirely), so this must be dropped here rather
                            // than reported onward.
                            continue;
                        }
                        let block_uid: FumeBlockUID = finish
                            .block_uid
                            .try_into()
                            .expect("block_uid size mismatch");
                        let completed = CompletedShard {
                            slot,
                            shard_idx,
                            block_uid,
                            block_meta: block_meta.take(),
                        };
                        if args.completed_tx.send(completed).await.is_err() {
                            // Driver is gone; nothing left to do but stop cleanly.
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

async fn send_command<S>(sink: &mut S, command: DataCommandV3Kind) -> Result<(), LaneFatalError>
where
    S: Sink<DataCommandV3> + Unpin,
{
    sink.send(DataCommandV3 {
        command: Some(command),
    })
    .await
    .map_err(|_| LaneFatalError::SinkDisconnected)
}
