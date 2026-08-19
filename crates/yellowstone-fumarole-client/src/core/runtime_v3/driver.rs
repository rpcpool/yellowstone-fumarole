//!
//! Control-plane loop and top-level orchestration for the V3 push-based runtime.
//!
//! [`PushFumaroleRuntime`] is the sibling of `FumaroleAsyncRuntime` in `core::runtime`: it owns
//! the [`FumaroleSM`], drives the `SubscribeV3` control-plane handshake and history-credit loop,
//! aggregates per-slot shard completions reported by the data-plane lanes (see
//! `super::data_plane`), and emits [`FumaroleRuntimeEvent`]s to the same dragonsmouth outlet
//! channel the V1/V2 runtime uses -- which is what lets this whole runtime plug in without any
//! change to [`crate::FumaroleSubscription`], [`crate::stream::FumaroleStream`], or
//! [`crate::stream::FumaroleSink`].
//!
use {
    super::{
        KnownSlots,
        data_plane::{self, CompletedShard, LaneCommand, LaneFatalError, PushShardLaneArgs},
    },
    crate::{
        core::{
            ports::{ControlPlaneConnector, ControlPlaneConnectorV3, ControlPlaneStreamError},
            runtime::{FumaroleRuntimeCommitEvent, FumaroleRuntimeDataEvent, FumaroleRuntimeEvent},
            state_machine::{FumaroleSM, FumeOffset, SlotDownloadState},
        },
        error::FumaroleSubscribeError,
        proto::{
            self, CommitOffset, ControlCommandV3, ControlResponseV3, GrantHistoryCredits,
            JoinControlPlaneV3, control_command_v3, control_response_v3,
        },
    },
    crossbeam::queue::SegQueue,
    futures::{SinkExt, StreamExt},
    solana_clock::Slot,
    std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::mpsc,
        task::{JoinHandle, JoinSet},
    },
    yellowstone_grpc_proto::geyser::{
        self, SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot,
    },
};

const HISTORY_CREDIT_TOP_UP: u32 = 100;
const HISTORY_CREDIT_LOW_WATER_MARK: u32 = 50;

/// How deep the "known but not yet downloaded" backlog is allowed to get, per data-plane lane,
/// before `grant_history_credits_if_needed` stops asking for more. See that function for why a
/// cap is needed at all. The cap is sized off `num_lanes` -- the actual download parallelism --
/// rather than a fraction of `slot_memory_retention`: a fixed fraction of retention (e.g. 5,000)
/// let the control plane build a backlog thousands of slots deep before ever throttling, and
/// since that backlog then only drains at the real per-lane completion rate (observed: ~1.5-3
/// slots/sec total across 6 lanes against a mainnet server), it took tens of minutes to drain
/// back under the cap -- indistinguishable, from the outside, from a permanent stall. Scaling
/// with `num_lanes` keeps the buffer just deep enough to hide RTT/scheduling jitter without
/// letting the overshoot-then-drain cycle take longer than a few seconds.
const INFLIGHT_THROTTLE_PER_LANE: usize = 50;

/// Minimum offset advance before an unsolicited `ReportHistoryProgress` broadcast, outside of
/// the periodic timer tick below. Purely a pacing hint for the data-plane server -- see
/// FUMAROLE_V3_PLAN.md.
const REPORT_PROGRESS_MIN_DELTA: FumeOffset = 50;
const REPORT_PROGRESS_INTERVAL: Duration = Duration::from_secs(2);

const CONTROL_PLANE_REJOIN_MAX_ATTEMPTS: usize = 3;
const CONTROL_PLANE_REJOIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_PLANE_REJOIN_BACKOFF: Duration = Duration::from_secs(2);

/// Purely diagnostic: periodic internal-state dump to help distinguish "the inflight throttle
/// found a lower but stable steady state" from "something is still degrading over time" (e.g. an
/// unbounded `slot_accum` leak, or the control plane's own incoming rate slowing down).
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct SlotAccum {
    block_meta: Option<SubscribeUpdate>,
}

///
/// V3 push-based Fumarole runtime. Generic over the control-plane connector only -- the
/// data-plane lanes are independent, self-contained tasks (see `super::data_plane::run_lane`)
/// spawned once at [`bootstrap`] time, so the driver never needs to be generic over the
/// data-plane connector type.
///
pub(crate) struct PushFumaroleRuntime<CP>
where
    CP: ControlPlaneConnectorV3,
{
    sm: FumaroleSM,
    known_slots: Arc<KnownSlots>,
    control_plane_connector: CP,
    control_plane_tx: CP::ControlPlaneSink,
    control_plane_rx: CP::ControlPlaneStream,
    persistent_subscriber_name: String,
    lane_cnc_txs: Vec<mpsc::Sender<LaneCommand>>,
    completed_rx: mpsc::Receiver<CompletedShard>,
    lane_joinset: JoinSet<Result<(), LaneFatalError>>,
    slot_accum: HashMap<Slot, SlotAccum>,
    outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    subscribe_request: Arc<SubscribeRequest>,
    subscribe_request_rx: mpsc::Receiver<SubscribeRequest>,
    shared_commit_offset_queue: Arc<SegQueue<FumaroleRuntimeCommitEvent>>,
    commit_interval: Duration,
    last_commit: Instant,
    no_commit: bool,
    gc_interval: usize,
    outstanding_history_credit: u32,
    highest_seen_offset: FumeOffset,
    last_reported_offset: Option<FumeOffset>,
    last_report_sent: Instant,
    /// Caps how many slots can be registered "inflight" (known, but not yet fully downloaded)
    /// before we stop asking the control plane for more history. See
    /// `grant_history_credits_if_needed` for why this exists -- without it, nothing bounds how
    /// far ahead of the data plane's actual completion rate the control plane can race.
    max_inflight_before_throttle: usize,
    last_stats_log: Instant,
    last_stats_offset: FumeOffset,
    stop: bool,
}

impl<CP> PushFumaroleRuntime<CP>
where
    CP: ControlPlaneConnectorV3,
{
    async fn grant_history_credits_if_needed(&mut self) {
        // `need_new_blockchain_events()` alone isn't enough here the way it is for the V1/V2
        // pull runtime: this driver eagerly drains every newly-queued event into
        // `inflight_slot_shard_download` bookkeeping the moment it arrives (see
        // `handle_control_response`), so `unprocessed_blockchain_event` -- the only thing that
        // method looks at -- is essentially always empty regardless of how much undownloaded
        // backlog has piled up. Left unchecked, that means nothing throttles how far ahead of
        // the data plane's actual completion rate the control plane is allowed to race. If that
        // gap exceeds `KnownSlots`'s retention window, a slow lane can permanently lose track of
        // a slot it's still waiting on (confirmed live: the control plane raced ~12,000 slots
        // ahead of a stalled lane against a 10,000-slot retention window, and that lane never
        // recovered). Gating on `inflight_download_count()` bounds that gap directly, at the
        // source, instead of just enlarging the window it can blow through.
        let too_much_inflight =
            self.sm.inflight_download_count() >= self.max_inflight_before_throttle;
        if too_much_inflight {
            return;
        }
        if self.sm.need_new_blockchain_events()
            && self.outstanding_history_credit <= HISTORY_CREDIT_LOW_WATER_MARK
        {
            let cmd = ControlCommandV3 {
                command: Some(control_command_v3::Command::GrantHistoryCredits(
                    GrantHistoryCredits {
                        max_events: HISTORY_CREDIT_TOP_UP,
                    },
                )),
            };
            if self.control_plane_tx.send(cmd).await.is_err() {
                panic!("control plane disconnected");
            }
            self.outstanding_history_credit = self
                .outstanding_history_credit
                .saturating_add(HISTORY_CREDIT_TOP_UP);
        }
    }

    async fn handle_control_response(&mut self, resp: ControlResponseV3) {
        let Some(response) = resp.response else {
            return;
        };
        match response {
            control_response_v3::Response::CommitOffset(result) => {
                tracing::debug!("received commit offset: {result:?}");
                self.sm.update_committed_offset(result.offset);
            }
            control_response_v3::Response::HistoryPush(history) => {
                if !history.events.is_empty() {
                    let min_slot = history.events.iter().map(|e| e.slot).min();
                    let max_slot = history.events.iter().map(|e| e.slot).max();
                    let min_offset = history.events.iter().map(|e| e.offset).min();
                    tracing::debug!(
                        "history push: {} events, slot range [{:?}..{:?}], offset range [{:?}..{:?}]",
                        history.events.len(),
                        min_slot,
                        max_slot,
                        min_offset,
                        history.events.iter().map(|e| e.offset).max()
                    );
                    self.outstanding_history_credit = self
                        .outstanding_history_credit
                        .saturating_sub(history.events.len() as u32);
                    if let Some(max_offset) = history.events.iter().map(|e| e.offset).max() {
                        self.highest_seen_offset = self.highest_seen_offset.max(max_offset);
                    }
                }
                self.sm.queue_blockchain_event(history.events);
                // Drain every newly-queued slot into `inflight_slot_shard_download` bookkeeping
                // and mark it known -- we never turn the returned request into an outgoing
                // command (the server is already pushing shard data on its own), we only need
                // the state machine's side effect of tracking the slot as in-progress.
                while let Some(req) = self.sm.pop_slot_to_download(None) {
                    self.known_slots.mark_known(req.slot);
                }
                self.maybe_report_history_progress(false).await;
            }
            control_response_v3::Response::Pong(_) => {
                tracing::debug!("pong");
            }
            control_response_v3::Response::Init(_) => {
                unreachable!("init should not be received here");
            }
        }
    }

    async fn maybe_report_history_progress(&mut self, force: bool) {
        let advanced_enough = match self.last_reported_offset {
            Some(last) => self.highest_seen_offset - last >= REPORT_PROGRESS_MIN_DELTA,
            None => true,
        };
        if !force && !advanced_enough {
            return;
        }
        self.last_reported_offset = Some(self.highest_seen_offset);
        self.last_report_sent = Instant::now();
        for cnc_tx in &self.lane_cnc_txs {
            // Deliberately non-blocking: a lane can legitimately be stuck for a while inside its
            // own `KnownSlots` wait (the shard-before-history race is expected, not an error --
            // see FUMAROLE_V3_PLAN.md), which means it won't be draining this channel. If this
            // used a blocking `send().await` and that lane's queue filled up, *this* call would
            // block, which would wedge the driver's entire main loop -- including the control-
            // plane processing that's the only thing that can ever resolve that lane's wait in
            // the first place. That's a real deadlock, not a hypothetical one: it's what "the
            // stream eventually locks" was. A dropped report just means that lane's pacing hint
            // goes stale a bit longer; the next tick tries again, and it's only a pacing hint
            // (not a correctness gate) in the first place.
            if let Err(e) =
                cnc_tx.try_send(LaneCommand::ReportHistoryProgress(self.highest_seen_offset))
            {
                tracing::debug!(
                    "failed to forward ReportHistoryProgress to a lane (queue full or lane gone): {e:?}"
                );
            }
        }
    }

    async fn handle_completed_shard(&mut self, completed: CompletedShard) {
        let CompletedShard {
            slot,
            shard_idx,
            block_uid: _,
            block_meta,
        } = completed;

        if let Some(bm) = block_meta {
            self.slot_accum.entry(slot).or_default().block_meta = Some(bm);
        }

        let state = self.sm.make_slot_download_progress(slot, Some(shard_idx));
        if matches!(state, SlotDownloadState::Done) {
            let accum = self.slot_accum.remove(&slot).unwrap_or_default();
            if let Some(bm) = accum.block_meta {
                let _ = self
                    .outlet
                    .send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
                        slot,
                        update: bm,
                    })))
                    .await;
            }
            let _ = self
                .outlet
                .send(Ok(FumaroleRuntimeEvent::SlotEnded(slot)))
                .await;
        }
    }

    async fn handle_new_subscribe_request(&mut self, subscribe_request: SubscribeRequest) {
        self.subscribe_request = Arc::new(subscribe_request);
        // Non-blocking for the same reason as `maybe_report_history_progress` above -- a stalled
        // lane must never be able to wedge the driver. A dropped filter update here is more
        // visible than a dropped pacing hint (a lane could miss a filter change), but a stalled
        // lane isn't reading its data stream either in the meantime, so it has no shard data to
        // apply a stale filter to until it un-stalls and reconnects/catches up regardless.
        for cnc_tx in &self.lane_cnc_txs {
            let _ = cnc_tx.try_send(LaneCommand::UpdateFilters(Arc::clone(
                &self.subscribe_request,
            )));
        }
    }

    async fn drain_slot_status(&mut self) {
        let commitment = self.subscribe_request.commitment();
        let mut slot_status_vec = VecDeque::new();
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
                    .outlet
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
            if self
                .outlet
                .send(Ok(FumaroleRuntimeEvent::Committable(
                    FumaroleRuntimeCommitEvent::new(slot_status.session_sequence),
                )))
                .await
                .is_err()
            {
                return;
            }
        }
    }

    async unsafe fn force_commit_offset(&mut self) {
        if self.no_commit {
            self.sm.update_committed_offset(self.sm.committable_offset);
            return;
        }
        let cmd = ControlCommandV3 {
            command: Some(control_command_v3::Command::CommitOffset(CommitOffset {
                offset: self.sm.committable_offset,
                shard_id: 0, /* control-plane sharding not supported yet, mirrors V1/V2 */
            })),
        };
        self.control_plane_tx
            .send(cmd)
            .await
            .unwrap_or_else(|_| panic!("failed to commit offset"));
    }

    async fn commit_offset(&mut self) {
        if self.sm.last_committed_offset < self.sm.committable_offset {
            unsafe {
                self.force_commit_offset().await;
            }
        }
        self.last_commit = Instant::now();
    }

    fn drain_commit_offset_queue(&mut self) {
        while let Some(mut commit_event) = self.shared_commit_offset_queue.pop() {
            let Some(commit_seq) = commit_event.take_sequence() else {
                continue;
            };
            self.sm.mark_event_as_processed(commit_seq);
        }
    }

    async fn rejoin_control_plane(&mut self) -> Result<(), CP::SubscribeError> {
        // The server never infers this -- resume from our own high-water mark on the offset
        // space, same as every lane does via `ReportHistoryProgress`. See the proto doc comment
        // on `JoinControlPlaneV3.initial_offset`.
        let initial_join = JoinControlPlaneV3 {
            consumer_group_name: Some(self.persistent_subscriber_name.clone()),
            initial_offset: Some(self.highest_seen_offset),
        };
        let (control_plane_tx, mut control_plane_rx) = self
            .control_plane_connector
            .subscribe_v3(initial_join)
            .await?;
        let initial_response = control_plane_rx
            .next()
            .await
            .expect("control plane closed before init")
            .expect("control plane init error");
        let response = initial_response.response.expect("none");
        let control_response_v3::Response::Init(initial_state) = response else {
            panic!("unexpected initial response: {response:?}")
        };
        self.control_plane_tx = control_plane_tx;
        self.control_plane_rx = control_plane_rx;
        tracing::info!("rejoined V3 control plane with initial state: {initial_state:?}");
        Ok(())
    }

    /// Returns `true` if the caller should keep running, `false` if this was unrecoverable.
    async fn handle_control_plane_error(&mut self, err: ControlPlaneStreamError) -> bool {
        match err {
            ControlPlaneStreamError::Disconnected(e) => {
                tracing::warn!(
                    "V3 control plane connection lost with error: {e:?}, attempting to rejoin..."
                );
                for attempt in 1..=CONTROL_PLANE_REJOIN_MAX_ATTEMPTS {
                    match tokio::time::timeout(
                        CONTROL_PLANE_REJOIN_ATTEMPT_TIMEOUT,
                        self.rejoin_control_plane(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            tracing::info!(
                                "V3 control plane rejoin succeeded on attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS}"
                            );
                            return true;
                        }
                        Ok(Err(rejoin_err)) => {
                            tracing::warn!(
                                "V3 control plane rejoin attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS} failed: {rejoin_err:?}"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                "V3 control plane rejoin attempt {attempt}/{CONTROL_PLANE_REJOIN_MAX_ATTEMPTS} timed out"
                            );
                        }
                    }
                    if attempt < CONTROL_PLANE_REJOIN_MAX_ATTEMPTS {
                        tokio::time::sleep(CONTROL_PLANE_REJOIN_BACKOFF).await;
                    }
                }
                tracing::error!(
                    "exhausted V3 control plane rejoin attempts ({CONTROL_PLANE_REJOIN_MAX_ATTEMPTS})"
                );
                let _ = self
                    .outlet
                    .send(Err(FumaroleSubscribeError::ControlPlaneRejoinFailed {
                        details: None,
                    }))
                    .await;
                false
            }
            ControlPlaneStreamError::ApplicationError(e) => {
                tracing::error!("V3 control plane application error: {e:?}");
                let _ = self
                    .outlet
                    .send(Err(FumaroleSubscribeError::ControlPlaneDisconnected))
                    .await;
                false
            }
        }
    }

    async fn handle_lane_result(
        &mut self,
        result: Result<Result<(), LaneFatalError>, tokio::task::JoinError>,
    ) {
        self.stop = true;
        match result {
            Ok(Ok(())) => {
                tracing::info!("data plane lane exited cleanly");
            }
            Ok(Err(LaneFatalError::Dataplane(err))) => {
                tracing::error!("data plane lane failed: {err:?}");
                let _ = self
                    .outlet
                    .send(Err(FumaroleSubscribeError::DataPlaneStreamError(err)))
                    .await;
            }
            Ok(Err(other)) => {
                tracing::error!("data plane lane failed: {other:?}");
            }
            Err(join_err) => {
                tracing::error!("data plane lane task panicked: {join_err:?}");
            }
        }
    }

    fn log_stats(&mut self) {
        let offset_delta = self.highest_seen_offset - self.last_stats_offset;
        self.last_stats_offset = self.highest_seen_offset;
        tracing::info!(
            inflight_download_count = self.sm.inflight_download_count(),
            max_inflight_before_throttle = self.max_inflight_before_throttle,
            slot_accum_len = self.slot_accum.len(),
            outstanding_history_credit = self.outstanding_history_credit,
            highest_seen_offset = self.highest_seen_offset,
            offset_delta_since_last_log = offset_delta,
            committable_offset = self.sm.committable_offset,
            last_committed_offset = self.sm.last_committed_offset,
            "push runtime stats"
        );
    }

    pub(crate) async fn run(mut self) {
        self.grant_history_credits_if_needed().await;
        unsafe {
            self.force_commit_offset().await;
        }

        let mut ticks = 0usize;
        while !self.stop {
            ticks += 1;
            if ticks.is_multiple_of(self.gc_interval.max(1)) {
                self.sm.gc();
                ticks = 0;
            }
            if self.outlet.is_closed() {
                tracing::debug!("detected dragonsmouth outlet closed");
                break;
            }

            let commit_deadline = self.last_commit + self.commit_interval;
            let report_deadline = self.last_report_sent + REPORT_PROGRESS_INTERVAL;
            let stats_deadline = self.last_stats_log + STATS_LOG_INTERVAL;

            self.drain_commit_offset_queue();
            self.grant_history_credits_if_needed().await;

            tokio::select! {
                Some(subscribe_request) = self.subscribe_request_rx.recv() => {
                    self.handle_new_subscribe_request(subscribe_request).await;
                }
                control_response = self.control_plane_rx.next() => {
                    match control_response {
                        Some(Ok(resp)) => {
                            self.handle_control_response(resp).await;
                        }
                        Some(Err(err)) => {
                            if !self.handle_control_plane_error(err).await {
                                break;
                            }
                        }
                        None => {
                            tracing::debug!("V3 control plane disconnected");
                            break;
                        }
                    }
                }
                Some(completed) = self.completed_rx.recv() => {
                    self.handle_completed_shard(completed).await;
                }
                Some(result) = self.lane_joinset.join_next() => {
                    self.handle_lane_result(result).await;
                }
                () = tokio::time::sleep_until(commit_deadline.into()) => {
                    self.commit_offset().await;
                }
                () = tokio::time::sleep_until(report_deadline.into()) => {
                    self.maybe_report_history_progress(true).await;
                }
                () = tokio::time::sleep_until(stats_deadline.into()) => {
                    self.last_stats_log = Instant::now();
                    self.log_stats();
                }
            }
            self.drain_slot_status().await;
        }
        self.stop = true;
        tracing::debug!("push fumarole runtime exiting");
    }
}

///
/// Discovers this consumer group's durably-committed offset via a quick V1/V2 control-plane
/// join, for use as `JoinControlPlaneV3.initial_offset` on a brand-new V3 session. There is no
/// V3-native way to learn this before ever joining -- the server deliberately never infers or
/// defaults a starting offset on the client's behalf (see the proto doc comment on
/// `JoinControlPlaneV3.initial_offset`), so something has to supply a real value up front, and
/// `Subscribe`/`SubscribeV2`'s existing `InitialConsumerGroupState` response is the only place
/// that already reports it. The V1/V2 connection is dropped immediately afterward; nothing
/// about it survives past this call.
///
async fn discover_last_committed_offset(
    fumarole_client: &crate::FumaroleClient,
    subscriber_name: &str,
) -> Result<FumeOffset, tonic::Status> {
    let initial_join = proto::JoinControlPlane {
        consumer_group_name: Some(subscriber_name.to_string()),
    };
    let (_tx, mut rx) = ControlPlaneConnector::subscribe(fumarole_client, initial_join).await?;
    let control_response = match rx.next().await {
        Some(Ok(resp)) => resp,
        Some(Err(ControlPlaneStreamError::Disconnected(err))) => {
            return Err(tonic::Status::unavailable(format!(
                "control plane disconnected before init while discovering last committed offset: {err}"
            )));
        }
        Some(Err(ControlPlaneStreamError::ApplicationError(err))) => {
            return Err(tonic::Status::internal(format!(
                "control plane init failed while discovering last committed offset: {err}"
            )));
        }
        None => {
            return Err(tonic::Status::unavailable(
                "control plane stream closed before init while discovering last committed offset",
            ));
        }
    };
    let response = control_response
        .response
        .ok_or_else(|| tonic::Status::internal("empty control plane response"))?;
    let proto::control_response::Response::Init(initial_state) = response else {
        return Err(tonic::Status::internal(format!(
            "unexpected initial control plane response: {response:?}"
        )));
    };
    Ok(initial_state
        .last_committed_offsets
        .get(&0)
        .copied()
        .unwrap_or(0))
}

///
/// Performs the `SubscribeV3` initial join, spawns the `N` data-plane lanes, and spawns the
/// driver task. Returns just the [`JoinHandle`] so the caller (`FumaroleClient`) can treat this
/// exactly like the V1/V2 runtime handle -- no other part of the public API needs to know which
/// runtime is behind it.
///
pub(crate) async fn bootstrap(
    fumarole_client: &crate::FumaroleClient,
    subscriber_name: String,
    request: Arc<SubscribeRequest>,
    config: &crate::FumaroleSubscribeConfig,
    dragonsmouth_outlet: mpsc::Sender<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    subscribe_request_rx: mpsc::Receiver<SubscribeRequest>,
    shared_commit_offset_queue: Arc<SegQueue<FumaroleRuntimeCommitEvent>>,
) -> Result<JoinHandle<()>, tonic::Status> {
    let discovered_offset =
        discover_last_committed_offset(fumarole_client, &subscriber_name).await?;

    let initial_join = JoinControlPlaneV3 {
        consumer_group_name: Some(subscriber_name.clone()),
        initial_offset: Some(discovered_offset),
    };
    let (control_plane_tx, mut control_plane_rx) =
        ControlPlaneConnectorV3::subscribe_v3(fumarole_client, initial_join).await?;
    let control_response = match control_plane_rx.next().await {
        Some(Ok(resp)) => resp,
        Some(Err(ControlPlaneStreamError::Disconnected(err))) => {
            return Err(tonic::Status::unavailable(format!(
                "V3 control plane disconnected before init: {err}"
            )));
        }
        Some(Err(ControlPlaneStreamError::ApplicationError(err))) => {
            return Err(tonic::Status::internal(format!(
                "V3 control plane init failed: {err}"
            )));
        }
        None => {
            return Err(tonic::Status::unavailable(
                "V3 control plane stream closed before init",
            ));
        }
    };
    let response = control_response
        .response
        .ok_or_else(|| tonic::Status::internal("empty V3 control plane response"))?;
    let control_response_v3::Response::Init(_initial_state) = response else {
        return Err(tonic::Status::internal(format!(
            "unexpected initial V3 response: {response:?}"
        )));
    };

    // Reuse the same value we asked the server to start from (`discovered_offset`) as the SM's
    // durable-commit baseline too, rather than re-deriving it from `_initial_state` -- in the
    // common case they're the same value anyway (both ultimately read the same durably-committed
    // position), and using one source avoids a spurious inconsistency if something else committed
    // in the brief window between the V1/V2 lookup and this join actually landing.
    let last_committed_offset = discovered_offset;

    let sm = FumaroleSM::new(last_committed_offset, config.slot_memory_retention);
    let known_slots = Arc::new(KnownSlots::new(config.slot_memory_retention));

    // Deliberately *not* multiplied by `concurrent_download_limit_per_tcp` the way V1's
    // `total_shard_downloaders` is -- measured empirically to make things worse, not better.
    // Each V3 lane independently re-scans the full history on its own poll tick server-side
    // (`PushDataPlaneLane::fetch_next_batch`, every 100ms) regardless of whether it ends up
    // owning any shards from that scan, so opening more lanes than the server's actual
    // shard-count buys no extra parallelism -- it's pure redundant load on the same storage
    // backend, competing with the lanes that are actually doing useful work. Unlike V1, where a
    // downloader only ever does work when explicitly asked for something, so "just open more of
    // them" is close to free.
    let num_lanes = u32::from(config.num_data_plane_tcp_connections.get());
    let (completed_tx, completed_rx) = mpsc::channel(1000);
    let mut lane_cnc_txs = Vec::with_capacity(num_lanes as usize);
    let mut lane_joinset = JoinSet::new();
    for lane_idx in 0..num_lanes {
        let (cnc_tx, cnc_rx) = mpsc::channel(10);
        lane_cnc_txs.push(cnc_tx);
        let args = PushShardLaneArgs {
            connector: fumarole_client.connector.clone(),
            lane_idx,
            num_lanes,
            subscribe_request: Arc::clone(&request),
            cnc_rx,
            completed_tx: completed_tx.clone(),
            dragonsmouth_outlet: dragonsmouth_outlet.clone(),
            known_slots: Arc::clone(&known_slots),
            initial_offset: last_committed_offset,
        };
        lane_joinset.spawn(data_plane::run_lane(args));
    }

    // Scaled by `num_lanes` (see `INFLIGHT_THROTTLE_PER_LANE`), and additionally clamped well
    // under `KnownSlots`'s own retention window (the same `slot_memory_retention` value) as a
    // safety margin so credit-granting still throttles down long before a slow lane's
    // still-needed slot could actually fall out of that window -- see
    // `grant_history_credits_if_needed`.
    let max_inflight_before_throttle = ((num_lanes as usize) * INFLIGHT_THROTTLE_PER_LANE)
        .max(200)
        .min((config.slot_memory_retention / 4).max(200));

    let driver = PushFumaroleRuntime {
        sm,
        known_slots,
        control_plane_connector: fumarole_client.clone(),
        control_plane_tx,
        control_plane_rx,
        persistent_subscriber_name: subscriber_name,
        lane_cnc_txs,
        completed_rx,
        lane_joinset,
        slot_accum: HashMap::new(),
        outlet: dragonsmouth_outlet,
        subscribe_request: request,
        subscribe_request_rx,
        shared_commit_offset_queue,
        commit_interval: config.commit_interval,
        last_commit: Instant::now(),
        no_commit: config.no_commit,
        gc_interval: config.gc_interval,
        outstanding_history_credit: 0,
        highest_seen_offset: last_committed_offset,
        last_reported_offset: None,
        last_report_sent: Instant::now(),
        max_inflight_before_throttle,
        last_stats_log: Instant::now(),
        last_stats_offset: last_committed_offset,
        stop: false,
    };

    Ok(tokio::spawn(driver.run()))
}
