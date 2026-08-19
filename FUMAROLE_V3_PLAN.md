# Push-based Fumarole protocol (V3): design + implementation plan

> This file has two parts. The section right below, up to the `--- END SERVER HANDOFF ---` marker, is self-contained and meant to be copy-pasted into a separate session working on the Fumarole **server**. Everything after that marker is client-implementation detail (Rust file paths, internal types) that only makes sense in the `yellowstone-fumarole-client` repo and isn't needed server-side.
>
> Status: both sides are implemented and have been verified working end-to-end against a live server and a real Solana mainnet-scale blockchain history, including a consumer group with a real backlog. This doc reflects the contract as actually implemented (it went through a few rounds of correction against the real running server -- see "Revision notes" at the bottom of the SERVER HANDOFF section for what changed and why, since both sides evolved this independently and needed reconciling more than once).

---

## SERVER HANDOFF: Fumarole V3 push protocol — wire contract & required behavior

### Problem being solved

Fumarole's existing protocol is client-driven ("pull") on both planes: the client polls `PollBlockchainHistory` for new blockchain history, and explicitly requests each `(block_uid, shard_idx)` it wants via `DownloadBlockShard` on one of several data-plane lanes it already maintains (lanes are pinned by `shard_idx % N == lane_idx`, N fixed per session). V3 flips both planes to server-push: the client just declares itself once per stream, then the server decides what to send and when.

- **Data plane**: a lane joins with `JoinDataPlane{lane_idx, num_lanes, starting_offset}`, then opens the valve with `StartDataPlane{}`. From then on, the server autonomously pushes every shard where `shard_idx % num_lanes == lane_idx`, starting from `starting_offset`, forever, with no further requests from the client. The affinity key is unchanged from the existing pull protocol — only who initiates sending changes.
- **Control plane**: the server pushes blockchain history proactively instead of waiting for a poll, but only up to an explicit credit budget the client grants (`GrantHistoryCredits{max_events}`), so a slow client is never flooded.

Why control plane needs explicit credits but data plane doesn't: HTTP2 (which gRPC rides on) already gives real backpressure at the byte level — if the client doesn't read, the stream's flow-control window empties and the server physically can't write more. That's a good proxy for the data plane, where "consumed a byte" ≈ "did a unit of application work." It's a bad proxy for the control plane, where the unit of work is a `BlockchainEvent` — individually cheap in bytes, but each one represents a block the client has to track/download/buffer. HTTP2 would happily let a burst of thousands of small history events through a generous window without ever stalling, even though the client's real constraint is *how many outstanding events it wants to hold*, not wire throughput. `GrantHistoryCredits` closes that gap explicitly instead of relying on a transport mechanism that can't see it.

**Important topology note:** the control-plane connection and the data-plane lane connections do **not** land on the same server — different, independently-scaled services, possibly even different instances per lane. This rules out any design where the data-plane server would cheaply check "what has the control-plane server already told this client" — that would require either a live cross-service call on the hot path of every shard push, or a shared low-latency store between two independently-scaled services. Two consequences of this baked into the design below:

1. Neither plane infers or defaults a starting position for a session/lane on the client's behalf — **the client always supplies it explicitly**, on every join (see "Offset semantics" below). Neither service has (or should build) a cross-service or cross-instance store of "where did this client leave off."
2. Instead of the data-plane server consulting control-plane state directly for pacing, the **client** carries a lightweight watermark from the control plane over to the data plane itself (`ReportHistoryProgress`) — the client is the only party that already sees both sides. This watermark is necessarily a **soft, best-effort throttle, not a correctness guarantee** — see "Cross-plane ordering" below.

This must land as a **strictly additive** change: two brand-new RPCs on the existing `Fumarole` service, reusing several existing message types where they already fit. `Subscribe`, `SubscribeV2`, `SubscribeData`, and `DownloadBlockShard` must keep working exactly as today — old clients/deployments are unaffected, and a client only ever calls the V3 RPCs after checking the server's advertised version supports them (via the existing `Version`/`VersionResponse` RPC).

### New RPCs

```proto
service Fumarole {
  // ... all existing rpcs unchanged ...

  // V3 control plane: server-pushed history with explicit credit-based backpressure.
  rpc SubscribeV3(stream ControlCommandV3) returns (stream ControlResponseV3) {}

  // V3 data plane: N persistent, server-pushed shard lanes, explicit start gate.
  rpc SubscribeDataV3(stream DataCommandV3) returns (stream DataResponseV3) {}
}
```

### Control plane V3 messages

```proto
message JoinControlPlaneV3 {
  optional string consumer_group_name = 1;
  // Required: the offset this session should start reading history from. The server only
  // validates the named consumer group exists (and is owned by this subscription) -- it does
  // not choose a starting offset on the client's behalf. It never substitutes the consumer
  // group's durably-committed offset for this either, even though InitialConsumerGroupState
  // reports that back too (for the client's own reference, not as a default).
  optional int64 initial_offset = 2;
}

message GrantHistoryCredits {
  uint32 max_events = 1; // additional events server is allowed to push, on top of any outstanding grant
}

message ControlCommandV3 {
  oneof command {
    JoinControlPlaneV3 initial_join = 1;
    CommitOffset commit_offset = 2;      // reuse existing message
    GrantHistoryCredits grant_history_credits = 3;
    Ping ping = 4;                        // reuse existing message
  }
}

message ControlResponseV3 {
  oneof response {
    InitialConsumerGroupState init = 1;   // reuse existing message
    CommitOffsetResult commit_offset = 2; // reuse existing message
    BlockchainHistory history_push = 3;   // reuse existing message — now sent unsolicited
    Pong pong = 4;                        // reuse existing message
  }
}
```

There is no `PollBlockchainHistory` in V3 at all — push + credits fully replace polling.

### Data plane V3 messages

```proto
message JoinDataPlane {
  uint32 lane_idx = 1;
  uint32 num_lanes = 2;
  optional BlockFilters block_filters = 3; // reuse existing message
  // Required: control-plane offset (same space as BlockchainEvent.offset) this lane starts
  // tailing shards from. The server never infers or defaults this -- in particular, never to
  // "current tip": doing that unconditionally is a bug, since it silently strands any consumer
  // with a real backlog (its data-plane lanes would only ever push shards for blocks arriving
  // from now on, never the backlog its control-plane session is still working through). See
  // JoinControlPlaneV3.initial_offset for the identical stance on the control plane.
  optional int64 starting_offset = 4;
}

message StartDataPlane {} // the "open the valve" signal — one-shot, per-lane, empty by design

message ReportHistoryProgress {
  int64 highest_known_offset = 1; // same offset space as control-plane BlockchainEvent.offset
}

message DataCommandV3 {
  oneof command {
    JoinDataPlane join = 1;
    StartDataPlane start = 2;
    BlockFilters filter_update = 3;       // reuse existing message, for mid-session filter changes
    ReportHistoryProgress report_history_progress = 4;
  }
}

message BlockShardDownloadStart {
  bytes blockchain_id = 1;
  bytes block_uid = 2;
  uint64 slot = 3;
  uint32 shard_idx = 4;
  uint32 num_shards = 5;
  // A separate, purely connection-local monotonic counter (starting at 0 on every fresh Join),
  // for the client's own gap/duplicate detection *within* one lane connection's lifetime. Plays
  // no role in resumption -- see JoinDataPlane.starting_offset for that.
  uint64 log_offset = 6;
}

message BlockShardDownloadFinishV3 {
  bytes block_uid = 1;
  uint64 slot = 2;
  repeated uint32 shard_indices = 3;
  // Same connection-local counter as BlockShardDownloadStart.log_offset -- not used for resumption.
  uint64 log_offset = 4;
}

message DataResponseV3 {
  oneof response {
    BlockShardDownloadStart shard_start = 1;
    geyser.SubscribeUpdate update = 2;
    BlockShardDownloadFinishV3 shard_finish = 3;
  }
}
```

**Why `BlockShardDownloadStart` exists at all**: a raw `geyser.SubscribeUpdate` carries no `(slot, block_uid, shard_idx)` envelope of its own. In the old pull protocol the client always knows what it's receiving because it's the one who asked (`DownloadBlockShard{slot, block_uid, shard_idx}`). In push mode nothing was requested, so the server must explicitly announce "the next burst of updates on this lane is shard N of block X at slot S" before sending them.

**Offset semantics — client-supplied, always, on both planes, no defaulting**: `JoinControlPlaneV3.initial_offset` and `JoinDataPlane.starting_offset` are both in the *same* offset space as `BlockchainEvent.offset`/`CommitOffset.offset`, and both are required on every join — first-time or reconnect, no exceptions, no "start from tip" fallback. The server's only job on join is to validate the named consumer group exists (and is owned by the subscription); it must never derive, infer, or default a starting position itself, on either plane. A client that wants to resume from its last commit reads `InitialConsumerGroupState.last_committed_offsets` back from a prior session (V1/V2's `Subscribe`/`SubscribeV2` reports the identical field, which is how a client bootstraps this before its very first V3 join ever) and hands it straight back in — that's a client-side policy choice, not something the server should infer.

**Why this matters in practice, not just in theory**: an earlier version of this design had the data plane default an unresumable lane to "current tip." That's a real bug, not a hypothetical one — it silently stranded any consumer group with an actual backlog, because the data plane would only ever push shards for blocks from *now* on, while the control plane was still working through history from far earlier; the client would sit there receiving history it can't act on (every shard for those old slots simply never arrives), with no error, no timeout, just silent lack of progress. Requiring an explicit, client-supplied starting offset on both planes — with no default at all — closes this off at the type level.

**Resuming a lane is inherently approximate ("roughly here"), not exact**: because lanes are stateless and independent of each other and of any prior connection (no cross-task/cross-instance log of "where did lane N leave off" — see the data-plane module's own docs on why that wouldn't reliably help anyway when lanes are scaled across independent instances), a lane resuming from `starting_offset` may legitimately re-deliver a shard the client already fully received before a disconnect. **The client must be able to tolerate a duplicate shard completion, not just duplicate individual updates within a shard** — this bit the client implementation once already (a duplicate `shard_finish` for an already-fully-processed slot reached code that assumed shard completions were exactly-once and crashed). Whatever the server does internally to decide where to actually start a reconnecting lane, it should assume the client is deduping on its side, and doesn't need special server-side effort to make resumption exact.

### Cross-plane ordering: the shard-before-history race, and why it can't be fully closed

Because the two planes are served independently (see topology note above), the data-plane server can legitimately push a shard for a block before the client has learned that block exists via `SubscribeV3` — there is no cheap way to prevent this outright. Concrete failure timeline: client reports `highest_known_offset=1000`; right after, the control-plane *connection* stalls for 200ms (retransmit, GC pause, reroute — doesn't matter why); the data-plane connection, unaffected, is still within its allowed slack and pushes shards up to offset ~1005; those arrive at the client while its control-plane knowledge is still stuck at 1000. This isn't a bug in the mechanism, it's what an async, independently-clocked pair of channels does under ordinary jitter — no report frequency or slack size closes it in the worst case, only shrinks it on average. Making it airtight would require a synchronous check between the two services on every shard push, which defeats the point of making this push-based at all.

Given that, `ReportHistoryProgress` is scoped as a **pacing hint, not a correctness gate**:

- Server behavior: track the last-reported `highest_known_offset` per data-plane session. Use it to avoid pointless work — e.g. hold off pushing shards for blocks more than some configurable slack (in offset units) ahead of the last report — but apply a timeout fallback that lets the lane proceed anyway if no report arrives for a while, so a delayed/lost report can't wedge a lane indefinitely. This bounds the *common case* (no gratuitous thousand-shard bursts at session start or after a client hiccup); it is explicitly not expected to bound the worst case.
- Because of that, the server does **not** need to guarantee shards never arrive before their history — it only needs to (a) support this message and use it as described, and (b) accept that the client will sometimes receive a `shard_start` for a slot it doesn't recognize yet, and that this is expected, not an error to be prevented at all costs.
- Client-side (implemented, see `core/runtime_v3/data_plane.rs` in "Client changes" below): hold such a shard as a provisional candidate and stop reading further from that lane until its own control-plane state catches up — which, because this rides on HTTP2, itself produces real backpressure back onto the server once the client's per-lane holding capacity (bounded to one shard, since only one is ever in flight per lane) is full.

### Commitment level: a deliberate, accepted trade-off

The existing pull protocol only triggers a shard download once a block's `BlockchainEvent` reaches the requesting session's minimum commitment level, specifically to avoid downloading data for slots that might still fork away before reaching that commitment. V3 push does **not** preserve this optimization — the server decides what to push based on shard affinity and its own data availability alone, with no per-session commitment awareness on the data plane (adding that would mean tracking per-session commitment preference in data-plane server state, which runs into the exact same cross-service cost problem described above). **V3 pushes shard data as soon as it exists, regardless of any session's requested commitment level.** Sessions that only want high-commitment data will receive (and pay bandwidth for) data on slots that may later fork away — a real cost compared to V1, accepted in exchange for the simplicity and latency benefits of push. No server-side commitment filtering is implemented for the data plane.

### Required server behavior

1. **`SubscribeV3`**: on `JoinControlPlaneV3{consumer_group_name, initial_offset}`, look up the named consumer group and fail (`not_found`) if it doesn't exist or isn't owned by this subscription — but do not derive a starting offset from it. Seed the session's read cursor directly from the client-supplied `initial_offset` (reject the join if it's missing). Respond with `InitialConsumerGroupState` exactly like `SubscribeV2` does today — it still reports the durably-committed offset from storage, for the client's own reference, but never substitutes that for `initial_offset`. Maintain a per-session history credit counter starting at 0. On `GrantHistoryCredits`, add `max_events` to it. Whenever the counter is > 0 and new history events exist at or after the read cursor, push a `BlockchainHistory` (unsolicited) and decrement the counter by however many events were sent. Pause pushing when the counter hits 0; resume as soon as more credit arrives.
2. **`SubscribeDataV3`**: on `JoinDataPlane{lane_idx, num_lanes, starting_offset}`, register the stream as lane `lane_idx` of `num_lanes`, but stay paused — push nothing yet. Reject the join if `starting_offset` is missing (no default, no "current tip" fallback — see above for why that's a real bug, not just an inelegance). On `StartDataPlane`, begin pushing `BlockShardDownloadStart` → N×`SubscribeUpdate` → `BlockShardDownloadFinishV3` for every shard whose `shard_idx % num_lanes == lane_idx` at or after `starting_offset`, using the same shard-assignment logic that already serves `DownloadBlockShard` today, **regardless of commitment level** (see above). The gate is one-shot per stream (no re-pausing once started) — a client that wants to pause must simply stop reading (relying on HTTP2 flow control) or drop and rejoin the stream. Never interleave two shards' `start...finish` spans on the same lane — exactly one shard in flight per lane at a time. `log_offset` is connection-local only; don't try to make it globally meaningful or persist it anywhere.
3. On `ReportHistoryProgress{highest_known_offset}`, update the session's watermark and use it as a soft pacing throttle per the "Cross-plane ordering" section above — a best-effort optimization, not a correctness mechanism.
4. Bump the `Version`/`VersionResponse` minor version so clients can feature-detect V3 support before attempting it, mirroring how sharded block download was previously gated by a minimum minor version.

### Revision notes (why this doc doesn't match its first draft)

Both sides were implemented independently against an initial draft of this contract, then reconciled against each other by actually running them together. Two real bugs surfaced that way, both now reflected above instead of in the original draft's design:

1. **The original draft had the data plane default an unresumable/first-time lane to "current tip."** This shipped, was tested only against consumer groups with no backlog (where it looks correct), and then silently failed the first time it was pointed at a consumer group that was actually behind — the data-plane lanes only ever pushed shards for new blocks, never the backlog the control plane was still replaying, so the session made no progress and produced no error. Fixed by making `starting_offset`/`initial_offset` mandatory, client-supplied, and never defaulted, on both planes.
2. **The original draft used a separate, lane-local monotonic offset (`from_log_offset`/`log_offset` in one unified space) as the resume mechanism**, which turned out to have no valid value to send on a lane's very first join (nothing to resume from yet). Fixed by having the client discover a real starting point once via the existing V1/V2 `InitialConsumerGroupState` response and supply that explicitly — which is also what led to (1) above being caught, since testing that path against a real backlogged consumer group is what surfaced the tip-default bug.
3. **A client-side bug, not a wire-contract issue, but worth recording**: resuming a lane from an approximate offset can re-deliver an already-completed shard. The client's per-update dedup handled duplicate *updates* within a shard fine, but nothing deduped the shard-*completion* signal itself before it reached the code that tracks per-slot download progress, which isn't idempotent to a second completion for an already-finished slot — it crashed. Fixed client-side (see "Client changes"), but it's a direct consequence of the "resumption is approximate" property above, so worth knowing if you're implementing another client against this same contract.

--- END SERVER HANDOFF ---

## Context

Today both Fumarole planes are client-driven ("pull"):

- **Control plane** (`Subscribe`/`SubscribeV2`): the client sends `PollBlockchainHistory` whenever `FumaroleSM::need_new_blockchain_events()` says its queue is running low, and the server replies once with a `BlockchainHistory` batch. This is a request/response loop wrapped in a bidi stream.
- **Data plane** (`SubscribeData`): the client already opens `N = num_data_plane_tcp_connections * concurrent_download_limit_per_tcp` persistent lanes (`ShardedDownloadOrchestrator` in `core/runtime.rs`), and already pins block-shards to a lane via the affinity rule `shard_idx % N == lane_idx`. But each lane only receives data because the client explicitly asks for it via a `DownloadBlockShard` command per `(block_uid, shard_idx)` it wants — it's push-shaped plumbing wrapped around a pull-driven protocol.

The goal: a clean, self-contained **V3 protocol** — two new RPCs, `SubscribeV3` (control) and `SubscribeDataV3` (data) — where the client only tells the server "who it is" once per stream, then explicitly opens the valve, and the server decides what/when to send from there. See the SERVER HANDOFF section above for the full, current wire contract and the rationale behind it (including two real bugs found and fixed by actually running both sides together — worth reading the "Revision notes" there before assuming the first version of any part of this is still accurate).

## Proto: new V3 service surface (`crates/yellowstone-fumarole-client/proto/fumarole.proto`)

The proto additions are identical to the SERVER HANDOFF section above — see there for the exact message/RPC definitions (`SubscribeV3`, `SubscribeDataV3`, `JoinControlPlaneV3`, `GrantHistoryCredits`, `ControlCommandV3`, `ControlResponseV3`, `JoinDataPlane`, `StartDataPlane`, `ReportHistoryProgress`, `DataCommandV3`, `BlockShardDownloadStart`, `BlockShardDownloadFinishV3`, `DataResponseV3`). They're implemented already, in `crates/yellowstone-fumarole-client/proto/fumarole.proto`.

## Client changes (implemented)

The client-side implementation lives in its own module rather than being folded into the existing pull-based runtime, and splits the same way the design does -- driver/orchestration vs. data-plane consumption:

- `proto/fumarole.proto`: the V3 RPCs + messages from the SERVER HANDOFF section, regenerated via `build.rs`/`tonic-build` automatically on build.
- `config.rs`: `FumaroleConfig.enable_push_based_runtime: bool` (default `false`), deserialized from the config-file key `xx_enable_push_based_runtime` (`#[serde(rename = ...)]`, matching this crate's convention for experimental/pre-stabilization flags -- see `xx_enable_sharded_download` historically) -- the opt-in switch.
- `core/ports.rs`: `ControlPlaneConnectorV3` and `FumaroleDataplaneConnectorV3` -- sibling traits to the V1/V2 `ControlPlaneConnector`/`FumaroleDataplaneConnector`, not extensions of them, since V3 has its own message envelopes. `FumaroleDataplaneConnectorV3::subscribe_data_v3` takes the `JoinDataPlane` message as a parameter and its implementation must send it into the request stream *before* awaiting the RPC's response -- the V3 data-plane server doesn't produce a response until it has read that first message, so sending it only after the call resolves deadlocks (bounded only by the server's initial-message timeout; this was a real client bug caught by testing against the real server, not a hypothetical). `ControlPlaneConnectorV3::subscribe_v3` already worked this way from the start (it mirrors the existing V1/V2 `ControlPlaneConnector::subscribe` pattern), which is exactly why only the data-plane side had this bug.
- `connectors/control_plane_v3.rs`, `connectors/dataplane_v3.rs`: the gRPC-level impls of those traits for `FumaroleClient`/`FumaroleGrpcConnector`, mirroring `connectors/control_plane.rs`/`connectors/dataplane.rs`'s existing adapter shape.
- `core/runtime_v3/` (new module, sibling of `core/runtime`):
  - `mod.rs`: module root, plus `KnownSlots` -- a small `Mutex<HashSet<Slot>> + Notify` structure shared between the driver and every lane. The driver marks a slot known right after it seeds the state machine's bookkeeping for it; lanes consult it before acting on a `shard_start`, and `.await` on it (bounded by a short fallback poll interval, since `Notify::notify_waiters` can miss a lane that starts waiting a moment too late) when the slot isn't known yet -- this is what implements "Handling the shard-before-history race" from the plan above. Not reading further from that lane's stream while waiting is what actually produces the HTTP2-level backpressure described in the plan.
  - `driver.rs`: `PushFumaroleRuntime<CP>` -- owns the `FumaroleSM`, the `SubscribeV3` handshake/rejoin logic, the history-credit grant loop, `ReportHistoryProgress` broadcast (on a delta-or-timer basis) to every lane, and aggregation of per-slot shard completions reported by lanes (block_meta accumulation + `sm.make_slot_download_progress` + emitting `Data`/`SlotEnded` to the dragonsmouth outlet once a slot's shards are all in). Generic only over the control-plane connector -- lanes are independent tasks it doesn't need to be generic over.
    - `discover_last_committed_offset(...)`: a small helper that does a quick, throwaway V1/V2 `Subscribe`/`SubscribeV2` join purely to read back `InitialConsumerGroupState.last_committed_offsets`, then drops that connection. This is the client's answer to "where do I get a value for `initial_offset`/`starting_offset` before I've ever joined V3" -- there's no V3-native way to learn it (by design, see SERVER HANDOFF), and V1/V2's existing init response is the one place that already reports it.
    - `bootstrap(...)` is the entry point: calls `discover_last_committed_offset` first, then does the `JoinControlPlaneV3{consumer_group_name, initial_offset: discovered}` handshake, spawns the `N` lanes (each seeded with that same `discovered` value as `PushShardLaneArgs::initial_offset`), and spawns the driver task -- returning just a `JoinHandle<()>`, the same shape the V1/V2 path produces. On a control-plane rejoin mid-session (`rejoin_control_plane`), `initial_offset` is instead the driver's own current `highest_seen_offset` watermark -- the same value being broadcast to lanes via `ReportHistoryProgress`, reused here as the control plane's own resume point.
  - `data_plane.rs`: `run_lane` -- one self-contained task per lane. Tracks a single `current_offset: FumeOffset`, seeded from `PushShardLaneArgs::initial_offset` on the very first join. Unlike an earlier version of this code, it does **not** update `current_offset` from `BlockShardDownloadStart`/`Finish.log_offset` -- that field turned out to be purely connection-local server-side and not a valid resume point (see SERVER HANDOFF "Revision notes"). Instead, `current_offset` is updated whenever a `LaneCommand::ReportHistoryProgress` arrives from the driver (the same broadcast that also gets forwarded to the server as a pacing hint) -- reusing the driver's own control-plane watermark as this lane's resume point on reconnect, which is exactly the quantity the driver already tracks and already has a reason to keep current. Sends `starting_offset: Some(current_offset)` on every `JoinDataPlane`, first-time or reconnect, then `StartDataPlane`. On the response stream: `ShardStart` waits on `KnownSlots` if the slot isn't recognized yet, `Update` runs through the existing `DedupState` (widened to `pub(crate)` and reused as-is from `core::runtime`) and forwards non-`BlockMeta` updates straight to the dragonsmouth outlet, `ShardFinish` calls `DedupState::mark_shard_done` (now returns `bool`: `false` means this exact shard was already marked done before -- a duplicate redelivery from an approximate resume, see SERVER HANDOFF "Revision notes" point 3) and **only reports a `CompletedShard` to the driver on the first, non-duplicate completion** -- the driver's slot-download state machine isn't idempotent to a second completion for an already-finished slot, so this has to be caught here rather than there. On a recoverable transport error, reconnects (bounded retries) and redoes `Join` (with the now-current `current_offset`) → `Start`.
- `core/runtime.rs`: small additive changes only -- `DedupState` widened from module-private to `pub(crate)` so `data_plane.rs` can reuse it, `DedupState::mark_shard_done` changed from returning `()` to `bool` (`true` = newly marked, `false` = already done -- V1's own call site ignores the return value, so this is backward compatible), and two small methods added to `FumaroleRuntimeCommitEvent` (`new`, `take_sequence`) so `driver.rs` can construct/drain commit events without needing access to its private field. Nothing existing was removed or changed in behavior; all 21 pre-existing unit tests still pass unmodified.
- `core/state_machine.rs`: unchanged. `pop_slot_to_download` is drained by the driver purely to seed `inflight_slot_shard_download` bookkeeping (needed so `make_slot_download_progress` and slot-status gating keep working) -- the returned `FumeDownloadRequest` is simply never turned into an outgoing command, since the server is already pushing shard data on its own.
- `lib.rs`: one new branch in `internal_subscribe_with_config`, gated on `enable_push_based_runtime` **and** a new `PUSH_V3_MINIMUM_MINOR_VERSION` server-version check (mirroring the existing `SHARDED_DOWNLOAD_MINIMUM_MINOR_VERSION` pattern) -- if the flag is on but the server doesn't advertise support, it logs a warning and falls through to the existing V1/V2 path rather than failing. When it does take the V3 path, it builds the exact same channels (`dragonsmouth_outlet`/`dragonsmouth_inlet`, `dm_tx`/`dm_rx`, `shared_commit_offset_queue`) as the V1 path and calls `core::runtime_v3::bootstrap(...)`, producing an identical `(FumaroleSubscription, JoinHandle<()>)`.

**On the public API**: no changes were needed to `FumaroleSubscription`, `stream::FumaroleStream`, or `stream::FumaroleSink`. They already only depend on channel types (`mpsc::Sender`/`Receiver<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>` etc.), not on which runtime is feeding those channels -- which is exactly what let the V3 runtime plug in underneath without touching any of them. A "PollBased/PushBased" enum wrapper was considered for these types but deliberately not built, since there is no behavioral difference for it to dispatch between.

## Testing / verification

Done:
- Full workspace (`cargo check --workspace`) builds clean with the V3 addition; all 21 pre-existing unit tests in `yellowstone-fumarole-client` pass unmodified; `cargo clippy` is clean on the client crate.
- **End-to-end, against the real running server, on real Solana mainnet-scale history**: `fume subscribe` with `xx_enable_push_based_runtime: true` against a fresh consumer group ran cleanly for 25+ seconds with zero panics/errors/warnings, real account/tx data printed throughout, and the commit-offset acknowledgment advancing monotonically and continuously (11 commits observed in that window). Also exercised: a consumer group with a genuine backlog (previously stuck at zero progress under the "current tip" server bug) now makes progress once both sides agree on an explicit, client-supplied starting offset.
- Both real bugs recorded in the SERVER HANDOFF "Revision notes" (the join-before-response deadlock, and the duplicate-shard-completion crash) were caught by this live testing, not by unit tests -- worth keeping in mind when deciding how much to invest in the mock-server testing gap below versus just continuing to dogfood against a real server.

Not yet done (gap, worth doing before flipping the flag on by default for anyone other than the two of us):
- No mock Fumarole server exists in this repo -- current tests (`core/state_machine.rs`, `core/runtime.rs` `#[cfg(test)]` modules) use hand-built `TestConnector` fakes against `ShardedDownloadOrchestrator` directly. The same pattern should be extended for V3: a `TestConnectorV3` that, given `Join`/`Start`, streams a scripted `shard_start/update/shard_finish` sequence to validate lane-affinity routing, dedup (including a scripted *duplicate* `shard_finish` for the same shard, to pin down the fix for bug 3 above), `current_offset` tracking via `ReportHistoryProgress`, and that a simulated reconnect sends `JoinDataPlane.starting_offset` matching the last-reported watermark -- without needing a real server.
- Unit tests for the history-credit grant/decrement logic in `driver.rs`, and for the `Join`/`Start` sequencing (no data flows before `Start`).
- A targeted test for the shard-before-history race: feed `run_lane` a `shard_start` for a slot `KnownSlots` doesn't have yet, assert it doesn't panic and stops reading that lane; then mark the slot known and assert the buffered shard gets adopted and reading resumes.
- Longer-running soak testing (minutes to hours, not seconds) against a real backlogged consumer group, watching for gaps or duplicate account/tx data downstream of the dedup fix, and confirming lane reconnects under real network conditions behave as expected.

## Suggested delivery order

1. ~~Implement the client-side V3 path behind `xx_enable_push_based_runtime`, off by default.~~ Done.
2. ~~Land the V3 proto + server-side `SubscribeV3`/`SubscribeDataV3` support behind the version bump.~~ Done.
3. ~~Verify both sides actually interoperate, against a real server and real chain data.~~ Done -- surfaced and fixed the three bugs recorded in the SERVER HANDOFF "Revision notes."
4. Fill the remaining testing gaps above, in particular a `TestConnectorV3`-based simulation of reconnect/resume and the shard-before-history race, so these three bug classes have regression coverage beyond "we happened to notice it live."
5. Longer soak test against a real backlogged consumer group.
6. Consider defaulting `xx_enable_push_based_runtime` to `true` once confidence is higher; keep the V1/V2 pull path as the permanent fallback for old servers (cheap to keep, since it's the existing, untouched code path).
