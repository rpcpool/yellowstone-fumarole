//!
//! V3 push-based Fumarole runtime.
//!
//! Sibling of [`crate::core::runtime`] (the V1/V2 poll/pull-based runtime), not a replacement --
//! see `FUMAROLE_V3_PLAN.md` at the repo root for the full protocol design. This module is split
//! along the same seam as the plan itself:
//!
//! - [`driver`]: the control-plane loop and top-level orchestration (owns the [`FumaroleSM`], tracks history credits, aggregates per-slot shard completions, emits to the dragonsmouth outlet). Analogous to `FumaroleAsyncRuntime` in `core::runtime`.
//! - [`data_plane`]: the `N` per-lane push consumers. Analogous to `ShardedDownloadOrchestrator` + `PipelinedShardDownloader` in `core::runtime`, but much simpler since there's no download scheduling, retry-with-affinity-recycling, or request queue -- lanes just consume whatever the server pushes.
//!
//! [`FumaroleSM`]: crate::core::state_machine::FumaroleSM
//!
use {
    solana_clock::Slot,
    std::{
        collections::{HashSet, VecDeque},
        sync::Mutex,
        time::Duration,
    },
    tokio::sync::Notify,
};

pub(crate) mod data_plane;
pub(crate) mod driver;

pub(crate) use driver::bootstrap;

/// How long a lane will wait between re-checks of [`KnownSlots`] while stalled on an
/// unrecognized slot. `Notify::notify_waiters` only wakes tasks that are *already* waiting at
/// the time it's called, so a lane that starts waiting in the small window right after a
/// notification can otherwise miss it -- this bounds that miss to one poll interval instead of
/// forever.
const KNOWN_SLOTS_POLL_FALLBACK: Duration = Duration::from_millis(50);

///
/// Shared, thread-safe record of which slots the control-plane side of the runtime has already
/// made the state machine aware of. Data-plane lanes consult this before acting on a
/// `shard_start` for a slot they don't recognize -- see "Handling the shard-before-history race"
/// in `FUMAROLE_V3_PLAN.md` for why this can only ever be a best-effort wait, never a strict
/// guarantee, and why that's fine.
///
pub(crate) struct KnownSlots {
    state: Mutex<KnownSlotsState>,
    notify: Notify,
}

struct KnownSlotsState {
    set: HashSet<Slot>,
    order: VecDeque<Slot>,
    retention: usize,
}

impl KnownSlots {
    pub(crate) fn new(retention: usize) -> Self {
        Self {
            state: Mutex::new(KnownSlotsState {
                set: HashSet::new(),
                order: VecDeque::new(),
                retention: retention.max(1),
            }),
            notify: Notify::new(),
        }
    }

    ///
    /// Records that `slot` is now safe to call into the state machine for.
    ///
    /// Callers (the driver only) must call this *after* the state machine has actually been
    /// made aware of the slot (e.g. after draining `FumaroleSM::pop_slot_to_download` for it) --
    /// that ordering is what keeps the state machine's own invariants/assertions intact for the
    /// push path, per the plan.
    ///
    pub(crate) fn mark_known(&self, slot: Slot) {
        {
            let mut state = self.state.lock().expect("KnownSlots mutex poisoned");
            if state.set.insert(slot) {
                state.order.push_back(slot);
                while state.order.len() > state.retention {
                    if let Some(oldest) = state.order.pop_front() {
                        state.set.remove(&oldest);
                    }
                }
            }
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn contains(&self, slot: Slot) -> bool {
        self.state
            .lock()
            .expect("KnownSlots mutex poisoned")
            .set
            .contains(&slot)
    }

    ///
    /// Waits for the known-slot set to change, or a short fallback timeout, whichever comes
    /// first. Callers must re-check [`Self::contains`] after this returns -- it's a wake-up
    /// hint, not a guarantee that the slot they're waiting on is now known.
    ///
    pub(crate) async fn wait_for_change(&self) {
        let notified = self.notify.notified();
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep(KNOWN_SLOTS_POLL_FALLBACK) => {},
        }
    }
}
