use {
    crate::{
        core::runtime::{FumaroleRuntimeCommitEvent, FumaroleRuntimeEvent},
        error::FumaroleSubscribeError,
    },
    crossbeam::queue::SegQueue,
    futures::{Sink, Stream, ready},
    std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
        task::Poll,
    },
    tokio::sync::mpsc,
    yellowstone_grpc_proto::geyser,
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum FumaroleEvent {
    Data {
        slot: u64,
        update: geyser::SubscribeUpdate,
    },
    SlotEnded(u64),
}

/// Sending half of a Fumarole subscription session.
///
/// This sink accepts [`geyser::SubscribeRequest`] values and forwards them to the
/// runtime over an internal bounded Tokio channel.
///
/// # Backpressure
///
/// The sink is backed by `mpsc::Sender::try_send`, so `start_send` can fail with
/// `resource_exhausted` when the channel is full.
///
/// # Errors
///
/// - `unavailable` when the underlying request channel is closed.
/// - `resource_exhausted` when the channel is full.
///
/// # Typical Usage
///
/// Use this sink from [`FumaroleSubscription`](crate::FumaroleSubscription) or
/// split APIs to dynamically update filters/commitment while the data stream is
/// active.
pub struct FumaroleSink {
    inner: mpsc::Sender<geyser::SubscribeRequest>,
}

impl FumaroleSink {
    pub(crate) const fn new(inner: mpsc::Sender<geyser::SubscribeRequest>) -> Self {
        Self { inner }
    }
}

impl Sink<geyser::SubscribeRequest> for FumaroleSink {
    type Error = tonic::Status;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.get_mut().inner.is_closed() {
            std::task::Poll::Ready(Err(tonic::Status::unavailable(
                "subscribe request channel is closed",
            )))
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: geyser::SubscribeRequest,
    ) -> Result<(), Self::Error> {
        match self.get_mut().inner.try_send(item) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Err(
                tonic::Status::resource_exhausted("subscribe request channel is full"),
            ),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(
                tonic::Status::unavailable("subscribe request channel is closed"),
            ),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.get_mut().inner.is_closed() {
            std::task::Poll::Ready(Err(tonic::Status::unavailable(
                "subscribe request channel is closed",
            )))
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.get_mut().inner.is_closed() {
            std::task::Poll::Ready(Err(tonic::Status::unavailable(
                "subscribe request channel is closed",
            )))
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }
}

///
/// The main fumarole stream type yielding [`FumaroleEvent`] values from the runtime.
///
pub struct FumaroleStream {
    inner: mpsc::Receiver<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
    commit_offset_queue: Arc<SegQueue<FumaroleRuntimeCommitEvent>>,
    pending_commits: VecDeque<FumaroleRuntimeCommitEvent>,
    auto_commit: bool,
}

/// Receiving half of a Fumarole subscription session.
///
/// This stream yields [`FumaroleEvent`] values wrapped in
/// [`FumaroleSubscribeError`] for transport/runtime failures.
///
/// It acts as the canonical source stream and can be adapted into richer views:
/// - [`slot_sequential`](Self::slot_sequential): enforces per-slot sequentiality.
/// - [`block_stream`](Self::block_stream): groups data into slot-scoped blocks.
/// - [`like_dragonsmouth`](Self::like_dragonsmouth): compatibility adapter that
///   yields raw `SubscribeUpdate` entries.
///
/// # Ordering
///
/// Ordering guarantees depend on the selected adapter. The base stream itself
/// forwards events as produced by the runtime.
impl FumaroleStream {
    pub(crate) const fn new(
        commit_offset_queue: Arc<SegQueue<FumaroleRuntimeCommitEvent>>,
        inner: mpsc::Receiver<Result<FumaroleRuntimeEvent, FumaroleSubscribeError>>,
        auto_commit: bool,
    ) -> Self {
        Self {
            inner,
            pending_commits: VecDeque::new(),
            auto_commit,
            commit_offset_queue,
        }
    }

    pub fn like_dragonsmouth(self) -> DragonsmouthLike {
        DragonsmouthLike {
            inner: self.slot_sequential(),
        }
    }

    pub fn slot_sequential(self) -> SlotSequentialStream {
        SlotSequentialStream {
            inner: self,
            state: Default::default(),
        }
    }

    pub fn block_stream(self) -> BlockStream {
        BlockStream {
            inner: self,
            state: Default::default(),
        }
    }
}

struct RopeDeque<T> {
    // this is a deque of deques, where each inner deque contains events from the same slot, and the outer deque is ordered by slot
    inner: VecDeque<VecDeque<T>>,
}

impl Default for RopeDeque<FumaroleEvent> {
    fn default() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }
}

impl<T> RopeDeque<T> {
    fn push_back(&mut self, item: T) {
        if let Some(back) = self.inner.back_mut() {
            back.push_back(item);
        } else {
            let mut new_back = VecDeque::new();
            new_back.push_back(item);
            self.inner.push_back(new_back);
        }
    }

    fn pop_front(&mut self) -> Option<T> {
        loop {
            if let Some(front) = self.inner.front_mut() {
                if let Some(item) = front.pop_front() {
                    return Some(item);
                } else {
                    self.inner.pop_front();
                }
            } else {
                return None;
            }
        }
    }

    fn extend_vecdeque(&mut self, items: VecDeque<T>) {
        self.inner.push_back(items);
    }

    fn is_empty(&self) -> bool {
        self.inner.iter().all(VecDeque::is_empty)
    }
}

#[derive(Default)]
struct BufferedSlotState {
    updates: VecDeque<geyser::SubscribeUpdate>,
    ended: bool,
}

#[derive(Default)]
struct SlotSequentialStreamState {
    current_slot: Option<u64>,
    buffered_slot: HashMap<u64, BufferedSlotState>,
    buffered_slot_order: VecDeque<u64>,
    poll_ready: RopeDeque<FumaroleEvent>,
}

impl SlotSequentialStreamState {
    fn buffer_data(&mut self, slot: u64, update: geyser::SubscribeUpdate) {
        let state = self.buffered_slot.entry(slot).or_default();
        if state.updates.is_empty() && !self.buffered_slot_order.contains(&slot) {
            self.buffered_slot_order.push_back(slot);
        }
        state.updates.push_back(update);
    }

    fn mark_buffered_slot_ended(&mut self, slot: u64) {
        let state = self.buffered_slot.entry(slot).or_default();
        if state.updates.is_empty() && !self.buffered_slot_order.contains(&slot) {
            self.buffered_slot_order.push_back(slot);
        }
        state.ended = true;
    }

    fn flush_next_buffered_slot(&mut self) {
        while self.current_slot.is_none() {
            let Some(slot) = self.buffered_slot_order.pop_front() else {
                return;
            };
            let Some(buffered) = self.buffered_slot.remove(&slot) else {
                continue;
            };

            if buffered.updates.is_empty() {
                if buffered.ended {
                    self.poll_ready.push_back(FumaroleEvent::SlotEnded(slot));
                }
                continue;
            }

            let mut emitted = VecDeque::new();
            for update in buffered.updates {
                emitted.push_back(FumaroleEvent::Data { slot, update });
            }

            let ended = buffered.ended;
            self.poll_ready.extend_vecdeque(emitted);
            if ended {
                self.poll_ready.push_back(FumaroleEvent::SlotEnded(slot));
            } else {
                self.current_slot = Some(slot);
            }
        }
    }

    fn handle_fumarole_ev_data(&mut self, slot: u64, update: geyser::SubscribeUpdate) {
        // Slot status and block metadata updates are guaranteed to be emitted after all data updates from the slot,
        // so we can let them pass through immediately regardless of the currently active slot.
        if matches!(
            update.update_oneof.as_ref(),
            Some(geyser::subscribe_update::UpdateOneof::Slot(_))
                | Some(geyser::subscribe_update::UpdateOneof::BlockMeta(_))
        ) {
            self.poll_ready
                .push_back(FumaroleEvent::Data { slot, update });
            return;
        }

        match self.current_slot {
            Some(current) if current == slot => {
                self.poll_ready
                    .push_back(FumaroleEvent::Data { slot, update });
            }
            Some(_) => {
                self.buffer_data(slot, update);
            }
            None => {
                self.current_slot = Some(slot);
                self.poll_ready
                    .push_back(FumaroleEvent::Data { slot, update });
            }
        }
    }

    fn handle_fumarole_ev_slot_ended(&mut self, slot: u64) {
        if self.current_slot == Some(slot) {
            self.poll_ready.push_back(FumaroleEvent::SlotEnded(slot));
            self.current_slot = None;
            self.flush_next_buffered_slot();
            return;
        }

        if self.current_slot.is_none() {
            if self.buffered_slot.contains_key(&slot) {
                self.mark_buffered_slot_ended(slot);
                self.flush_next_buffered_slot();
            } else {
                self.poll_ready.push_back(FumaroleEvent::SlotEnded(slot));
            }
            return;
        }

        self.mark_buffered_slot_ended(slot);
    }

    fn handle_fumarole_ev(&mut self, event: FumaroleEvent) {
        match event {
            FumaroleEvent::Data { slot, update } => {
                self.handle_fumarole_ev_data(slot, update);
            }
            FumaroleEvent::SlotEnded(slot) => {
                self.handle_fumarole_ev_slot_ended(slot);
            }
        }
    }

    fn poll_next(&mut self) -> Option<Result<FumaroleEvent, FumaroleSubscribeError>> {
        if let Some(ev) = self.poll_ready.pop_front() {
            return Some(Ok(ev));
        }
        None
    }
}

///
/// A streams that yeild [`FumaroleEvent`] in slot order, meaning that while a slot is active, only events from that
/// slot will be yielded, and once the slot ends, events from the next slot will be yielded, and so on. So while a slot
/// has not ended, event's from that slot won't be interleaved with events from other slots.
///
/// This is usefulfor consumers that want to process events in slot order and don't care about processing events from
/// multiple slots concurrently.
///
/// If fumarole download two slots in parallel, (when replaying from the past), say the slot 1, 2. Then whatever slot
/// gives yield the first event, say slot 2, will be the only slot that is yielded until it ends, and then events from
/// slot 1 will be yielded.
///
pub struct SlotSequentialStream {
    inner: FumaroleStream,
    state: SlotSequentialStreamState,
}

impl SlotSequentialStream {
    ///
    /// See [`FumaroleStream::commit`] for more details.
    ///
    pub fn commit(&mut self) {
        self.inner.commit();
    }
}

/// Stream adapter that enforces slot-local sequential emission.
///
/// Given an input stream of [`FumaroleEvent`], this adapter ensures that regular
/// data events from one active slot are emitted contiguously until that slot
/// ends. Data from other slots is buffered until the active slot emits
/// [`FumaroleEvent::SlotEnded`].
///
/// `Slot` status and `BlockMeta` updates are intentionally passed through
/// immediately, because runtime semantics guarantee they are emitted after block
/// payload data for the slot.
///
/// # Generic Parameter
///
/// `S` is any stream yielding `Result<FumaroleEvent, FumaroleSubscribeError>`.
/// This allows composing adapters on top of `FumaroleStream` or custom sources.
///
/// # Use Cases
///
/// Useful for consumers that want deterministic per-slot processing without
/// interleaving payload updates from concurrent slot downloads.
///
impl Stream for SlotSequentialStream {
    type Item = Result<FumaroleEvent, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(item) = self.state.poll_next() {
                return std::task::Poll::Ready(Some(item));
            }

            match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => {
                    self.state.handle_fumarole_ev(event);
                    continue;
                }
                std::task::Poll::Ready(Some(Err(err))) => {
                    return std::task::Poll::Ready(Some(Err(err)));
                }
                std::task::Poll::Ready(None) => {
                    if let Some(item) = self.state.poll_next() {
                        return std::task::Poll::Ready(Some(item));
                    }
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    if self.state.poll_ready.is_empty() {
                        return std::task::Poll::Pending;
                    }
                }
            }
        }
    }
}

impl TryFrom<FumaroleRuntimeEvent> for FumaroleEvent {
    type Error = FumaroleRuntimeEvent;

    fn try_from(ev: FumaroleRuntimeEvent) -> Result<Self, Self::Error> {
        match ev {
            FumaroleRuntimeEvent::Data(data) => Ok(FumaroleEvent::Data {
                slot: data.slot,
                update: data.update,
            }),
            FumaroleRuntimeEvent::SlotEnded(slot) => Ok(FumaroleEvent::SlotEnded(slot)),
            other => Err(other),
        }
    }
}

impl FumaroleStream {
    ///
    /// Commits all pending progress to the fumarole service.
    ///
    /// If `auto_commit` is enabled, this method is called automatically after every slot ends or new commitment level update.
    ///
    /// If `auto_commit` is disabled, this method needs to be called manually to commit progress to the fumarole service.
    /// In this case, the client is responsible for deciding when to commit progress, which can be useful for advanced use cases.
    ///
    ///
    pub fn commit(&mut self) {
        self.pending_commits.drain(..).for_each(|commit| {
            self.commit_offset_queue.push(commit);
        });
    }
}

impl Stream for FumaroleStream {
    type Item = Result<FumaroleEvent, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let maybe = ready!(self.inner.poll_recv(cx));
            let Some(result) = maybe else {
                return Poll::Ready(None);
            };

            match result {
                Ok(ev) => match FumaroleEvent::try_from(ev) {
                    Ok(ev) => return Poll::Ready(Some(Ok(ev))),
                    Err(other) => match other {
                        FumaroleRuntimeEvent::Committable(commit) => {
                            self.pending_commits.push_back(commit);
                            if self.auto_commit {
                                self.commit();
                            }
                            continue;
                        }
                        _ => unreachable!("try_from should only fail for commit events"),
                    },
                },
                Err(e) => {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum FumaroleBlockStreamEvent {
    Block(FumaroleBlockEvent),
    SlotStatus(FumaroleSlotStatusEvent),
}

#[derive(Debug)]
pub struct FumaroleBlockEvent {
    pub slot: u64,
    updates: Vec<geyser::SubscribeUpdate>,
}

///
/// An iterator over the updates contained in a [`FumaroleBlockStreamEvent`].
///
pub struct FumaroleBlockIterator {
    curr: usize,
    inner: Vec<geyser::SubscribeUpdate>,
}

impl Iterator for FumaroleBlockIterator {
    type Item = geyser::SubscribeUpdate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.inner.len() {
            None
        } else {
            let item = self.inner[self.curr].clone();
            self.curr += 1;
            Some(item)
        }
    }
}

impl IntoIterator for FumaroleBlockEvent {
    type Item = geyser::SubscribeUpdate;
    type IntoIter = FumaroleBlockIterator;

    fn into_iter(self) -> Self::IntoIter {
        FumaroleBlockIterator {
            curr: 0,
            inner: self.updates,
        }
    }
}

///
/// An iterator over the references of updates contained in a [`FumaroleBlockEvent`].
///
pub struct FumaroleBlockIter<'a> {
    curr: usize,
    inner: &'a [geyser::SubscribeUpdate],
}

impl<'a> Iterator for FumaroleBlockIter<'a> {
    type Item = &'a geyser::SubscribeUpdate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr >= self.inner.len() {
            None
        } else {
            let item = &self.inner[self.curr];
            self.curr += 1;
            Some(item)
        }
    }
}

impl FumaroleBlockEvent {
    #[allow(clippy::missing_const_for_fn)]
    pub fn iter(&self) -> FumaroleBlockIter<'_> {
        FumaroleBlockIter {
            curr: 0,
            inner: &self.updates,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlotStatusUpdateLense {
    inner: geyser::SubscribeUpdate,
}

impl SlotStatusUpdateLense {
    const unsafe fn new_unchecked(update: geyser::SubscribeUpdate) -> Self {
        Self { inner: update }
    }

    ///
    /// Focuses the lense on the [`geyser::SubscribeUpdateSlot`].
    ///
    pub fn focus(&self) -> &geyser::SubscribeUpdateSlot {
        match self.inner.update_oneof.as_ref() {
            Some(geyser::subscribe_update::UpdateOneof::Slot(slot)) => slot,
            _ => panic!("not a slot update"),
        }
    }

    ///
    /// Returns the root [`geyser::SubscribeUpdate`] the lense can focus on.
    ///
    pub const fn inner_ref(&self) -> &geyser::SubscribeUpdate {
        &self.inner
    }

    ///
    /// Consumes the lense and returns the root [`geyser::SubscribeUpdate`] the lense can focus on.
    ///
    pub fn into_inner(self) -> geyser::SubscribeUpdate {
        self.inner
    }

    ///
    /// Consumes the lense and returns the focused [`geyser::SubscribeUpdateSlot`].
    ///
    pub fn into_focused(self) -> geyser::SubscribeUpdateSlot {
        match self.inner.update_oneof {
            Some(geyser::subscribe_update::UpdateOneof::Slot(slot)) => slot,
            _ => panic!("not a slot update"),
        }
    }
}

#[derive(Debug)]
pub struct FumaroleSlotStatusEvent {
    pub slot: u64,
    pub lense: SlotStatusUpdateLense,
}

#[derive(Default)]
struct BlockStreamState {
    buffered_block_updates: HashMap<u64, VecDeque<geyser::SubscribeUpdate>>,
    poll_ready: VecDeque<FumaroleBlockStreamEvent>,
}

impl BlockStreamState {
    fn handle_fumarole_ev_data(&mut self, slot: u64, update: geyser::SubscribeUpdate) {
        match update.update_oneof.as_ref() {
            Some(geyser::subscribe_update::UpdateOneof::Slot(_)) => {
                self.poll_ready
                    .push_back(FumaroleBlockStreamEvent::SlotStatus(
                        FumaroleSlotStatusEvent {
                            slot,
                            lense: unsafe { SlotStatusUpdateLense::new_unchecked(update) },
                        },
                    ));
            }
            _ => {
                self.buffered_block_updates
                    .entry(slot)
                    .or_default()
                    .push_back(update);
            }
        }
    }

    fn handle_fumarole_ev_slot_ended(&mut self, slot: u64) {
        let updates = self
            .buffered_block_updates
            .remove(&slot)
            .unwrap_or_default()
            .into_iter()
            .collect();
        self.poll_ready
            .push_back(FumaroleBlockStreamEvent::Block(FumaroleBlockEvent {
                slot,
                updates,
            }));
    }

    fn handle_fumarole_ev(&mut self, event: FumaroleEvent) {
        match event {
            FumaroleEvent::Data { slot, update } => self.handle_fumarole_ev_data(slot, update),
            FumaroleEvent::SlotEnded(slot) => self.handle_fumarole_ev_slot_ended(slot),
        }
    }

    fn poll_next(&mut self) -> Option<Result<FumaroleBlockStreamEvent, FumaroleSubscribeError>> {
        self.poll_ready.pop_front().map(Ok)
    }
}

pub struct BlockStream {
    inner: FumaroleStream,
    state: BlockStreamState,
}

impl BlockStream {
    ///
    /// See [`FumaroleStream::commit`] for more details.
    ///
    pub fn commit(&mut self) {
        self.inner.commit();
    }
}

/// Stream adapter that groups payload updates into slot-scoped blocks.
///
/// The adapter buffers regular data updates by slot (concurrently across slots).
/// When [`FumaroleEvent::SlotEnded(slot)`] arrives, the buffered payload updates
/// for that slot are emitted as [`FumaroleBlockStreamEvent::Block`].
///
/// `Slot` status and `BlockMeta` updates are surfaced immediately as
/// [`FumaroleBlockStreamEvent::SlotStatus`] and
/// [`FumaroleBlockStreamEvent::BlockMeta`] respectively.
///
/// # Generic Parameter
///
/// `S` is any stream yielding `Result<FumaroleEvent, FumaroleSubscribeError>`.
///
/// # Notes
///
/// This adapter is intended for block-oriented consumers that want explicit block
/// boundaries while still receiving slot-status and block-meta events as soon as
/// they appear.
///
impl Stream for BlockStream {
    type Item = Result<FumaroleBlockStreamEvent, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(item) = self.state.poll_next() {
                return std::task::Poll::Ready(Some(item));
            }

            match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => {
                    self.state.handle_fumarole_ev(event);
                    continue;
                }
                std::task::Poll::Ready(Some(Err(err))) => {
                    return std::task::Poll::Ready(Some(Err(err)));
                }
                std::task::Poll::Ready(None) => {
                    if let Some(item) = self.state.poll_next() {
                        return std::task::Poll::Ready(Some(item));
                    }
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    if self.state.poll_ready.is_empty() {
                        return std::task::Poll::Pending;
                    }
                }
            }
        }
    }
}

///
/// A stream that yields [`geyser::SubscribeUpdate`] one by one, without any guarantee on the order of the updates.
///
/// Prefer to use [`SlotSequentialStream`], it yields richer data types that indicate slot boundaries.
///
pub struct DragonsmouthLike {
    inner: SlotSequentialStream,
}

impl DragonsmouthLike {
    ///
    /// See [`FumaroleStream::commit`] for more details.
    ///
    pub fn commit(&mut self) {
        self.inner.commit();
    }
}

/// Compatibility adapter exposing a Dragonsmouth-like stream shape.
///
/// This adapter consumes a slot-sequential stream and yields only raw
/// [`geyser::SubscribeUpdate`] values, filtering out explicit slot boundary
/// markers (`SlotEnded`).
///
/// # Generic Parameter
///
/// `S` is any stream yielding `Result<FumaroleEvent, FumaroleSubscribeError>`.
///
/// # Behavior
///
/// - `FumaroleEvent::Data` -> forwarded as `Ok(SubscribeUpdate)`
/// - `FumaroleEvent::SlotEnded` -> skipped
/// - errors -> forwarded unchanged
///
/// This is useful when migrating existing Dragonsmouth consumers that are not
/// yet aware of explicit slot boundary events.
///
impl Stream for DragonsmouthLike {
    type Item = Result<geyser::SubscribeUpdate, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(FumaroleEvent::Data { slot: _, update }))) => {
                    return std::task::Poll::Ready(Some(Ok(update)));
                }
                std::task::Poll::Ready(Some(Ok(FumaroleEvent::SlotEnded(_)))) => {
                    continue;
                }
                std::task::Poll::Ready(Some(Err(err))) => {
                    return std::task::Poll::Ready(Some(Err(err)));
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::core::runtime::FumaroleRuntimeDataEvent,
        futures::{StreamExt, pin_mut},
        tokio::sync::mpsc,
        yellowstone_grpc_proto::geyser::{
            SubscribeUpdate, SubscribeUpdateEntry, SubscribeUpdateSlot,
            subscribe_update::UpdateOneof,
        },
    };

    fn mk_entry_update(slot: u64, index: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Entry(SubscribeUpdateEntry {
                slot,
                index,
                num_hashes: 0,
                hash: vec![],
                executed_transaction_count: 0,
                starting_transaction_index: 0,
            })),
        }
    }

    fn mk_slot_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: None,
                status: 0,
                dead_error: None,
            })),
        }
    }

    #[tokio::test]
    async fn slot_sequential_keeps_slot_events_grouped_until_slot_end() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 1),
        })))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 1,
            update: mk_entry_update(1, 1),
        })))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 2),
        })))
        .await
        .expect("send second data slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 1,
            update: mk_entry_update(1, 2),
        })))
        .await
        .expect("send second data slot 1");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1");
        drop(tx);

        let stream = FumaroleStream::new(Default::default(), rx, true).slot_sequential();
        pin_mut!(stream);

        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item.expect("stream should yield ok") {
                FumaroleEvent::Data { slot, .. } => got.push(format!("d{slot}")),
                FumaroleEvent::SlotEnded(slot) => got.push(format!("e{slot}")),
            }
        }

        assert_eq!(got, vec!["d2", "d2", "e2", "d1", "d1", "e1"]);
    }

    #[tokio::test]
    async fn slot_sequential_buffers_other_slot_end_until_turn() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 1),
        })))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 1,
            update: mk_entry_update(1, 1),
        })))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1 while slot 2 active");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        drop(tx);

        let stream = FumaroleStream::new(Default::default(), rx, true).slot_sequential();
        pin_mut!(stream);

        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item.expect("stream should yield ok") {
                FumaroleEvent::Data { slot, .. } => got.push(format!("d{slot}")),
                FumaroleEvent::SlotEnded(slot) => got.push(format!("e{slot}")),
            }
        }

        assert_eq!(got, vec!["d2", "e2", "d1", "e1"]);
    }

    #[tokio::test]
    async fn slot_sequential_passes_through_slot_and_block_meta_updates() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 1),
        })))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 1,
            update: mk_entry_update(1, 1),
        })))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 99,
            update: mk_slot_update(99),
        })))
        .await
        .expect("send slot status update");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 100,
            update: SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(UpdateOneof::BlockMeta(Default::default())),
            },
        })))
        .await
        .expect("send block meta update");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1");
        drop(tx);

        let stream = FumaroleStream::new(Default::default(), rx, true).slot_sequential();
        pin_mut!(stream);

        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item.expect("stream should yield ok") {
                FumaroleEvent::Data { slot, .. } => got.push(format!("d{slot}")),
                FumaroleEvent::SlotEnded(slot) => got.push(format!("e{slot}")),
            }
        }

        assert_eq!(got, vec!["d2", "d99", "d100", "e2", "d1", "e1"]);
    }

    #[tokio::test]
    async fn block_stream_buffers_by_slot_and_emits_block_on_slot_end() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 1),
        })))
        .await
        .expect("send entry slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 1,
            update: mk_entry_update(1, 1),
        })))
        .await
        .expect("send entry slot 1");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 2),
        })))
        .await
        .expect("send second entry slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1");
        drop(tx);

        let stream = FumaroleStream::new(Default::default(), rx, true).block_stream();
        pin_mut!(stream);

        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item.expect("block stream should yield ok") {
                FumaroleBlockStreamEvent::Block(FumaroleBlockEvent { slot, updates }) => {
                    got.push(format!("b{slot}:{}", updates.len()))
                }
                FumaroleBlockStreamEvent::SlotStatus(FumaroleSlotStatusEvent { slot, .. }) => {
                    got.push(format!("s{slot}"))
                }
            }
        }

        assert_eq!(got, vec!["b2:2", "b1:1"]);
    }

    #[tokio::test]
    async fn block_stream_passes_through_slot_status_and_block_meta() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 2,
            update: mk_entry_update(2, 1),
        })))
        .await
        .expect("send entry slot 2");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 99,
            update: mk_slot_update(99),
        })))
        .await
        .expect("send slot status");
        tx.send(Ok(FumaroleRuntimeEvent::Data(FumaroleRuntimeDataEvent {
            slot: 100,
            update: SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(UpdateOneof::BlockMeta(Default::default())),
            },
        })))
        .await
        .expect("send block meta");
        tx.send(Ok(FumaroleRuntimeEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        drop(tx);

        let stream = FumaroleStream::new(Default::default(), rx, true).block_stream();
        pin_mut!(stream);

        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            match item.expect("block stream should yield ok") {
                FumaroleBlockStreamEvent::Block(FumaroleBlockEvent { slot, updates }) => {
                    got.push(format!("b{slot}:{}", updates.len()))
                }
                FumaroleBlockStreamEvent::SlotStatus(FumaroleSlotStatusEvent { slot, .. }) => {
                    got.push(format!("s{slot}"))
                }
            }
        }

        assert_eq!(got, vec!["s99", "b2:1"]);
    }
}
