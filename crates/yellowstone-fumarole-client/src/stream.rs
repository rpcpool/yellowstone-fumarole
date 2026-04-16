use {
    crate::error::FumaroleSubscribeError,
    futures::{Sink, Stream},
    std::collections::{HashMap, VecDeque},
    tokio::sync::mpsc,
    yellowstone_grpc_proto::geyser,
};

#[derive(Debug)]
pub enum FumaroleEvent {
    Data { 
        slot: u64,
        update: geyser::SubscribeUpdate 
    },
    SlotEnded(u64),
}

pub struct FumaroleSink {
    inner: mpsc::Sender<geyser::SubscribeRequest>,
}

impl FumaroleSink {
    pub(crate) fn new(inner: mpsc::Sender<geyser::SubscribeRequest>) -> Self {
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

pub struct FumaroleStream {
    inner: mpsc::Receiver<Result<FumaroleEvent, FumaroleSubscribeError>>,
}

impl FumaroleStream {
    pub(crate) fn new(
        inner: mpsc::Receiver<Result<FumaroleEvent, FumaroleSubscribeError>>,
    ) -> Self {
        Self { inner }
    }

    pub fn like_dragonsmouth(self) -> DragonsmouthLike<Self> {
        DragonsmouthLike { 
            inner: self.slot_sequential(),
        }
    }

    pub fn slot_sequential(self) -> SlotSequentialStream<Self> {
        SlotSequentialStream {
            inner: self,
            state: Default::default(),
        }
    }

    pub fn block_stream(self) -> BlockStream {
        BlockStream { inner: self }
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
            self.poll_ready.push_back(FumaroleEvent::Data { slot, update });
            return;
        }

        match self.current_slot {
            Some(current) if current == slot => {
                self.poll_ready.push_back(FumaroleEvent::Data { slot, update });
            }
            Some(_) => {
                self.buffer_data(slot, update);
            }
            None => {
                self.current_slot = Some(slot);
                self.poll_ready.push_back(FumaroleEvent::Data { slot, update });
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
pub struct SlotSequentialStream<S> {
    inner: S,
    state: SlotSequentialStreamState,
}

impl<S> Stream for SlotSequentialStream<S>
where
    S: Stream<Item = Result<FumaroleEvent, FumaroleSubscribeError>> + Unpin,
{
    type Item = S::Item;

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

// pub struct FumaroleBLock {
//     inner: Vec<geyser::SubscribeUpdate>,
// }

// /// 
// /// A stream that yields [`FumaroleBlock`] which is a vector of [`yellowstone_grpc_proto::geyser::SubscribeUpdate`].
// /// 
// pub struct BlockStream {
//     inner: FumaroleStream,
// }


impl Stream for FumaroleStream {
    type Item = Result<FumaroleEvent, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

pub struct BlockStream {
    inner: FumaroleStream,
}

impl Stream for BlockStream {
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
        futures::{pin_mut, StreamExt},
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
        tx.send(Ok(FumaroleEvent::Data {
            slot: 2,
            update: mk_entry_update(2, 1),
        }))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 1,
            update: mk_entry_update(1, 1),
        }))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 2,
            update: mk_entry_update(2, 2),
        }))
        .await
        .expect("send second data slot 2");
        tx.send(Ok(FumaroleEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 1,
            update: mk_entry_update(1, 2),
        }))
        .await
        .expect("send second data slot 1");
        tx.send(Ok(FumaroleEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1");
        drop(tx);

        let stream = FumaroleStream::new(rx).slot_sequential();
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
        tx.send(Ok(FumaroleEvent::Data {
            slot: 2,
            update: mk_entry_update(2, 1),
        }))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 1,
            update: mk_entry_update(1, 1),
        }))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1 while slot 2 active");
        tx.send(Ok(FumaroleEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        drop(tx);

        let stream = FumaroleStream::new(rx).slot_sequential();
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
        tx.send(Ok(FumaroleEvent::Data {
            slot: 2,
            update: mk_entry_update(2, 1),
        }))
        .await
        .expect("send data slot 2");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 1,
            update: mk_entry_update(1, 1),
        }))
        .await
        .expect("send data slot 1");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 99,
            update: mk_slot_update(99),
        }))
        .await
        .expect("send slot status update");
        tx.send(Ok(FumaroleEvent::Data {
            slot: 100,
            update: SubscribeUpdate {
                filters: vec![],
                created_at: None,
                update_oneof: Some(UpdateOneof::BlockMeta(Default::default())),
            },
        }))
        .await
        .expect("send block meta update");
        tx.send(Ok(FumaroleEvent::SlotEnded(2)))
            .await
            .expect("send slot ended 2");
        tx.send(Ok(FumaroleEvent::SlotEnded(1)))
            .await
            .expect("send slot ended 1");
        drop(tx);

        let stream = FumaroleStream::new(rx).slot_sequential();
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
}

///
/// A stream that yields [`geyser::SubscribeUpdate`] one by one, without any guarantee on the order of the updates.
/// 
/// Prefer to use [`SlotSequentialStream`], it yields richer data types that indicate slot boundaries.
/// 
pub struct DragonsmouthLike<S> {
    inner: SlotSequentialStream<S>,
}

impl<S> Stream for DragonsmouthLike<S>
where
    S: Stream<Item = Result<FumaroleEvent, FumaroleSubscribeError>> + Unpin,
{
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
