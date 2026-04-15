use {
    crate::error::FumaroleSubscribeError,
    futures::{Sink, Stream},
    tokio::sync::mpsc,
    yellowstone_grpc_proto::geyser,
};

#[derive(Debug)]
pub enum FumaroleEvent {
    Data(geyser::SubscribeUpdate),
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

    pub fn like_dragonsmouth(self) -> DragonsmouthLike {
        DragonsmouthLike { inner: self }
    }
}

impl Stream for FumaroleStream {
    type Item = Result<FumaroleEvent, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

pub struct DragonsmouthLike {
    inner: FumaroleStream,
}

impl Stream for DragonsmouthLike {
    type Item = Result<geyser::SubscribeUpdate, FumaroleSubscribeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(FumaroleEvent::Data(update)))) => {
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
