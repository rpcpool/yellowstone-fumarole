use {
    crate::{
        FumaroleGrpcConnector,
        core::{
            ports::FumaroleDataplaneConnector,
            runtime::{DataplaneErrorKind, DataplaneStreamError},
        },
        proto::{DataCommand, DataResponse},
    },
    futures::{Future, Sink, Stream, StreamExt},
    std::{error::Error as _, pin::Pin, task::Poll},
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{Code, Streaming, transport},
};

impl From<tonic::Status> for DataplaneStreamError {
    fn from(status: tonic::Status) -> Self {
        let message = status.message().to_ascii_lowercase();
        if let Some(source) = status.source() {
            if source.downcast_ref::<transport::Error>().is_some() {
                return Self::new(
                    DataplaneErrorKind::RecoverableTransport,
                    status.to_string(),
                    Some(Box::new(status)),
                );
            }
        }

        let kind = match status.code() {
            Code::Unavailable
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::DataLoss
            | Code::Unknown
            | Code::Cancelled
            | Code::DeadlineExceeded => DataplaneErrorKind::RecoverableTransport,
            Code::NotFound => DataplaneErrorKind::SlotNotFound,
            Code::InvalidArgument if message.contains("filter") => {
                DataplaneErrorKind::InvalidSubscribeFilter
            }
            _ => DataplaneErrorKind::NonRecoverable,
        };

        Self::new(kind, status.to_string(), Some(Box::new(status)))
    }
}

type DataplaneSinkSendError = mpsc::error::SendError<DataCommand>;

fn create_dataplane_sink(
    tx: mpsc::Sender<DataCommand>,
) -> impl Sink<DataCommand, Error = DataplaneSinkSendError> + Send {
    futures::sink::unfold(tx, |tx, cmd| async move {
        tx.send(cmd).await?;
        Ok::<_, DataplaneSinkSendError>(tx)
    })
}

impl FumaroleDataplaneConnector for FumaroleGrpcConnector {
    type DataplaneSubscribeError = tonic::Status;
    type DataplaneSinkError = DataplaneSinkSendError;
    type DataplaneSink = Pin<Box<dyn Sink<DataCommand, Error = Self::DataplaneSinkError> + Send>>;
    type DataplaneStream = TonicDataplaneStreamAdapter;
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
        let mut client = self.connect_lazy();
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(100);
            let response = client.subscribe_data(ReceiverStream::new(rx)).await?;
            let sink: Self::DataplaneSink = Box::pin(create_dataplane_sink(tx));
            let stream: Self::DataplaneStream = TonicDataplaneStreamAdapter {
                inner: response.into_inner(),
            };
            Ok((sink, stream))
        })
    }
}

pub struct TonicDataplaneStreamAdapter {
    inner: Streaming<DataResponse>,
}

impl Stream for TonicDataplaneStreamAdapter {
    type Item = Result<DataResponse, DataplaneStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(response))) => Poll::Ready(Some(Ok(response))),
            Poll::Ready(Some(Err(status))) => Poll::Ready(Some(Err(DataplaneStreamError::from(status)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}