use {
    crate::{
        FumaroleGrpcConnector,
        core::{ports::FumaroleDataplaneConnectorV3, runtime::DataplaneStreamError},
        proto::{DataCommandV3, DataResponseV3, JoinDataPlane, data_command_v3},
    },
    futures::{Future, Sink, Stream, StreamExt},
    std::pin::Pin,
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::Streaming,
};

type DataplaneSinkSendError = mpsc::error::SendError<DataCommandV3>;

fn create_dataplane_sink(
    tx: mpsc::Sender<DataCommandV3>,
) -> impl Sink<DataCommandV3, Error = DataplaneSinkSendError> + Send {
    futures::sink::unfold(tx, |tx, cmd| async move {
        tx.send(cmd).await?;
        Ok::<_, DataplaneSinkSendError>(tx)
    })
}

impl FumaroleDataplaneConnectorV3 for FumaroleGrpcConnector {
    type DataplaneSubscribeError = tonic::Status;
    type DataplaneSinkError = DataplaneSinkSendError;
    type DataplaneSink = Pin<Box<dyn Sink<DataCommandV3, Error = Self::DataplaneSinkError> + Send>>;
    type DataplaneStream = TonicDataplaneStreamAdapterV3;
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

    fn subscribe_data_v3(&self, initial_join: JoinDataPlane) -> Self::DataplaneSubscribeFut {
        let mut client = self.connect_lazy();
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(100);
            // Must be sent before awaiting the call below: the server doesn't produce a
            // response until it has read this first message, so this has to already be
            // buffered on `rx` before it's handed off as the request stream, not sent
            // afterward (see the trait doc comment on `subscribe_data_v3`).
            tx.send(DataCommandV3 {
                command: Some(data_command_v3::Command::Join(initial_join)),
            })
            .await
            .expect("failed to send initial join");

            let response = client.subscribe_data_v3(ReceiverStream::new(rx)).await?;
            let sink: Self::DataplaneSink = Box::pin(create_dataplane_sink(tx));
            let stream: Self::DataplaneStream = TonicDataplaneStreamAdapterV3 {
                inner: response.into_inner(),
            };
            Ok((sink, stream))
        })
    }
}

pub struct TonicDataplaneStreamAdapterV3 {
    inner: Streaming<DataResponseV3>,
}

impl Stream for TonicDataplaneStreamAdapterV3 {
    type Item = Result<DataResponseV3, DataplaneStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(response))) => {
                std::task::Poll::Ready(Some(Ok(response)))
            }
            std::task::Poll::Ready(Some(Err(status))) => {
                std::task::Poll::Ready(Some(Err(DataplaneStreamError::from(status))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
