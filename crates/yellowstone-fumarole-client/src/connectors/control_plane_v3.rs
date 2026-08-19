use {
    crate::{
        FumaroleClient,
        core::ports::{ControlPlaneConnectorV3, ControlPlaneStreamError},
        proto::{self, ControlCommandV3, JoinControlPlaneV3},
    },
    futures::{Future, Stream, StreamExt},
    std::pin::Pin,
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::PollSender,
    tonic::{Code, Streaming},
};

pub struct GrpcControlPlaneStreamV3 {
    inner: Streaming<proto::ControlResponseV3>,
}

impl ControlPlaneConnectorV3 for FumaroleClient {
    type SubscribeError = tonic::Status;
    type ControlPlaneSink = PollSender<proto::ControlCommandV3>;
    type ControlPlaneStream = GrpcControlPlaneStreamV3;
    type SubscribeFut = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (Self::ControlPlaneSink, Self::ControlPlaneStream),
                        Self::SubscribeError,
                    >,
                > + Send,
        >,
    >;

    fn subscribe_v3(&self, initial_join: JoinControlPlaneV3) -> Self::SubscribeFut {
        let mut client = self.connector.connect_lazy();
        Box::pin(async move {
            let (control_plane_tx, control_plane_rx) = mpsc::channel(100);
            let initial_join_command = ControlCommandV3 {
                command: Some(proto::control_command_v3::Command::InitialJoin(
                    initial_join,
                )),
            };
            control_plane_tx
                .send(initial_join_command)
                .await
                .expect("failed to send initial join");

            let resp = client
                .subscribe_v3(ReceiverStream::new(control_plane_rx))
                .await?;
            let streaming = resp.into_inner();

            let streaming = GrpcControlPlaneStreamV3 { inner: streaming };

            Ok((PollSender::new(control_plane_tx), streaming))
        })
    }
}

impl Stream for GrpcControlPlaneStreamV3 {
    type Item = Result<proto::ControlResponseV3, ControlPlaneStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(resp))) => std::task::Poll::Ready(Some(Ok(resp))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(Err(status))) => {
                let err = match status.code() {
                    Code::Unavailable | Code::DataLoss | Code::Internal => {
                        ControlPlaneStreamError::Disconnected(Box::new(status))
                    }
                    _ => ControlPlaneStreamError::ApplicationError(Box::new(status)),
                };
                std::task::Poll::Ready(Some(Err(err)))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
