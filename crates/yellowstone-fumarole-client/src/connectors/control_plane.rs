use {
    crate::{
        FumaroleClient,
        core::ports::{ControlPlaneConnector, ControlPlaneStreamError},
        proto::{self, ControlCommand, JoinControlPlane},
    },
    futures::Future,
    std::pin::Pin,
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::PollSender,
    tonic::Code,
};

impl ControlPlaneConnector for FumaroleClient {
    type SubscribeError = tonic::Status;
    type ControlPlaneSink = PollSender<proto::ControlCommand>;
    type ControlPlaneStream = ReceiverStream<Result<proto::ControlResponse, ControlPlaneStreamError>>;
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

    fn subscribe(&self, initial_join: JoinControlPlane) -> Self::SubscribeFut {
        let mut client = self.inner.clone();
        Box::pin(async move {
            let (control_plane_tx, control_plane_rx) = mpsc::channel(100);
            let initial_join_command = ControlCommand {
                command: Some(proto::control_command::Command::InitialJoin(initial_join)),
            };
            control_plane_tx
                .send(initial_join_command)
                .await
                .expect("failed to send initial join");

            let resp = client.subscribe_v2(ReceiverStream::new(control_plane_rx)).await?;
            let mut streaming = resp.into_inner();

            let (bounded_tx, bounded_rx) = mpsc::channel(100);
            tokio::spawn(async move {
                while let Some(result) = streaming.message().await.transpose() {
                    let mapped = result.map_err(|status| match status.code() {
                        Code::Unavailable | Code::DataLoss | Code::Internal => {
                            ControlPlaneStreamError::Disconnected(Box::new(status))
                        }
                        _ => ControlPlaneStreamError::ApplicationError(Box::new(status)),
                    });
                    if bounded_tx.send(mapped).await.is_err() {
                        break;
                    }
                }
            });

            Ok((PollSender::new(control_plane_tx), ReceiverStream::new(bounded_rx)))
        })
    }
}