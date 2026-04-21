use {
    crate::{
        core::runtime::DataplaneStreamError,
        proto::{self, DataCommand, DataResponse, JoinControlPlane},
    },
    futures::{Sink, Stream},
    std::{error::Error as StdError, future::Future},
};

pub type BoxedProtocolError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub enum ControlPlaneStreamError {
    Disconnected(BoxedProtocolError),
    ApplicationError(BoxedProtocolError),
}

impl std::fmt::Display for ControlPlaneStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected(err) => write!(f, "control plane disconnected: {err}"),
            Self::ApplicationError(err) => write!(f, "control plane application error: {err}"),
        }
    }
}

impl StdError for ControlPlaneStreamError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Disconnected(err) | Self::ApplicationError(err) => Some(err.as_ref()),
        }
    }
}

pub trait ControlPlaneConnector {
    type SubscribeError: StdError + Send + Sync + 'static;

    type ControlPlaneSink: Sink<proto::ControlCommand> + Send + Unpin;
    type ControlPlaneStream: Stream<Item = Result<proto::ControlResponse, ControlPlaneStreamError>>
        + Unpin;

    type SubscribeFut: Future<
            Output = Result<
                (Self::ControlPlaneSink, Self::ControlPlaneStream),
                Self::SubscribeError,
            >,
        > + Send;

    fn subscribe(&self, initial_join: JoinControlPlane) -> Self::SubscribeFut;
}

pub(crate) trait FumaroleDataplaneConnector {
    type DataplaneSubscribeError: StdError + Send + Sync + 'static;
    type DataplaneSinkError: StdError + Send + Sync + 'static;
    type DataplaneSink: Sink<DataCommand, Error = Self::DataplaneSinkError> + Send + Unpin + 'static;
    type DataplaneStream: Stream<Item = Result<DataResponse, DataplaneStreamError>>
        + Send
        + Unpin
        + 'static;

    type DataplaneSubscribeFut: Future<
            Output = Result<
                (Self::DataplaneSink, Self::DataplaneStream),
                Self::DataplaneSubscribeError,
            >,
        > + Send;

    fn subscribe_data(&self) -> Self::DataplaneSubscribeFut;
}
