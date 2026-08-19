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

///
/// V3 (push-based) control plane connector. Sibling of [`ControlPlaneConnector`] rather than an
/// extension of it, since V3 uses its own RPC (`SubscribeV3`) and message envelopes
/// (`ControlCommandV3`/`ControlResponseV3`) instead of the V1/V2 pull-shaped ones.
///
pub(crate) trait ControlPlaneConnectorV3 {
    type SubscribeError: StdError + Send + Sync + 'static;

    type ControlPlaneSink: Sink<proto::ControlCommandV3> + Send + Unpin;
    type ControlPlaneStream: Stream<Item = Result<proto::ControlResponseV3, ControlPlaneStreamError>>
        + Unpin;

    type SubscribeFut: Future<
            Output = Result<
                (Self::ControlPlaneSink, Self::ControlPlaneStream),
                Self::SubscribeError,
            >,
        > + Send;

    fn subscribe_v3(&self, initial_join: proto::JoinControlPlaneV3) -> Self::SubscribeFut;
}

///
/// V3 (push-based) data plane connector. Sibling of [`FumaroleDataplaneConnector`] — see
/// [`ControlPlaneConnectorV3`] for why this isn't just an extension of the V1 trait.
///
pub(crate) trait FumaroleDataplaneConnectorV3 {
    type DataplaneSubscribeError: StdError + Send + Sync + 'static;
    type DataplaneSinkError: StdError + Send + Sync + 'static;
    type DataplaneSink: Sink<proto::DataCommandV3, Error = Self::DataplaneSinkError>
        + Send
        + Unpin
        + 'static;
    type DataplaneStream: Stream<Item = Result<proto::DataResponseV3, DataplaneStreamError>>
        + Send
        + Unpin
        + 'static;

    type DataplaneSubscribeFut: Future<
            Output = Result<
                (Self::DataplaneSink, Self::DataplaneStream),
                Self::DataplaneSubscribeError,
            >,
        > + Send;

    /// Opens one data-plane lane. `initial_join` must be sent by the implementation *before*
    /// awaiting the RPC's response, not afterward -- the V3 data-plane server doesn't produce a
    /// response until it has read the client's first (`Join`) message, so sending it only after
    /// the call resolves deadlocks (bounded only by the server's initial-message timeout). This
    /// is the same reason `ControlPlaneConnectorV3::subscribe_v3` takes `initial_join` directly
    /// rather than leaving it to the caller to send over the returned sink. `StartDataPlane` has
    /// no such constraint (the server reads it later, from a spawned task) and is sent by the
    /// caller as a regular message over the returned sink.
    fn subscribe_data_v3(&self, initial_join: proto::JoinDataPlane) -> Self::DataplaneSubscribeFut;
}
