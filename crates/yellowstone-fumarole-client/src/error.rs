pub use crate::runtime::tokio::{DataplaneErrorKind, DataplaneStreamError};

pub type BoxedStdError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum InvalidMetadataHeader {
    #[error("invalid metadata key '{key}'")]
    InvalidMetadataKey {
        key: String,
        #[source]
        source: Option<BoxedStdError>,
    },
    #[error("invalid metadata value for key '{key}'")]
    InvalidMetadataValue {
        key: String,
        #[source]
        source: Option<BoxedStdError>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid endpoint URI: {endpoint}")]
    InvalidEndpoint {
        endpoint: String,
        #[source]
        source: Option<BoxedStdError>,
    },
    #[error("failed to configure TLS")]
    TlsConfiguration {
        #[source]
        source: Option<BoxedStdError>,
    },
    #[error("transport connection failed")]
    Transport {
        #[source]
        source: Option<BoxedStdError>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum FumaroleSubscribeError {
    #[error("control plane disconnected")]
    ControlPlaneDisconnected,
    #[error("control plane rejoin failed")]
    ControlPlaneRejoinFailed {
        #[source]
        details: Option<BoxedStdError>,
    },
    #[error(transparent)]
    DataPlaneStreamError(#[from] DataplaneStreamError),
}
