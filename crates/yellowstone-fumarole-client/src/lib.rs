//!
//! A Rust implementation of the Yellowstone Fumarole Client using Tokio and Tonic.
//!
//! Fumarole Client uses gRPC connections to communicate with the Fumarole service.
//!
//! # Yellowstone-GRPC vs Yellowstone-Fumarole
//!
//! For the most part, the API is similar to the original [`yellowstone-grpc`] client.
//!
//! However, there are some differences:
//!
//! - The `yellowstone-fumarole` client uses multiple gRPC connections to communicate with the Fumarole service : avoids [`HoL`] blocking.
//! - The `yellowstone-fumarole` subscribers are persistent and can be reused across multiple sessions (not computer).
//! - The `yellowstone-fumarole` can reconnect to the Fumarole service if the connection is lost.
//!
//! # Examples
//!
//! Examples can be found in the [`examples`] directory.
//!
//! ## Create a `FumaroleClient`
//!
//! ```ignore
//! use yellowstone_fumarole_client::FumaroleClient;
//! use yellowstone_fumarole_client::config::FumaroleConfig;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = FumaroleConfig {
//!         endpoint: "https://example.com".to_string(),
//!         x_token: Some("00000000-0000-0000-0000-000000000000".to_string()),
//!         max_decoding_message_size_bytes: FumaroleConfig::default_max_decoding_message_size_bytes(),
//!         x_metadata: Default::default(),
//!     };
//!     let fumarole_client = FumaroleClient::connect(config)
//!         .await
//!         .expect("Failed to connect to fumarole");
//! }
//! ```
//!
//! **NOTE**: The struct `FumaroleConfig` supports deserialization from a YAML file.
//!
//! Here's an example of a YAML file:
//!
//! ```yaml
//! endpoint: https://example.com
//! x-token: 00000000-0000-0000-0000-000000000000
//! ```
//! ## Dragonsmouth-like Subscribe
//!
//! ```ignore
//! use yellowstone_fumarole_client::FumaroleClient;
//!
//!
//! let mut client = FumaroleClient::connect(config).await.unwrap();
//!
//! let request = geyser::SubscribeRequest {
//!     accounts: HashMap::from([("f1".to_owned(), SubscribeRequestFilterAccounts::default())]),
//!     transactions: HashMap::from([("f1".to_owned(), SubscribeRequestFilterTransactions::default())]),
//!     ..Default::default()
//! };
//!
//!
//! let dragonsmouth_adapter = client.dragonsmouth_subscribe("my-consumer-group", request).await.unwrap();
//!
//! let DragonsmouthAdapterSession {
//!     sink: _, // Channel to update [`SubscribeRequest`] requests to the fumarole service
//!     mut source, // Channel to receive updates from the fumarole service
//!     runtime_handle: _, // Handle to the fumarole session client runtime
//! } = dragonsmouth_adapter;
//!
//! while let Some(result) = source.recv().await {
//!    let event = result.expect("Failed to receive event");
//!    // ... do something with the event
//! }
//! ```
//!
//! ## Enable Prometheus Metrics
//!
//! To enable Prometheus metrics, add the `features = [prometheus]` to your `Cargo.toml` file:
//! ```toml
//! [dependencies]
//! yellowstone-fumarole-client = { version = "x.y.z", features = ["prometheus"] }
//! ```
//!
//! Then, you can use the `metrics` module to register and expose metrics:
//!
//! ```rust
//! use yellowstone_fumarole_client::metrics;
//! use prometheus::{Registry};
//!
//! let r = Registry::new();
//!
//! metrics::register_metrics(&r);
//!
//! // After registering, you should see `fumarole_` prefixed metrics in the registry.
//! ```
//!
//! # Getting Started
//!
//! Follows the instruction in the [`README`] file to get started.
//!
//! # Feature Flags
//!
//! - `prometheus`: Enables Prometheus metrics for the Fumarole client.
//!
//! [`examples`]: https://github.com/rpcpool/yellowstone-fumarole/tree/main/examples
//! [`README`]: https://github.com/rpcpool/yellowstone-fumarole/tree/main/README.md
//! [`yellowstone-grpc`]: https://github.com/rpcpool/yellowstone-grpc
//! [`HoL`]: https://en.wikipedia.org/wiki/Head-of-line_blocking

pub mod config;

#[cfg(feature = "prometheus")]
pub mod metrics;

pub(crate) mod runtime;
pub(crate) mod util;

use {
    config::FumaroleConfig,
    futures::future::{select, Either},
    proto::control_response::Response,
    runtime::{
        tokio::{DownloadTaskRunnerChannels, GrpcDownloadTaskRunner, TokioFumeDragonsmouthRuntime},
        FumaroleSM,
    },
    std::{
        collections::HashMap,
        num::{NonZeroU8, NonZeroUsize},
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        metadata::{
            errors::{InvalidMetadataKey, InvalidMetadataValue},
            Ascii, MetadataKey, MetadataValue,
        },
        service::{interceptor::InterceptedService, Interceptor},
        transport::{Channel, ClientTlsConfig},
    },
    util::grpc::into_bounded_mpsc_rx,
};

mod solana {
    #[allow(unused_imports)]
    pub use yellowstone_grpc_proto::solana::{
        storage,
        storage::{confirmed_block, confirmed_block::*},
    };
}

mod geyser {
    pub use yellowstone_grpc_proto::geyser::*;
}

#[allow(clippy::missing_const_for_fn)]
#[allow(clippy::all)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/fumarole_v2.rs"));
}

use {
    proto::{fumarole_client::FumaroleClient as TonicFumaroleClient, JoinControlPlane},
    runtime::tokio::DataPlaneConn,
    tonic::transport::Endpoint,
};

#[derive(Clone)]
struct FumeInterceptor {
    x_token: Option<MetadataValue<Ascii>>,
    metadata: HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>,
}

impl Interceptor for FumeInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = request;
        let metadata = request.metadata_mut();
        if let Some(x_token) = &self.x_token {
            metadata.insert("x-token", x_token.clone());
        }
        for (key, value) in &self.metadata {
            metadata.insert(key.clone(), value.clone());
        }
        Ok(request)
    }
}

///
/// A builder for creating a [`FumaroleClient`].
///
#[derive(Default)]
pub struct FumaroleClientBuilder {
    pub metadata: HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>,
    pub with_compression: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidMetadataHeader {
    #[error(transparent)]
    InvalidMetadataKey(#[from] InvalidMetadataKey),
    #[error(transparent)]
    InvalidMetadataValue(#[from] InvalidMetadataValue),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    InvalidXToken(#[from] tonic::metadata::errors::InvalidMetadataValue),
    #[error(transparent)]
    InvalidMetadataHeader(#[from] InvalidMetadataHeader),
}

///
/// Default gRPC buffer capacity
///
pub const DEFAULT_DRAGONSMOUTH_CAPACITY: usize = 10000;

///
/// Default Fumarole commit offset interval
///
pub const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_secs(5);

///
/// Default maximum number of consecutive failed slot download attempts before failing the fumarole session.
///
pub const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT: usize = 3;

///
/// Default number of parallel data streams (TCP connections) to open to fumarole.
///
pub const DEFAULT_PARA_DATA_STREAMS: u8 = 3;

///
/// Default maximum number of concurrent download requests to the fumarole service inside a single data plane TCP connection.
///
pub const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP: usize = 10;

pub(crate) type GrpcFumaroleClient =
    TonicFumaroleClient<InterceptedService<Channel, FumeInterceptor>>;
///
/// Yellowstone Fumarole SDK.
///
#[derive(Clone)]
pub struct FumaroleClient {
    connector: FumaroleGrpcConnector,
    inner: GrpcFumaroleClient,
}

#[derive(Debug, thiserror::Error)]
pub enum DragonsmouthSubscribeError {
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
    #[error("grpc stream closed")]
    StreamClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum FumaroleStreamError {
    #[error(transparent)]
    Custom(Box<dyn std::error::Error + Send + Sync>),
    #[error("grpc stream closed")]
    StreamClosed,
}

///
/// Configuration for the Fumarole subscription session
///
pub struct FumaroleSubscribeConfig {
    ///
    /// Number of parallel data streams (TCP connections) to open to fumarole
    ///
    pub num_data_plane_tcp_connections: NonZeroU8,

    ///
    /// Maximum number of concurrent download requests to the fumarole service inside a single data plane TCP connection.
    ///
    pub concurrent_download_limit_per_tcp: NonZeroUsize,

    ///
    /// Commit interval for the fumarole client
    ///
    pub commit_interval: Duration,

    ///
    /// Maximum number of consecutive failed slot download attempts before failing the fumarole session.
    ///
    pub max_failed_slot_download_attempt: usize,

    ///
    /// Capacity of each data channel for the fumarole client
    ///
    pub data_channel_capacity: NonZeroUsize,
}

impl Default for FumaroleSubscribeConfig {
    fn default() -> Self {
        Self {
            num_data_plane_tcp_connections: NonZeroU8::new(DEFAULT_PARA_DATA_STREAMS).unwrap(),
            concurrent_download_limit_per_tcp: NonZeroUsize::new(
                DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
            )
            .unwrap(),
            commit_interval: DEFAULT_COMMIT_INTERVAL,
            max_failed_slot_download_attempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
            data_channel_capacity: NonZeroUsize::new(DEFAULT_DRAGONSMOUTH_CAPACITY).unwrap(),
        }
    }
}

pub enum FumeControlPlaneError {
    Disconnected,
}

pub enum FumeDataPlaneError {
    Disconnected,
}

pub enum FumaroleError {
    ControlPlaneDisconnected,
    DataPlaneDisconnected,
    InvalidSubscribeRequest,
}

impl From<tonic::Status> for FumaroleError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => FumaroleError::ControlPlaneDisconnected,
            tonic::Code::Internal => FumaroleError::DataPlaneDisconnected,
            _ => FumaroleError::InvalidSubscribeRequest,
        }
    }
}

///
/// Dragonsmouth flavor fumarole session.
/// Mimics the same API as dragonsmouth but uses fumarole as the backend.
///
pub struct DragonsmouthAdapterSession {
    ///
    /// Channel to send requests to the fumarole service.
    /// If you don't need to change the subscribe request, you can drop this channel.
    ///
    pub sink: mpsc::Sender<geyser::SubscribeRequest>,
    ///
    /// Channel to receive updates from the fumarole service.
    /// Dropping this channel will stop the fumarole session.
    ///
    pub source: mpsc::Receiver<Result<geyser::SubscribeUpdate, tonic::Status>>,
    ///
    /// Handle to the fumarole session client runtime.
    /// Dropping this handle does not stop the fumarole session.
    ///
    /// If you want to stop the fumarole session, you need to drop the [`DragonsmouthAdapterSession::source`] channel,
    /// then you could wait for the handle to finish.
    ///
    pub fumarole_handle: tokio::task::JoinHandle<()>,
}

fn string_pairs_to_metadata_header(
    headers: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
) -> Result<HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>, InvalidMetadataHeader> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let key = MetadataKey::from_bytes(k.as_ref().as_bytes())?;
            let value: MetadataValue<Ascii> = v.as_ref().try_into()?;
            Ok((key, value))
        })
        .collect()
}

impl FumaroleClient {
    pub async fn connect(config: FumaroleConfig) -> Result<FumaroleClient, ConnectError> {
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?;

        let connector = FumaroleGrpcConnector {
            config: config.clone(),
            endpoint: endpoint.clone(),
        };

        let client = connector.connect().await?;
        Ok(FumaroleClient {
            connector,
            inner: client,
        })
    }

    ///
    /// Subscribe to a stream of updates from the Fumarole service
    ///
    pub async fn dragonsmouth_subscribe<S>(
        &mut self,
        consumer_group_name: S,
        request: geyser::SubscribeRequest,
    ) -> Result<DragonsmouthAdapterSession, tonic::Status>
    where
        S: AsRef<str>,
    {
        let handle = tokio::runtime::Handle::current();
        self.dragonsmouth_subscribe_with_config_on(
            consumer_group_name,
            request,
            Default::default(),
            handle,
        )
        .await
    }

    pub async fn dragonsmouth_subscribe_with_config<S>(
        &mut self,
        consumer_group_name: S,
        request: geyser::SubscribeRequest,
        config: FumaroleSubscribeConfig,
    ) -> Result<DragonsmouthAdapterSession, tonic::Status>
    where
        S: AsRef<str>,
    {
        let handle = tokio::runtime::Handle::current();
        self.dragonsmouth_subscribe_with_config_on(consumer_group_name, request, config, handle)
            .await
    }

    ///
    /// Same as [`FumaroleClient::dragonsmouth_subscribe`] but allows you to specify a custom runtime handle
    /// the underlying fumarole runtie will use
    ///
    pub async fn dragonsmouth_subscribe_with_config_on<S>(
        &mut self,
        consumer_group_name: S,
        request: geyser::SubscribeRequest,
        config: FumaroleSubscribeConfig,
        handle: tokio::runtime::Handle,
    ) -> Result<DragonsmouthAdapterSession, tonic::Status>
    where
        S: AsRef<str>,
    {
        use {proto::ControlCommand, runtime::tokio::DragonsmouthSubscribeRequestBidi};

        let (dragonsmouth_outlet, dragonsmouth_inlet) =
            mpsc::channel(DEFAULT_DRAGONSMOUTH_CAPACITY);
        let (fume_control_plane_tx, fume_control_plane_rx) = mpsc::channel(100);

        let initial_join = JoinControlPlane {
            consumer_group_name: Some(consumer_group_name.as_ref().to_string()),
        };
        let initial_join_command = ControlCommand {
            command: Some(proto::control_command::Command::InitialJoin(initial_join)),
        };

        // IMPORTANT: Make sure we send the request here before we subscribe to the stream
        // Otherwise this will block until timeout by remote server.
        fume_control_plane_tx
            .send(initial_join_command)
            .await
            .expect("failed to send initial join");

        let resp = self
            .inner
            .subscribe(ReceiverStream::new(fume_control_plane_rx))
            .await?;

        let mut streaming = resp.into_inner();
        let fume_control_plane_tx = fume_control_plane_tx.clone();
        let control_response = streaming.message().await?.expect("none");
        let fume_control_plane_rx = into_bounded_mpsc_rx(100, streaming);
        let response = control_response.response.expect("none");
        let Response::Init(initial_state) = response else {
            panic!("unexpected initial response: {response:?}")
        };

        /* WE DON'T SUPPORT SHARDING YET */
        assert!(
            initial_state.last_committed_offsets.len() == 1,
            "sharding not supported"
        );
        let last_committed_offset = initial_state
            .last_committed_offsets
            .get(&0)
            .expect("no last committed offset");

        let sm = FumaroleSM::new(*last_committed_offset);

        let (dm_tx, dm_rx) = mpsc::channel(100);
        let dm_bidi = DragonsmouthSubscribeRequestBidi {
            tx: dm_tx.clone(),
            rx: dm_rx,
        };

        let mut data_plane_channel_vec =
            Vec::with_capacity(config.num_data_plane_tcp_connections.get() as usize);
        for _ in 0..config.num_data_plane_tcp_connections.get() {
            let client = self
                .connector
                .connect()
                .await
                .expect("failed to connect to fumarole");
            let conn = DataPlaneConn::new(client, config.concurrent_download_limit_per_tcp.get());
            data_plane_channel_vec.push(conn);
        }

        let (download_task_runner_cnc_tx, download_task_runner_cnc_rx) = mpsc::channel(10);
        // Make sure the channel capacity is really low, since the grpc runner already implements its own concurrency control
        let (download_task_queue_tx, download_task_queue_rx) = mpsc::channel(10);
        let (download_result_tx, download_result_rx) = mpsc::channel(10);
        let grpc_download_task_runner = GrpcDownloadTaskRunner::new(
            handle.clone(),
            data_plane_channel_vec,
            self.connector.clone(),
            download_task_runner_cnc_rx,
            download_task_queue_rx,
            download_result_tx,
            config.max_failed_slot_download_attempt,
        );

        let download_task_runner_chans = DownloadTaskRunnerChannels {
            download_task_queue_tx,
            cnc_tx: download_task_runner_cnc_tx,
            download_result_rx,
        };

        let tokio_rt = TokioFumeDragonsmouthRuntime {
            sm,
            dragonsmouth_bidi: dm_bidi,
            subscribe_request: request,
            download_task_runner_chans,
            consumer_group_name: consumer_group_name.as_ref().to_string(),
            control_plane_tx: fume_control_plane_tx,
            control_plane_rx: fume_control_plane_rx,
            dragonsmouth_outlet,
            commit_interval: config.commit_interval,
            last_commit: Instant::now(),
        };
        let download_task_runner_jh = handle.spawn(grpc_download_task_runner.run());
        let fumarole_rt_jh = handle.spawn(tokio_rt.run());
        let fut = async move {
            let either = select(download_task_runner_jh, fumarole_rt_jh).await;
            match either {
                Either::Left((result, _)) => {
                    let _ = result.expect("fumarole download task runner failed");
                }
                Either::Right((result, _)) => {
                    let _ = result.expect("fumarole runtime failed");
                }
            }
        };
        let fumarole_handle = handle.spawn(fut);
        let dm_session = DragonsmouthAdapterSession {
            sink: dm_tx,
            source: dragonsmouth_inlet,
            fumarole_handle,
        };
        Ok(dm_session)
    }

    pub async fn list_consumer_groups(
        &mut self,
        request: impl tonic::IntoRequest<proto::ListConsumerGroupsRequest>,
    ) -> std::result::Result<tonic::Response<proto::ListConsumerGroupsResponse>, tonic::Status>
    {
        self.inner.list_consumer_groups(request).await
    }

    pub async fn get_consumer_group_info(
        &mut self,
        request: impl tonic::IntoRequest<proto::GetConsumerGroupInfoRequest>,
    ) -> std::result::Result<tonic::Response<proto::ConsumerGroupInfo>, tonic::Status> {
        self.inner.get_consumer_group_info(request).await
    }

    pub async fn delete_consumer_group(
        &mut self,
        request: impl tonic::IntoRequest<proto::DeleteConsumerGroupRequest>,
    ) -> std::result::Result<tonic::Response<proto::DeleteConsumerGroupResponse>, tonic::Status>
    {
        self.inner.delete_consumer_group(request).await
    }

    pub async fn create_consumer_group(
        &mut self,
        request: impl tonic::IntoRequest<proto::CreateConsumerGroupRequest>,
    ) -> std::result::Result<tonic::Response<proto::CreateConsumerGroupResponse>, tonic::Status>
    {
        self.inner.create_consumer_group(request).await
    }
}

#[derive(Clone)]
pub(crate) struct FumaroleGrpcConnector {
    config: FumaroleConfig,
    endpoint: Endpoint,
}

impl FumaroleGrpcConnector {
    async fn connect(
        &self,
    ) -> Result<
        TonicFumaroleClient<InterceptedService<Channel, FumeInterceptor>>,
        tonic::transport::Error,
    > {
        let channel = self.endpoint.connect().await?;
        let interceptor = FumeInterceptor {
            x_token: self
                .config
                .x_token
                .as_ref()
                .map(|token| token.try_into())
                .transpose()
                .unwrap(),
            metadata: string_pairs_to_metadata_header(self.config.x_metadata.clone()).unwrap(),
        };
        Ok(TonicFumaroleClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(self.config.max_decoding_message_size_bytes))
    }
}
