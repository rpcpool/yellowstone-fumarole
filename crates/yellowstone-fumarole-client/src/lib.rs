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
    proto::control_response::Response,
    runtime::{
        tokio::{DataPlaneBidi, DataPlaneBidiFactory, TokioFumeDragonsmouthRuntime},
        FumaroleSM,
    },
    std::{
        collections::{HashMap, VecDeque},
        num::NonZeroU8,
        sync::Arc,
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

use proto::{fumarole_client::FumaroleClient as TonicFumaroleClient, JoinControlPlane};

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
pub const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT: u8 = 3;

///
/// Default number of parallel data streams (TCP connections) to open to fumarole.
///
pub const DEFAULT_PARA_DATA_STREAMS: u8 = 3;

///
/// Yellowstone Fumarole gRPC Client
///
#[derive(Clone)]
pub struct FumaroleClient {
    inner: TonicFumaroleClient<InterceptedService<Channel, FumeInterceptor>>,
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
    pub num_data_streams: NonZeroU8,
    ///
    /// Commit interval for the fumarole client
    ///
    pub commit_interval: Duration,
    ///
    /// Maximum number of consecutive failed slot download attempts before failing the fumarole session.
    ///
    pub max_failed_slot_download_attempt: u8,
    ///
    /// Capacity of each data channel for the fumarole client
    ///
    pub data_channel_capacity: usize,
}

impl Default for FumaroleSubscribeConfig {
    fn default() -> Self {
        Self {
            num_data_streams: NonZeroU8::new(DEFAULT_PARA_DATA_STREAMS).unwrap(),
            commit_interval: DEFAULT_COMMIT_INTERVAL,
            max_failed_slot_download_attempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
            data_channel_capacity: DEFAULT_DRAGONSMOUTH_CAPACITY,
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
    pub runtime_handle:
        tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
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
        let channel = Channel::from_shared(config.endpoint.clone())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;

        Self::connect_with_channel(config, channel).await
    }

    pub async fn connect_with_channel(
        config: FumaroleConfig,
        channel: tonic::transport::Channel,
    ) -> Result<FumaroleClient, ConnectError> {
        let interceptor = FumeInterceptor {
            x_token: config
                .x_token
                .map(|token: String| token.try_into())
                .transpose()?,
            metadata: string_pairs_to_metadata_header(config.x_metadata)?,
        };

        let client = TonicFumaroleClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(config.max_decoding_message_size_bytes);

        Ok(FumaroleClient { inner: client })
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
        let data_bidi_factory = GrpcDataPlaneBidiFactory {
            client: self.clone(),
            channel_capacity: config.data_channel_capacity,
        };

        let mut data_bidi_vec = VecDeque::with_capacity(config.num_data_streams.get() as usize);
        for _ in 0..config.num_data_streams.get() {
            let data_bidi = data_bidi_factory.build().await;
            data_bidi_vec.push_back(data_bidi);
        }

        let (dm_tx, dm_rx) = mpsc::channel(100);
        let dm_bidi = DragonsmouthSubscribeRequestBidi {
            tx: dm_tx.clone(),
            rx: dm_rx,
        };

        let tokio_rt = TokioFumeDragonsmouthRuntime {
            rt: handle.clone(),
            sm,
            data_plane_bidi_factory: Arc::new(data_bidi_factory),
            dragonsmouth_bidi: dm_bidi,
            subscribe_request: request,
            consumer_group_name: consumer_group_name.as_ref().to_string(),
            control_plane_tx: fume_control_plane_tx,
            control_plane_rx: fume_control_plane_rx,
            data_plane_bidi_vec: data_bidi_vec,
            data_plane_tasks: Default::default(),
            data_plane_task_meta: Default::default(),
            dragonsmouth_outlet,
            download_to_retry: Default::default(),
            download_attempts: Default::default(),
            max_slot_download_attempt: config.max_failed_slot_download_attempt,
            commit_interval: config.commit_interval,
            last_commit: Instant::now(),
        };

        let jh = handle.spawn(tokio_rt.run());
        let dm_session = DragonsmouthAdapterSession {
            sink: dm_tx,
            source: dragonsmouth_inlet,
            runtime_handle: jh,
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

pub(crate) struct GrpcDataPlaneBidiFactory {
    client: FumaroleClient,
    channel_capacity: usize,
}

#[async_trait::async_trait]
impl DataPlaneBidiFactory for GrpcDataPlaneBidiFactory {
    async fn build(&self) -> DataPlaneBidi {
        let mut client = self.client.clone();
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let rx = ReceiverStream::new(rx);
        let resp = client
            .inner
            .subscribe_data(rx)
            .await
            .expect("failed to subscribe");
        let streaming = resp.into_inner();

        let rx = into_bounded_mpsc_rx(self.channel_capacity, streaming);

        DataPlaneBidi { tx, rx }
    }
}
