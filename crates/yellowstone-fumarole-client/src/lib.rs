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
//! - The `yellowstone-fumarole` (Coming soon) client uses multiple gRPC connections to communicate with the Fumarole service : avoids [`HoL`] blocking.
//! - The `yellowstone-fumarole` subscribers are persistent and can be reused across multiple sessions (not computer).
//! - The `yellowstone-fumarole` can reconnect to the Fumarole service if the connection is lost.
//!
//! # Examples
//!
//! Examples can be found in the [`examples`] directory.
//!
//! ## Create a `FumaroleClient`
//!
//! To create a `FumaroleClient`, you need to provide a configuration object.
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
//! The prefered way to create `FumaroleConfig` is use `serde_yaml` to deserialize from a YAML file.
//!
//! ```ignore
//! let config_file = std::fs::File::open("path/to/config.yaml").unwrap();
//! let config: FumaroleConfig = serde_yaml::from_reader(config_file).unwrap();
//! ```
//!
//! Here's an example of a YAML file:
//!
//! ```yaml
//! endpoint: https://example.com
//! x-token: 00000000-0000-0000-0000-000000000000
//! response_compression: zstd
//! ```
//!
//!
//! ## Dragonsmouth-like Subscribe
//!
//! ```rust
//! use {
//!     clap::Parser,
//!     solana_sdk::{bs58, pubkey::Pubkey},
//!     std::{collections::HashMap, path::PathBuf},
//!     yellowstone_fumarole_client::{
//!         config::FumaroleConfig, DragonsmouthAdapterSession, FumaroleClient,
//!     },
//!     yellowstone_grpc_proto::geyser::{
//!         subscribe_update::UpdateOneof, SubscribeRequest,
//!         SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateTransaction,
//!     },
//! };
//!
//! #[derive(Debug, Clone, Parser)]
//! #[clap(author, version, about = "Yellowstone Fumarole Example")]
//! struct Args {
//!     /// Path to static config file
//!     #[clap(long)]
//!     config: PathBuf,
//!
//!     #[clap(subcommand)]
//!     action: Action,
//! }
//!
//! #[derive(Debug, Clone, Parser)]
//! enum Action {
//!     /// Subscribe to fumarole events
//!     Subscribe(SubscribeArgs),
//! }
//!
//! #[derive(Debug, Clone, Parser)]
//! struct SubscribeArgs {
//!     /// Name of the persistent subscriber to use
//!     #[clap(long)]
//!     name: String,
//! }
//!
//! fn summarize_account(account: SubscribeUpdateAccount) -> Option<String> {
//!     let slot = account.slot;
//!     let account = account.account?;
//!     let pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
//!     let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
//!     Some(format!("account,{},{},{}", slot, pubkey, owner))
//! }
//!
//! fn summarize_tx(tx: SubscribeUpdateTransaction) -> Option<String> {
//!     let slot = tx.slot;
//!     let tx = tx.transaction?;
//!     let sig = bs58::encode(tx.signature).into_string();
//!     Some(format!("tx,{slot},{sig}"))
//! }
//!
//! async fn subscribe(args: SubscribeArgs, config: FumaroleConfig) {
//!     // This request listen for all account updates and transaction updates
//!     let request = SubscribeRequest {
//!         transactions: HashMap::from([(
//!             "f1".to_owned(),
//!             SubscribeRequestFilterTransactions::default(),
//!         )]),
//!         ..Default::default()
//!     };
//!
//!     let mut fumarole_client = FumaroleClient::connect(config)
//!         .await
//!         .expect("Failed to connect to fumarole");
//!
//!     let dragonsmouth_session = fumarole_client
//!         .dragonsmouth_subscribe(args.name, request)
//!         .await
//!         .expect("Failed to subscribe");
//!
//!     let DragonsmouthAdapterSession {
//!         sink: _,
//!         mut source,
//!         mut fumarole_handle,
//!     } = dragonsmouth_session;
//!     
//!     loop {
//!
//!         tokio::select! {
//!             result = &mut fumarole_handle => {
//!                 eprintln!("Fumarole handle closed: {:?}", result);
//!                 break;
//!             }
//!             maybe = source.recv() => {
//!                 match maybe {
//!                     None => {
//!                         eprintln!("Source closed");
//!                         break;
//!                     }
//!                     Some(result) => {
//!                         let event = result.expect("Failed to receive event");
//!                         let message = if let Some(oneof) = event.update_oneof {
//!                             match oneof {
//!                                 UpdateOneof::Account(account_update) => {
//!                                     summarize_account(account_update)
//!                                 }
//!                                 UpdateOneof::Transaction(tx) => {
//!                                     summarize_tx(tx)
//!                                 }                    
//!                                 _ => None,
//!                             }
//!                         } else {
//!                             None
//!                         };
//!
//!                         if let Some(message) = message {
//!                             println!("{}", message);
//!                         }
//!                     }
//!                 }
//!             }
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let args: Args = Args::parse();
//!     let config = std::fs::read_to_string(&args.config).expect("Failed to read config file");
//!     let config: FumaroleConfig =
//!         serde_yaml::from_str(&config).expect("Failed to parse config file");
//!
//!     match args.action {
//!         Action::Subscribe(sub_args) => {
//!             subscribe(sub_args, config).await;
//!         }
//!     }
//! }
//! ```
//!
//! ## High-traffic workload: parallel subscription + zstd
//!
//! For high-traffic workload or for higher latency connection, using parallel subscription and zstd will greatly improve performance to stay on-tip.
//!
//! Inside your `config.yaml` file enable compression with `response_compression: zstd`:
//!
//! ```yaml
//! endpoint: https://fumarole.endpoint.rpcpool.com
//! x-token: 00000000-0000-0000-0000-000000000000
//! response_compression: zstd
//! ```
//!
//! Uses `FumaroleSubscribeConfig` to configure the parallel subscription and zstd compression.
//!
//! ```rust
//! let config: FumaroleConfig = serde_yaml::from_reader("<path/to/config.yaml>").expect("failed to parse fumarole config");
//!
//! let request = SubscribeRequest {
//!    transactions: HashMap::from([(
//!        "f1".to_owned(),
//!        SubscribeRequestFilterTransactions::default(),
//!    )]),
//!    ..Default::default()
//! };
//!
//!
//! let mut fumarole_client = FumaroleClient::connect(config)
//!    .await
//!    .expect("Failed to connect to fumarole");
//!
//! let subscribe_config = FumaroleSubscribeConfig {
//!    num_data_plane_tcp_connections: NonZeroU8::new(4).unwrap(), // maximum of 4 TCP connections is allowed
//!    ..Default::default()
//! };
//!
//! let dragonsmouth_session = fumarole_client
//!    .dragonsmouth_subscribe_with_config(args.name, request, subscribe_config)
//!    .await
//!    .expect("Failed to subscribe");
//! ```
//!
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

pub(crate) mod grpc;
pub(crate) mod runtime;
pub(crate) mod util;

use {
    crate::proto::GetSlotRangeRequest,
    config::FumaroleConfig,
    futures::future::{Either, select},
    proto::control_response::Response,
    runtime::{
        state_machine::{DEFAULT_SLOT_MEMORY_RETENTION, FumaroleSM},
        tokio::{
            DEFAULT_GC_INTERVAL, DownloadTaskRunnerChannels, GrpcDownloadTaskRunner,
            TokioFumeDragonsmouthRuntime,
        },
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
            Ascii, MetadataKey, MetadataValue,
            errors::{InvalidMetadataKey, InvalidMetadataValue},
        },
        service::{Interceptor, interceptor::InterceptedService},
        transport::{Channel, ClientTlsConfig},
    },
    util::grpc::into_bounded_mpsc_rx,
    uuid::Uuid,
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
    tonic::include_proto!("fumarole");
}

use {
    crate::grpc::FumaroleGrpcConnector,
    proto::{JoinControlPlane, fumarole_client::FumaroleClient as TonicFumaroleClient},
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
pub const DEFAULT_DRAGONSMOUTH_CAPACITY: usize = 100000;

///
/// Default Fumarole commit offset interval
///
pub const DEFAULT_COMMIT_INTERVAL: Duration = Duration::from_secs(10);

///
/// Default maximum number of consecutive failed slot download attempts before failing the fumarole session.
///
pub const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT: usize = 3;

///
/// MAXIMUM number of parallel data streams (TCP connections) to open to fumarole.
///
const MAX_PARA_DATA_STREAMS: u8 = 4;

///
/// Default number of parallel data streams (TCP connections) to open to fumarole.
///
pub const DEFAULT_PARA_DATA_STREAMS: u8 = 4;

///
/// Default maximum number of concurrent download requests to the fumarole service inside a single data plane TCP connection.
///
pub const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP: usize = 1;

///
/// Default refresh tip interval for the fumarole client.
/// Only useful if you enable `prometheus` feature flags.
///
pub const DEFAULT_REFRESH_TIP_INTERVAL: Duration = Duration::from_secs(5); // seconds

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

    ///
    /// Garbage collection interval for the fumarole client in ticks (loop iteration of the fumarole runtime)
    ///
    pub gc_interval: usize,

    ///
    /// How far back in time the fumarole client should retain slot memory.
    /// This is used to avoid downloading the same slot multiple times.
    pub slot_memory_retention: usize,

    ///
    /// Interval to refresh the tip stats from the fumarole service.
    ///
    pub refresh_tip_stats_interval: Duration,

    ///
    /// Whether to disable committing offsets to the fumarole service.
    /// This is useful for testing or when you don't care about committing offsets.
    /// If set to `true`, the fumarole client will not commit offsets to the fumarole service.
    /// This mean the current session will never commit progression.
    /// If set to `true`, [`FumaroleSubscribeConfig::commit_interval`] will be ignored.
    pub no_commit: bool,
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
            gc_interval: DEFAULT_GC_INTERVAL,
            slot_memory_retention: DEFAULT_SLOT_MEMORY_RETENTION,
            refresh_tip_stats_interval: DEFAULT_REFRESH_TIP_INTERVAL, // Default to 5 seconds
            no_commit: false,
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
        let connection_window_size: u32 = config
            .initial_connection_window_size
            .as_u64()
            .try_into()
            .expect("initial_connection_window_size must fit in u32");
        let stream_window_size: u32 = config
            .initial_stream_window_size
            .as_u64()
            .try_into()
            .expect("initial_stream_window_size must fit in u32");
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .initial_connection_window_size(connection_window_size)
            .initial_stream_window_size(stream_window_size)
            .http2_adaptive_window(config.enable_http2_adaptive_window);

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
    /// Returns the current version of the Fumarole service.
    ///
    pub async fn version(&mut self) -> Result<proto::VersionResponse, tonic::Status> {
        let request = tonic::Request::new(proto::VersionRequest {});
        let response = self.inner.version(request).await?;
        Ok(response.into_inner())
    }

    ///
    /// Subscribe to a stream of updates from the Fumarole service
    ///
    pub async fn dragonsmouth_subscribe<S>(
        &mut self,
        subscriber_name: S,
        request: geyser::SubscribeRequest,
    ) -> Result<DragonsmouthAdapterSession, tonic::Status>
    where
        S: AsRef<str>,
    {
        let handle = tokio::runtime::Handle::current();
        self.dragonsmouth_subscribe_with_config_on(
            subscriber_name,
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
        subscriber_name: S,
        request: geyser::SubscribeRequest,
        config: FumaroleSubscribeConfig,
        handle: tokio::runtime::Handle,
    ) -> Result<DragonsmouthAdapterSession, tonic::Status>
    where
        S: AsRef<str>,
    {
        assert!(
            config.num_data_plane_tcp_connections.get() <= MAX_PARA_DATA_STREAMS,
            "num_data_plane_tcp_connections must be less than or equal to {MAX_PARA_DATA_STREAMS}"
        );

        assert!(
            config.refresh_tip_stats_interval >= Duration::from_secs(5),
            "refresh_tip_stats_interval must be greater than or equal to 5 seconds"
        );

        use {proto::ControlCommand, runtime::tokio::DragonsmouthSubscribeRequestBidi};

        let (dragonsmouth_outlet, dragonsmouth_inlet) =
            mpsc::channel(DEFAULT_DRAGONSMOUTH_CAPACITY);
        let (fume_control_plane_tx, fume_control_plane_rx) = mpsc::channel(100);

        let initial_join = JoinControlPlane {
            consumer_group_name: Some(subscriber_name.as_ref().to_string()),
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

        let sm = FumaroleSM::new(*last_committed_offset, config.slot_memory_retention);

        let (dm_tx, dm_rx) = mpsc::channel(100);
        let dm_bidi = DragonsmouthSubscribeRequestBidi {
            tx: dm_tx.clone(),
            rx: dm_rx,
        };

        let mut data_plane_channel_vec = Vec::with_capacity(1);
        // TODO: support config.num_data_plane_tcp_connections
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
            data_plane_channel_vec,
            self.connector.clone(),
            download_task_runner_cnc_rx,
            download_task_queue_rx,
            download_result_tx,
            config.max_failed_slot_download_attempt,
            request.clone(),
        );

        let download_task_runner_chans = DownloadTaskRunnerChannels {
            download_task_queue_tx,
            cnc_tx: download_task_runner_cnc_tx,
            download_result_rx,
        };

        let tokio_rt = TokioFumeDragonsmouthRuntime {
            sm,
            fumarole_client: self.clone(),
            blockchain_id: initial_state.blockchain_id,
            dragonsmouth_bidi: dm_bidi,
            subscribe_request: request,
            download_task_runner_chans,
            consumer_group_name: subscriber_name.as_ref().to_string(),
            control_plane_tx: fume_control_plane_tx,
            control_plane_rx: fume_control_plane_rx,
            dragonsmouth_outlet,
            commit_interval: config.commit_interval,
            last_commit: Instant::now(),
            get_tip_interval: config.refresh_tip_stats_interval,
            last_tip: Instant::now(),
            gc_interval: config.gc_interval,
            non_critical_background_jobs: Default::default(),
            last_history_poll: Default::default(),
            no_commit: config.no_commit,
            stop: false,
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
        tracing::trace!("list_consumer_groups called");
        self.inner.list_consumer_groups(request).await
    }

    pub async fn get_consumer_group_info(
        &mut self,
        request: impl tonic::IntoRequest<proto::GetConsumerGroupInfoRequest>,
    ) -> std::result::Result<tonic::Response<proto::ConsumerGroupInfo>, tonic::Status> {
        tracing::trace!("get_consumer_group_info called");
        self.inner.get_consumer_group_info(request).await
    }

    pub async fn delete_consumer_group(
        &mut self,
        request: impl tonic::IntoRequest<proto::DeleteConsumerGroupRequest>,
    ) -> std::result::Result<tonic::Response<proto::DeleteConsumerGroupResponse>, tonic::Status>
    {
        tracing::trace!("delete_consumer_group called");
        self.inner.delete_consumer_group(request).await
    }

    pub async fn create_consumer_group(
        &mut self,
        request: impl tonic::IntoRequest<proto::CreateConsumerGroupRequest>,
    ) -> std::result::Result<tonic::Response<proto::CreateConsumerGroupResponse>, tonic::Status>
    {
        tracing::trace!("create_consumer_group called");
        self.inner.create_consumer_group(request).await
    }

    pub async fn get_chain_tip(
        &mut self,
        request: impl tonic::IntoRequest<proto::GetChainTipRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetChainTipResponse>, tonic::Status> {
        tracing::trace!("get_chain_tip called");
        self.inner.get_chain_tip(request).await
    }

    pub async fn get_slot_range(
        &mut self,
    ) -> std::result::Result<tonic::Response<proto::GetSlotRangeResponse>, tonic::Status> {
        tracing::trace!("get_slot_range called");
        self.inner
            .get_slot_range(GetSlotRangeRequest {
                blockchain_id: Uuid::nil().as_bytes().to_vec(),
            })
            .await
    }
}
