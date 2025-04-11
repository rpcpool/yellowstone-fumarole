///
/// Fumarole's client library.
///
pub mod config;

pub(crate) mod runtime;
pub(crate) mod util;

use {
    config::FumaroleConfig,
    core::num,
    proto::{BlockFilters, BlockchainEvent, ControlCommand, PollBlockchainHistory},
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey},
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
    },
    tokio::{
        sync::mpsc,
        task::{self, JoinError, JoinSet},
    },
    tokio_stream::{wrappers::ReceiverStream, StreamMap},
    tonic::{
        async_trait,
        metadata::{
            errors::{InvalidMetadataKey, InvalidMetadataValue},
            Ascii, MetadataKey, MetadataValue,
        },
        service::Interceptor,
        transport::{Channel, ClientTlsConfig},
    },
    tower::{util::BoxService, ServiceBuilder, ServiceExt},
    util::collections::KeyedVecDeque,
    yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    },
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

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/fumarole_v2.rs"));
}

use proto::fumarole_client::FumaroleClient as TonicFumaroleClient;

#[derive(Clone)]
struct TritonAuthInterceptor {
    x_token: MetadataValue<Ascii>,
}

#[derive(Clone)]
struct AsciiMetadataInterceptor {
    metadata: HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>,
}

impl Interceptor for TritonAuthInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = request;
        let metadata = request.metadata_mut();
        metadata.insert("x-token", self.x_token.clone());
        Ok(request)
    }
}

impl Interceptor for AsciiMetadataInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = request;
        let metadata = request.metadata_mut();
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
pub enum ConnectError {
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    InvalidXToken(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

pub type FumaroleBoxedChannel = BoxService<
    hyper::Request<tonic::body::BoxBody>,
    hyper::Response<tonic::body::BoxBody>,
    tonic::transport::Error,
>;

pub type BoxedTonicFumaroleClient = TonicFumaroleClient<FumaroleBoxedChannel>;

///
/// Yellowstone Fumarole gRPC Client
///
pub struct FumaroleClient {
    inner: BoxedTonicFumaroleClient,
}

#[async_trait::async_trait]
pub trait FumaroleSender {
    // async fn send_request(
    //     &mut self,
    //     request: proto::SubscribeRequest,
    // ) -> Result<tonic::Response<tonic::codec::Streaming<geyser::SubscribeUpdate>>, tonic::Status>;
}

impl FumaroleClient {
    ///
    /// Subscribe to a stream of updates from the Fumarole service
    ///
    pub async fn dragonsmouth_subscribe<S>(
        consumer_group_name: S,
        request: geyser::SubscribeRequest,
    ) -> mpsc::Receiver<geyser::SubscribeUpdate>
    where
        S: AsRef<str>,
    {
        todo!()
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

#[derive(Debug, thiserror::Error)]
pub enum InvalidMetadataHeader {
    #[error(transparent)]
    InvalidMetadataKey(#[from] InvalidMetadataKey),
    #[error(transparent)]
    InvalidMetadataValue(#[from] InvalidMetadataValue),
}

///
/// A builder for creating a FumaroleClient.
///
impl FumaroleClientBuilder {
    ///
    /// Add a metadata header to the client for each request.
    ///
    pub fn add_metadata_header(
        mut self,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<Self, InvalidMetadataHeader> {
        let key = MetadataKey::from_bytes(key.as_ref().as_bytes())?;
        let value: MetadataValue<Ascii> = value.as_ref().try_into()?;
        self.metadata.insert(key, value);
        Ok(self)
    }

    ///
    /// Add multiple metadata headers to the client for each request.
    ///
    pub fn add_metadata_headers<IT, KV>(self, headers: IT) -> Result<Self, InvalidMetadataHeader>
    where
        KV: AsRef<str>,
        IT: IntoIterator<Item = (KV, KV)>,
    {
        headers
            .into_iter()
            .try_fold(self, |this, (k, v)| this.add_metadata_header(k, v))
    }

    ///
    /// Enable compression for the client.
    ///
    pub const fn enable_compression(mut self) -> Self {
        self.with_compression = true;
        self
    }

    ///
    /// Disable compression for the client.
    ///
    pub const fn disable_compression(mut self) -> Self {
        self.with_compression = false;
        self
    }

    ///
    /// Connect to a Fumarole service.
    ///
    pub async fn connect(self, config: FumaroleConfig) -> Result<FumaroleClient, ConnectError> {
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let channel = Channel::from_shared(config.endpoint.clone())?
            .tls_config(tls_config)?
            .connect()
            .await?;
        self.connect_with_channel(config, channel).await
    }

    ///
    /// Connect to a Fumarole service with an existing channel.
    ///
    pub async fn connect_with_channel(
        self,
        config: FumaroleConfig,
        channel: tonic::transport::Channel,
    ) -> Result<FumaroleClient, ConnectError> {
        let x_token_layer = if let Some(x_token) = config.x_token {
            let metadata = x_token.try_into()?;
            let interceptor = TritonAuthInterceptor { x_token: metadata };
            Some(tonic::service::interceptor(interceptor))
        } else {
            None
        };

        let metadata_layer = if self.metadata.is_empty() {
            None
        } else {
            let interceptor = AsciiMetadataInterceptor {
                metadata: self.metadata,
            };
            Some(tonic::service::interceptor(interceptor))
        };

        let svc = ServiceBuilder::new()
            .option_layer(x_token_layer)
            .option_layer(metadata_layer)
            .service(channel)
            .boxed();

        let tonic_client = TonicFumaroleClient::new(svc)
            .max_decoding_message_size(config.max_decoding_message_size_bytes);

        let tonic_client = if self.with_compression {
            tonic_client
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        } else {
            tonic_client
        };

        Ok(FumaroleClient {
            inner: tonic_client,
        })
    }
}
