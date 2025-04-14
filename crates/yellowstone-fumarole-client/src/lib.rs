///
/// Fumarole's client library.
///
pub mod config;

use {
    config::FumaroleConfig,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        metadata::{
            errors::{InvalidMetadataKey, InvalidMetadataValue},
            Ascii, MetadataKey, MetadataValue,
        },
        service::Interceptor,
        transport::{Channel, ClientTlsConfig},
    },
    tower::{util::BoxService, ServiceBuilder, ServiceExt},
    yellowstone_grpc_proto::geyser::{
        SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    },
};

pub(crate) mod solana {
    #[allow(unused_imports)]
    pub use yellowstone_grpc_proto::solana::{
        storage,
        storage::{confirmed_block, confirmed_block::*},
    };
}

pub(crate) mod geyser {
    pub use yellowstone_grpc_proto::geyser::*;
}

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/fumarole.rs"));
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

impl FumaroleClient {
    pub async fn subscribe_with_request(
        &mut self,
        request: proto::SubscribeRequest,
    ) -> Result<tonic::Response<tonic::codec::Streaming<geyser::SubscribeUpdate>>, tonic::Status>
    {
        let (tx, rx) = mpsc::channel(100);
        let rx = ReceiverStream::new(rx);

        // NOTE: Make sure send request before giving the stream to the service
        // Otherwise, the service will not be able to send the response
        // This is due to how fumarole works in the background for auto-commit offset management.
        tx.send(request)
            .await
            .expect("Failed to send request to Fumarole service");
        self.inner.subscribe(rx).await
    }

    pub async fn list_available_commitment_levels(
        &mut self,
        request: impl tonic::IntoRequest<proto::ListAvailableCommitmentLevelsRequest>,
    ) -> std::result::Result<
        tonic::Response<proto::ListAvailableCommitmentLevelsResponse>,
        tonic::Status,
    > {
        self.inner.list_available_commitment_levels(request).await
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
        request: impl tonic::IntoRequest<proto::CreateStaticConsumerGroupRequest>,
    ) -> std::result::Result<tonic::Response<proto::CreateStaticConsumerGroupResponse>, tonic::Status>
    {
        self.inner.create_static_consumer_group(request).await
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

///
/// A builder for creating a SubscribeRequest.
///
/// Example:
///
/// ```rust
/// use yellowstone_fumarole_client::SubscribeRequestBuilder;
/// use solana_sdk::pubkey::Pubkey;
///
/// let accounts = vec![Pubkey::new_keypair()];
/// let owners = vec![Pubkey::new_keypair()];
/// let tx_accounts = vec![Pubkey::new_keypair()];
///
/// let request = SubscribeRequestBuilder::default()
///     .with_accounts(Some(accounts))
///     .with_owners(Some(owners))
///     .with_tx_accounts(Some(tx_accounts))
///     .build("my_consumer".to_string());
/// ```
#[derive(Clone)]
pub struct SubscribeRequestBuilder {
    accounts: Option<Vec<Pubkey>>,
    owners: Option<Vec<Pubkey>>,
    tx_includes: Option<Vec<Pubkey>>,
    tx_excludes: Option<Vec<Pubkey>>,
    tx_requires: Option<Vec<Pubkey>>,
    tx_fail: Option<bool>,
    tx_vote: Option<bool>,
}

impl Default for SubscribeRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscribeRequestBuilder {
    pub const fn new() -> Self {
        Self {
            accounts: None,
            owners: None,
            tx_includes: None,
            tx_excludes: None,
            tx_requires: None,
            tx_fail: None,
            tx_vote: None,
        }
    }

    ///
    /// Sets the accounts to subscribe to.
    ///
    pub fn with_accounts(mut self, accounts: Option<Vec<Pubkey>>) -> Self {
        self.accounts = accounts;
        self
    }

    ///
    /// Sets the owners of the accounts to subscribe to.
    ///
    pub fn with_owners(mut self, owners: Option<Vec<Pubkey>>) -> Self {
        self.owners = owners;
        self
    }

    ///
    /// A transaction is included if it has at least one of the provided accounts in its list of instructions.
    ///
    pub fn with_tx_includes(mut self, tx_accounts: Option<Vec<Pubkey>>) -> Self {
        self.tx_includes = tx_accounts;
        self
    }

    ///
    /// A transaction is excluded if it has at least one of the provided accounts in its list of instructions.
    ///
    pub fn with_tx_excludes(mut self, tx_excludes: Option<Vec<Pubkey>>) -> Self {
        self.tx_includes = tx_excludes;
        self
    }

    ///
    /// A transaction is included if all of the provided accounts in its list of instructions.
    ///
    pub fn with_tx_requires(mut self, tx_requires: Option<Vec<Pubkey>>) -> Self {
        self.tx_requires = tx_requires;
        self
    }

    ///
    /// Include failed transactions.
    ///
    pub const fn include_fail_tx(mut self) -> Self {
        self.tx_fail = None;
        self
    }

    ///
    /// Include vote transactions.
    ///
    pub const fn include_vote_tx(mut self) -> Self {
        self.tx_vote = None;
        self
    }

    ///
    /// Exclude failed transactions.
    ///
    pub const fn no_vote_tx(mut self) -> Self {
        self.tx_vote = Some(false);
        self
    }

    ///
    /// Exclude vote transactions.
    ///
    pub const fn no_fail_tx(mut self) -> Self {
        self.tx_fail = Some(false);
        self
    }

    ///
    /// Builds a SubscribeRequest.
    ///
    /// If the consumer index is not provided, it defaults to 0.
    ///
    pub fn build(self, consumer_group: String) -> proto::SubscribeRequest {
        self.build_with_consumer_idx(consumer_group, 0)
    }

    ///
    /// Builds a vector of SubscribeRequests where each request has a different consumer index.
    ///
    pub fn build_vec(self, consumer_group: String, counts: u32) -> Vec<proto::SubscribeRequest> {
        (0..counts)
            .map(|i| {
                self.clone()
                    .build_with_consumer_idx(consumer_group.clone(), i)
            })
            .collect()
    }

    ///
    /// Builds a SubscribeRequest with a consumer index.
    ///
    pub fn build_with_consumer_idx(
        self,
        consumer_group: String,
        consumer_idx: u32,
    ) -> proto::SubscribeRequest {
        let account = self
            .accounts
            .map(|vec| vec.into_iter().map(|pubkey| pubkey.to_string()).collect());

        let owner = self
            .owners
            .map(|vec| vec.into_iter().map(|pubkey| pubkey.to_string()).collect());

        let tx_includes = self
            .tx_includes
            .map(|vec| vec.iter().map(|pubkey| pubkey.to_string()).collect());

        let tx_excludes = self
            .tx_excludes
            .map(|vec| vec.iter().map(|pubkey| pubkey.to_string()).collect());

        let tx_requires = self
            .tx_requires
            .map(|vec| vec.iter().map(|pubkey| pubkey.to_string()).collect());

        let tx_filter = SubscribeRequestFilterTransactions {
            vote: self.tx_vote,
            failed: self.tx_fail,
            account_exclude: tx_excludes.unwrap_or_default(),
            account_include: tx_includes.unwrap_or_default(),
            account_required: tx_requires.unwrap_or_default(),
            signature: None,
        };

        let account_filter = SubscribeRequestFilterAccounts {
            account: account.unwrap_or_default(),
            owner: owner.unwrap_or_default(),
            ..Default::default()
        };

        proto::SubscribeRequest {
            consumer_group_label: consumer_group,
            consumer_id: Some(consumer_idx),
            accounts: HashMap::from([("default".to_string(), account_filter)]),
            transactions: HashMap::from([("default".to_string(), tx_filter)]),
        }
    }
}
