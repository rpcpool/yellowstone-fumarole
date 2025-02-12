pub mod config;

use {
    config::FumaroleConfig,
    fumarole::{AccountUpdateFilter, TransactionFilter},
    solana_sdk::pubkey::Pubkey,
    tonic::{
        metadata::{Ascii, MetadataValue},
        service::Interceptor,
        transport::{Channel, ClientTlsConfig},
    },
    tower::{util::BoxService, ServiceBuilder, ServiceExt},
};

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            include!(concat!(
                env!("OUT_DIR"),
                "/solana.storage.confirmed_block.rs"
            ));
        }
    }
}

pub mod geyser {
    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
}

pub mod fumarole {
    include!(concat!(env!("OUT_DIR"), "/fumarole.rs"));
}

use fumarole::fumarole_client::FumaroleClient;

#[derive(Clone)]
struct TritonAuthInterceptor {
    x_token: MetadataValue<Ascii>,
}

impl Interceptor for TritonAuthInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut request = request;
        let metadata = request.metadata_mut();
        metadata.insert("x-token", self.x_token.clone());
        Ok(request)
    }
}

pub struct FumaroleClientBuilder {}

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

pub type BoxedFumaroleClient = FumaroleClient<FumaroleBoxedChannel>;

impl FumaroleClientBuilder {
    ///
    /// Connect to a Fumarole service.
    ///
    pub async fn connect(
        config: FumaroleConfig,
    ) -> Result<FumaroleClient<FumaroleBoxedChannel>, ConnectError> {
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let channel = Channel::from_shared(config.endpoint.clone())?
            .tls_config(tls_config)?
            .connect()
            .await?;
        Self::connect_with_channel(config, channel).await
    }

    ///
    /// Connect to a Fumarole service with an existing channel.
    ///
    pub async fn connect_with_channel(
        config: FumaroleConfig,
        channel: tonic::transport::Channel,
    ) -> Result<FumaroleClient<FumaroleBoxedChannel>, ConnectError> {
        let svc = if let Some(x_token) = config.x_token {
            let metadata = x_token.try_into()?;
            let interceptor = TritonAuthInterceptor { x_token: metadata };
            ServiceBuilder::new()
                .layer(tonic::service::interceptor(interceptor))
                .service(channel)
                .boxed()
        } else {
            channel.boxed()
        };

        Ok(FumaroleClient::new(svc)
            .max_decoding_message_size(config.max_decoding_message_size_bytes))
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
///     .with_accounts(accounts)
///     .with_owners(owners)
///     .with_tx_accounts(tx_accounts)
///     .build("my_consumer".to_string());
/// ```
#[derive(Clone)]
pub struct SubscribeRequestBuilder {
    accounts: Option<Vec<Pubkey>>,
    owners: Option<Vec<Pubkey>>,
    tx_account_keys: Option<Vec<Pubkey>>,
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
            tx_account_keys: None,
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
    /// Sets the account pubkeys list that needs to be included in each transaction we subscribe to.
    ///
    pub fn with_tx_accounts(mut self, tx_accounts: Option<Vec<Pubkey>>) -> Self {
        self.tx_account_keys = tx_accounts;
        self
    }

    ///
    /// Builds a SubscribeRequest.
    ///
    /// If the consumer index is not provided, it defaults to 0.
    ///
    pub fn build(self, consumer_group: String) -> fumarole::SubscribeRequest {
        self.build_with_consumer_idx(consumer_group, 0)
    }

    ///
    /// Builds a vector of SubscribeRequests where each request has a different consumer index.
    ///
    pub fn build_vec(self, consumer_group: String, counts: u32) -> Vec<fumarole::SubscribeRequest> {
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
    ) -> fumarole::SubscribeRequest {
        let account = self
            .accounts
            .map(|vec| vec.into_iter().map(|pubkey| pubkey.to_string()).collect());

        let owner = self
            .owners
            .map(|vec| vec.into_iter().map(|pubkey| pubkey.to_string()).collect());

        let account_filter = match (account, owner) {
            (Some(accounts), Some(owners)) => Some(AccountUpdateFilter {
                account: accounts,
                owner: owners,
            }),
            (Some(accounts), None) => Some(AccountUpdateFilter {
                account: accounts,
                owner: vec![],
            }),
            (None, Some(owners)) => Some(AccountUpdateFilter {
                account: vec![],
                owner: owners,
            }),
            _ => None,
        };

        let tx_filter = self.tx_account_keys.map(|vec| TransactionFilter {
            account_keys: vec.into_iter().map(|pubkey| pubkey.to_string()).collect(),
        });

        fumarole::SubscribeRequest {
            consumer_group_label: consumer_group,
            consumer_id: Some(consumer_idx),
            accounts: account_filter,
            transactions: tx_filter,
        }
    }
}
