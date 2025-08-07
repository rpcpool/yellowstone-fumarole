use {
    clap::Parser,
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_signature::Signature,
    std::{
        collections::{HashMap, HashSet},
        fmt, fs,
        io::Write,
        path::PathBuf,
        str::FromStr,
    },
    tokio_stream::{Stream, StreamExt},
    tonic::Status,
    yellowstone_fumarole_client::config::FumaroleConfig,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateBlockMeta, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone Dragonsmouth Example")]
struct Args {
    /// Path to static config file
    #[clap(long)]
    config: PathBuf,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Clone, Parser)]
enum Action {
    /// Run the block stats example
    Block(SubscribeArgs),
    /// Run the account and transaction updates example
    Stream(SubscribeArgs),
}

#[derive(Debug, Clone, Parser, Default, Copy)]
pub enum CommitmentOption {
    Finalized,
    Confirmed,
    #[default]
    Processed,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid commitment option {0}")]
pub struct FromStrCommitmentOptionErr(String);

impl FromStr for CommitmentOption {
    type Err = FromStrCommitmentOptionErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "finalized" => Ok(CommitmentOption::Finalized),
            "confirmed" => Ok(CommitmentOption::Confirmed),
            "processed" => Ok(CommitmentOption::Processed),
            whatever => Err(FromStrCommitmentOptionErr(whatever.to_owned())),
        }
    }
}

impl fmt::Display for CommitmentOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitmentOption::Finalized => write!(f, "finalized"),
            CommitmentOption::Confirmed => write!(f, "confirmed"),
            CommitmentOption::Processed => write!(f, "processed"),
        }
    }
}

impl From<CommitmentOption> for CommitmentLevel {
    fn from(commitment: CommitmentOption) -> Self {
        match commitment {
            CommitmentOption::Finalized => CommitmentLevel::Finalized,
            CommitmentOption::Confirmed => CommitmentLevel::Confirmed,
            CommitmentOption::Processed => CommitmentLevel::Processed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscribeDataType {
    Account,
    Transaction,
    Slot,
    BlockMeta,
    Entry,
}

#[derive(Debug, Clone)]
pub struct SubscribeInclude {
    set: HashSet<SubscribeDataType>,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid include type {0}")]
pub struct FromStrSubscribeIncludeErr(String);

impl FromStr for SubscribeInclude {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let include = s
            .split(',')
            .map(|s| s.trim())
            .map(|s| match s {
                "account" => Ok(vec![SubscribeDataType::Account]),
                "tx" => Ok(vec![SubscribeDataType::Transaction]),
                "meta" => Ok(vec![SubscribeDataType::BlockMeta]),
                "slot" => Ok(vec![SubscribeDataType::Slot]),
                "all" => Ok(vec![
                    SubscribeDataType::Account,
                    SubscribeDataType::Transaction,
                    SubscribeDataType::Slot,
                    SubscribeDataType::BlockMeta,
                    SubscribeDataType::Entry,
                ]),
                "entry" => Ok(vec![SubscribeDataType::Entry]),
                unknown => Err(format!("Invalid include type: {unknown}")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let include = include.into_iter().flatten().collect::<HashSet<_>>();
        Ok(SubscribeInclude { set: include })
    }
}

///
/// Represents a subscription to a specific pubkey with an optional filterset name.
///
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscribePubkeyValue {
    pub filter: Option<String>,
    pub pubkey: Pubkey,
}

#[derive(Debug, thiserror::Error)]
pub enum FromStrSubscribePubkeyValueErr {
    #[error(transparent)]
    ParsePubkeyError(#[from] ParsePubkeyError),
    #[error("{0}")]
    InvalidValue(String),
}

impl FromStr for SubscribePubkeyValue {
    type Err = FromStrSubscribePubkeyValueErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.len() {
            0 => Err(FromStrSubscribePubkeyValueErr::InvalidValue(
                "invalid pubkey filter, empty value".to_string(),
            )),
            1 => {
                let pubkey = Pubkey::from_str(parts[0])?;
                Ok(SubscribePubkeyValue {
                    filter: None,
                    pubkey,
                })
            }
            2 => {
                let filter = parts[0].to_string();
                let pubkey = Pubkey::from_str(parts[1])?;
                Ok(SubscribePubkeyValue {
                    filter: Some(filter),
                    pubkey,
                })
            }
            _ => Err(FromStrSubscribePubkeyValueErr::InvalidValue(
                "invalid pubkey filter, too many parts".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Parser)]
struct SubscribeArgs {
    ///
    /// Comma separate list of Geyser event types you want to subscribe to.
    /// Valid values are: [account, tx, slot, block_meta, all]
    /// If not specified, all event types will be subscribed to.
    /// Examples: account,tx, all, slot,meta,tx, tx
    #[clap(long, default_value = "all")]
    include: SubscribeInclude,

    #[clap(long, default_value = "processed")]
    commitment: CommitmentOption,

    /// List of account public keys to subscribe to
    #[clap(short, long)]
    account: Vec<SubscribePubkeyValue>,

    /// List of account owners to subscribe to
    #[clap(short, long)]
    owner: Vec<SubscribePubkeyValue>,

    /// List of account public keys, any of which must be included in the transaction
    #[clap(long, short)]
    tx_account: Vec<SubscribePubkeyValue>,

    /// List of account public keys that must be required in the transaction
    #[clap(long)]
    tx_account_required: Vec<SubscribePubkeyValue>,

    #[clap(long)]
    /// Path to the output transaction collected during the subscription
    tx_out: Option<PathBuf>,
}

fn summarize_account(account: SubscribeUpdateAccount) -> Option<String> {
    let slot = account.slot;
    let account = account.account?;
    let pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
    let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
    Some(format!("account,{},{},{}", slot, pubkey, owner))
}

fn summarize_tx(tx: SubscribeUpdateTransaction) -> Option<String> {
    let slot = tx.slot;
    let tx = tx.transaction?;
    let sig = bs58::encode(tx.signature).into_string();
    Some(format!("tx,{slot},{sig}"))
}

struct TxInfo {
    account_keys: Vec<Pubkey>,
    matched_filters: Vec<String>,
}

#[derive(Default)]
struct BlockInfo {
    success_tx: HashMap<Signature, TxInfo>,
    failed_tx: HashSet<Signature>,
    account_updates: HashSet<(Pubkey, Signature)>,
    entry_count: usize,
    block_meta: Option<SubscribeUpdateBlockMeta>,
}

async fn block_stats<S, F>(rx: S, mut on_block: Option<F>)
where
    S: Stream<Item = Result<SubscribeUpdate, Status>>,
    F: FnMut(u64, BlockInfo) + Send + 'static,
{
    let mut block_map: HashMap<u64, BlockInfo> = HashMap::new();

    let summarized_block = |block_info: &BlockInfo| {
        let success_count = block_info.success_tx.len();
        let failed_count = block_info.failed_tx.len();
        let entry_count = block_info.entry_count;
        let account_updates = block_info.account_updates.len();
        let block_meta = block_info
            .block_meta
            .as_ref()
            .expect("Block meta should be present");
        let expected_tx_count = block_meta.executed_transaction_count;
        let expected_entry_count = block_meta.entries_count;
        let total_tx_cnt = success_count + failed_count;
        format!(
            "good tx: {success_count}, failed tx: {failed_count}, total tx: {total_tx_cnt}/{expected_tx_count}, entries: {entry_count}/{expected_entry_count}, account updates: {account_updates}"
        )
    };
    tokio::pin!(rx);
    while let Some(result) = rx.next().await {
        let event = result.expect("Failed to receive event");
        let matched_filters = event.filters.clone();
        let Some(oneof) = event.update_oneof else {
            continue;
        };
        match oneof {
            UpdateOneof::Account(account_update) => {
                let slot = account_update.slot;
                let Some(block) = block_map.get_mut(&slot) else {
                    continue;
                };
                let account = account_update
                    .account
                    .as_ref()
                    .expect("Account should be present");
                let pubkey =
                    Pubkey::try_from(account.pubkey.as_slice()).expect("Failed to parse pubkey");
                let Some(tx_sig_bytes) = account.txn_signature.as_ref() else {
                    continue;
                };
                let tx_sig = Signature::try_from(tx_sig_bytes.as_slice())
                    .expect("Failed to parse transaction signature");
                block.account_updates.insert((pubkey, tx_sig));
            }
            UpdateOneof::Transaction(tx) => {
                let slot = tx.slot;
                let Some(block) = block_map.get_mut(&slot) else {
                    continue;
                };
                let transaction = tx
                    .transaction
                    .as_ref()
                    .expect("Transaction should be present");
                let tx_sig = Signature::try_from(transaction.signature.as_slice())
                    .expect("Failed to parse transaction signature");
                if transaction.meta.as_ref().unwrap().err.is_some() {
                    block.failed_tx.insert(tx_sig);
                } else {
                    let mut pubkeys = vec![];
                    for pubkey in transaction
                        .transaction
                        .as_ref()
                        .unwrap()
                        .message
                        .as_ref()
                        .unwrap()
                        .account_keys
                        .iter()
                    {
                        pubkeys.push(
                            Pubkey::try_from(pubkey.as_slice())
                                .expect("Failed to parse account key"),
                        );
                    }
                    block.success_tx.insert(
                        tx_sig,
                        TxInfo {
                            account_keys: pubkeys,
                            matched_filters,
                        },
                    );
                }
            }
            UpdateOneof::Entry(entry) => {
                let slot = entry.slot;
                let Some(block) = block_map.get_mut(&slot) else {
                    continue;
                };
                block.entry_count += 1;
            }
            UpdateOneof::BlockMeta(block_meta) => {
                let slot = block_meta.slot;
                let Some(mut block) = block_map.remove(&slot) else {
                    continue;
                };
                block.block_meta = Some(block_meta);
                println!("{slot} -- {}", summarized_block(&block));
                if let Some(on_block) = &mut on_block {
                    on_block(slot, block);
                }
            }
            UpdateOneof::Slot(slot) => {
                if matches!(
                    slot.status(),
                    SlotStatus::SlotFirstShredReceived | SlotStatus::SlotCreatedBank
                ) {
                    let slot = slot.slot;
                    block_map.entry(slot).or_default();
                }
            }
            _ => {}
        }
    }
}

/// This code serves as a reference to compare fumarole against dragonsmouth original client
async fn block_stats_example<I: Interceptor>(mut client: GeyserGrpcClient<I>, args: SubscribeArgs) {
    let request = args.as_subscribe_request();
    println!("Subscribing with request: {request:?}");
    let (_sink, rx) = client
        .subscribe_with_request(Some(request))
        .await
        .expect("Failed to subscribe");

    let on_block_cb = if let Some(path) = args.tx_out {
        let mut file = fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Failed to open output file");
        Some(move |slot: u64, block_info: BlockInfo| {
            for (tx, info) in block_info.success_tx {
                let account_keys_str = info
                    .account_keys
                    .into_iter()
                    .map(|pk| pk.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let filters = info.matched_filters.join(",");
                writeln!(file, "{slot} -- {tx}, {filters} {account_keys_str}")
                    .expect("Failed to write to output file");
            }
        })
    } else {
        None
    };

    block_stats(rx, on_block_cb).await;
}

impl SubscribeArgs {
    fn default_filter_name(&self) -> String {
        "fumarole".to_string()
    }

    fn build_subscribe_account_filter(&self) -> HashMap<String, SubscribeRequestFilterAccounts> {
        if self.account.is_empty() && self.owner.is_empty() {
            // If no accounts or owners are specified, we return an empty filter
            return HashMap::from([(self.default_filter_name(), Default::default())]);
        }

        let mut filter = HashMap::new();
        for account in self.account.iter().cloned() {
            let account_filter: &mut SubscribeRequestFilterAccounts = filter
                .entry(account.filter.unwrap_or(self.default_filter_name()))
                .or_default();
            account_filter.account.push(account.pubkey.to_string());
        }

        for owner in self.owner.iter().cloned() {
            let account_filter: &mut SubscribeRequestFilterAccounts = filter
                .entry(owner.filter.unwrap_or(self.default_filter_name()))
                .or_default();
            account_filter.owner.push(owner.pubkey.to_string());
        }

        filter
    }

    fn build_subscribe_tx_filter(&self) -> HashMap<String, SubscribeRequestFilterTransactions> {
        if self.tx_account.is_empty() && self.tx_account_required.is_empty() {
            // If no tx accounts are specified, we return an empty filter
            return HashMap::from([(self.default_filter_name(), Default::default())]);
        }

        let mut filter = HashMap::new();
        for tx_account in self.tx_account.iter().cloned() {
            let tx_filter: &mut SubscribeRequestFilterTransactions = filter
                .entry(tx_account.filter.unwrap_or(self.default_filter_name()))
                .or_default();
            tx_filter
                .account_include
                .push(tx_account.pubkey.to_string());
        }

        for tx_account in self.tx_account_required.iter().cloned() {
            let tx_filter: &mut SubscribeRequestFilterTransactions = filter
                .entry(tx_account.filter.unwrap_or(self.default_filter_name()))
                .or_default();
            tx_filter
                .account_required
                .push(tx_account.pubkey.to_string());
        }
        filter
    }

    fn as_subscribe_request(&self) -> SubscribeRequest {
        let commitment_level: CommitmentLevel = self.commitment.into();
        // This request listen for all account updates and transaction updates
        let mut request = SubscribeRequest {
            commitment: Some(commitment_level.into()),
            ..Default::default()
        };

        for to_include in &self.include.set {
            match to_include {
                SubscribeDataType::Account => {
                    request.accounts = self.build_subscribe_account_filter();
                }
                SubscribeDataType::Transaction => {
                    request.transactions = self.build_subscribe_tx_filter();
                }
                SubscribeDataType::Slot => {
                    request.slots = HashMap::from([(
                        self.default_filter_name(),
                        SubscribeRequestFilterSlots {
                            interslot_updates: Some(true),
                            ..Default::default()
                        },
                    )]);
                }
                SubscribeDataType::BlockMeta => {
                    request.blocks_meta = HashMap::from([(
                        self.default_filter_name(),
                        SubscribeRequestFilterBlocksMeta::default(),
                    )]);
                }
                SubscribeDataType::Entry => {
                    request.entry =
                        HashMap::from([(self.default_filter_name(), Default::default())]);
                }
            }
        }
        request
    }
}

async fn stream_example<I: Interceptor>(mut client: GeyserGrpcClient<I>, args: SubscribeArgs) {
    let request = args.as_subscribe_request();

    let (_sink, mut rx) = client
        .subscribe_with_request(Some(request))
        .await
        .expect("Failed to subscribe");

    while let Some(result) = rx.next().await {
        let event = result.expect("Failed to receive event");
        if let Some(oneof) = event.update_oneof {
            match oneof {
                UpdateOneof::Account(account_update) => {
                    if let Some(message) = summarize_account(account_update) {
                        println!("{}", message);
                    }
                }
                UpdateOneof::Transaction(tx) => {
                    if let Some(message) = summarize_tx(tx) {
                        println!("{}", message);
                    }
                }
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config_path = args.config;
    let config = std::fs::read_to_string(config_path).expect("Failed to read config file");
    let config: FumaroleConfig =
        serde_yaml::from_str(&config).expect("Failed to parse config file");

    let endpoint = config.endpoint.clone();

    let geyser = GeyserGrpcBuilder::from_shared(endpoint)
        .expect("Failed to parse endpoint")
        .x_token(config.x_token)
        .expect("x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("tls_config")
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .http2_adaptive_window(true)
        .initial_stream_window_size(9_000_000)
        .initial_connection_window_size(100_000_000)
        .max_decoding_message_size(config.max_decoding_message_size_bytes)
        .connect()
        .await
        .expect("Failed to connect to geyser");

    match args.action {
        Action::Block(args) => block_stats_example(geyser, args).await,
        Action::Stream(args) => stream_example(geyser, args).await,
    }
}
