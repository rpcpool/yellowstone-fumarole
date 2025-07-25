#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use {
    clap::Parser,
    futures::{FutureExt, future::BoxFuture},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        env,
        fmt::{self, Debug},
        fs::File,
        io::{Write, stdout},
        net::{AddrParseError, SocketAddr},
        num::{NonZeroU8, NonZeroUsize},
        path::PathBuf,
        str::FromStr,
        time::Duration,
    },
    tabled::{Table, builder::Builder},
    tokio::{
        io::{self, AsyncBufReadExt, BufReader},
        signal::unix::{SignalKind, signal},
    },
    tonic::Code,
    tracing_subscriber::EnvFilter,
    yellowstone_fumarole_cli::prom::prometheus_server,
    yellowstone_fumarole_client::{
        DragonsmouthAdapterSession, FumaroleClient, FumaroleSubscribeConfig,
        config::FumaroleConfig,
        proto::{
            ConsumerGroupInfo, CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
            GetConsumerGroupInfoRequest, InitialOffsetPolicy, ListConsumerGroupsRequest,
        },
    },
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
    },
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const FUMAROLE_CONFIG_ENV: &str = "FUMAROLE_CONFIG";

#[derive(Debug, Clone)]
pub struct PrometheusBindAddr(SocketAddr);

impl From<PrometheusBindAddr> for SocketAddr {
    fn from(addr: PrometheusBindAddr) -> Self {
        addr.0
    }
}

impl fmt::Display for PrometheusBindAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<AddrParseError> for PrometheusBindAddrParseError {
    fn from(err: AddrParseError) -> Self {
        PrometheusBindAddrParseError(err.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid prometheus bind address {0}")]
pub struct PrometheusBindAddrParseError(String);

impl FromStr for PrometheusBindAddr {
    type Err = PrometheusBindAddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "0" {
            Ok(PrometheusBindAddr("127.0.0.1:0".parse()?))
        } else {
            let ip_addr = s.parse()?;
            Ok(PrometheusBindAddr(ip_addr))
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone Fumarole CLI")]
struct Args {
    /// Path to the config file.
    /// If not specified, the default config file will be used.
    /// The default config file is ~/.fumarole/config.yaml.
    /// You can also set the FUMAROLE_CONFIG environment variable to specify the config file.
    /// If the config file is not found, the program will exit with an error.
    #[clap(long)]
    config: Option<PathBuf>,

    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Clone, Parser)]
enum Action {
    /// Test the connection to the fumarole service
    TestConfig,
    /// Get Persistent Subscriber Info
    GetInfo(GetCgInfoArgs),
    /// Create Persistent Subscriber
    Create(CreateCgArgs),
    /// Delete a Persistent Subscriber
    Delete(DeleteCgArgs),
    /// List all persistent subscribers
    List,
    /// Delete all persistent subscribers
    DeleteAll,
    /// Subscribe to fumarole events
    Subscribe(SubscribeArgs),
    /// Returns the slot range of remote fumarole service
    SlotRange,
}

#[derive(Debug, Clone, Parser)]
pub struct GetCgInfoArgs {
    /// Name of the persistent subscriber to get info for
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone)]
pub struct InitialOffsetPolicyArg(pub InitialOffsetPolicy);

#[derive(Debug, thiserror::Error)]
#[error("Invalid initial offset policy: {0}")]
pub struct InitialOffsetPolicyParseError(String);

impl FromStr for InitialOffsetPolicyArg {
    type Err = InitialOffsetPolicyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(InitialOffsetPolicyArg(InitialOffsetPolicy::Latest)),
            "from-slot" => Ok(InitialOffsetPolicyArg(InitialOffsetPolicy::FromSlot)),
            unknown => Err(InitialOffsetPolicyParseError(unknown.to_owned())),
        }
    }
}

impl Default for InitialOffsetPolicyArg {
    fn default() -> Self {
        InitialOffsetPolicyArg(InitialOffsetPolicy::Latest)
    }
}

#[derive(Debug, Clone, Parser)]
pub struct CreateCgArgs {
    /// Name of the persistent subscriber to create
    #[clap(long)]
    name: String,

    /// Initial offset policy for the persistent subscriber (from-slot or latest)
    #[clap(short, long, default_value = "latest")]
    initial_offset_policy: InitialOffsetPolicyArg,

    /// If the initial offset policy is "from-slot", this is the slot to start from.
    /// If not specified, the subscriber will start from the latest slot.
    #[clap(long)]
    from_slot: Option<u64>,
}

#[derive(Debug, Clone, Parser)]
pub struct DeleteCgArgs {
    /// Name of the persistent subscriber to delete
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone, Parser, Default)]
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
                ]),
                unknown => Err(format!("Invalid include type: {unknown}")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let include = include.into_iter().flatten().collect::<HashSet<_>>();
        Ok(SubscribeInclude { set: include })
    }
}

#[derive(Debug, Clone, Parser)]
struct SubscribeArgs {
    /// bind address <IP:PORT> for prometheus HTTP server endpoint, or "0" to bind to a random localhost port.
    #[clap(long)]
    prometheus: Option<PrometheusBindAddr>,

    /// Output to write geyser events to.
    /// If not specified, output will be written to stdout
    #[clap(long)]
    out: Option<String>,

    ///
    /// Comma separate list of Geyser event types you want to subscribe to.
    /// Valid values are: [account, tx, slot, block_meta, all]
    /// If not specified, all event types will be subscribed to.
    /// Examples: account,tx, all, slot,meta,tx, tx
    #[clap(long, default_value = "all")]
    include: SubscribeInclude,

    /// Name of the persistent subscriber
    #[clap(long)]
    name: String,

    #[clap(long, default_value = "processed")]
    commitment: CommitmentOption,

    /// List of account public keys to subscribe to
    #[clap(short, long)]
    account: Vec<Pubkey>,

    /// List of account owners to subscribe to
    #[clap(short, long)]
    owner: Vec<Pubkey>,

    /// List of account public keys that must be included in the transaction
    #[clap(long, short)]
    tx_account: Vec<Pubkey>,

    /// Number of parallel data streams (TCP connections) to open to fumarole.
    #[clap(long, short, default_value = "1")]
    para: NonZeroU8,
}

fn summarize_account(account: SubscribeUpdateAccount) -> Option<String> {
    let slot = account.slot;
    let account = account.account?;
    // let pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
    // let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
    let tx_sig = account.txn_signature;
    let account_pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
    let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
    let tx_sig = if let Some(tx_sig_bytes) = tx_sig {
        bs58::encode(tx_sig_bytes).into_string()
    } else {
        "None".to_string()
    };
    Some(format!(
        "account,{slot},pk={account_pubkey},owner={owner},tx={tx_sig}"
    ))
}

fn summarize_tx(tx: SubscribeUpdateTransaction) -> Option<String> {
    let slot = tx.slot;
    let tx = tx.transaction?;
    let sig = bs58::encode(tx.signature).into_string();
    Some(format!("tx,{slot},{sig}"))
}

fn build_consumer_group_table<IT>(infos: IT) -> Table
where
    IT: IntoIterator<Item = ConsumerGroupInfo>,
{
    let mut b = Builder::default();

    b.push_record(vec!["Uid", "Name", "Stale"]);
    for info in infos {
        let uid = info.id;
        let name = info.consumer_group_name;
        let stale = info.is_stale;
        b.push_record(vec![uid, name, stale.to_string()]);
    }

    b.build()
}

async fn get_cg_info(args: GetCgInfoArgs, mut client: FumaroleClient) {
    let GetCgInfoArgs { name } = args;

    let request = GetConsumerGroupInfoRequest {
        consumer_group_name: name.clone(),
    };

    let response = client.get_consumer_group_info(request).await;

    match response {
        Ok(response) => {
            let info = response.into_inner();
            let table = build_consumer_group_table(vec![info.clone()]);
            println!("{}", table);
        }
        Err(e) => {
            if e.code() == Code::NotFound {
                eprintln!("Consumer group {name} not found");
                return;
            }
            eprintln!(
                "Failed to get consumer group info: {} {}",
                e.code(),
                e.message()
            );
        }
    }
}

async fn create_cg(args: CreateCgArgs, mut client: FumaroleClient) {
    let CreateCgArgs {
        name,
        initial_offset_policy,
        from_slot,
    } = args;
    let request = CreateConsumerGroupRequest {
        consumer_group_name: name.clone(),
        initial_offset_policy: initial_offset_policy.0.into(),
        from_slot,
    };

    let result = client.create_consumer_group(request).await;
    // .expect("Failed to create consumer group");

    match result {
        Ok(_) => {
            println!("Consumer group {name} created!");
        }
        Err(e) => {
            if e.code() == Code::AlreadyExists {
                eprintln!("Consumer group {name} already exists");
                return;
            }
            eprintln!(
                "Failed to create consumer group: {}, {}",
                e.code(),
                e.message()
            );
        }
    }
}

async fn list_all_cg(mut client: FumaroleClient) {
    let request = ListConsumerGroupsRequest {};
    let response = client
        .list_consumer_groups(request)
        .await
        .expect("Failed to list consumer groups");

    let infos = response.into_inner().consumer_groups;
    if infos.is_empty() {
        println!("No consumer groups found");
        return;
    }
    let table = build_consumer_group_table(infos);
    println!("{}", table);
}

async fn delete_cg(args: DeleteCgArgs, mut client: FumaroleClient) {
    let DeleteCgArgs { name } = args;
    let request = DeleteConsumerGroupRequest {
        consumer_group_name: name.clone(),
    };
    let response = client
        .delete_consumer_group(request)
        .await
        .expect("Failed to list consumer groups");

    if response.into_inner().success {
        println!("Consumer group {name} deleted");
    } else {
        eprintln!("Failed to delete consumer group {name}");
    }
}

async fn prompt_yes_no(question: &str) -> io::Result<bool> {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin).lines();

    println!("{question} [y/n]");

    let Some(line) = reader.next_line().await? else {
        return Ok(false);
    };

    match line.trim().to_lowercase().as_str() {
        "y" | "yes" => Ok(true),
        _ => Ok(false),
    }
}

async fn delete_all_cg(mut client: FumaroleClient) {
    let request = ListConsumerGroupsRequest {};
    let response = client
        .list_consumer_groups(request)
        .await
        .expect("Failed to list consumer groups");

    let infos = response.into_inner().consumer_groups;

    if infos.is_empty() {
        println!("No consumer groups found");
        return;
    }

    let table = build_consumer_group_table(infos.clone());

    println!("{}", table);

    let yes = prompt_yes_no("Are you sure you want to delete all consumer groups?")
        .await
        .expect("Failed to read input");

    if !yes {
        println!("Aborting delete operation");
        return;
    }

    for info in infos {
        let name = info.consumer_group_name;
        let request = DeleteConsumerGroupRequest {
            consumer_group_name: name.clone(),
        };
        client
            .delete_consumer_group(request)
            .await
            .expect("Failed to delete consumer group");
        println!("Consumer group {name} deleted");
    }
}

pub fn create_shutdown() -> BoxFuture<'static, ()> {
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create signal");
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create signal");
    async move {
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {}
        };
    }
    .boxed()
}

async fn subscribe(mut client: FumaroleClient, args: SubscribeArgs) {
    let SubscribeArgs {
        prometheus,
        name: cg_name,
        include,
        commitment,
        account: pubkey,
        owner,
        tx_account: tx_pubkey,
        out,
        para,
    } = args;

    let mut out: Box<dyn Write> = if let Some(out) = out {
        Box::new(
            File::options()
                .write(true)
                .open(PathBuf::from(out))
                .expect("Failed to open output file"),
        )
    } else {
        Box::new(stdout())
    };

    let registry = prometheus::Registry::new();
    yellowstone_fumarole_client::metrics::register_metrics(&registry);

    if let Some(bind_addr) = prometheus {
        let socket_addr: SocketAddr = bind_addr.into();
        tokio::spawn(prometheus_server(socket_addr, registry));
    }

    let commitment_level: CommitmentLevel = commitment.into();
    // This request listen for all account updates and transaction updates
    let mut request = SubscribeRequest {
        commitment: Some(commitment_level.into()),
        ..Default::default()
    };

    for to_include in include.set {
        match to_include {
            SubscribeDataType::Account => {
                request.accounts = HashMap::from([(
                    "fumarole".to_owned(),
                    SubscribeRequestFilterAccounts {
                        account: pubkey.iter().map(|p| p.to_string()).collect(),
                        owner: owner.iter().map(|p| p.to_string()).collect(),
                        ..Default::default()
                    },
                )]);
            }
            SubscribeDataType::Transaction => {
                request.transactions = HashMap::from([(
                    "fumarole".to_owned(),
                    SubscribeRequestFilterTransactions {
                        account_include: tx_pubkey.iter().map(|p| p.to_string()).collect(),
                        ..Default::default()
                    },
                )]);
            }
            SubscribeDataType::Slot => {
                request.slots = HashMap::from([(
                    "fumarole".to_owned(),
                    SubscribeRequestFilterSlots::default(),
                )]);
            }
            SubscribeDataType::BlockMeta => {
                request.blocks_meta = HashMap::from([(
                    "fumarole".to_owned(),
                    SubscribeRequestFilterBlocksMeta::default(),
                )]);
            }
        }
    }

    println!("Subscribing to consumer group {}", cg_name);
    let subscribe_config = FumaroleSubscribeConfig {
        concurrent_download_limit_per_tcp: NonZeroUsize::new(1).unwrap(),
        commit_interval: Duration::from_secs(1),
        num_data_plane_tcp_connections: para,
        ..Default::default()
    };
    let dragonsmouth_session = client
        .dragonsmouth_subscribe_with_config(cg_name.clone(), request, subscribe_config)
        .await
        .expect("Failed to subscribe");
    let DragonsmouthAdapterSession {
        sink: _,
        mut source,
        fumarole_handle: _,
    } = dragonsmouth_session;

    let mut shutdown = create_shutdown();

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("Shutting down...");
                break;
            }
            result = source.recv() => {
                let Some(result) = result else {
                    println!("grpc stream closed!");
                    break;
                };

                let event = result.expect("Failed to receive event");

                let message = if let Some(oneof) = event.update_oneof {
                    match oneof {
                        UpdateOneof::Account(account_update) => {
                            summarize_account(account_update)
                        },
                        UpdateOneof::Transaction(tx) => {
                            summarize_tx(tx)
                        },
                        UpdateOneof::Slot(slot) => {
                            let SubscribeUpdateSlot {
                                slot,
                                parent,
                                status,
                                dead_error: _
                            } = slot;
                            let cl = CommitmentLevel::try_from(status).unwrap();
                            Some(format!("slot={slot}, parent={parent:?}, status={cl:?}"))
                        }
                        UpdateOneof::BlockMeta(block_meta) => {
                            let SubscribeUpdateBlockMeta {
                                slot,
                                ..
                            } = block_meta;
                            Some(format!("block={slot}, tx_count={}, entry_count={}", block_meta.executed_transaction_count, block_meta.entries_count))
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                if let Some(message) = message {
                    writeln!(out, "{}", message).expect("Failed to write to output file");
                }
            }
        }
    }
}

async fn slot_range(mut fumarole_client: FumaroleClient) {
    let result = fumarole_client.get_slot_range().await;
    match result {
        Ok(response) => {
            let slot_range = response.into_inner();
            println!(
                "Slot range: {} - {}",
                slot_range.min_slot, slot_range.max_slot
            );
        }
        Err(e) => {
            eprintln!("Failed to get slot range: {}", e);
        }
    }
}

async fn test_config(mut fumarole_client: FumaroleClient) {
    let result = fumarole_client.version().await;
    match result {
        Ok(version) => {
            println!(
                "Successfully connected to Fumarole Service -- version: {}",
                version.version
            );
        }
        Err(e) => match e.code() {
            Code::Unauthenticated => {
                eprintln!("Missing authentication token or invalid token in configuration file");
            }
            _ => {
                eprintln!("Failed to connect to fumarole: {}", e);
            }
        },
    }
}

fn home_dir() -> Option<PathBuf> {
    if cfg!(target_os = "windows") {
        env::var("USERPROFILE").ok().map(PathBuf::from)
    } else {
        env::var("HOME").ok().map(PathBuf::from)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let verbosity = args.verbose.tracing_level_filter();
    let curr_exec = env::current_exe()
        .expect("Failed to get current executable path")
        .file_name()
        .expect("Failed to get current executable file name")
        .to_string_lossy()
        .to_string();

    let filter = format!("{curr_exec}={verbosity},yellowstone_fumarole_client={verbosity}");
    let env_filter = EnvFilter::new(filter);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_line_number(true)
        .init();

    tracing::trace!("starting Yellowstone Fumarole CLI");

    let maybe_config = args.config;
    let config_file = if let Some(config_path) = maybe_config {
        std::fs::File::open(config_path.clone())
            .unwrap_or_else(|_| panic!("Failed to read config file at {config_path:?}"))
    } else {
        let mut default_config_path = home_dir().expect("Failed to get home directory");
        default_config_path.push(".fumarole");
        default_config_path.push("config.yaml");

        let config_path = std::env::var(FUMAROLE_CONFIG_ENV)
            .map(PathBuf::from)
            .unwrap_or(default_config_path);
        std::fs::File::open(config_path.clone())
            .unwrap_or_else(|_| panic!("Failed to read config file at {config_path:?}"))
    };
    let config: FumaroleConfig =
        serde_yaml::from_reader(config_file).expect("failed to parse fumarole config");

    tracing::debug!("Using config: {config:?}");

    let fumarole_client = FumaroleClient::connect(config.clone())
        .await
        .expect("Failed to connect to fumarole");

    match args.action {
        Action::TestConfig => {
            test_config(fumarole_client).await;
        }
        Action::GetInfo(get_cg_info_args) => {
            get_cg_info(get_cg_info_args, fumarole_client).await;
        }
        Action::Create(create_cg_args) => {
            create_cg(create_cg_args, fumarole_client).await;
        }
        Action::Delete(delete_cg_args) => {
            delete_cg(delete_cg_args, fumarole_client).await;
        }
        Action::List => {
            list_all_cg(fumarole_client).await;
        }
        Action::DeleteAll => {
            delete_all_cg(fumarole_client).await;
        }
        Action::Subscribe(subscribe_args) => {
            subscribe(fumarole_client, subscribe_args).await;
        }
        Action::SlotRange => {
            slot_range(fumarole_client).await;
        }
    }
}
