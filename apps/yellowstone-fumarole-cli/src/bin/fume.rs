use {
    clap::Parser,
    futures::{future::BoxFuture, FutureExt},
    solana_sdk::{bs58, pubkey::Pubkey},
    std::{
        collections::HashMap,
        fmt::{self, Debug},
        fs::File,
        io::{stdout, Write},
        net::{AddrParseError, SocketAddr},
        num::NonZeroUsize,
        path::PathBuf,
        str::FromStr,
        time::Duration,
    },
    tabled::{builder::Builder, Table},
    tokio::{
        io::{self, AsyncBufReadExt, BufReader},
        signal::unix::{signal, SignalKind},
    },
    tonic::Code,
    tracing_subscriber::EnvFilter,
    yellowstone_fumarole_cli::prom::prometheus_server,
    yellowstone_fumarole_client::{
        config::FumaroleConfig,
        proto::{
            ConsumerGroupInfo, CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
            GetConsumerGroupInfoRequest, InitialOffsetPolicy, ListConsumerGroupsRequest,
        },
        DragonsmouthAdapterSession, FumaroleClient, FumaroleSubscribeConfig,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdateAccount,
        SubscribeUpdateBlockMeta, SubscribeUpdateSlot, SubscribeUpdateTransaction,
    },
};

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
    /// Path to static config file
    #[clap(long)]
    config: PathBuf,

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
}

#[derive(Debug, Clone, Parser)]
pub struct GetCgInfoArgs {
    /// Name of the persistent subscriber to get info for
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone, Parser)]
pub struct CreateCgArgs {
    /// Name of the persistent subscriber to create
    #[clap(long)]
    name: String,
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

#[derive(Debug, Clone, Parser)]
struct SubscribeArgs {
    /// bind address <IP:PORT> for prometheus HTTP server endpoint, or "0" to bind to a random localhost port.
    #[clap(long)]
    prometheus: Option<PrometheusBindAddr>,

    /// Output to write geyser events to.
    /// If not specified, output will be written to stdout
    #[clap(long)]
    out: Option<String>,

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
}

fn summarize_account(account: SubscribeUpdateAccount) -> Option<String> {
    let slot = account.slot;
    let account = account.account?;
    // let pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
    // let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
    let tx_sig = account.txn_signature;
    let tx_sig = if let Some(tx_sig_bytes) = tx_sig {
        bs58::encode(tx_sig_bytes).into_string()
    } else {
        "None".to_string()
    };
    Some(format!("account,{slot},{tx_sig}"))
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
    let CreateCgArgs { name } = args;
    let request = CreateConsumerGroupRequest {
        consumer_group_name: name.clone(),
        initial_offset_policy: InitialOffsetPolicy::Latest.into(),
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
        commitment,
        account: pubkey,
        owner,
        tx_account: tx_pubkey,
        out,
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
    let request = SubscribeRequest {
        accounts: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterAccounts {
                account: pubkey.iter().map(|p| p.to_string()).collect(),
                owner: owner.iter().map(|p| p.to_string()).collect(),
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions {
                account_include: tx_pubkey.iter().map(|p| p.to_string()).collect(),
                ..Default::default()
            },
        )]),
        blocks_meta: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]),
        slots: HashMap::from([("f1".to_owned(), SubscribeRequestFilterSlots::default())]),
        commitment: Some(commitment_level.into()),
        ..Default::default()
    };

    println!("Subscribing to consumer group {}", cg_name);
    let subscribe_config = FumaroleSubscribeConfig {
        concurrent_download_limit_per_tcp: NonZeroUsize::new(1).unwrap(),
        commit_interval: Duration::from_secs(1),
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
                            Some(format!("block_meta={slot}"))
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
    println!("Exiting subscribe loop");
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

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let verbosity = args.verbose.tracing_level_filter();
    let curr_crate = env!("CARGO_PKG_NAME");

    let filter = format!("{curr_crate}={verbosity},yellowstone_fumarole_client={verbosity}");
    let env_filter = EnvFilter::new(filter);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_line_number(true)
        .init();

    // setup_tracing_test_many(["yellowstone_fumarole_client"]);
    let config = std::fs::read_to_string(&args.config).expect("Failed to read config file");

    let config = serde_yaml::from_str::<FumaroleConfig>(config.as_str())
        .expect("failed to parse fumarole config");

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
    }
}
