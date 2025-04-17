use {
    clap::Parser,
    futures::{future::BoxFuture, FutureExt},
    solana_sdk::{bs58, pubkey::Pubkey},
    std::{
        collections::HashMap,
        io::{stderr, stdout, IsTerminal},
        path::PathBuf,
    },
    tabled::{builder::Builder, Table},
    tokio::{
        io::{self, AsyncBufReadExt, BufReader},
        signal::unix::{signal, SignalKind},
    },
    tonic::Code,
    tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter},
    yellowstone_fumarole_client::{
        config::FumaroleConfig,
        proto::{
            ConsumerGroupInfo, CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
            GetConsumerGroupInfoRequest, InitialOffsetPolicy, ListConsumerGroupsRequest,
        },
        DragonsmouthAdapterSession, FumaroleClient,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateSlot,
        SubscribeUpdateTransaction,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC ScyllaDB Tool")]
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
    /// Get Consumer Group Info
    GetCgInfo(GetCgInfoArgs),
    /// Create a consumer group
    CreateCg(CreateCgArgs),
    /// Delete a consumer group
    DeleteCg(DeleteCgArgs),
    /// List all consumer groups
    ListCg,
    /// Delete all consumer groups
    DeleteAllCg,
    /// Subscribe to fumarole events
    Subscribe(SubscribeArgs),
}

#[derive(Debug, Clone, Parser)]
pub struct GetCgInfoArgs {
    /// Name of the consumer group to get info for
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone, Parser)]
pub struct CreateCgArgs {
    /// Name of the consumer group to create
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone, Parser)]
pub struct DeleteCgArgs {
    /// Name of the consumer group to delete
    #[clap(long)]
    name: String,
}

#[derive(Debug, Clone, Parser)]
struct SubscribeArgs {
    /// Name of the consumer group to subscribe to
    #[clap(long)]
    cg_name: String,
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
                "Failed to create consumer group: {} {}",
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
    let SubscribeArgs { cg_name } = args;

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: HashMap::from([("f1".to_owned(), SubscribeRequestFilterAccounts::default())]),
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        slots: HashMap::from([("f1".to_owned(), SubscribeRequestFilterSlots::default())]),
        ..Default::default()
    };

    println!("Subscribing to consumer group {}", cg_name);
    let dragonsmouth_session = client
        .dragonsmouth_subscribe(cg_name.clone(), request)
        .await
        .expect("Failed to subscribe");
    println!("Subscribed to consumer group {}", cg_name);
    let DragonsmouthAdapterSession {
        sink: _,
        mut source,
        runtime_handle: _,
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
                    break;
                };

                let event = result.expect("Failed to receive event");

                let message = if let Some(oneof) = event.update_oneof {
                    match oneof {
                        UpdateOneof::Account(account_update) => summarize_account(account_update),
                        UpdateOneof::Transaction(tx) => summarize_tx(tx),
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
                        _ => None,
                    }
                } else {
                    None
                };

                if let Some(message) = message {
                    println!("{}", message);
                }
            }
        }
    }
}

#[allow(dead_code)]
fn setup_tracing_test_many(
    modules: impl IntoIterator<Item = &'static str>,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    let is_atty = stdout().is_terminal() && stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(is_atty)
        .with_line_number(true);

    let directives = modules
        .into_iter()
        .fold(EnvFilter::default(), |filter, module| {
            filter.add_directive(format!("{module}=debug").parse().expect("invalid module"))
        });

    tracing_subscriber::registry()
        .with(io_layer)
        .with(directives)
        .try_init()
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // setup_tracing_test_many(["yellowstone_fumarole_client"]);
    let config = std::fs::read_to_string(&args.config).expect("Failed to read config file");

    let config = serde_yaml::from_str::<FumaroleConfig>(config.as_str())
        .expect("failed to parse fumarole config");

    let fumarole_client = FumaroleClient::connect(config.clone())
        .await
        .expect("Failed to connect to fumarole");

    match args.action {
        Action::GetCgInfo(get_cg_info_args) => {
            get_cg_info(get_cg_info_args, fumarole_client).await;
        }
        Action::CreateCg(create_cg_args) => {
            create_cg(create_cg_args, fumarole_client).await;
        }
        Action::DeleteCg(delete_cg_args) => {
            delete_cg(delete_cg_args, fumarole_client).await;
        }
        Action::ListCg => {
            list_all_cg(fumarole_client).await;
        }
        Action::DeleteAllCg => {
            delete_all_cg(fumarole_client).await;
        }
        Action::Subscribe(subscribe_args) => {
            subscribe(fumarole_client, subscribe_args).await;
        }
    }
}
