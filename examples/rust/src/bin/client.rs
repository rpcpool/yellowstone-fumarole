use {
    clap::Parser,
    solana_sdk::{bs58, pubkey::Pubkey},
    std::path::PathBuf,
    tokio::{sync::mpsc, task::JoinSet},
    yellowstone_fumarole_client::{
        config::FumaroleConfig, proto::SubscribeRequest, FumaroleClient, FumaroleClientBuilder,
    },
    yellowstone_grpc_proto::geyser::{SubscribeUpdateAccount, SubscribeUpdateTransaction},
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC ScyllaDB Tool")]
struct Args {
    /// Path to static config file
    #[clap(long)]
    config: PathBuf,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Clone, Parser)]
enum Action {
    /// Subscribe to fumarole events
    Subscribe(SubscribeArgs),
}

#[derive(Debug, Clone, Parser)]
struct SubscribeArgs {
    /// Name of the consumer group to subscribe to
    #[clap(long)]
    cg_name: String,

    /// List of account pubkeys that we want to suscribe to account updates
    #[clap(long, required = false)]
    accounts: Option<Vec<Pubkey>>,

    /// List of owner pubkeys that we want to suscribe to account updates
    #[clap(long, required = false)]
    owners: Option<Vec<Pubkey>>,

    /// A transaction is included if it has at least one of the provided accounts in its list of instructions
    #[clap(long, required = false)]
    tx_includes: Option<Vec<Pubkey>>,

    /// A transaction is excluded if it has at least one of the provided accounts in its list of instructions
    #[clap(long, required = false)]
    tx_excludes: Option<Vec<Pubkey>>,

    /// A transaction is included if all of the provided accounts in its list of instructions
    #[clap(long, required = false)]
    tx_requires: Option<Vec<Pubkey>>,

    #[clap(long, required = false)]
    include_vote_tx: Option<bool>,

    #[clap(long, required = false)]
    include_failed_tx: Option<bool>,

    /// Number of parallel streams to open: must be lower or equal to the size of your consumer group, otherwise the program will return an error
    #[clap(long)]
    par: Option<u32>,
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

async fn subscribe_with_request(
    mut fumarole: FumaroleClient,
    request: SubscribeRequest,
    out_tx: mpsc::Sender<String>,
) {
    // NOTE: Make sure send request before giving the stream to the service
    // Otherwise, the service will not be able to send the response
    // This is due to how fumarole works in the background for auto-commit offset management.
    let rx = fumarole
        .subscribe_with_request(request)
        .await
        .expect("Failed to subscribe to Fumarole service");
    println!("Subscribed to Fumarole service");
    println!("Request sent");
    let mut rx = rx.into_inner();

    loop {
        match rx.message().await {
            Ok(Some(event)) => {
                let message = if let Some(oneof) = event.update_oneof {
                    match oneof {
                        yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Account(account_update) => {
                            summarize_account(account_update)
                        }
                        yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Transaction(tx) => {
                            summarize_tx(tx)
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                if let Some(message) = message {
                    if out_tx.send(message).await.is_err() {
                        break;
                    }
                }
            }
            Ok(None) => println!("Stream finished!"),
            Err(e) => {
                eprintln!("Error receiving event: {:?}", e);
                break;
            }
        }
    }
}

async fn subscribe(args: SubscribeArgs, config: FumaroleConfig) {
    let accounts = args.accounts;
    let owners = args.owners;
    let tx_includes = args.tx_includes;
    let tx_requires = args.tx_requires;
    let tx_excludes = args.tx_excludes;
    let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
        .with_accounts(accounts)
        .with_owners(owners)
        .with_tx_includes(tx_includes)
        .with_tx_requires(tx_requires)
        .with_tx_excludes(tx_excludes)
        .build_vec(args.cg_name, args.par.unwrap_or(1));

    let mut task_set = JoinSet::new();

    let (shared_tx, mut rx) = mpsc::channel(1000);
    for request in requests {
        let fumarole = FumaroleClientBuilder::default()
            .connect(config.clone())
            .await
            .expect("Failed to connect to Fumarole service");
        let tx = shared_tx.clone();
        task_set.spawn(subscribe_with_request(fumarole, request, tx));
    }

    loop {
        tokio::select! {
            maybe = task_set.join_next() => {
                let result = maybe.expect("no task");
                if let Err(e) = result {
                    eprintln!("Task failed: {:?}", e);
                }
                break
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(message) => {
                        println!("{}", message);
                    }
                    None => {
                        break;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let config = std::fs::read_to_string(&args.config).expect("Failed to read config file");
    let config: FumaroleConfig =
        serde_yaml::from_str(&config).expect("Failed to parse config file");

    match args.action {
        Action::Subscribe(sub_args) => {
            subscribe(sub_args, config).await;
        }
    }
}
