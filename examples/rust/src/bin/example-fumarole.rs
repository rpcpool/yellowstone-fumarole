use {
    clap::Parser,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, path::PathBuf},
    yellowstone_fumarole_client::{
        config::FumaroleConfig, DragonsmouthAdapterSession, FumaroleClient,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
        SubscribeUpdateAccount, SubscribeUpdateTransaction,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone Fumarole Example")]
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
    /// Name of the persistent subscriber to use
    #[clap(long)]
    name: String,
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

async fn subscribe(args: SubscribeArgs, config: FumaroleConfig) {
    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        ..Default::default()
    };

    let mut fumarole_client = FumaroleClient::connect(config)
        .await
        .expect("Failed to connect to fumarole");

    let dragonsmouth_session = fumarole_client
        .dragonsmouth_subscribe(args.name, request)
        .await
        .expect("Failed to subscribe");

    let DragonsmouthAdapterSession {
        sink: _,
        mut source,
        mut fumarole_handle,
    } = dragonsmouth_session;

    loop {
        tokio::select! {
            result = &mut fumarole_handle => {
                eprintln!("Fumarole handle closed: {:?}", result);
                break;
            }
            maybe = source.recv() => {
                match maybe {
                    None => {
                        eprintln!("Source closed");
                        break;
                    }
                    Some(result) => {
                        let event = result.expect("Failed to receive event");
                        let message = if let Some(oneof) = event.update_oneof {
                            match oneof {
                                UpdateOneof::Account(account_update) => {
                                    summarize_account(account_update)
                                }
                                UpdateOneof::Transaction(tx) => {
                                    summarize_tx(tx)
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
