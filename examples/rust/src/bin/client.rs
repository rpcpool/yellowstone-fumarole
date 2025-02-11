use std::path::PathBuf;

use clap::{command, Parser, Subcommand};
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use yellowstone_fumarole_client::{config::FumaroleConfig, FumaroleClientBuilder};




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
    Subscribe(SubscribeArgs)
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

    /// List of account pubkeys that we want transaction to include
    #[clap(long, required = false)]
    tx_accounts: Option<Vec<Pubkey>>,
}

async fn subscribe(
    args: SubscribeArgs,
    config: FumaroleConfig,
) {
    let mut fumarole = FumaroleClientBuilder::connect(config)
        .await
        .expect("Failed to connect to Fumarole service");
    let accounts = args.accounts;
    let owners = args.owners;
    let tx_accounts = args.tx_accounts;

    let request = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
        .with_accounts(accounts)
        .with_owners(owners)
        .with_tx_accounts(tx_accounts)
        .build("my_consumer".to_string());

    let (tx, rx) = mpsc::channel(100);
    let rx = ReceiverStream::new(rx);
    let rx = fumarole.subscribe(rx).await.expect("Failed to subscribe to Fumarole service");

    tx.send(request).await.expect("Failed to send request to Fumarole service");

    let mut rx = rx.into_inner();

    loop {
        match rx.message().await {
            Ok(Some(event)) => {
                if let Some(oneof) = event.update_oneof {
                    match oneof {
                        yellowstone_fumarole_client::geyser::subscribe_update::UpdateOneof::Account(account_update) => {
                            let account = match account_update.account {
                                Some(account_update) => account_update,
                                None => continue,
                            };
                            let pubkey = Pubkey::try_from(account.pubkey).expect("Failed to parse pubkey");
                            let owner = Pubkey::try_from(account.owner).expect("Failed to parse owner");
                            let slot = account_update.slot;
                            println!("account,{slot},{pubkey},{owner}");
                        }
                        yellowstone_fumarole_client::geyser::subscribe_update::UpdateOneof::Transaction(tx) => {
                            let slot = tx.slot;
                            let tx = match tx.transaction {
                                Some(tx) => tx,
                                None => continue,
                            };
                            let tx_id = Pubkey::try_from(tx.signature).expect("Failed to parse tx signature");
                            println!("tx,{slot},{tx_id}");
                        }
                        _ => continue,
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


#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let config = std::fs::read_to_string(&args.config).expect("Failed to read config file");
    let config: FumaroleConfig = serde_yaml::from_str(&config).expect("Failed to parse config file");

    match args.action {
        Action::Subscribe(sub_args) => {
            subscribe(sub_args, config).await;
        }
    }
}