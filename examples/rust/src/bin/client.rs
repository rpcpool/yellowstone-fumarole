use {
    clap::Parser,
    solana_sdk::{bs58, pubkey::Pubkey},
    std::{collections::HashMap, path::PathBuf},
    yellowstone_fumarole_client::{
        config::FumaroleConfig, DragonsmouthAdapterSession, FumaroleClient, FumaroleSubscribeConfig,
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateTransaction,
    },
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

async fn subscribe(args: SubscribeArgs, config: FumaroleConfig) {
    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: HashMap::from([("f1".to_owned(), SubscribeRequestFilterAccounts::default())]),
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        ..Default::default()
    };

    let mut fumarole_client = FumaroleClient::connect(config)
        .await
        .expect("Failed to connect to fumarole");

    let subscribe_config = FumaroleSubscribeConfig {
        ..Default::default()
    };
    let dragonsmouth_session = fumarole_client
        .dragonsmouth_subscribe(args.cg_name, request, subscribe_config)
        .await
        .expect("Failed to subscribe");

    let DragonsmouthAdapterSession {
        sink: _,
        mut source,
        runtime_handle: _,
    } = dragonsmouth_session;

    while let Some(result) = source.recv().await {
        let event = result.expect("Failed to receive event");

        let message = if let Some(oneof) = event.update_oneof {
            match oneof {
                UpdateOneof::Account(account_update) => summarize_account(account_update),
                UpdateOneof::Transaction(tx) => summarize_tx(tx),
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
