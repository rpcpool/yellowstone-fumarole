use {
    clap::Parser,
    solana_sdk::{bs58, pubkey::Pubkey},
    std::{collections::HashMap, path::PathBuf},
    tokio::{sync::mpsc, task::JoinSet},
    tokio_stream::StreamExt,
    tonic::transport::Endpoint,
    yellowstone_fumarole_client::config::FumaroleConfig,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder},
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

/// This code serves as a reference to compare fumarole against dragonsmouth original client

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config_path = args.config;
    let config = std::fs::read_to_string(config_path).expect("Failed to read config file");
    let config: FumaroleConfig =
        serde_yaml::from_str(&config).expect("Failed to parse config file");

    let endpoint = config.endpoint.clone();

    let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
        .expect("Failed to parse endpoint")
        .x_token(config.x_token)
        .expect("x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("tls_config")
        .connect()
        .await
        .expect("Failed to connect to geyser");

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: HashMap::from([("f1".to_owned(), SubscribeRequestFilterAccounts::default())]),
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        ..Default::default()
    };
    let (_sink, mut rx) = geyser
        .subscribe_with_request(Some(request))
        .await
        .expect("Failed to subscribe");

    while let Some(result) = rx.next().await {
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
