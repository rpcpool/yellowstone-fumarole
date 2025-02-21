use {
    clap::Parser, solana_sdk::{bs58, pubkey::Pubkey, signature::Signature, signer::Signer}, std::{collections::{BTreeMap, HashMap, HashSet}, marker::PhantomData, ops::Sub, path::PathBuf, time::Duration}, tokio::time::Instant, tokio_stream::StreamExt, yellowstone_fumarole_client::config::FumaroleConfig, yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder}, yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction
    }
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


enum BlockEvent {
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
    Slot(SubscribeUpdateSlot),
    Tx(SubscribeUpdateTransaction),
    Account(SubscribeUpdateAccount),
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum BlockConstructionError {
    #[error("Too many transactions")]
    TooManyTx,
    #[error("Too many entries")]
    TooManyEntries,
    #[error("Too many account writes")]
    TooManyAccountWrites,
    #[error("Unexpected account update")]
    UnexpectedAccountUpdate,
}

pub struct BlockConstruction {
    slot: u64,
    events: Vec<(u32, BlockEvent)>,
    entries_vec: Vec<usize>,
    block_meta_vec: Vec<usize>,
    slot_vec: Vec<usize>,
    tx_vec: Vec<usize>,
    tx_account_writes: BTreeMap<Pubkey, usize>,
    account_update_cnt_map: BTreeMap<Pubkey, usize>,
    account_vec: Vec<usize>,
    time: u32,
    is_sealed: bool,
    error: Option<BlockConstructionError>,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidBlockEvent {
    #[error("Slot mismatch")]
    SlotMismatch,
    #[error("Block already sealed")]
    BlockAlreadySealed,
    #[error("Invalid block event")]
    CorruptedBlock(#[from] BlockConstructionError),
    #[error("Unsupported event")]
    Unsupported(UpdateOneof),
}

pub struct ProjectionIterator<'a, T> {
    events: &'a Vec<(u32, BlockEvent)>,
    index_vec: &'a Vec<usize>,
    dest_type: PhantomData<T>,
    curr: usize,
}

macro_rules! impl_projection_for {
    ($src:path, $dest:ty) => {

        impl<'a> Iterator for ProjectionIterator<'a, $dest> {
            type Item = &'a $dest;

            fn next(&mut self) -> Option<Self::Item> {
                if self.curr < self.index_vec.len() {
                    let i = self.index_vec[self.curr];
                    self.curr += 1;
                    if let $src(event) = &self.events[i].1 {
                        Some(event)
                    } else {
                        panic!("unexpected type at {i}, expected $type");
                    }
                } else {
                    None
                }
            }
        }


        impl<'a> Projection<'a, $dest> {
            pub fn iter(&'a self) -> ProjectionIterator<'a, $dest> {
                ProjectionIterator {
                    events: self.events,
                    index_vec: self.index_vec,
                    dest_type: PhantomData,
                    curr: 0,
                }
            }
        }
    };
}

impl_projection_for!(BlockEvent::Tx, SubscribeUpdateTransaction);
impl_projection_for!(BlockEvent::Account, SubscribeUpdateAccount);
impl_projection_for!(BlockEvent::BlockMeta, SubscribeUpdateBlockMeta);
impl_projection_for!(BlockEvent::Slot, SubscribeUpdateSlot);
impl_projection_for!(BlockEvent::Entry, SubscribeUpdateEntry);

pub struct Projection<'a, T> {
    events: &'a Vec<(u32, BlockEvent)>,
    index_vec: &'a Vec<usize>,
    dest_type: PhantomData<T>,
}

pub fn extract_writable_address<'a, IT>(tx_updates: IT) -> Vec<Pubkey> 
    where IT: IntoIterator<Item = &'a SubscribeUpdateTransaction>
{
    let mut ret = vec![];
    for tx in tx_updates.into_iter() {
        let tx2 = match &tx.transaction {
            Some(x ) => x,
            None => continue,
        };
        let meta = match tx2.meta.as_ref() {
            Some(x) => x,
            None => continue,
        };
        meta.loaded_writable_addresses.iter().for_each(|x| {
            let pk = Pubkey::try_from(x.as_slice()).expect("Failed to parse pubkey");
            ret.push(pk);
        });
    }
    ret
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum BlockConstructSummarizedEvents {
    TxOrAccount,
    BlockMeta,
    Entry,
    Slot(CommitmentLevel)
}

impl BlockConstruction {
    pub fn init(slot: u64) -> Self {
        Self {
            slot,
            events: Vec::new(),
            entries_vec: Vec::new(),
            block_meta_vec: Vec::new(),
            tx_vec: Vec::new(),
            slot_vec: Vec::new(),
            tx_account_writes: BTreeMap::new(),
            account_update_cnt_map: BTreeMap::new(),
            account_vec: Vec::new(),
            time: 0,
            error: None,
            is_sealed: false,
        }
    }

    fn project_accounts(&self) -> Projection<SubscribeUpdateAccount> {
        Projection {
            events: &self.events,
            index_vec: &self.account_vec,
            dest_type: PhantomData,
        }
    }

    fn project_txs(&self) -> Projection<SubscribeUpdateTransaction> {
        Projection {
            events: &self.events,
            index_vec: &self.tx_vec,
            dest_type: PhantomData,
        }
    }

    fn try_seal(&mut self) {
        if let Some(i) = self.block_meta_vec.last() {
            let boxed_event = &self.events[*i].1;
            if let BlockEvent::BlockMeta(block_meta) = boxed_event {
                let entry_count_match = block_meta.entries_count == self.entries_vec.len() as u64;
                let tx_count_match = block_meta.executed_transaction_count == self.tx_vec.len() as u64;
                let total_account_writes: usize = self.tx_account_writes.values().sum();
                let account_update_match = self.account_vec.len() == total_account_writes;
                
                if block_meta.entries_count < self.entries_vec.len() as u64 {
                    self.error = Some(BlockConstructionError::TooManyEntries);
                }

                if block_meta.executed_transaction_count < self.tx_vec.len() as u64 {
                    self.error = Some(BlockConstructionError::TooManyTx);
                }

                if entry_count_match && tx_count_match && account_update_match {
                    for (pair1, pair2) in self.account_update_cnt_map.iter().zip(self.tx_account_writes.iter()) {
                        let (pubkey1, cnt1) = pair1;
                        let (pubkey2, cnt2) = pair2;
                        if pubkey1 != pubkey2 {
                            self.error = Some(BlockConstructionError::UnexpectedAccountUpdate);
                            return;
                        }
                        if cnt1 != cnt2 {
                            self.error = Some(BlockConstructionError::UnexpectedAccountUpdate);
                            return;
                        }
                    }

                    println!("Block sealed tx_account_writes={}, account_writes={:?}, entry count={}", total_account_writes, self.account_vec.len(), block_meta.entries_count);
                    self.is_sealed = true;
                } else {
                    let missing_tx = block_meta.executed_transaction_count as usize - self.tx_vec.len();
                    let missing_entries = block_meta.entries_count as usize - self.entries_vec.len();
                    let missing_accounts= if missing_tx == 0 {
                        let left_join = self.account_tx_sig_left_outer_join_tx();
                        Some((total_account_writes, self.account_vec.len(), left_join))
                    } else {
                        None
                    };
                    // let missing_account_writes = total_account_writes - self.account_vec.len();
                    eprintln!("Block not sealed, missing tx={}, missing entries={}, missing accountw = {:?}", missing_tx, missing_entries, missing_accounts);
                }

            } else {
                panic!("unexpected type at {i}, expected BlockMeta");
            }
        } 
    }

    fn tick(&mut self) -> u32 {
        self.time += 1;
        return self.time
    }

    fn account_tx_sig_left_outer_join_tx(&self) -> Vec<Signature> {
        let mut ret = vec![];
        let tx_sig = self.project_txs()
            .iter()
            .flat_map(|tx| tx.transaction.as_ref().map(|tx| tx.signature.as_slice()))
            .map(|raw_sig| Signature::try_from(raw_sig).expect("Failed to parse signature"))
            .collect::<HashSet<_>>();

        let mut account_sig_matches: BTreeMap<Pubkey, Vec<u64>> = BTreeMap::default();
        for account in self.project_accounts().iter() {
            let account2 = account.account.as_ref().expect("no account_info");
            let pubkey = Pubkey::try_from(account2.pubkey.as_slice()).expect("Failed to parse pubkey");
            let write_version = account2.write_version;
            let raw_sig = account2.txn_signature();
            let sig = Signature::try_from(raw_sig).expect("Failed to parse signature");
            let write_versions = account_sig_matches
                .entry(pubkey)
                .or_default();
            write_versions.push(write_version);
            if tx_sig.contains(&sig) {
                continue;
            } else {
                ret.push(sig);
            }
        }
        ret
    }

    fn add_event_checked(&mut self, event: BlockEvent) -> Result<(), InvalidBlockEvent> {
        if let Some(err) = &self.error {
            return Err(InvalidBlockEvent::CorruptedBlock(err.clone()));
        }
        
        if self.is_sealed {
            return Err(InvalidBlockEvent::BlockAlreadySealed);
        }

        let slot = match &event {
            BlockEvent::Tx(tx) => tx.slot,
            BlockEvent::Account(account) => account.slot,
            BlockEvent::BlockMeta(block_meta) => block_meta.slot,
            BlockEvent::Entry(entry) => entry.slot,
            BlockEvent::Slot(slot) => slot.slot,
        };

        if slot != self.slot {
            return Err(InvalidBlockEvent::SlotMismatch);
        }
        
        let time = self.tick();
        match &event {
            BlockEvent::Tx(subscribe_update_transaction) => { 
                self.handle_add_tx(subscribe_update_transaction);
            },
            BlockEvent::Account(subscribe_update_account) => {
                self.handle_add_account(subscribe_update_account);
            }
            _ => {},
        }
        self.events.push((time, event));
        let i = self.events.len() - 1;
        match self.events.last().expect("unexpected").1 {
            BlockEvent::Tx(_) => {
                self.tx_vec.push(i);
            },
            BlockEvent::Account(_) => {
                self.account_vec.push(i);
            },
            BlockEvent::BlockMeta(_) => {
                self.block_meta_vec.push(i);
            },
            BlockEvent::Entry(_) => {
                self.entries_vec.push(i);
            },
            BlockEvent::Slot(_) => {
                self.slot_vec.push(i);
            },
        }
        self.try_seal();
        if let Some(e) = &self.error {
            return Err(InvalidBlockEvent::CorruptedBlock(e.clone()));
        }
        Ok(())
    }


    pub fn try_add_event(&mut self, oneof: UpdateOneof) -> Result<(), InvalidBlockEvent> {
        let event = match oneof {
            UpdateOneof::Account(account) => {
                Ok(BlockEvent::Account(account))
            },
            UpdateOneof::Transaction(tx) => {
                Ok(BlockEvent::Tx(tx))
            },
            UpdateOneof::BlockMeta(block_meta) => {
                Ok(BlockEvent::BlockMeta(block_meta))
            },
            UpdateOneof::Entry(entry) => {
                Ok(BlockEvent::Entry(entry))
            },
            UpdateOneof::Slot(slot) => {
                Ok(BlockEvent::Slot(slot))
            },
            whatever => Err(InvalidBlockEvent::Unsupported(whatever)),
        }?;
        self.add_event_checked(event)
    }

    fn handle_add_tx(&mut self, tx: &SubscribeUpdateTransaction) {
        let write_accounts = extract_writable_address([tx]);
        for account in write_accounts {
            let cnt = self.tx_account_writes
                .entry(account)
                .or_default();
            *cnt += 1;
        }
    }

    fn handle_add_account(&mut self, account: &SubscribeUpdateAccount) {
        let pubkey = Pubkey::try_from(
            account
                .account
                .as_ref()
                .expect("no account_info")
                .pubkey
                .as_slice()
        )
        .expect("Failed to parse pubkey");

        let cnt = self.account_update_cnt_map
            .entry(pubkey)
            .or_default();
        *cnt += 1;
    }

    pub fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    pub fn summarize_timeline(&self) -> Vec<BlockConstructSummarizedEvents> {
        if self.events.is_empty() {
            return Vec::new();
        }

        let mut summarized = vec![];

        for (_, event) in self.events.iter() {
            match event {
                BlockEvent::Tx(_) | BlockEvent::Account(_) => {
                    summarized.push(BlockConstructSummarizedEvents::TxOrAccount);
                },
                BlockEvent::BlockMeta(_) => {
                    summarized.push(BlockConstructSummarizedEvents::BlockMeta);
                },
                BlockEvent::Entry(_) => {
                    summarized.push(BlockConstructSummarizedEvents::Entry);
                },
                BlockEvent::Slot(slot) => {
                    summarized.push(BlockConstructSummarizedEvents::Slot(CommitmentLevel::try_from(slot.status).expect("invalid commitment level")));
                },
            }
        }

        // Now coalesce all TxOrAccount events
        let mut compressed_summary = vec![summarized[0]];

        for event in summarized.iter().skip(1) {
            let prev = compressed_summary.last().expect("empty summary");
            match (prev, event) {
                (BlockConstructSummarizedEvents::TxOrAccount, BlockConstructSummarizedEvents::TxOrAccount) => {
                    continue;
                },
                _ => {
                    compressed_summary.push(*event);
                }
            }
        }

        compressed_summary
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

    let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
        .expect("Failed to parse endpoint")
        .x_token(config.x_token)
        .expect("x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("tls_config")
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .connect()
        .await
        .expect("Failed to connect to geyser");

    // This request listen for all account updates and transaction updates
    let request = SubscribeRequest {
        accounts: HashMap::from([
            (
                "f1".to_owned(),
                SubscribeRequestFilterAccounts {
                    nonempty_txn_signature: Some(true),
                    ..Default::default()
                }
            )
        ]),
        transactions: HashMap::from([("f1".to_owned(), Default::default())]),
        slots: HashMap::from([("f1".to_owned(), Default::default())]),
        blocks_meta: HashMap::from([("f1".to_owned(), Default::default())]),
        entry: HashMap::from([("f1".to_owned(), Default::default())]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };
    let (_sink, mut rx) = geyser
        .subscribe_with_request(Some(request))
        .await
        .expect("Failed to subscribe");

    let mut block_construction_map = BTreeMap::default();

    let mut t = Instant::now();
    while let Some(result) = rx.next().await {
        let event = result.expect("Failed to receive event");

        if t.elapsed() >= Duration::from_secs(1) {
            let pending_blocks = block_construction_map.len();
            println!("Pending blocks: {}", pending_blocks);
            t = Instant::now();
        }

        if let Some(oneof) = event.update_oneof {
            let maybe_slot = match &oneof {
                UpdateOneof::Account(subscribe_update_account) => Some(subscribe_update_account.slot),
                UpdateOneof::Slot(subscribe_update_slot) => Some(subscribe_update_slot.slot),
                UpdateOneof::Transaction(subscribe_update_transaction) => Some(subscribe_update_transaction.slot),
                UpdateOneof::TransactionStatus(subscribe_update_transaction_status) => Some(subscribe_update_transaction_status.slot),
                UpdateOneof::Block(subscribe_update_block) => Some(subscribe_update_block.slot),
                UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                    println!("Block meta: {:?}", subscribe_update_block_meta.slot);
                    Some(subscribe_update_block_meta.slot)
                },
                UpdateOneof::Entry(subscribe_update_entry) => Some(subscribe_update_entry.slot),
                _ => None,
            };
            let slot = match maybe_slot {
                Some(slot) => slot,
                None => {
                    println!("Unsupported event");
                    continue;
                }
            };

            if !block_construction_map.contains_key(&slot) {
                if block_construction_map.len() > 10 {
                    continue
                }
            }

            let block = block_construction_map
                .entry(slot)
                .or_insert_with(|| BlockConstruction::init(slot));

            match block.try_add_event(oneof) {
                Ok(_) => {},
                Err(e) => {
                    match e {
                        InvalidBlockEvent::BlockAlreadySealed => {
                            let block = block_construction_map.remove(&slot).expect("block not found");
                            let summary = block.summarize_timeline();
                            eprintln!("Block already sealed: {:?}", summary);
                            break;
                        },
                        InvalidBlockEvent::CorruptedBlock(err) => {
                            eprintln!("Block error: {:?}", err);
                        },
                        InvalidBlockEvent::Unsupported(_) => {
                            continue;
                        },
                        InvalidBlockEvent::SlotMismatch => {
                            panic!("Slot mismatch");
                        },
                    }
                }
            }

        }
    }
}
