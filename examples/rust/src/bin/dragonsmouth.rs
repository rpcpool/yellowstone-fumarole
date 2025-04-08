use {
    clap::Parser, solana_sdk::{
        blake3::Hash,
        bs58,
        pubkey::{self, Pubkey},
        signature::Signature,
        signer::Signer,
    }, spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount}, std::{
        collections::{BTreeMap, HashMap, HashSet},
        marker::PhantomData,
        ops::Sub,
        path::PathBuf,
        time::Duration,
    }, tokio::time::Instant, tokio_stream::StreamExt, yellowstone_fumarole_client::config::FumaroleConfig, yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder}, yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
            SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        },
        prelude::{Message, TransactionStatusMeta},
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

#[derive(Debug)]
enum BlockEvent {
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
    Slot(SubscribeUpdateSlot),
    Tx(SubscribeUpdateTransaction),
    Account(SubscribeUpdateAccount),
}

impl BlockEvent {
    pub fn unwrap(self) -> UpdateOneof {
        match self {
            BlockEvent::Entry(entry) => UpdateOneof::Entry(entry),
            BlockEvent::BlockMeta(block_meta) => UpdateOneof::BlockMeta(block_meta),
            BlockEvent::Slot(slot) => UpdateOneof::Slot(slot),
            BlockEvent::Tx(tx) => UpdateOneof::Transaction(tx),
            BlockEvent::Account(account) => UpdateOneof::Account(account),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum BlockConstructionError {
    #[error("Too many transactions")]
    TooManyTx,
    #[error("Too many entries")]
    TooManyEntries,
    #[error("Sum of all transaction account in entries does not match block meta")]
    MismatchEntriesTxCount,
    #[error("Too many account writes")]
    TooManyAccountWrites,
    #[error("Unexpected account update")]
    UnexpectedAccountUpdate,
}

pub struct BlockConstruction {
    slot: u64,
    events: Vec<BlockEvent>,
    entries_vec: Vec<usize>,
    block_meta_vec: Vec<usize>,
    slot_vec: Vec<usize>,
    tx_vec: Vec<usize>,
    account_vec: Vec<usize>,
    time: u32,
    is_sealed: bool,
    error: Option<BlockConstructionError>,

    /// Map of transaction signature to account writes
    tx_account_writes_index: HashMap<Signature, HashSet<Pubkey>>,
    account_update_tx_sig_index: HashMap<Signature, HashSet<Pubkey>>,
    emptysig_accounts_index: Vec<usize>,
    failed_tx_index: HashSet<Signature>,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidBlockEvent {
    #[error("Slot mismatch")]
    SlotMismatch(UpdateOneof),
    #[error("Block already sealed")]
    BlockAlreadySealed(UpdateOneof),
    #[error("Invalid block event")]
    CorruptedBlock {
        event: UpdateOneof,
        err: BlockConstructionError,
    },
    #[error("Unsupported event")]
    Unsupported(UpdateOneof),
}

pub struct ProjectionIterator<'a, T> {
    events: &'a Vec<BlockEvent>,
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
                    if let $src(event) = &self.events[i] {
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

        impl<'a> IntoIterator for Projection<'a, $dest> {
            type Item = &'a $dest;
            type IntoIter = ProjectionIterator<'a, $dest>;

            fn into_iter(self) -> Self::IntoIter {
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
    events: &'a Vec<BlockEvent>,
    index_vec: &'a Vec<usize>,
    dest_type: PhantomData<T>,
}

impl<'a, T> Clone for Projection<'a, T> {
    fn clone(&self) -> Self {
        Self {
            events: self.events,
            index_vec: self.index_vec,
            dest_type: PhantomData,
        }
    }
}

pub fn extract_static_writable_account_keys<'a>(
    tx: &'a Message,
) -> impl Iterator<Item = Pubkey> + 'a {
    let header = tx.header.as_ref().expect("no header");
    let account_keys = tx.account_keys.as_slice();

    let signed_keys = &account_keys[0..header.num_required_signatures as usize];
    let unsigned_keys = &account_keys[header.num_required_signatures as usize..];

    let num_writable_signed = header
        .num_required_signatures
        .saturating_sub(header.num_readonly_signed_accounts) as usize;

    let num_unsigned_accounts = account_keys.len() - header.num_required_signatures as usize;
    let num_writable_unsigned =
        num_unsigned_accounts.saturating_sub(header.num_readonly_unsigned_accounts as usize);

    let writable_signed_accounts = &signed_keys[0..num_writable_signed];
    let writable_unsigned_accounts = &unsigned_keys[0..num_writable_unsigned];

    writable_signed_accounts
        .iter()
        .chain(writable_unsigned_accounts)
        .map(|x| Pubkey::try_from(x.as_slice()).expect("Failed to parse pubkey"))
}

pub fn get_all_writable_address_mapping<'a, IT>(
    tx_updates: IT,
) -> HashMap<Signature, HashSet<Pubkey>>
where
    IT: IntoIterator<Item = &'a SubscribeUpdateTransaction> + 'a,
{
    let mut ret = HashMap::new();
    for tx in tx_updates {
        let pubkeys = tx.iter_writable_addresses().collect();
        ret.insert(tx.sig(), pubkeys);
    }
    ret
}

pub fn extract_writable_address_from_txs<'a, IT>(
    tx_updates: IT,
) -> impl Iterator<Item = Pubkey> + 'a
where
    IT: Clone + IntoIterator<Item = &'a SubscribeUpdateTransaction> + 'a,
{
    tx_updates
        .into_iter()
        .flat_map(|tx| tx.iter_writable_addresses())
}

pub trait SubscribeUpdateTransactionExt {
    fn this(&self) -> &SubscribeUpdateTransaction;

    fn tx_info(&self) -> &SubscribeUpdateTransactionInfo {
        self.this().transaction.as_ref().expect("no transaction")
    }

    fn meta(&self) -> &TransactionStatusMeta {
        self.tx_info().meta.as_ref().expect("no meta")
    }

    fn sig(&self) -> Signature {
        Signature::try_from(self.tx_info().signature.as_slice()).expect("Failed to parse signature")
    }

    fn iter_writable_addresses(&self) -> impl Iterator<Item = Pubkey> {
        let alt_address = self
            .meta()
            .loaded_writable_addresses
            .iter()
            .map(|x| Pubkey::try_from(x.as_slice()).expect("Failed to parse pubkey"));

        let static_address = self
            .tx_info()
            .transaction
            .as_ref()
            .into_iter()
            .flat_map(|tx| tx.message.as_ref())
            .flat_map(|msg| extract_static_writable_account_keys(msg));

        alt_address.chain(static_address)
    }

    fn is_failed(&self) -> bool {
        self.meta().err.is_some()
    }
}

pub trait SubscribeUpdateAccountExt {
    fn this(&self) -> &SubscribeUpdateAccount;

    fn pubkey(&self) -> Pubkey {
        Pubkey::try_from(
            self.this()
                .account
                .as_ref()
                .expect("no account_info")
                .pubkey
                .as_slice(),
        )
        .expect("Failed to parse pubkey")
    }

    fn data(&self) -> &[u8] {
        self.account_info().data.as_slice()
    }

    fn account_info(&self) -> &SubscribeUpdateAccountInfo {
        self.this().account.as_ref().expect("no account_info")
    }

    fn txn_signature(&self) -> Option<Signature> {
        let raw_sig = self.account_info().txn_signature();
        Signature::try_from(raw_sig).ok()
    }
}

impl SubscribeUpdateAccountExt for SubscribeUpdateAccount {
    fn this(&self) -> &SubscribeUpdateAccount {
        self
    }
}

impl SubscribeUpdateTransactionExt for SubscribeUpdateTransaction {
    fn this(&self) -> &SubscribeUpdateTransaction {
        self
    }
}

trait RollbackBlockConstruction {
    fn rollback(&mut self, this: &mut BlockConstruction, event_to_rollback: &BlockEvent);
}

type BoxedRollbackBlockConstruction = Box<dyn RollbackBlockConstruction>;

struct NullRollbackBlockConstruction;

impl RollbackBlockConstruction for NullRollbackBlockConstruction {
    fn rollback(&mut self, _this: &mut BlockConstruction, _event_to_rollback: &BlockEvent) {}
}

pub fn rollback_plan_from<F>(f: F) -> BoxedRollbackBlockConstruction 
    where F: FnMut(&mut BlockConstruction, &BlockEvent) + 'static
{
    struct RollbackBlockConstructionFn<F>{
        func: F,
        called: bool,
    }

    impl<F> RollbackBlockConstruction for RollbackBlockConstructionFn<F>
    where
        F: FnMut(&mut BlockConstruction, &BlockEvent),
    {
        fn rollback(&mut self, this: &mut BlockConstruction, event_to_rollback: &BlockEvent) {
            if self.called {
                panic!("rollback called twice");
            }
            (self.func)(this, event_to_rollback);
        }
    }
    Box::new(RollbackBlockConstructionFn { func: f, called: false })
}


#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum BlockConstructSummarizedEvents {
    TxOrAccount,
    BlockMeta,
    Entry,
    Slot(CommitmentLevel),
}

impl BlockConstruction {
    pub fn init(slot: u64) -> Self {
        Self {
            slot,
            events: Default::default(),
            entries_vec: Default::default(),
            block_meta_vec: Default::default(),
            tx_vec: Default::default(),
            slot_vec: Default::default(),
            account_vec: Default::default(),
            time: 0,
            error: None,
            is_sealed: false,
            tx_account_writes_index: Default::default(),
            account_update_tx_sig_index: Default::default(),
            failed_tx_index: Default::default(),
            emptysig_accounts_index: Default::default(),
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

    fn project_entries(&self) -> Projection<SubscribeUpdateEntry> {
        Projection {
            events: &self.events,
            index_vec: &self.entries_vec,
            dest_type: PhantomData,
        }
    }

    fn project_slots(&self) -> Projection<SubscribeUpdateSlot> {
        Projection {
            events: &self.events,
            index_vec: &self.slot_vec,
            dest_type: PhantomData,
        }
    }

    fn count_fail_tx(&self) -> usize {
        self.project_txs()
            .iter()
            .flat_map(|tx| tx.transaction.as_ref())
            .flat_map(|tx| tx.meta.as_ref())
            .filter(|tx_meta| tx_meta.err.is_some())
            .count()
    }

    fn iter_non_failed_tx<'a>(&'a self) -> impl Iterator<Item = &'a SubscribeUpdateTransaction> {
        self.project_txs().into_iter().filter(|tx| {
            tx.transaction
                .as_ref()
                .and_then(|tx2| tx2.meta.as_ref())
                .and_then(|meta| meta.err.as_ref())
                .is_none()
        })
    }

    fn sum_entries_tx_count(&self) -> u64 {
        self.project_entries()
            .iter()
            .map(|entry| entry.executed_transaction_count)
            .sum()
    }

    fn get_latest_slot_status(&self) -> Option<CommitmentLevel> {
        self.project_slots()
            .into_iter()
            .map(|slot| slot.status)
            .max()
            .map(|status_code| CommitmentLevel::try_from(status_code).expect("invalid commitment level"))
    }

    fn try_seal(&mut self) {
        
        if let Some(i) = self.block_meta_vec.last() {
            let boxed_event = &self.events[*i];
            if let BlockEvent::BlockMeta(block_meta) = boxed_event {
                let entry_count_match = block_meta.entries_count == self.entries_vec.len() as u64;
                let tx_count_match =
                    block_meta.executed_transaction_count == self.tx_vec.len() as u64;
                let entry_nested_count_match =
                    block_meta.executed_transaction_count == self.sum_entries_tx_count();

                let slot_at_least_confirmed = self.get_latest_slot_status()
                    .filter(|status| status >= &CommitmentLevel::Confirmed)
                    .is_some();

                if block_meta.entries_count < self.entries_vec.len() as u64 {
                    self.error = Some(BlockConstructionError::TooManyEntries);
                    return;
                }

                if block_meta.executed_transaction_count < self.tx_vec.len() as u64 {
                    self.error = Some(BlockConstructionError::TooManyTx);
                    return;
                }

                if entry_count_match && !entry_nested_count_match {
                    self.error = Some(BlockConstructionError::MismatchEntriesTxCount);
                    return;
                }

                if entry_count_match
                    && tx_count_match
                    && slot_at_least_confirmed
                    && entry_nested_count_match
                {
                    self.is_sealed = true;
                } else {
                    let missing_tx =
                        block_meta.executed_transaction_count as usize - self.tx_vec.len();
                    let missing_entries =
                        block_meta.entries_count as usize - self.entries_vec.len();
                    let failed_tx = self.count_fail_tx();
                    let unknown_account_tx_sigs_cnt: usize =
                        self.list_unknown_account_tx_sig().len();
                    let missing_account_update_cnt: usize =
                        self.count_missing_account_update_with_tx_sig(false);
                    // let missing_account_writes = total_account_writes - self.account_vec.len();
                    // eprintln!(
                    //     r#"Block not sealed {}: 
                    //         Total account updates without tx sig = {},
                    //             - Sysvar account updates = {},
                    //             - off-curve PDA updates = {},
                    //             - token account updates = {},
                    //             - ??Unexplained?? = {}
                    //         failed tx = {}
                    //         missing tx = {}
                    //         missing entries = {}
                    //         unknown account tx sigs = {} 
                    //         missing account updates / ex. failed tx  = {}
                    //     "#, 
                    //     self.slot,
                    //     self.emptysig_accounts_index.len(),
                    //     self.emptysig_accounts_index.len() - self.count_non_sysvar_account_empty_tx_sig(),
                    //     self.count_off_curve_empty_tx_sig(),
                    //     self.count_token_account_with_empty_tx_sig(),
                    //     self.emptysig_accounts_index.len()
                    //         .saturating_sub(self.emptysig_accounts_index.len() - self.count_non_sysvar_account_empty_tx_sig())
                    //         .saturating_sub(self.count_off_curve_empty_tx_sig())
                    //         .saturating_sub(self.count_token_account_with_empty_tx_sig()),
                    //     failed_tx, 
                    //     missing_tx, 
                    //     missing_entries,
                    //     unknown_account_tx_sigs_cnt,
                    //     missing_account_update_cnt
                    // );
                }
            } else {
                panic!("unexpected type at {i}, expected BlockMeta");
            }
        }
    }

    fn count_token_account_with_empty_tx_sig(&self) -> usize {
        let mut cnt = 0;
        for i in self.emptysig_accounts_index.as_slice() {
            let i = *i;
            let account = &self.events[i];
            if let BlockEvent::Account(account) = account {
                assert!(account.txn_signature().is_none());
                if !account.pubkey().is_on_curve() {
                    continue;
                }
                if TokenAccount::valid_account_data(account.data()) {
                    cnt += 1;
                }
            } else {
                panic!("unexpected type at {i}, expected Account, got {account:?}");
            }
        }
        cnt
    }

    fn count_off_curve_empty_tx_sig(&self) -> usize {
        let mut cnt = 0;
        for i in self.emptysig_accounts_index.as_slice() {
            let i = *i;
            let account = &self.events[i];
            if let BlockEvent::Account(account) = account {
                assert!(account.txn_signature().is_none());
                if !account.pubkey().is_on_curve() {
                    cnt += 1;
                }
            } else {
                panic!("unexpected type at {i}, expected Account, got {account:?}");
            }
        }
        cnt
    }

    fn count_non_sysvar_account_empty_tx_sig(&self) -> usize {
        let mut cnt = 0;
        for i in self.emptysig_accounts_index.as_slice() {
            let i = *i;
            let account = &self.events[i];
            if let BlockEvent::Account(account) = account {
                assert!(account.txn_signature().is_none());
                let pubkey_str = account.pubkey().to_string();
                if !pubkey_str.starts_with("Sysvar") {
                    cnt += 1;
                }
            } else {
                panic!("unexpected type at {i}, expected Account, got {account:?}");
            }
        }
        cnt
    }

    ///
    /// Lists all transaction signatures referenced in account updates but not yet received in transaction updates.
    ///
    fn list_unknown_account_tx_sig(&self) -> HashSet<Signature> {
        let mut ret: HashSet<Signature> = Default::default();
        for account in self.project_accounts() {
            let sig = if let Some(sig) = account.txn_signature() {
                sig
            } else {
                continue;
            };
            if self.tx_account_writes_index.contains_key(&sig) {
                continue;
            } else {
                ret.insert(sig);
            }
        }
        ret
    }

    ///
    /// For each transaction, iterates through all account writable addresses and checks if the received account update contains the address.
    ///
    fn count_missing_account_update_with_tx_sig(&self, include_failed: bool) -> usize {
        self.tx_account_writes_index
            .iter()
            .filter(|(tx_sig, _)| include_failed || !self.failed_tx_index.contains(tx_sig))
            // For each received tx signature to account writable addresses
            // Check if received account update contains
            .map(|(sig, pubkeyset)| {
                if let Some(pubkeyset2) = self.account_update_tx_sig_index.get(sig) {
                    pubkeyset
                        .iter()
                        // Count all missing pubkey
                        .filter(|pubkey| {
                            if !pubkeyset2.contains(pubkey) {
                                // println!("Missing pubkey: {:?}, in tx {:?}", pubkey, sig);
                                true
                            } else {
                                false
                            }
                        })
                        .count()
                } else {
                    // If none, the entire pubkeyset is missing
                    pubkeyset.len()
                }
            })
            .sum()
    }

    fn add_event_checked(&mut self, event: BlockEvent) -> Result<(), InvalidBlockEvent> {
        if let Some(err) = &self.error {
            return Err(InvalidBlockEvent::CorruptedBlock {
                event: event.unwrap(),
                err: err.clone()
            });
        }

        if self.is_sealed && !matches!(event, BlockEvent::Slot(_)) {
            return Err(InvalidBlockEvent::BlockAlreadySealed(event.unwrap()));
        }

        let slot = match &event {
            BlockEvent::Tx(tx) => tx.slot,
            BlockEvent::Account(account) => account.slot,
            BlockEvent::BlockMeta(block_meta) => block_meta.slot,
            BlockEvent::Entry(entry) => entry.slot,
            BlockEvent::Slot(slot) => slot.slot,
        };

        if slot != self.slot {
            return Err(InvalidBlockEvent::SlotMismatch(event.unwrap()));
        }

        let event_id = self.events.len();

        let rollback_plan = match &event {
            BlockEvent::Tx(subscribe_update_transaction) => {
                self.handle_add_tx(subscribe_update_transaction)
            }
            BlockEvent::Account(subscribe_update_account) => {
                self.handle_add_account(event_id, subscribe_update_account)
            }
            _ => {
                Box::new(NullRollbackBlockConstruction)
            }
        };

        self.events.push(event);

        let i = self.events.len() - 1;

        let mut rollback_plans = vec![rollback_plan];

        let rollback_plan = match self.events.last().expect("unexpected") {
            BlockEvent::Tx(_) => {
                self.tx_vec.push(i);
                rollback_plan_from(|this, _| { this.tx_vec.pop(); })
            }
            BlockEvent::Account(_) => {
                self.account_vec.push(i);
                rollback_plan_from(|this, _| { this.account_vec.pop(); })
            }
            BlockEvent::BlockMeta(_) => {
                self.block_meta_vec.push(i);
                rollback_plan_from(|this, _| { this.block_meta_vec.pop(); })
            }
            BlockEvent::Entry(_) => {
                self.entries_vec.push(i);
                rollback_plan_from(|this, _| { this.entries_vec.pop(); })
            }
            BlockEvent::Slot(_) => {
                self.slot_vec.push(i);
                rollback_plan_from(|this, _| { this.slot_vec.pop(); })
            }
        };

        rollback_plans.push(rollback_plan);
        self.try_seal();

        if let Some(e) = self.error.clone() {
            // rollback
            let event = self.events.pop().expect("unexpected");
            rollback_plans
                .drain(..)
                .for_each(|mut rb_plan| rb_plan.rollback(self, &event));

            return Err(InvalidBlockEvent::CorruptedBlock {
                event: event.unwrap(),
                err: e.clone()
            });
        }
        Ok(())
    }

    pub fn try_add_event(&mut self, oneof: UpdateOneof) -> Result<(), InvalidBlockEvent> {
        let event = match oneof {
            UpdateOneof::Account(account) => Ok(BlockEvent::Account(account)),
            UpdateOneof::Transaction(tx) => Ok(BlockEvent::Tx(tx)),
            UpdateOneof::BlockMeta(block_meta) => Ok(BlockEvent::BlockMeta(block_meta)),
            UpdateOneof::Entry(entry) => Ok(BlockEvent::Entry(entry)),
            UpdateOneof::Slot(slot) => Ok(BlockEvent::Slot(slot)),
            whatever => Err(InvalidBlockEvent::Unsupported(whatever)),
        }?;
        self.add_event_checked(event)
    }

    fn handle_add_tx(&mut self, tx: &SubscribeUpdateTransaction) -> BoxedRollbackBlockConstruction {

        tx.iter_writable_addresses().for_each(|pubkey| {
            self.tx_account_writes_index
                .entry(tx.sig())
                .or_default()
                .insert(pubkey);
        });
        if tx.is_failed() {
            self.failed_tx_index.insert(tx.sig());
        }

        struct RollbackBlockConstructionTx {
            sig: Signature,
            is_failed: bool,
            called: bool
        }

        impl RollbackBlockConstruction for RollbackBlockConstructionTx {
            fn rollback(&mut self, this: &mut BlockConstruction, _event_to_rollback: &BlockEvent) {
                if self.called {
                    panic!("rollback called twice");
                }
                this.tx_account_writes_index.remove(&self.sig);
                if self.is_failed {
                    this.failed_tx_index.remove(&self.sig);
                }
            }
        }

        Box::new(RollbackBlockConstructionTx { sig: tx.sig(), is_failed: tx.is_failed(), called: false })
    }

    fn handle_add_account(&mut self, event_idx: usize, account: &SubscribeUpdateAccount) -> BoxedRollbackBlockConstruction {
        let pubkey = account.pubkey();
        if let Some(sig) = account.txn_signature() {
            self.account_update_tx_sig_index
                .entry(sig)
                .or_default()
                .insert(pubkey);
        } else {
            self.emptysig_accounts_index.push(event_idx);
        }

        struct RollbackBlockConstructionAccount {
            tx_sig: Option<Signature>,
            pubkey: Pubkey,
            event_idx: usize,
            called: bool
        }

        impl RollbackBlockConstruction for RollbackBlockConstructionAccount {
            fn rollback(&mut self, this: &mut BlockConstruction, _event_to_rollback: &BlockEvent) {
                if self.called {
                    panic!("rollback called twice");
                }
                if let Some(sig) = self.tx_sig {
                    this.account_update_tx_sig_index.get_mut(&sig).map(|set| set.remove(&self.pubkey));
                } else {
                    this.emptysig_accounts_index.remove(self.event_idx);
                }
            }
        }

        Box::new(RollbackBlockConstructionAccount {
            tx_sig: account.txn_signature(),
            pubkey,
            event_idx,
            called: false,
        })
    }

    pub fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    pub fn summarize_timeline(&self) -> Vec<BlockConstructSummarizedEvents> {
        if self.events.is_empty() {
            return Vec::new();
        }

        let mut summarized = vec![];

        for event in self.events.iter() {
            match event {
                BlockEvent::Tx(_) | BlockEvent::Account(_) => {
                    summarized.push(BlockConstructSummarizedEvents::TxOrAccount);
                }
                BlockEvent::BlockMeta(_) => {
                    summarized.push(BlockConstructSummarizedEvents::BlockMeta);
                }
                BlockEvent::Entry(_) => {
                    summarized.push(BlockConstructSummarizedEvents::Entry);
                }
                BlockEvent::Slot(slot) => {
                    summarized.push(BlockConstructSummarizedEvents::Slot(
                        CommitmentLevel::try_from(slot.status).expect("invalid commitment level"),
                    ));
                }
            }
        }

        // Now coalesce all TxOrAccount events
        let mut compressed_summary = vec![summarized[0]];

        for event in summarized.iter().skip(1) {
            let prev = compressed_summary.last().expect("empty summary");
            match (prev, event) {
                (
                    BlockConstructSummarizedEvents::TxOrAccount,
                    BlockConstructSummarizedEvents::TxOrAccount,
                ) => {
                    continue;
                }
                (
                    BlockConstructSummarizedEvents::Entry,
                    BlockConstructSummarizedEvents::Entry,
                ) => {
                    continue;
                }
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
        accounts: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterAccounts {
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([(
            "f1".to_owned(),
            SubscribeRequestFilterTransactions {
                ..Default::default()
            },
        )]),
        slots: HashMap::from([("f1".to_owned(), Default::default())]),
        blocks_meta: HashMap::from([("f1".to_owned(), Default::default())]),
        entry: HashMap::from([("f1".to_owned(), Default::default())]),
        commitment: Some(CommitmentLevel::Processed as i32),
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
                UpdateOneof::Account(subscribe_update_account) => {
                    Some(subscribe_update_account.slot)
                }
                UpdateOneof::Slot(subscribe_update_slot) => Some(subscribe_update_slot.slot),
                UpdateOneof::Transaction(subscribe_update_transaction) => {
                    Some(subscribe_update_transaction.slot)
                }
                UpdateOneof::TransactionStatus(subscribe_update_transaction_status) => {
                    Some(subscribe_update_transaction_status.slot)
                }
                UpdateOneof::Block(subscribe_update_block) => Some(subscribe_update_block.slot),
                UpdateOneof::BlockMeta(subscribe_update_block_meta) => {
                    Some(subscribe_update_block_meta.slot)
                }
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

            // if !block_construction_map.contains_key(&slot) {
            //     if block_construction_map.len() > 10 {
            //         continue;
            //     }
            // }

            let block = block_construction_map
                .entry(slot)
                .or_insert_with(|| BlockConstruction::init(slot));

            match block.try_add_event(oneof) {
                Ok(_) => {
                    if block.is_sealed() {
                        // let block = block_construction_map.remove(&slot).expect("block not found");
                        let summary = block.summarize_timeline();
                        println!("Block {slot} sealed: {:?}", summary);
                    }
                }
                Err(e) => match e {
                    InvalidBlockEvent::BlockAlreadySealed(event) => {
                        let block = block_construction_map
                            .remove(&slot)
                            .expect("block not found");
                        let summary = block.summarize_timeline();
                        panic!("Block {slot} already sealed, but received {event:?}");
                        break;
                    }
                    InvalidBlockEvent::CorruptedBlock{ event, err } => {
                        panic!("Block error: {:?}", err);
                    }
                    InvalidBlockEvent::Unsupported(event) => {
                        continue;
                    }
                    InvalidBlockEvent::SlotMismatch(event) => {
                        panic!("Slot mismatch");
                    }
                },
            }
        }
    }
}
