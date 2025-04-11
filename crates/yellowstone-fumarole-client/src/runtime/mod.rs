pub(crate) mod tokio;

use {
    crate::{
        proto::{self, BlockchainEvent},
        util::collections::KeyedVecDeque,
    },
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
        convert::identity,
    },
    yellowstone_grpc_proto::geyser,
};

type FumeBlockchainId = [u8; 16];

type FumeBlockUID = [u8; 16];

type FumeNumShards = u32;

type FumeShardIdx = u32;

type FumeBlockShard = u32;

type FumeDataBusId = u8;

type FumeOffset = u64;

#[derive(Debug, Clone)]
struct FumeDownloadRequest {
    slot: Slot,
    blockchain_id: FumeBlockchainId,
    block_uid: FumeBlockUID,
    num_shards: FumeNumShards, // First version of fumarole, it should always be 1
}

#[derive(Clone, Debug)]
struct FumeSlotStatus {
    parent_offset: FumeOffset,
    offset: FumeOffset,
    slot: Slot,
    parent_slot: Option<Slot>,
    block_uid: FumeBlockUID,
    blockchain_id: FumeBlockchainId,
    num_shards: FumeNumShards,
    commitment_level: geyser::CommitmentLevel,
}

#[derive(Debug, Default)]
struct SlotInfoProcessed {
    processed_commitment_levels: HashSet<geyser::CommitmentLevel>,
}

struct SlotDownloadProgress {
    num_shards: FumeNumShards,
    shard_remaining: Vec<bool>,
}

enum SlotDownloadState {
    Downloading,
    Done,
}

impl SlotDownloadProgress {
    pub fn do_progress(&mut self, shard_idx: FumeShardIdx) -> SlotDownloadState {
        self.shard_remaining[shard_idx as usize % self.num_shards as usize] = true;

        if self.shard_remaining.iter().all(|b| *b) {
            SlotDownloadState::Done
        } else {
            SlotDownloadState::Downloading
        }
    }
}

///
/// Sans-IO Fumarole State Machine
///
/// This state machine manages in-flight slot downloads and ensures correct ordering of slot statuses,
/// without performing any actual I/O operations itself.
///
/// # Overview
///
/// The state machine starts empty. To drive progress, feed it blockchain events using
/// [`FumaroleSM::queue_blockchain_event`]. This allows the state machine to advance and reveal "work" that
/// needs to be done.
///
/// ## Type of Work: Slot Downloads
///
/// To determine which slot should be downloaded, call [`FumaroleSM::pop_slot_to_download`].  
/// If it returns a [`FumeDownloadRequest`], it’s up to the runtime to interpret the request and handle the
/// actual I/O using the framework of your choice.
///
/// **Note:**  
/// Once [`pop_slot_to_download`] returns a [`FumeDownloadRequest`], the state machine considers the download
/// in progress. The runtime must report progress using [`FumaroleSM::make_slot_download_progress`] by
/// specifying the slot number and shard number that has been downloaded.
///
/// As of now, the Fumarole backend does **not** support block-sharding.  
/// Therefore, you can assume [`FumeDownloadRequest::num_shards`] will always be `1`.
/// However, the API is already shard-aware, allowing runtimes to opt into sharding support in the future.
///
/// ## Type of Work: Slot Statuses
///
/// Once a slot download is complete (via [`make_slot_download_progress`]), the state machine may release
/// corresponding slot statuses that were waiting on that download. These can be retrieved using
/// [`FumaroleSM::pop_next_slot_status`].
///
/// Each [`FumeSlotStatus`] has an offset. Once your runtime processes it, acknowledge it by calling
/// [`FumaroleSM::mark_offset_as_processed`]. This ensures that the [`FumaroleSM::committable_offset`] only
/// advances when there are no gaps in the slot status timeline.
///
/// # Concurrency and Progress
///
/// There is no strict call order for the `FumaroleSM` API. The state machine tracks all progress concurrently,
/// ensuring coherence. It automatically blocks operations that depend on unfinished work.
///
/// # Suggested Runtime Loop
///
/// A typical runtime loop using the state machine might look like:
///
/// 1. Check if new blockchain events are needed with [`FumaroleSM::need_new_blockchain_events`].
///     - If so, fetch some and call [`FumaroleSM::queue_blockchain_event`].
/// 2. Check for any slots to download.
///     - If so, call [`FumaroleSM::pop_slot_to_download`] and handle the download.
/// 3. Check for completed downloads from the previous iteration.
///     - If any, report progress with [`FumaroleSM::make_slot_download_progress`].
/// 4. Check for any available slot statuses to consume.
///     - Use [`FumaroleSM::pop_next_slot_status`] to retrieve them.
///
/// [Safety]
///
/// The state-machine manage deduping of slot-status, so is slot-download request.
/// You will never get [`FumeDownloadRequest`] twice for the same slot, even if multiple slot status happens for that given slot.
///
pub(crate) struct FumaroleSM {
    /// The last committed offset
    pub last_committed_offset: FumeOffset,
    /// Slot that have been downloaded in the current session along side slot status update
    slot_downloaded: BTreeMap<Slot, SlotInfoProcessed>,
    /// Inlfight slot download
    inflight_slot_shard_download: HashMap<Slot, SlotDownloadProgress>,
    /// Slot download queue
    slot_download_queue: KeyedVecDeque<Slot, FumeDownloadRequest>,
    /// Slot blocked by a slot download (inflight or in queue)
    blocked_slot_status_update: HashMap<Slot, VecDeque<FumeSlotStatus>>,
    /// Slot status queue whose slot have been completely downloaded in the current session.
    slot_status_update_queue: VecDeque<FumeSlotStatus>,
    /// Keeps track of each offset have been processed by the underlying runtime.
    /// Fumarole State Machine emits slot status in disorder, but still requires ordering
    /// when computing the `committable_offset`
    processed_offset: BinaryHeap<Reverse<FumeOffset>>,

    /// Represents the high-water mark fume offset that can be committed to the remote fumarole service.
    /// It means the runtime processed everything <= committable offset.
    pub committable_offset: FumeOffset,
}

impl FumaroleSM {
    ///
    /// Updates the committed offset
    ///
    pub(crate) fn update_committed_offset(&mut self, offset: FumeOffset) {
        assert!(
            offset > self.last_committed_offset,
            "offset must be greater than last committed offset"
        );
        self.last_committed_offset = offset;
    }

    ///
    /// Queues incoming **ordered** blockchain events
    pub(crate) fn queue_blockchain_event<IT>(&mut self, events: IT)
    where
        IT: IntoIterator<Item = proto::BlockchainEvent>,
    {
        let mut last_offset = self.last_committed_offset;
        for events in events {
            let BlockchainEvent {
                offset,
                blockchain_id,
                block_uid,
                num_shards,
                slot,
                parent_slot,
                commitment_level,
            } = events;

            assert!(
                offset > last_offset,
                "offset must be greater than last offset"
            );
            let blockchain_id: [u8; 16] = blockchain_id
                .try_into()
                .expect("blockchain_id must be 16 bytes");
            let block_uid: [u8; 16] = block_uid.try_into().expect("block_uid must be 16 bytes");

            let cl = geyser::CommitmentLevel::try_from(commitment_level)
                .expect("invalid commitment level");
            let fume_slot_status = FumeSlotStatus {
                parent_offset: last_offset,
                offset,
                slot,
                block_uid,
                parent_slot,
                blockchain_id,
                num_shards,
                commitment_level: cl,
            };
            last_offset = offset;
            // We don't download the same slot twice in the same session.
            if !self.slot_downloaded.contains_key(&slot) {
                // if the slot is already in-download, we don't need to schedule it for download again
                if !self.inflight_slot_shard_download.contains_key(&slot) {
                    let download_request = FumeDownloadRequest {
                        slot,
                        blockchain_id,
                        block_uid,
                        num_shards,
                    };
                    self.slot_download_queue.push_back(slot, download_request);
                }
                self.blocked_slot_status_update
                    .entry(slot)
                    .or_default()
                    .push_back(fume_slot_status);
            } else {
                self.slot_status_update_queue.push_back(fume_slot_status);
            }
        }
    }

    ///
    /// Returns true if there are slot to download, otherwise false.
    ///
    pub(crate) fn has_any_slot_to_download(&self) -> bool {
        !self.slot_download_queue.is_empty()
    }

    ///
    /// Returns the [`Some(FumeDownloadRequest)`]  to download if any, otherwise `None`.
    ///
    pub(crate) fn pop_slot_to_download(&mut self) -> Option<FumeDownloadRequest> {
        let download_req = self.slot_download_queue.pop_front()?;
        let download_progress = SlotDownloadProgress {
            num_shards: download_req.num_shards,
            shard_remaining: vec![false; download_req.num_shards as usize],
        };
        let old = self
            .inflight_slot_shard_download
            .insert(download_req.slot, download_progress);
        assert!(old.is_none(), "slot already in download");
        Some(download_req)
    }

    ///
    /// Update download progression for a given `Slot` download
    ///
    pub(crate) fn make_slot_download_progress(&mut self, slot: Slot, shard_idx: FumeShardIdx) {
        let download_progress = self
            .inflight_slot_shard_download
            .get_mut(&slot)
            .expect("slot not in download");

        let download_state = download_progress.do_progress(shard_idx);

        if matches!(download_state, SlotDownloadState::Done) {
            // all shards downloaded
            self.inflight_slot_shard_download.remove(&slot);
            self.slot_downloaded.insert(slot, Default::default());

            let blocked_slot_status = self
                .blocked_slot_status_update
                .remove(&slot)
                .unwrap_or_default();
            self.slot_status_update_queue.extend(blocked_slot_status);
        }
    }

    ///
    /// Pop next slot status to process
    ///
    pub(crate) fn pop_next_slot_status(&mut self) -> Option<FumeSlotStatus> {
        let slot_status = self.slot_status_update_queue.pop_front()?;
        let info = self.slot_downloaded.get_mut(&slot_status.slot)?;
        if info
            .processed_commitment_levels
            .insert(slot_status.commitment_level)
        {
            // We handle duplicate slot status event here.
            Some(slot_status)
        } else {
            None
        }
    }

    #[inline]
    fn missing_process_offset(&self) -> FumeOffset {
        self.committable_offset + 1
    }

    ///
    /// Marks this [`FumeOffset`] has processed by the runtime.
    ///
    pub(crate) fn mark_offset_as_processed(&mut self, offset: FumeOffset) {
        if offset == self.missing_process_offset() {
            self.committable_offset = offset;

            while let Some(offset2) = self.processed_offset.peek().copied() {
                let offset2 = offset2.0;
                if offset2 == self.missing_process_offset() {
                    assert!(self.processed_offset.pop().is_some());
                    self.committable_offset = offset2
                }
            }
        } else {
            self.processed_offset.push(Reverse(offset));
        }
    }

    ///
    /// Returns true if there is no blockchain event history to track or progress on.
    ///
    pub(crate) fn need_new_blockchain_events(&self) -> bool {
        self.slot_status_update_queue.is_empty() && self.blocked_slot_status_update.is_empty()
    }
}
