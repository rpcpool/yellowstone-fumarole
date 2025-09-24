use {
    crate::proto::{self, BlockchainEvent},
    fxhash::FxHashMap,
    solana_clock::Slot,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque, hash_map},
    },
    yellowstone_grpc_proto::geyser::{self, CommitmentLevel},
};

pub(crate) type FumeBlockchainId = [u8; 16];

pub(crate) type FumeBlockUID = [u8; 16];

pub(crate) type FumeNumShards = u32;

pub(crate) type FumeShardIdx = u32;

pub(crate) type FumeOffset = i64;

pub(crate) type FumeSessionSequence = u64;

pub(crate) const DEFAULT_SLOT_MEMORY_RETENTION: usize = 10000;

#[derive(Debug, Clone)]
pub(crate) struct FumeDownloadRequest {
    pub slot: Slot,
    pub blockchain_id: FumeBlockchainId,
    pub block_uid: FumeBlockUID,
    #[allow(dead_code)]
    pub num_shards: FumeNumShards, // First version of fumarole, it should always be 1
    #[allow(dead_code)]
    pub commitment_level: geyser::CommitmentLevel,
}

#[derive(Clone, Debug)]
pub(crate) struct FumeSlotStatus {
    /// Rough unit of time inside fumarole state machine.
    pub session_sequence: FumeSessionSequence,
    #[allow(dead_code)]
    pub offset: FumeOffset,
    pub slot: Slot,
    pub parent_slot: Option<Slot>,
    pub commitment_level: geyser::CommitmentLevel,
    pub dead_error: Option<String>,
}

#[derive(Debug, Default)]
struct SlotCommitmentProgression {
    processed_commitment_levels: HashSet<geyser::CommitmentLevel>,
}

struct SlotDownloadProgress {
    num_shards: FumeNumShards,
    shard_remaining: Vec<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotDownloadState {
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
    slot_commitment_progression: BTreeMap<Slot, SlotCommitmentProgression>,
    /// As we download and process slot status, we keep track of the progression of each slot here.
    downloaded_slot: BTreeSet<Slot>,
    /// Inlfight slot download
    inflight_slot_shard_download: HashMap<Slot, SlotDownloadProgress>,
    /// Slot blocked by a slot download (inflight or in queue)
    blocked_slot_status_update: HashMap<Slot, VecDeque<FumeSlotStatus>>,
    /// Slot status queue whose slot have been completely downloaded in the current session.
    slot_status_update_queue: VecDeque<FumeSlotStatus>,
    /// Keeps track of each offset have been processed by the underlying runtime.
    /// Fumarole State Machine emits slot status in disorder, but still requires ordering
    /// when computing the `committable_offset`
    processed_offset: BinaryHeap<Reverse<(FumeSessionSequence, FumeOffset)>>,

    /// Represents the high-water mark fume offset that can be committed to the remote fumarole service.
    /// It means the runtime processed everything <= committable offset.
    pub committable_offset: FumeOffset,

    last_processed_fume_sequence: FumeSessionSequence,

    /// Represents the max slot detected in the current session.
    /// This is used to detect rough slot lag.
    /// this slot is not necessarily processed by the underlying runtime yet.
    pub max_slot_detected: Slot,

    /// Unprocessed blockchain events
    unprocessed_blockchain_event: VecDeque<(FumeSessionSequence, proto::BlockchainEvent)>,

    sequence: u64,

    sequence_to_offset: FxHashMap<FumeSessionSequence, FumeOffset>,

    slot_memory_retention: usize,
}

impl FumaroleSM {
    pub fn new(last_committed_offset: FumeOffset, slot_memory_retention: usize) -> Self {
        Self {
            last_committed_offset,
            slot_commitment_progression: Default::default(),
            downloaded_slot: Default::default(),
            inflight_slot_shard_download: Default::default(),
            blocked_slot_status_update: Default::default(),
            slot_status_update_queue: Default::default(),
            processed_offset: Default::default(),
            committable_offset: last_committed_offset,
            max_slot_detected: 0,
            unprocessed_blockchain_event: Default::default(),
            sequence: 1,
            last_processed_fume_sequence: 0,
            sequence_to_offset: Default::default(),
            slot_memory_retention,
        }
    }

    ///
    /// Updates the committed offset
    ///
    pub fn update_committed_offset(&mut self, offset: FumeOffset) {
        assert!(
            offset >= self.last_committed_offset,
            "offset must be greater than or equal to last committed offset"
        );
        self.last_committed_offset = offset;
    }

    const fn next_sequence(&mut self) -> u64 {
        let ret = self.sequence;
        self.sequence += 1;
        ret
    }

    pub fn gc(&mut self) {
        while self.downloaded_slot.len() > self.slot_memory_retention {
            let Some(slot) = self.downloaded_slot.pop_first() else {
                break;
            };

            self.slot_commitment_progression.remove(&slot);
            self.inflight_slot_shard_download.remove(&slot);
            self.blocked_slot_status_update.remove(&slot);
        }
    }

    pub fn queue_blockchain_event<IT>(&mut self, events: IT)
    where
        IT: IntoIterator<Item = proto::BlockchainEvent>,
    {
        for event in events {
            if event.offset < self.last_committed_offset {
                continue;
            }

            if event.slot > self.max_slot_detected {
                self.max_slot_detected = event.slot;
            }
            let sequence = self.next_sequence();

            self.sequence_to_offset.insert(sequence, event.offset);

            if self.downloaded_slot.contains(&event.slot) {
                let fume_status = FumeSlotStatus {
                    session_sequence: sequence,
                    offset: event.offset,
                    slot: event.slot,
                    parent_slot: event.parent_slot,
                    commitment_level: geyser::CommitmentLevel::try_from(event.commitment_level)
                        .expect("invalid commitment level"),
                    dead_error: event.dead_error,
                };
                if self.inflight_slot_shard_download.contains_key(&event.slot) {
                    // This event is blocked by a slot download currently in progress
                    self.blocked_slot_status_update
                        .entry(event.slot)
                        .or_default()
                        .push_back(fume_status);
                } else {
                    // Fast track this event, since the slot has been downloaded in the current session
                    // and we are not waiting for any shard to be downloaded.
                    self.slot_status_update_queue.push_back(fume_status);
                }
            } else {
                self.unprocessed_blockchain_event
                    .push_back((sequence, event));
            }
        }
    }

    ///
    /// Update download progression for a given `Slot` download
    ///
    pub fn make_slot_download_progress(
        &mut self,
        slot: Slot,
        shard_idx: FumeShardIdx,
    ) -> SlotDownloadState {
        let download_progress = self
            .inflight_slot_shard_download
            .get_mut(&slot)
            .expect("slot not in download");

        let download_state = download_progress.do_progress(shard_idx);

        if matches!(download_state, SlotDownloadState::Done) {
            // all shards downloaded
            self.inflight_slot_shard_download.remove(&slot);
            self.downloaded_slot.insert(slot);
            self.slot_commitment_progression.entry(slot).or_default();

            let blocked_slot_status = self
                .blocked_slot_status_update
                .remove(&slot)
                .unwrap_or_default();
            self.slot_status_update_queue.extend(blocked_slot_status);
        }
        download_state
    }

    pub fn pop_next_slot_status(&mut self) -> Option<FumeSlotStatus> {
        loop {
            let slot_status = self.slot_status_update_queue.pop_front()?;
            match self.slot_commitment_progression.get_mut(&slot_status.slot) {
                Some(commitment_history) => {
                    if commitment_history
                        .processed_commitment_levels
                        .insert(slot_status.commitment_level)
                    {
                        return Some(slot_status);
                    } else {
                        tracing::warn!(
                            "Deduped slot status for slot {}, commitment level {:?}, fuamrole offset {:?}",
                            slot_status.slot,
                            slot_status.commitment_level,
                            slot_status.offset
                        );
                        self.mark_event_as_processed(slot_status.session_sequence);
                        // We already processed this commitment level
                        continue;
                    }
                }
                _ => {
                    // This slot has not been downloaded yet, but still has a status to process
                    unreachable!("slot status should not be available here");
                }
            }
        }
    }

    fn make_sure_slot_commitment_progression_exists(
        &mut self,
        slot: Slot,
    ) -> &mut SlotCommitmentProgression {
        self.slot_commitment_progression.entry(slot).or_default()
    }

    ///
    /// Pop next slot status to process
    ///
    pub fn pop_slot_to_download(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> Option<FumeDownloadRequest> {
        loop {
            let min_commitment = commitment.unwrap_or(CommitmentLevel::Processed);
            let (session_sequence, blockchain_event) =
                self.unprocessed_blockchain_event.pop_front()?;
            let BlockchainEvent {
                offset,
                blockchain_id,
                block_uid,
                num_shards,
                slot,
                parent_slot,
                commitment_level,
                blockchain_shard_id: _,
                dead_error,
            } = blockchain_event;

            let event_cl = geyser::CommitmentLevel::try_from(commitment_level)
                .expect("invalid commitment level");

            if event_cl != min_commitment {
                self.slot_status_update_queue.push_back(FumeSlotStatus {
                    session_sequence,
                    offset,
                    slot,
                    parent_slot,
                    commitment_level: event_cl,
                    dead_error,
                });
                self.make_sure_slot_commitment_progression_exists(slot);
                continue;
            }

            if self.downloaded_slot.contains(&slot) {
                // This slot has been fully downloaded by the runtime
                self.make_sure_slot_commitment_progression_exists(slot);
                let Some(progression) = self.slot_commitment_progression.get_mut(&slot) else {
                    unreachable!("slot status should not be available here");
                };

                if progression.processed_commitment_levels.contains(&event_cl) {
                    // We already processed this commitment level
                    self.mark_event_as_processed(session_sequence);
                    continue;
                }

                // We have a new commitment level for this slot and slot has been downloaded in the current session.
                self.slot_status_update_queue.push_back(FumeSlotStatus {
                    session_sequence,
                    offset,
                    slot,
                    parent_slot,
                    commitment_level: event_cl,
                    dead_error,
                });
            } else {
                // This slot has not been downloaded yet
                let blockchain_id: [u8; 16] = blockchain_id
                    .try_into()
                    .expect("blockchain_id must be 16 bytes");
                let block_uid: [u8; 16] = block_uid.try_into().expect("block_uid must be 16 bytes");

                // We have a new commitment level for this slot and slot has not been downloaded in the current session.
                self.blocked_slot_status_update
                    .entry(slot)
                    .or_default()
                    .push_back(FumeSlotStatus {
                        session_sequence,
                        offset,
                        slot,
                        parent_slot,
                        commitment_level: event_cl,
                        dead_error,
                    });

                if let hash_map::Entry::Vacant(e) = self.inflight_slot_shard_download.entry(slot) {
                    // This slot has not been schedule for download yet
                    let download_request = FumeDownloadRequest {
                        slot,
                        blockchain_id,
                        block_uid,
                        num_shards,
                        commitment_level: event_cl,
                    };
                    let download_progress = SlotDownloadProgress {
                        num_shards,
                        shard_remaining: vec![false; num_shards as usize],
                    };
                    e.insert(download_progress);
                    return Some(download_request);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn slot_status_update_queue_len(&self) -> usize {
        self.slot_status_update_queue.len()
    }

    ///
    /// Marks this [`FumeOffset`] has processed by the runtime.
    ///
    pub fn mark_event_as_processed(&mut self, event_seq_number: FumeSessionSequence) {
        let fume_offset = self
            .sequence_to_offset
            .remove(&event_seq_number)
            .expect("event sequence number not found");
        self.processed_offset
            .push(Reverse((event_seq_number, fume_offset)));

        loop {
            let Some(tuple) = self.processed_offset.peek().copied() else {
                break;
            };
            let (blocked_event_seq_number2, fume_offset2) = tuple.0;
            if blocked_event_seq_number2 != self.last_processed_fume_sequence + 1 {
                break;
            }

            let _ = self.processed_offset.pop().unwrap();
            self.committable_offset = fume_offset2;
            self.last_processed_fume_sequence = blocked_event_seq_number2;
        }
    }

    #[allow(dead_code)]
    pub fn processed_offset_queue_len(&self) -> usize {
        self.processed_offset.len()
    }

    ///
    /// Returns true if there is no blockchain event history to track or progress on.
    ///
    pub fn need_new_blockchain_events(&self) -> bool {
        const MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT: usize = 10;
        // We don't want wait until the entire state machine is empty of work before polling for new events.
        // We want to poll for ahead of time, so we can keep the state machine busy.
        self.unprocessed_blockchain_event.len() < MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT
            || (self.slot_status_update_queue.is_empty()
                && self.blocked_slot_status_update.is_empty())
    }
}

#[cfg(test)]
mod tests {

    use {super::*, uuid::Uuid, yellowstone_grpc_proto::geyser::CommitmentLevel};

    fn random_blockchain_event(
        offset: FumeOffset,
        slot: Slot,
        commitment_level: CommitmentLevel,
    ) -> BlockchainEvent {
        let blockchain_id = Uuid::nil().as_bytes().to_vec();
        let block_uid = Uuid::new_v4().as_bytes().to_vec();
        BlockchainEvent {
            offset,
            blockchain_id,
            block_uid,
            num_shards: 1,
            slot,
            parent_slot: None,
            commitment_level: commitment_level.into(),
            blockchain_shard_id: 0,
            dead_error: None,
        }
    }

    #[test]
    fn test_fumarole_sm_happy_path() {
        let mut sm = FumaroleSM::new(0, DEFAULT_SLOT_MEMORY_RETENTION);

        let event = random_blockchain_event(1, 1, CommitmentLevel::Processed);
        sm.queue_blockchain_event(vec![event.clone()]);

        // Slot status should not be available, since we didn't download it yet.
        let download_req = sm.pop_slot_to_download(None).unwrap();

        assert_eq!(download_req.slot, 1);

        assert!(sm.pop_slot_to_download(None).is_none());
        assert!(sm.pop_next_slot_status().is_none());

        let download_state = sm.make_slot_download_progress(1, 0);
        assert_eq!(download_state, SlotDownloadState::Done);

        let status = sm.pop_next_slot_status().unwrap();

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment_level, CommitmentLevel::Processed);
        sm.mark_event_as_processed(status.session_sequence);

        // All subsequent commitment level should be available right away
        let mut event2 = event.clone();
        event2.offset += 1;
        event2.commitment_level = CommitmentLevel::Confirmed.into();
        sm.queue_blockchain_event(vec![event2.clone()]);

        // It should not cause new slot download request
        assert!(sm.pop_slot_to_download(None).is_none());

        let status = sm.pop_next_slot_status().unwrap();
        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment_level, CommitmentLevel::Confirmed);
        sm.mark_event_as_processed(status.session_sequence);

        assert_eq!(sm.committable_offset, event2.offset);
    }

    #[test]
    fn it_should_dedup_slot_status() {
        let mut sm = FumaroleSM::new(0, DEFAULT_SLOT_MEMORY_RETENTION);

        let event = random_blockchain_event(1, 1, CommitmentLevel::Processed);
        sm.queue_blockchain_event(vec![event.clone()]);

        // Slot status should not be available, since we didn't download it yet.
        assert!(sm.pop_next_slot_status().is_none());

        let download_req = sm.pop_slot_to_download(None).unwrap();

        assert_eq!(download_req.slot, 1);

        assert!(sm.pop_slot_to_download(None).is_none());

        sm.make_slot_download_progress(1, 0);

        let status = sm.pop_next_slot_status().unwrap();

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment_level, CommitmentLevel::Processed);

        // Putting the same event back should be ignored
        sm.queue_blockchain_event(vec![event]);

        assert!(sm.pop_slot_to_download(None).is_none());
        assert!(sm.pop_next_slot_status().is_none());
    }

    #[test]
    fn it_should_handle_min_commitment_level() {
        let mut sm = FumaroleSM::new(0, DEFAULT_SLOT_MEMORY_RETENTION);

        let event = random_blockchain_event(1, 1, CommitmentLevel::Processed);
        sm.queue_blockchain_event(vec![event.clone()]);

        // Slot status should not be available, since we didn't download it yet.
        assert!(sm.pop_next_slot_status().is_none());

        // Use finalized commitment level here
        let download_req = sm.pop_slot_to_download(Some(CommitmentLevel::Finalized));
        assert!(download_req.is_none());

        assert!(sm.pop_slot_to_download(None).is_none());

        // It should not cause the slot status to be available here even if we have a finalized commitment level filtered out before
        let status = sm.pop_next_slot_status().unwrap();

        assert_eq!(status.slot, 1);
        assert_eq!(status.commitment_level, CommitmentLevel::Processed);
    }
}
