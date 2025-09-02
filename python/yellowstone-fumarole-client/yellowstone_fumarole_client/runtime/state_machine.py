from typing import Optional, Set, Deque, Sequence
from collections import deque, defaultdict
from yellowstone_fumarole_proto.fumarole_pb2 import (
    CommitmentLevel,
    BlockchainEvent,
)
from yellowstone_fumarole_client.utils.collections import OrderedSet
import heapq
from enum import Enum

__all__ = [
    "DEFAULT_SLOT_MEMORY_RETENTION",
    "FumeBlockchainId",
    "FumeBlockUID",
    "FumeNumShards",
    "FumeShardIdx",
    "FumeOffset",
    "FumeSessionSequence",
    "Slot",
    "FumeDownloadRequest",
    "FumeSlotStatus",
    "SlotCommitmentProgression",
    "SlotDownloadProgress",
    "SlotDownloadState",
    "FumaroleSM",
]

# Constants
DEFAULT_SLOT_MEMORY_RETENTION = 10000

# Type aliases
FumeBlockchainId = bytes  # Equivalent to [u8; 16]
FumeBlockUID = bytes  # Equivalent to [u8; 16]
FumeNumShards = int  # Equivalent to u32
FumeShardIdx = int  # Equivalent to u32
FumeOffset = int  # Equivalent to i64
FumeSessionSequence = int  # Equivalent to u64
Slot = int  # From solana_sdk::clock::Slot


# Data structures
class FumeDownloadRequest:
    def __init__(
        self,
        slot: Slot,
        blockchain_id: FumeBlockchainId,
        block_uid: FumeBlockUID,
        num_shards: FumeNumShards,
        commitment_level: CommitmentLevel,
    ):
        self.slot = slot
        self.blockchain_id = blockchain_id
        self.block_uid = block_uid
        self.num_shards = num_shards
        self.commitment_level = commitment_level


class FumeSlotStatus:
    def __init__(
        self,
        session_sequence: FumeSessionSequence,
        offset: FumeOffset,
        slot: Slot,
        parent_slot: Optional[Slot],
        commitment_level: CommitmentLevel,
        dead_error: Optional[str],
    ):
        self.session_sequence = session_sequence
        self.offset = offset
        self.slot = slot
        self.parent_slot = parent_slot
        self.commitment_level = commitment_level
        self.dead_error = dead_error


class SlotCommitmentProgression:
    def __init__(self):
        self.processed_commitment_levels: Set[CommitmentLevel] = set()


class SlotDownloadProgress:
    def __init__(self, num_shards: FumeNumShards):
        self.num_shards = num_shards
        self.shard_remaining = [False] * num_shards

    def do_progress(self, shard_idx: FumeShardIdx) -> "SlotDownloadState":
        self.shard_remaining[shard_idx % self.num_shards] = True
        return (
            SlotDownloadState.Done
            if all(self.shard_remaining)
            else SlotDownloadState.Downloading
        )


class SlotDownloadState(Enum):
    Downloading = "Downloading"
    Done = "Done"


class FumaroleSM:
    """
    Sans-IO Fumarole State Machine

    Manages in-flight slot downloads and ensures correct ordering of slot statuses without performing I/O.
    """

    def __init__(self, last_committed_offset: FumeOffset, slot_memory_retention: int):
        self.last_committed_offset = last_committed_offset
        self.slot_commitment_progression = dict()  # Slot -> SlotCommitmentProgression
        self.downloaded_slot = OrderedSet()  # Set of downloaded slots
        self.inflight_slot_shard_download = {}  # Slot -> SlotDownloadProgress
        self.blocked_slot_status_update = defaultdict(
            deque
        )  # Slot -> Deque[FumeSlotStatus]
        self.slot_status_update_queue = deque()  # Deque[FumeSlotStatus]
        self.processed_offset = []  # Min-heap for (sequence, offset)
        self.committable_offset = last_committed_offset
        self.max_slot_detected = 0
        self.unprocessed_blockchain_event: Deque[
            (FumeSessionSequence, BlockchainEvent)
        ] = deque()
        self.sequence = 1
        self.last_processed_fume_sequence = 0
        self.sequence_to_offset: dict[FumeSessionSequence, FumeOffset] = (
            {}
        )  # FumeSessionSequence -> FumeOffset
        self.slot_memory_retention = slot_memory_retention

    def update_committed_offset(self, offset: FumeOffset) -> None:
        assert (
            offset >= self.last_committed_offset
        ), "Offset must be >= last committed offset"
        self.last_committed_offset = offset

    def next_sequence(self) -> int:
        ret = self.sequence
        self.sequence += 1
        return ret

    def gc(self) -> None:
        """Garbage collect old slots to respect memory retention limit."""
        while len(self.downloaded_slot) > self.slot_memory_retention:
            slot = self.downloaded_slot.popfirst() if self.downloaded_slot else None
            if slot is None:
                break
            self.slot_commitment_progression.pop(slot, None)
            self.inflight_slot_shard_download.pop(slot, None)
            self.blocked_slot_status_update.pop(slot, None)

    def queue_blockchain_event(self, events: Sequence[BlockchainEvent]) -> None:
        """Queue blockchain events for processing."""
        for event in events:

            if event.offset < self.last_committed_offset:
                continue

            if event.slot > self.max_slot_detected:
                self.max_slot_detected = event.slot

            sequence = self.next_sequence()
            self.sequence_to_offset[sequence] = event.offset

            if event.slot in self.downloaded_slot:
                fume_status = FumeSlotStatus(
                    session_sequence=sequence,
                    offset=event.offset,
                    slot=event.slot,
                    parent_slot=event.parent_slot,
                    commitment_level=event.commitment_level,
                    dead_error=event.dead_error,
                )

                if event.slot in self.inflight_slot_shard_download:
                    self.blocked_slot_status_update[event.slot].append(fume_status)
                else:
                    self.slot_status_update_queue.append(fume_status)
            else:
                self.unprocessed_blockchain_event.append((sequence, event))

    def make_slot_download_progress(
        self, slot: Slot, shard_idx: FumeShardIdx
    ) -> SlotDownloadState:
        """Update download progress for a given slot."""
        download_progress = self.inflight_slot_shard_download.get(slot)
        if not download_progress:
            raise ValueError("Slot not in download")

        download_state = download_progress.do_progress(shard_idx)

        if download_state == SlotDownloadState.Done:
            self.inflight_slot_shard_download.pop(slot)
            self.downloaded_slot.add(slot)
            self.slot_commitment_progression.setdefault(
                slot, SlotCommitmentProgression()
            )
            blocked_statuses = self.blocked_slot_status_update.pop(slot, deque())
            self.slot_status_update_queue.extend(blocked_statuses)

        return download_state

    def pop_next_slot_status(self) -> Optional[FumeSlotStatus]:
        """Pop the next slot status to process."""
        while self.slot_status_update_queue:
            slot_status = self.slot_status_update_queue.popleft()
            commitment_history = self.slot_commitment_progression.get(slot_status.slot)
            if (
                commitment_history
                and slot_status.commitment_level
                not in commitment_history.processed_commitment_levels
            ):
                commitment_history.processed_commitment_levels.add(
                    slot_status.commitment_level
                )
                return slot_status
            elif not commitment_history:
                raise RuntimeError("Slot status should not be available here")
        return None

    def make_sure_slot_commitment_progression_exists(
        self, slot: Slot
    ) -> SlotCommitmentProgression:
        """Ensure a slot has a commitment progression entry."""
        return self.slot_commitment_progression.setdefault(
            slot, SlotCommitmentProgression()
        )

    def pop_slot_to_download(self, commitment=None) -> Optional[FumeDownloadRequest]:
        """Pop the next slot to download."""
        min_commitment = commitment or CommitmentLevel.PROCESSED
        while self.unprocessed_blockchain_event:
            session_sequence, blockchain_event = (
                self.unprocessed_blockchain_event.popleft()
            )
            blockchain_event: BlockchainEvent = blockchain_event
            event_cl = blockchain_event.commitment_level

            if event_cl != min_commitment:
                self.slot_status_update_queue.append(
                    FumeSlotStatus(
                        session_sequence=session_sequence,
                        offset=blockchain_event.offset,
                        slot=blockchain_event.slot,
                        parent_slot=blockchain_event.parent_slot,
                        commitment_level=event_cl,
                        dead_error=blockchain_event.dead_error,
                    )
                )
                self.make_sure_slot_commitment_progression_exists(blockchain_event.slot)
                continue

            if blockchain_event.slot in self.downloaded_slot:
                self.make_sure_slot_commitment_progression_exists(blockchain_event.slot)
                progression = self.slot_commitment_progression[blockchain_event.slot]
                if event_cl in progression.processed_commitment_levels:
                    self.mark_event_as_processed(session_sequence)
                    continue

                self.slot_status_update_queue.append(
                    FumeSlotStatus(
                        session_sequence=session_sequence,
                        offset=blockchain_event.offset,
                        slot=blockchain_event.slot,
                        parent_slot=blockchain_event.parent_slot,
                        commitment_level=event_cl,
                        dead_error=blockchain_event.dead_error,
                    )
                )
            else:
                blockchain_id = bytes(blockchain_event.blockchain_id)
                block_uid = bytes(blockchain_event.block_uid)

                self.blocked_slot_status_update[blockchain_event.slot].append(
                    FumeSlotStatus(
                        session_sequence=session_sequence,
                        offset=blockchain_event.offset,
                        slot=blockchain_event.slot,
                        parent_slot=blockchain_event.parent_slot,
                        commitment_level=event_cl,
                        dead_error=blockchain_event.dead_error,
                    )
                )

                if blockchain_event.slot not in self.inflight_slot_shard_download:
                    download_request = FumeDownloadRequest(
                        slot=blockchain_event.slot,
                        blockchain_id=blockchain_id,
                        block_uid=block_uid,
                        num_shards=blockchain_event.num_shards,
                        commitment_level=event_cl,
                    )
                    download_progress = SlotDownloadProgress(
                        num_shards=blockchain_event.num_shards
                    )
                    self.inflight_slot_shard_download[blockchain_event.slot] = (
                        download_progress
                    )
                    return download_request
        return None

    def mark_event_as_processed(self, event_seq_number: FumeSessionSequence) -> None:
        """Mark an event as processed and update committable offset."""
        fume_offset = self.sequence_to_offset.pop(event_seq_number, None)
        if fume_offset is None:
            raise ValueError("Event sequence number not found")
        heapq.heappush(self.processed_offset, (event_seq_number, fume_offset))

        while self.processed_offset:
            seq, offset = self.processed_offset[0]
            if seq != self.last_processed_fume_sequence + 1:
                break
            heapq.heappop(self.processed_offset)
            self.committable_offset = offset
            self.last_processed_fume_sequence = seq

    def slot_status_update_queue_len(self) -> int:
        """Return the length of the slot status update queue."""
        return len(self.slot_status_update_queue)

    def processed_offset_queue_len(self) -> int:
        """Return the length of the processed offset queue."""
        return len(self.processed_offset)

    def need_new_blockchain_events(self) -> bool:
        """Check if new blockchain events are needed."""
        MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT = 10
        return len(self.unprocessed_blockchain_event) < MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT \
            or (not self.slot_status_update_queue and not self.blocked_slot_status_update)
