import uuid
from yellowstone_fumarole_client.runtime.state_machine import (
    DEFAULT_SLOT_MEMORY_RETENTION,
    FumaroleSM,
    FumeOffset,
    Slot,
    SlotDownloadState,
)

from yellowstone_api.fumarole_v2_pb2 import BlockchainEvent, CommitmentLevel


# Tests
def random_blockchain_event(
    offset: FumeOffset, slot: Slot, commitment_level
) -> BlockchainEvent:
    blockchain_id = uuid.UUID(int=0).bytes
    block_uid = uuid.uuid4().bytes
    return BlockchainEvent(
        offset=offset,
        blockchain_id=blockchain_id,
        block_uid=block_uid,
        num_shards=1,
        slot=slot,
        parent_slot=None,
        commitment_level=commitment_level,
        blockchain_shard_id=0,
        dead_error=None,
    )


def test_fumarole_sm_happy_path():
    sm = FumaroleSM(
        last_committed_offset=0, slot_memory_retention=DEFAULT_SLOT_MEMORY_RETENTION
    )
    event = random_blockchain_event(
        offset=1, slot=1, commitment_level=CommitmentLevel.PROCESSED
    )
    sm.queue_blockchain_event([event])

    download_req = sm.pop_slot_to_download(None)
    assert download_req is not None
    assert download_req.slot == 1

    assert sm.pop_slot_to_download(None) is None
    assert sm.pop_next_slot_status() is None

    download_state = sm.make_slot_download_progress(slot=1, shard_idx=0)
    assert download_state == SlotDownloadState.Done

    status = sm.pop_next_slot_status()
    assert status is not None
    assert status.slot == 1
    assert status.commitment_level == CommitmentLevel.PROCESSED
    sm.mark_event_as_processed(status.session_sequence)

    event2 = random_blockchain_event(
        offset=2, slot=1, commitment_level=CommitmentLevel.CONFIRMED
    )
    sm.queue_blockchain_event([event2])

    assert sm.pop_slot_to_download(None) is None

    status = sm.pop_next_slot_status()
    assert status is not None
    assert status.slot == 1
    assert status.commitment_level == CommitmentLevel.CONFIRMED
    sm.mark_event_as_processed(status.session_sequence)

    assert sm.committable_offset == event2.offset


def test_it_should_dedup_slot_status():
    sm = FumaroleSM(
        last_committed_offset=0, slot_memory_retention=DEFAULT_SLOT_MEMORY_RETENTION
    )
    event = random_blockchain_event(
        offset=1, slot=1, commitment_level=CommitmentLevel.PROCESSED
    )
    sm.queue_blockchain_event([event])

    assert sm.pop_next_slot_status() is None

    download_req = sm.pop_slot_to_download(None)
    assert download_req is not None
    assert download_req.slot == 1

    assert sm.pop_slot_to_download(None) is None

    sm.make_slot_download_progress(slot=1, shard_idx=0)

    status = sm.pop_next_slot_status()
    assert status is not None
    assert status.slot == 1
    assert status.commitment_level == CommitmentLevel.PROCESSED

    sm.queue_blockchain_event([event])

    assert sm.pop_slot_to_download(None) is None
    assert sm.pop_next_slot_status() is None


def test_it_should_handle_min_commitment_level():
    sm = FumaroleSM(
        last_committed_offset=0, slot_memory_retention=DEFAULT_SLOT_MEMORY_RETENTION
    )
    event = random_blockchain_event(
        offset=1, slot=1, commitment_level=CommitmentLevel.PROCESSED
    )
    sm.queue_blockchain_event([event])

    assert sm.pop_next_slot_status() is None

    download_req = sm.pop_slot_to_download(CommitmentLevel.FINALIZED)
    assert download_req is None

    assert sm.pop_slot_to_download(None) is None

    status = sm.pop_next_slot_status()
    assert status is not None
    assert status.slot == 1
    assert status.commitment_level == CommitmentLevel.PROCESSED
