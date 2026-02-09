# Fumarole API Guide

This document serves as a guide to understanding how the Fumarole API works.

## Control vs Data Plane

Fumarole API has two planes that the client must manage:

1. The control plane:
    - Commit the consumer group offset
    - Fetch blockchain slot history slice
2. The data plane:
    - Slot Download stream request

## Slot history

The Fumarole backend offers a reliable and persistent slot history with a well-defined ordering.

Persistent: The slot history is stored in our durable storage system.

Stable: Each event is assigned a fixed, ever-increasing offset that never changes.

Every message includes both a slot number and its corresponding slot commitment.
The slot history records when each slot becomes available and how it progresses through different commitment levels.

Here is an example:

![](images/blockchain_history.png.crdownload)

> [!NOTE]
> - The message offset: 1,2,...,N are ordered.
> - The messages are NOT SORTED by slot: you may receive a message about slot 11 before slot 10.

I deliberately plotted a sequence where slots are disordered because that will happen, so do not assume slots happen in order, because this is not the case.
They will mostly happen in order, but the order you see is the fumarole processed slot ordering.

## Initiate a connection

Upon connecting to the `Subscribe` method in the `Fumarole` service, the first message you need to send is a control-plane command called `JoinControlPlane`

```rust
let initial_join = JoinControlPlane {
    consumer_group_name: Some(<CONSUMER_GROUP_NAME>),
};
let initial_join_command = ControlCommand {
    command: Some(proto::control_command::Command::InitialJoin(initial_join)),
};
```

Once you send the command, you should receive back `ControlResponse::InitialConsumerGroupState`:

```proto
message InitialConsumerGroupState {
  bytes blockchain_id = 1;
  map<int32, int64> last_committed_offsets = 2; 
}
```

The `last_committed_offsets` field should contain exactly one entry with key the int32 `0` => `<your offset>`:

```rust
response.last_committed_offsets.get(0).expect("should not be none")
```

The `last_committed_offsets` field is a map in case we want to support sharded control plane in the future.

## Polling Slot history

Inside `Fumarole.Subscribe` stream, you can poll Slot history using the command:

```proto
message PollBlockchainHistory {
  int32 shard_id = 1;   // ALWAYS SET IT TO 0, THIS IS THE SUPPORT SHARDED HISTORY
  optional int64 from = 2;
  optional int64 limit = 3;
}
```

Technically speaking, you don't need to provide the `from` or `limit` parameters, as the remote server remembers where you left off.
If you want to force a specific offset, you can fill the `from` field.

The return result is a set of historical events:

```proto
message BlockchainEvent {
  int64 offset = 1; // the current offset in the log
  bytes blockchain_id = 2; // the blockchain unique id
  bytes block_uid = 3; // the block UUID bound to this event.
  uint32 num_shards = 4; // ALWAYS 1.
  uint64 slot = 5; // The slot number
  optional uint64 parent_slot = 6; // maybe the parent slot
  geyser.CommitmentLevel commitment_level = 7; // the commitment level this event belongs to.
  int32 blockchain_shard_id = 8; // ALWAYS 0
  optional string dead_error = 9; // Slot dead error message.
}
```

The `block_uid` is the unique identifier used to download the slot content in the data-plane.

## Offset commitment

It's the client's responsibility to "commit" its offset, stating that it has fully processed a specific slot event in our history. Next time the client connects, the Fumarole service will remember where the user left off.

Still inside the control plane `Fumarole.Subscribe` bidi-stream, to commit your offset you need to send:

```proto
message CommitOffset {
  int64 offset = 1; // the offset you processed.
  int32 shard_id = 2; // ALWAYS 0
}
```

## Stream Slot data rows

Using the data-plane unary-stream method `Fumarole.DownloadBlock`, you can download a slot using the following request :

```proto
message DownloadBlockShard {
    bytes blockchain_id = 1; // COMES FROM InitialConsumerGroupState
    bytes block_uid = 2;    // COMES FROM BlockchainEvent
    int32 shard_idx = 3; // ALWAYS 0
    optional BlockFilters blockFilters = 4;
}
```

And `blockFilters` :

```proto
message BlockFilters {
    map<string, geyser.SubscribeRequestFilterAccounts> accounts = 1;
    map<string, geyser.SubscribeRequestFilterTransactions> transactions = 2;
    map<string, geyser.SubscribeRequestFilterEntry> entries = 3;
    map<string, geyser.SubscribeRequestFilterBlocksMeta> blocks_meta = 4;
}
```

Notice `BlockFilters` are 100% compatible with `geyser.SubscribeRequest` fields. In fact, it is a subset of `geyser.SubscribeRequest`.

Return result of `Fumarole.DownloadBlock` is a Stream of `DataResponse`:

```proto
message DataResponse {
  oneof response {
    geyser.SubscribeUpdate update = 1;
    BlockShardDownloadFinish block_shard_download_finish = 2;
  }
}
```
It is either a `geyser.SubscribeUpdate` or a signal that the slot data has been fully streamed out, so you can stop your streaming process.

If you don't receive `block_shard_download_finish`, then the stream is not yet complete. If the stream closed before receiving `block_shard_download_finish`, then something must be wrong, and you should throw an Exception or crash the download process.

## Simple Loop

```python

taskset = {}

loop:
    if not state.has_any_event_to_process?():
        taskset.add( spawn(poll_new_unprocess_event()) )
    while state.has_slot_to_download?():
        slot_to_download = state.slot_to_download.pop()    
        taskset.add( spawn(download_slot(slot_to_download)) )
    taskset.wait_for_next()
```

This is a really "simplified" process loop; your client has to keep tracking which slot to download and download any slot that is ready to be downloaded.

## Fumarole State Machine

The State of Fumarole and its business logic, which decides what to download, should be handled by a state machine.

Use the [python SDK implementation](../python//yellowstone-fumarole-client/yellowstone_fumarole_client/runtime/state_machine.py) as a reference.

Here's the API spec of the Fumarole state-machine:

```python
class FumaroleSM:
    """
    Sans-IO Fumarole State Machine

    Manages in-flight slot downloads and ensures correct ordering of slot statuses without performing I/O.
    """

    def gc(self) -> None:
        """Garbage collect old slots to respect memory retention limit."""

    def queue_blockchain_event(self, events: List[BlockchainEvent]) -> None:
        """Queue blockchain events for processing."""

    def make_slot_download_progress(
        self, slot: Slot, shard_idx: FumeShardIdx
    ) -> SlotDownloadState:
        """Update download progress for a given slot."""

    def pop_next_slot_status(self) -> Optional[FumeSlotStatus]:
        """Pop the next slot status to process."""

    def pop_slot_to_download(self, commitment=None) -> Optional[FumeDownloadRequest]:
        """Pop the next slot to download."""

    def mark_event_as_processed(self, event_seq_number: FumeSessionSequence) -> None:
        """Mark an event as processed and update committable offset."""

    def slot_status_update_queue_len(self) -> int:
        """Return the length of the slot status update queue."""

    def processed_offset_queue_len(self) -> int:
        """Return the length of the processed offset queue."""

    def need_new_blockchain_events(self) -> bool:
        """Check if new blockchain events are needed."""
```

As you poll historical events from the fumarole service, you first register them through `queue_blockchain_event`.

If you have registered a historical blockchain event in your state machine, a new slot to download should become available to you via `pop_slot_to_download`. Your driver implementation should do the actual slot download and
track the download progress through `make_slot_download_progress`.

As you complete the slot download, you should be able to retrieve the slot status update through `pop_next_slot_status`.
Essentially, we only send a slot commitment update if you have seen the entire slot. 
By "seeing" the entire slot, I mean downloading the entire slot locally.

Once you have downloaded the slot and sent the slot status to the end user, you can then mark the slot status as "processed" through `mark_event_as_processed`.

Recap of the loop:

1. Poll new historical events if none are left.
    - Register events to `queue_blockchain_event`
2. call `pop_slot_to_download`
    - If not Null, do the actual download.
3. On slot download completed: `make_slot_download_progress`
4. Call `pop_next_slot_status`
5. Once you have sent the slot status to the consumer, call `mark_event_as_processed`
