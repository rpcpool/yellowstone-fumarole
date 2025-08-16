import { BlockchainEvent } from "../grpc/fumarole";
import { CommitmentLevel } from "../grpc/geyser";
import { BinaryHeap } from "./binary-heap";

class Queue<T> {
  private items: T[] = [];
  
  push(item: T): void {
    this.items.push(item);
  }

  shift(): T | undefined {
    return this.items.shift();
  }

  get length(): number {
    return this.items.length;
  }
}

// Constants
export const DEFAULT_SLOT_MEMORY_RETENTION = 10000;
export const MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT = 10;

// Type aliases
export type FumeBlockchainId = Uint8Array; // Equivalent to [u8; 16]
export type FumeBlockUID = Uint8Array; // Equivalent to [u8; 16]
export type FumeNumShards = number; // Equivalent to u32
export type FumeShardIdx = number; // Equivalent to u32
export type FumeOffset = bigint; // Equivalent to i64 as string for large numbers
export type FumeSessionSequence = bigint; // Equivalent to u64
export type Slot = bigint; // From solana_sdk::clock::Slot

// Data structures
export class FumeDownloadRequest {
  constructor(
    public readonly slot: Slot,
    public readonly blockchainId: FumeBlockchainId,
    public readonly blockUid: FumeBlockUID,
    public readonly numShards: FumeNumShards,
    public readonly commitmentLevel: CommitmentLevel
  ) {}
}

export class FumeSlotStatus {
  constructor(
    public readonly sessionSequence: FumeSessionSequence,
    public readonly offset: FumeOffset,
    public readonly slot: Slot,
    public readonly parentSlot: Slot | undefined,
    public readonly commitmentLevel: CommitmentLevel,
    public readonly deadError: string | undefined
  ) {}
}

export class SlotCommitmentProgression {
  private processedCommitmentLevels = new Set<CommitmentLevel>();

  public hasProcessedCommitment(level: CommitmentLevel): boolean {
    return this.processedCommitmentLevels.has(level);
  }

  public addProcessedCommitment(level: CommitmentLevel): void {
    this.processedCommitmentLevels.add(level);
  }
}

export class SlotDownloadProgress {
  private shardRemaining: boolean[];

  constructor(public readonly numShards: FumeNumShards) {
    this.shardRemaining = new Array(numShards).fill(false);
  }

  public doProgress(shardIdx: FumeShardIdx): SlotDownloadState {
    this.shardRemaining[shardIdx % this.numShards] = true;
    return this.shardRemaining.every((x) => x)
      ? SlotDownloadState.Done
      : SlotDownloadState.Downloading;
  }
}

export enum SlotDownloadState {
  Downloading = "Downloading",
  Done = "Done",
}

export class FumaroleSM {
  private slot_commitment_progression = new Map<
    Slot,
    SlotCommitmentProgression
  >();
  private downloaded_slot = new Set<Slot>();
  private inflight_slot_shard_download = new Map<Slot, SlotDownloadProgress>();
  private blocked_slot_status_update = new Map<Slot, Queue<FumeSlotStatus>>();
  private slot_status_update_queue = new Queue<FumeSlotStatus>();
  private processed_offset = new BinaryHeap<[FumeSessionSequence, FumeOffset]>(
    (
      a: [FumeSessionSequence, FumeOffset],
      b: [FumeSessionSequence, FumeOffset]
    ) => {
      // Implementing Reverse ordering as in Rust
      if (a[0] === b[0]) return 0;
      return a[0] > b[0] ? 1 : -1;
    }
  );
  private unprocessed_blockchain_event = new Queue<
    [FumeSessionSequence, BlockchainEvent]
  >();
  private sequence = 1n;
  private sequence_to_offset = new Map<FumeSessionSequence, FumeOffset>();
  public max_slot_detected: Slot = 0n;
  private last_processed_fume_sequence = 0n;
  public committable_offset: FumeOffset;

  constructor(
    public last_committed_offset: FumeOffset,
    private slot_memory_retention: number = DEFAULT_SLOT_MEMORY_RETENTION
  ) {
    this.committable_offset = last_committed_offset;
  }

  public updateCommittedOffset(offset: FumeOffset): void {
    if (offset < this.last_committed_offset) {
      throw new Error(
        "offset must be greater than or equal to last committed offset"
      );
    }
    this.last_committed_offset = offset;
  }

  private nextSequence(): FumeSessionSequence {
    const ret = this.sequence;
    this.sequence = this.sequence + 1n;
    return ret;
  }

  public gc(): void {
    while (this.downloaded_slot.size > this.slot_memory_retention) {
      const firstSlot = Array.from(this.downloaded_slot)[0];
      if (!firstSlot) break;

      this.downloaded_slot.delete(firstSlot);
      this.slot_commitment_progression.delete(firstSlot);
      this.inflight_slot_shard_download.delete(firstSlot);
      this.blocked_slot_status_update.delete(firstSlot);
    }
  }

  public queueBlockchainEvent(events: BlockchainEvent[]): void {
    for (const event of events) {
      if (event.offset < this.last_committed_offset) {
        continue;
      }

      if (event.slot > this.max_slot_detected) {
        this.max_slot_detected = event.slot;
      }

      const sequence = this.nextSequence();
      this.sequence_to_offset.set(sequence, event.offset);

      if (this.downloaded_slot.has(event.slot)) {
        const fumeStatus = new FumeSlotStatus(
          sequence,
          event.offset,
          event.slot,
          event.parentSlot,
          event.commitmentLevel as CommitmentLevel,
          event.deadError
        );

        if (this.inflight_slot_shard_download.has(event.slot)) {
          // This event is blocked by a slot download currently in progress
          let queue = this.blocked_slot_status_update.get(event.slot);
          if (!queue) {
            queue = new Queue<FumeSlotStatus>();
            this.blocked_slot_status_update.set(event.slot, queue);
          }
          queue.push(fumeStatus);
        } else {
          // Fast track this event
          this.slot_status_update_queue.push(fumeStatus);
        }
      } else {
        this.unprocessed_blockchain_event.push([sequence, event]);
      }
    }
  }

  public makeSlotDownloadProgress(
    slot: Slot,
    shardIdx: FumeShardIdx
  ): SlotDownloadState {
    const downloadProgress = this.inflight_slot_shard_download.get(slot);
    if (!downloadProgress) {
      throw new Error("slot not in download");
    }

    const downloadState = downloadProgress.doProgress(shardIdx);

    if (downloadState === SlotDownloadState.Done) {
      this.inflight_slot_shard_download.delete(slot);
      this.downloaded_slot.add(slot);
      if (!this.slot_commitment_progression.has(slot)) {
        this.slot_commitment_progression.set(
          slot,
          new SlotCommitmentProgression()
        );
      }

      const blockedSlotStatus =
        this.blocked_slot_status_update.get(slot) ??
        new Queue<FumeSlotStatus>();
      this.blocked_slot_status_update.delete(slot);
      while (blockedSlotStatus.length > 0) {
        const status = blockedSlotStatus.shift();
        if (status) this.slot_status_update_queue.push(status);
      }
    }
    return downloadState;
  }

  public popNextSlotStatus(): FumeSlotStatus | undefined {
    while (this.slot_status_update_queue.length > 0) {
      const slotStatus = this.slot_status_update_queue.shift();
      if (!slotStatus) return undefined;

      const commitmentHistory = this.slot_commitment_progression.get(
        slotStatus.slot
      );
      if (commitmentHistory) {
        if (
          !commitmentHistory.hasProcessedCommitment(slotStatus.commitmentLevel)
        ) {
          commitmentHistory.addProcessedCommitment(slotStatus.commitmentLevel);
          return slotStatus;
        }
        // We already processed this commitment level
        continue;
      } else {
        // This should be unreachable as per Rust implementation
        throw new Error("slot status should not be available here");
      }
    }
    return undefined;
  }

  private makeSlotCommitmentProgressionExists(
    slot: Slot
  ): SlotCommitmentProgression {
    let progression = this.slot_commitment_progression.get(slot);
    if (!progression) {
      progression = new SlotCommitmentProgression();
      this.slot_commitment_progression.set(slot, progression);
    }
    return progression;
  }

  public popSlotToDownload(
    commitment?: CommitmentLevel
  ): FumeDownloadRequest | undefined {
    while (this.unprocessed_blockchain_event.length > 0) {
      const minCommitment = commitment ?? CommitmentLevel.PROCESSED;
      const next = this.unprocessed_blockchain_event.shift();
      if (!next) return undefined;
      const [sessionSequence, event] = next;
      if (!event) return undefined;

      const eventCommitmentLevel = event.commitmentLevel as CommitmentLevel;

      if (eventCommitmentLevel !== minCommitment) {
        this.slot_status_update_queue.push(
          new FumeSlotStatus(
            sessionSequence,
            event.offset,
            event.slot,
            event.parentSlot,
            eventCommitmentLevel,
            event.deadError
          )
        );
        this.makeSlotCommitmentProgressionExists(event.slot);
        continue;
      }

      if (this.downloaded_slot.has(event.slot)) {
        this.makeSlotCommitmentProgressionExists(event.slot);
        const progression = this.slot_commitment_progression.get(event.slot);
        if (!progression) {
          throw new Error("slot status should not be available here");
        }

        if (progression.hasProcessedCommitment(eventCommitmentLevel)) {
          this.markEventAsProcessed(sessionSequence);
          continue;
        }

        this.slot_status_update_queue.push(
          new FumeSlotStatus(
            sessionSequence,
            event.offset,
            event.slot,
            event.parentSlot,
            eventCommitmentLevel,
            event.deadError
          )
        );
      } else {
        let queue = this.blocked_slot_status_update.get(event.slot);
        if (!queue) {
          queue = new Queue<FumeSlotStatus>();
          this.blocked_slot_status_update.set(event.slot, queue);
        }
        queue.push(
          new FumeSlotStatus(
            sessionSequence,
            event.offset,
            event.slot,
            event.parentSlot,
            eventCommitmentLevel,
            event.deadError
          )
        );

        if (!this.inflight_slot_shard_download.has(event.slot)) {
          const downloadRequest = new FumeDownloadRequest(
            event.slot,
            event.blockchainId,
            event.blockUid,
            event.numShards,
            eventCommitmentLevel
          );
          const downloadProgress = new SlotDownloadProgress(event.numShards);
          this.inflight_slot_shard_download.set(event.slot, downloadProgress);
          return downloadRequest;
        }
      }
    }
    return undefined;
  }

  public slotStatusUpdateQueueLen(): number {
    return this.slot_status_update_queue.length;
  }

  public markEventAsProcessed(eventSeqNumber: FumeSessionSequence): void {
    const fumeOffset = this.sequence_to_offset.get(eventSeqNumber);
    if (!fumeOffset) {
      throw new Error("event sequence number not found");
    }
    this.sequence_to_offset.delete(eventSeqNumber);
    this.processed_offset.push([eventSeqNumber, fumeOffset]);

    while (true) {
      const tuple = this.processed_offset.peek();
      if (!tuple) break;

      const [blockedEventSeqNumber2, fumeOffset2] = tuple;
      if (blockedEventSeqNumber2 !== this.last_processed_fume_sequence + 1n) {
        break;
      }

      this.processed_offset.pop();
      this.committable_offset = fumeOffset2;
      this.last_processed_fume_sequence = blockedEventSeqNumber2;
    }
  }

  public processedOffsetQueueLen(): number {
    return this.processed_offset.length;
  }

  public needNewBlockchainEvents(): boolean {
    return (
      this.unprocessed_blockchain_event.length <
        MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT ||
      (this.slot_status_update_queue.length === 0 &&
        this.blocked_slot_status_update.size === 0)
    );
  }
}
