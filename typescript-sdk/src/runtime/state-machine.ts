// import { OrderedSet } from "@js-sdsl/ordered-set";
import { OrderedSet } from "@js-sdsl/ordered-set";
import { BlockchainEvent } from "../grpc/fumarole";
import { CommitmentLevel } from "../grpc/geyser";
import { BinaryHeap } from "./binary-heap";
import { Deque } from "@datastructures-js/deque";

// Constants
export const DEFAULT_SLOT_MEMORY_RETENTION = 10000;
export const MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT = 10;

// Type aliases
export type FumeBlockchainId = Uint8Array; // Equivalent to [u8; 16]
export type FumeBlockUID = Uint8Array; // Equivalent to [u8; 16]
export type FumeNumShards = number; // Equivalent to u32
export type FumeShardIdx = number; // Equivalent to u32
export type FumeOffset = bigint; // Equivalent to i64
export type FumeSessionSequence = bigint; // Equivalent to u64
export type Slot = bigint; // From solana_sdk::clock::Slot

// Data structures
export type FumeDownloadRequest = {
  slot: Slot;
  blockchainId: FumeBlockchainId;
  blockUid: FumeBlockUID;
  numShards: FumeNumShards;
  commitmentLevel: CommitmentLevel;
};

export class FumeSlotStatus {
  constructor(
    public readonly sessionSequence: FumeSessionSequence,
    public readonly offset: FumeOffset,
    public readonly slot: Slot,
    public readonly parentSlot: Slot | undefined,
    public readonly commitmentLevel: CommitmentLevel,
    public readonly deadError: string | undefined,
  ) {}
}

export class SlotCommitmentProgression {
  processedCommitmentLevels = new Set<CommitmentLevel>();
}

export class SlotDownloadProgress {
  private numShards: number;
  private shardRemaining: boolean[];

  constructor(numShards: number) {
    this.numShards = numShards;
    this.shardRemaining = new Array(numShards).fill(false);
  }

  doProgress(shardIdx: number): SlotDownloadState {
    this.shardRemaining[shardIdx % this.numShards] = true;
    return this.shardRemaining.every(Boolean)
      ? SlotDownloadState.Done
      : SlotDownloadState.Downloading;
  }
}

export enum SlotDownloadState {
  Downloading = "Downloading",
  Done = "Done",
}

export function orderedSetContains<T>(set: OrderedSet<T>, value: T): boolean {
  return !set.find(value).equals(set.end());
}

export class FumaroleSM {
  lastCommittedOffset: FumeOffset;
  slotCommitmentProgression: Map<bigint, SlotCommitmentProgression>;
  downloadedSlotSet: OrderedSet<bigint>;
  inflightSlotShardDownload: Map<bigint, SlotDownloadProgress>;
  blockedSlotStatusUpdate: Map<bigint, Deque<FumeSlotStatus>>;
  slotStatusUpdateQueue: Deque<FumeSlotStatus>;

  // processedOffset: [bigint, FumeOffset][]; // min-heap equivalent
  processedOffset: BinaryHeap<[FumeSessionSequence, FumeOffset]>;

  committableOffset: FumeOffset;
  maxSlotDetected: bigint;
  unprocessedBlockchainEvent: Deque<[FumeSessionSequence, BlockchainEvent]>;
  sequence: bigint;
  lastProcessedFumeSequence: bigint;
  sequenceToOffset: Map<FumeSessionSequence, FumeOffset>;
  slotMemoryRetention: number;

  constructor(lastCommittedOffset: FumeOffset, slotMemoryRetention: number) {
    this.lastCommittedOffset = lastCommittedOffset;
    this.slotCommitmentProgression = new Map();
    this.downloadedSlotSet = new OrderedSet();
    this.inflightSlotShardDownload = new Map();
    this.blockedSlotStatusUpdate = new Map();
    this.slotStatusUpdateQueue = new Deque();
    this.processedOffset = new BinaryHeap<[FumeSessionSequence, FumeOffset]>(
      (a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0), // min-heap by sequence number
    );
    this.committableOffset = lastCommittedOffset;
    this.maxSlotDetected = 0n;
    this.unprocessedBlockchainEvent = new Deque();
    this.sequence = 1n;
    this.lastProcessedFumeSequence = 0n;
    this.sequenceToOffset = new Map();
    this.slotMemoryRetention = slotMemoryRetention;
  }

  updateCommittedOffset(offset: FumeOffset): void {
    console.assert(
      offset >= this.lastCommittedOffset,
      "Offset must be >= last committed offset",
    );
    this.lastCommittedOffset = offset;
  }

  nextSequence(): bigint {
    const ret = this.sequence;
    this.sequence += 1n;
    return ret;
  }

  gc(): void {
    // Garbage collect old slots to respect memory retention limit.
    while (this.downloadedSlotSet.size() > this.slotMemoryRetention) {
      // mimic pythons downloaded_slot.popfirst()
      const ds = this.downloadedSlotSet.getElementByPos(0);
      this.downloadedSlotSet.eraseElementByPos(0);

      const slot = ds ?? null;
      if (slot === null) {
        break;
      }
      this.slotCommitmentProgression.delete(slot);
      this.inflightSlotShardDownload.delete(slot);
      this.blockedSlotStatusUpdate.delete(slot);
    }
  }

  queueBlockchainEvent(events: BlockchainEvent[]): void {
    // Queue blockchain events for processing.
    for (const blockchainEvent of events) {
      if (blockchainEvent.offset < this.lastCommittedOffset) {
        continue;
      }

      if (blockchainEvent.slot > this.maxSlotDetected) {
        this.maxSlotDetected = blockchainEvent.slot;
      }

      const sequence = this.nextSequence();
      this.sequenceToOffset.set(sequence, blockchainEvent.offset);

      if (orderedSetContains(this.downloadedSlotSet, blockchainEvent.slot)) {
        const fumeStatus: FumeSlotStatus = {
          sessionSequence: sequence,
          offset: blockchainEvent.offset,
          slot: blockchainEvent.slot,
          parentSlot: blockchainEvent.parentSlot,
          commitmentLevel: blockchainEvent.commitmentLevel,
          deadError: blockchainEvent.deadError,
        };

        if (this.inflightSlotShardDownload.has(blockchainEvent.slot)) {
          this.blockedSlotStatusUpdate
            .get(blockchainEvent.slot)
            ?.pushBack(fumeStatus);
        } else {
          this.slotStatusUpdateQueue.pushBack(fumeStatus);
        }
      } else {
        this.unprocessedBlockchainEvent.pushBack([sequence, blockchainEvent]);
      }
    }
  }

  makeSlotDownloadProgress(
    slot: Slot,
    shardIdx: FumeShardIdx,
  ): SlotDownloadState {
    // Update download progress for a given slot.
    const downloadProgress = this.inflightSlotShardDownload.get(slot);
    if (!downloadProgress) {
      throw new Error("Slot not in download");
    }

    const downloadState = downloadProgress.doProgress(shardIdx);

    if (downloadState === SlotDownloadState.Done) {
      this.inflightSlotShardDownload.delete(slot);
      this.downloadedSlotSet.insert(slot);

      if (!this.slotCommitmentProgression.has(slot)) {
        this.slotCommitmentProgression.set(
          slot,
          new SlotCommitmentProgression(),
        );
      }

      const blockedStatuses =
        this.blockedSlotStatusUpdate.get(slot) ?? new Deque<FumeSlotStatus>();
      this.blockedSlotStatusUpdate.delete(slot);

      for (const status of blockedStatuses.toArray()) {
        this.slotStatusUpdateQueue.pushBack(status);
      }
    }

    return downloadState;
  }

  popNextSlotStatus(): FumeSlotStatus | null {
    // Pop the next slot status to process.
    while (!this.slotStatusUpdateQueue.isEmpty()) {
      const slotStatus = this.slotStatusUpdateQueue.popFront();
      if (!slotStatus) {
        continue;
      }

      const commitmentHistory = this.slotCommitmentProgression.get(
        slotStatus.slot,
      );

      if (commitmentHistory === null || commitmentHistory === undefined) {
        throw new Error("Slot status should not be available here");
      }

      if (
        !commitmentHistory.processedCommitmentLevels.has(
          slotStatus.commitmentLevel,
        )
      ) {
        commitmentHistory.processedCommitmentLevels.add(
          slotStatus.commitmentLevel,
        );
        return slotStatus;
      } else {
        // Already processed this commitment level
        this.markEventAsProcessed(slotStatus.sessionSequence);
      }
    }
    return null;
  }

  makeSureSlotCommitmentProgressionExists(
    slot: Slot,
  ): SlotCommitmentProgression {
    // Ensure a slot has a commitment progression entry.
    if (!this.slotCommitmentProgression.has(slot)) {
      this.slotCommitmentProgression.set(slot, new SlotCommitmentProgression());
    }
    return this.slotCommitmentProgression.get(slot)!;
  }

  popSlotToDownload(commitment?: CommitmentLevel): FumeDownloadRequest | null {
    // Pop the next slot to download.
    const minCommitment = commitment ?? CommitmentLevel.PROCESSED;

    while (!this.unprocessedBlockchainEvent.isEmpty()) {
      const [sessionSequence, blockchainEvent] =
        this.unprocessedBlockchainEvent.popFront()!;

      const eventCommitmentLevel = blockchainEvent.commitmentLevel;

      if (eventCommitmentLevel !== minCommitment) {
        this.slotStatusUpdateQueue.pushBack(
          new FumeSlotStatus(
            sessionSequence,
            blockchainEvent.offset,
            blockchainEvent.slot,
            blockchainEvent.parentSlot,
            eventCommitmentLevel,
            blockchainEvent.deadError,
          ),
        );
        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
        continue;
      }

      if (orderedSetContains(this.downloadedSlotSet, blockchainEvent.slot)) {
        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
        const progression = this.slotCommitmentProgression.get(
          blockchainEvent.slot,
        )!;

        if (progression.processedCommitmentLevels.has(eventCommitmentLevel)) {
          this.markEventAsProcessed(sessionSequence);
          continue;
        }

        this.slotStatusUpdateQueue.pushBack(
          new FumeSlotStatus(
            sessionSequence,
            blockchainEvent.offset,
            blockchainEvent.slot,
            blockchainEvent.parentSlot,
            eventCommitmentLevel,
            blockchainEvent.deadError,
          ),
        );
      } else {
        const blockchainId = blockchainEvent.blockchainId;
        const blockUid = blockchainEvent.blockUid;

        if (!this.blockedSlotStatusUpdate.get(blockchainEvent.slot)) {
          this.blockedSlotStatusUpdate.set(
            blockchainEvent.slot,
            new Deque<FumeSlotStatus>(),
          );
        }
        // this won't be undefined because if it is then we just created it above
        this.blockedSlotStatusUpdate
          .get(blockchainEvent.slot)!
          .pushBack(
            new FumeSlotStatus(
              sessionSequence,
              blockchainEvent.offset,
              blockchainEvent.slot,
              blockchainEvent.parentSlot,
              eventCommitmentLevel,
              blockchainEvent.deadError,
            ),
          );

        if (!this.inflightSlotShardDownload.has(blockchainEvent.slot)) {
          const downloadRequest: FumeDownloadRequest = {
            slot: blockchainEvent.slot,
            blockchainId,
            blockUid,
            numShards: blockchainEvent.numShards,
            commitmentLevel: eventCommitmentLevel,
          };
          const downloadProgress = new SlotDownloadProgress(
            blockchainEvent.numShards,
          );
          this.inflightSlotShardDownload.set(
            blockchainEvent.slot,
            downloadProgress,
          );
          return downloadRequest;
        }
      }
    }
    return null;
  }

  markEventAsProcessed(eventSeqNumber: FumeSessionSequence): void {
    const fumeOffset = this.sequenceToOffset.get(eventSeqNumber);
    this.sequenceToOffset.delete(eventSeqNumber);

    if (fumeOffset === undefined || fumeOffset === null) {
      throw new Error("Event sequence number not found");
    }

    // push into min-heap (compare by sequence number)
    this.processedOffset.push([eventSeqNumber, fumeOffset]);

    while (this.processedOffset.length > 0) {
      const [seq, offset] = this.processedOffset.peek()!;

      if (seq !== this.lastProcessedFumeSequence + 1n) {
        break;
      }

      this.processedOffset.pop();
      this.committableOffset = offset;
      this.lastProcessedFumeSequence = seq;
    }
  }

  slotStatusUpdateQueueLen(): number {
    return this.slotStatusUpdateQueue.size();
  }

  processedOffsetQueueLen(): number {
    return this.processedOffset.length;
  }

  needNewBlockchainEvents(): boolean {
    const MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT = 10;
    return (
      this.unprocessedBlockchainEvent.size() <
        MINIMUM_UNPROCESSED_BLOCKCHAIN_EVENT ||
      (this.slotStatusUpdateQueue.size() === 0 &&
        this.blockedSlotStatusUpdate.size === 0)
    );
  }
}
