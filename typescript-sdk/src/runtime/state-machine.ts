import { BlockchainEvent } from "../grpc/fumarole";
import { CommitmentLevel } from "../grpc/geyser";
import { Queue as Deque } from "./queue";

// Constants
export const DEFAULT_SLOT_MEMORY_RETENTION = 10000;

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
  private slotCommitmentProgression = new Map<
    Slot,
    SlotCommitmentProgression
  >();
  private downloadedSlot = new Set<Slot>();
  private inflightSlotShardDownload = new Map<Slot, SlotDownloadProgress>();
  private blockedSlotStatusUpdate = new Map<Slot, Deque<FumeSlotStatus>>();
  private slotStatusUpdateQueue = new Deque<FumeSlotStatus>();
  private processedOffset: [bigint, bigint][] = []; // Min-heap for (sequence, offset)
  private maxSlotDetected = 0n;
  private unprocessedBlockchainEvent = new Deque<
    [FumeSessionSequence, BlockchainEvent]
  >();
  private sequence = 1n;
  private lastProcessedFumeSequence = 0n;
  private sequenceToOffset = new Map<FumeSessionSequence, FumeOffset>();
  private _committableOffset: FumeOffset;
  private _lastCommittedOffset: FumeOffset;

  constructor(
    lastCommittedOffset: FumeOffset,
    private readonly slotMemoryRetention: number
  ) {
    this._lastCommittedOffset = lastCommittedOffset;
    this._committableOffset = lastCommittedOffset;
  }

  get lastCommittedOffset(): FumeOffset {
    return this._lastCommittedOffset;
  }

  get committableOffset(): FumeOffset {
    return this._committableOffset;
  }

  public updateCommittedOffset(offset: FumeOffset): void {
    if (BigInt(offset) < BigInt(this._lastCommittedOffset)) {
      throw new Error("Offset must be >= last committed offset");
    }
    this._lastCommittedOffset = offset;
  }

  private nextSequence(): bigint {
    const ret = this.sequence;
    this.sequence += 1n;
    return ret;
  }

  public gc(): void {
    while (this.downloadedSlot.size > this.slotMemoryRetention) {
      // Get the first slot (oldest) from the set
      const slot = this.downloadedSlot.values().next().value;
      if (!slot) break;

      this.downloadedSlot.delete(slot);
      this.slotCommitmentProgression.delete(slot);
      this.inflightSlotShardDownload.delete(slot);
      this.blockedSlotStatusUpdate.delete(slot);
    }
  }

  public async queueBlockchainEvent(events: BlockchainEvent[]): Promise<void> {
    for (const event of events) {
      if (BigInt(event.offset) < BigInt(this._lastCommittedOffset)) {
        continue;
      }

      if (event.slot > this.maxSlotDetected) {
        this.maxSlotDetected = event.slot;
      }

      const sequence = this.nextSequence();
      this.sequenceToOffset.set(sequence, event.offset);

      if (this.downloadedSlot.has(event.slot)) {
        const fumeStatus = new FumeSlotStatus(
          sequence,
          event.offset,
          event.slot,
          event.parentSlot,
          event.commitmentLevel,
          event.deadError
        );

        if (this.inflightSlotShardDownload.has(event.slot)) {
          let blockedQueue = this.blockedSlotStatusUpdate.get(event.slot);
          if (!blockedQueue) {
            blockedQueue = new Deque<FumeSlotStatus>();
            this.blockedSlotStatusUpdate.set(event.slot, blockedQueue);
          }
          await blockedQueue.put(fumeStatus);
        } else {
          await this.slotStatusUpdateQueue.put(fumeStatus);
        }
      } else {
        await this.unprocessedBlockchainEvent.put([sequence, event]);
      }
    }
  }

  public async makeSlotDownloadProgress(
    slot: Slot,
    shardIdx: FumeShardIdx
  ): Promise<SlotDownloadState> {
    const downloadProgress = this.inflightSlotShardDownload.get(slot);
    if (!downloadProgress) {
      throw new Error("Slot not in download");
    }

    const downloadState = downloadProgress.doProgress(shardIdx);

    if (downloadState === SlotDownloadState.Done) {
      this.inflightSlotShardDownload.delete(slot);
      this.downloadedSlot.add(slot);

      if (!this.slotCommitmentProgression.has(slot)) {
        this.slotCommitmentProgression.set(
          slot,
          new SlotCommitmentProgression()
        );
      }

      const blockedStatuses = this.blockedSlotStatusUpdate.get(slot);
      if (blockedStatuses) {
        // Move all blocked statuses to the main queue
        while (!blockedStatuses.isEmpty()) {
          const status = await blockedStatuses.get();
          if (status) await this.slotStatusUpdateQueue.put(status);
        }
        this.blockedSlotStatusUpdate.delete(slot);
      }
    }

    return downloadState;
  }

  public async popNextSlotStatus(): Promise<FumeSlotStatus | null> {
    while (!this.slotStatusUpdateQueue.isEmpty()) {
      const slotStatus = await this.slotStatusUpdateQueue.get();
      if (!slotStatus) continue;

      const commitmentHistory = this.slotCommitmentProgression.get(
        slotStatus.slot
      );
      if (
        commitmentHistory &&
        !commitmentHistory.hasProcessedCommitment(slotStatus.commitmentLevel)
      ) {
        commitmentHistory.addProcessedCommitment(slotStatus.commitmentLevel);
        return slotStatus;
      } else if (!commitmentHistory) {
        throw new Error("Slot status should not be available here");
      }
    }
    return null;
  }

  private makeSureSlotCommitmentProgressionExists(
    slot: Slot
  ): SlotCommitmentProgression {
    let progression = this.slotCommitmentProgression.get(slot);
    if (!progression) {
      progression = new SlotCommitmentProgression();
      this.slotCommitmentProgression.set(slot, progression);
    }
    return progression;
  }

  public async popSlotToDownload(
    commitment = CommitmentLevel.PROCESSED
  ): Promise<FumeDownloadRequest | null> {
    while (!this.unprocessedBlockchainEvent.isEmpty()) {
      const eventPair = await this.unprocessedBlockchainEvent.get();
      if (!eventPair) continue;

      const [sessionSequence, blockchainEvent] = eventPair;
      const eventCl = blockchainEvent.commitmentLevel;

      if (eventCl < commitment) {
        await this.slotStatusUpdateQueue.put(
          new FumeSlotStatus(
            sessionSequence,
            blockchainEvent.offset,
            blockchainEvent.slot,
            blockchainEvent.parentSlot,
            eventCl,
            blockchainEvent.deadError
          )
        );
        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
        continue;
      }

      if (this.downloadedSlot.has(blockchainEvent.slot)) {
        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
        const progression = this.slotCommitmentProgression.get(
          blockchainEvent.slot
        );
        if (progression && progression.hasProcessedCommitment(eventCl)) {
          this.markEventAsProcessed(sessionSequence);
          continue;
        }

        await this.slotStatusUpdateQueue.put(
          new FumeSlotStatus(
            sessionSequence,
            blockchainEvent.offset,
            blockchainEvent.slot,
            blockchainEvent.parentSlot,
            eventCl,
            blockchainEvent.deadError
          )
        );
      } else {
        const blockchainId = new Uint8Array(blockchainEvent.blockchainId);
        const blockUid = new Uint8Array(blockchainEvent.blockUid);
        if (!this.inflightSlotShardDownload.has(blockchainEvent.slot)) {
          const downloadRequest = new FumeDownloadRequest(
            blockchainEvent.slot,
            blockchainId,
            blockUid,
            blockchainEvent.numShards,
            eventCl
          );

          const downloadProgress = new SlotDownloadProgress(
            blockchainEvent.numShards
          );
          this.inflightSlotShardDownload.set(
            blockchainEvent.slot,
            downloadProgress
          );

          let blockedQueue = this.blockedSlotStatusUpdate.get(
            blockchainEvent.slot
          );
          if (!blockedQueue) {
            blockedQueue = new Deque<FumeSlotStatus>();
            this.blockedSlotStatusUpdate.set(
              blockchainEvent.slot,
              blockedQueue
            );
          }

          await blockedQueue.put(
            new FumeSlotStatus(
              sessionSequence,
              blockchainEvent.offset,
              blockchainEvent.slot,
              blockchainEvent.parentSlot,
              eventCl,
              blockchainEvent.deadError
            )
          );

          return downloadRequest;
        }
      }
    }
    return null;
  }

  public markEventAsProcessed(eventSeqNumber: FumeSessionSequence): void {
    const fumeOffset = this.sequenceToOffset.get(eventSeqNumber);
    if (!fumeOffset) {
      throw new Error("Event sequence number not found");
    }
    this.sequenceToOffset.delete(eventSeqNumber);

    // Use negative values for the min-heap (to simulate max-heap behavior)
    this.processedOffset.push([-eventSeqNumber, fumeOffset]);
    this.processedOffset.sort((a, b) => {
      if (a[0] < b[0]) return -1;
      if (a[0] > b[0]) return 1;
      return 0;
    });// Keep sorted as a min-heap

    while (this.processedOffset.length > 0) {
      const [seq, offset] = this.processedOffset[0];
      const positiveSeq = -seq; // Convert back to positive

      if (positiveSeq !== this.lastProcessedFumeSequence + 1n) {
        break;
      }

      this.processedOffset.shift();
      this._committableOffset = offset;
      this.lastProcessedFumeSequence = positiveSeq;
    }
  }

  public slotStatusUpdateQueueLen(): number {
    return this.slotStatusUpdateQueue.size();
  }

  public processedOffsetQueueLen(): number {
    return this.processedOffset.length;
  }

  public needNewBlockchainEvents(): boolean {
    return (
      this.slotStatusUpdateQueue.isEmpty() &&
      this.blockedSlotStatusUpdate.size === 0
    );
  }
}
