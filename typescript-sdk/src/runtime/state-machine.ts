export type FumeShardIdx = number;
export type FumeOffset = string;

export interface FumeDownloadRequest {
  slot: number;
  blockchainId: Uint8Array;
  blockUid: Uint8Array;
}

export class FumaroleSM {
  private _lastCommittedOffset: FumeOffset;
  private _committableOffset: FumeOffset;
  private _slotStatusQueue: any[];
  private _needNewEvents: boolean;

  constructor() {
    this._lastCommittedOffset = "0";
    this._committableOffset = "0";
    this._slotStatusQueue = [];
    this._needNewEvents = true;
  }

  get lastCommittedOffset(): FumeOffset {
    return this._lastCommittedOffset;
  }

  get committableOffset(): FumeOffset {
    return this._committableOffset;
  }

  needNewBlockchainEvents(): boolean {
    return this._needNewEvents;
  }

  updateCommittedOffset(offset: FumeOffset): void {
    this._lastCommittedOffset = offset;
  }

  queueBlockchainEvent(events: any[]): void {
    // Implementation would go here
    this._needNewEvents = false;
  }

  gc(): void {
    // Implementation of garbage collection
  }

  popSlotToDownload(commitment: number): FumeDownloadRequest | null {
    // Implementation would go here
    return null;
  }

  makeSlotDownloadProgress(slot: number, shardIdx: FumeShardIdx): void {
    // Implementation would go here
  }

  popNextSlotStatus(): any | null {
    return this._slotStatusQueue.shift() || null;
  }

  markEventAsProcessed(sessionSequence: number): void {
    // Implementation would go here
  }
}
