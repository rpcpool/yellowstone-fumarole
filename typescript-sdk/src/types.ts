import { SubscribeRequest, SubscribeUpdate } from "./grpc/geyser";

// Constants
export const DEFAULT_DRAGONSMOUTH_CAPACITY = 10000;
export const DEFAULT_COMMIT_INTERVAL = 5.0; // seconds
export const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3;
export const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 10;
export const DEFAULT_GC_INTERVAL = 60; // seconds
export const DEFAULT_SLOT_MEMORY_RETENTION = 300; // seconds

export interface FumaroleSubscribeConfig {
  // The maximum number of concurrent download tasks per TCP connection.
  concurrentDownloadLimit?: number;

  // The interval at which to commit the slot memory.
  commitInterval?: number;

  // The maximum number of failed slot download attempts before giving up.
  maxFailedSlotDownloadAttempt?: number;

  // The maximum number of slots to download concurrently.
  dataChannelCapacity?: number;

  // The interval at which to perform garbage collection on the slot memory.
  gcInterval?: number;

  // The retention period for slot memory in seconds.
  slotMemoryRetention?: number;
}

export interface DragonsmouthAdapterSession {
  // The queue for sending SubscribeRequest update to the dragonsmouth stream
  sink: AsyncQueue<SubscribeRequest>;

  // The queue for receiving SubscribeUpdate from the dragonsmouth stream
  source: AsyncQueue<SubscribeUpdate>;

  // The handle for the fumarole runtime
  fumaroleHandle: Promise<void>;
}

// Generic async queue interface to mimic Python's asyncio.Queue
export class AsyncQueue<T> {
  private queue: T[] = [];
  private maxSize: number;
  private resolvers: ((value: T) => void)[] = [];
  private full_resolvers: (() => void)[] = [];
  private closed = false;

  constructor(maxSize = 0) {
    this.maxSize = maxSize;
  }

  async put(item: T): Promise<void> {
    if (this.closed) {
      throw new Error("Queue is closed");
    }

    if (this.maxSize > 0 && this.queue.length >= this.maxSize) {
      return new Promise<void>((resolve) => {
        this.full_resolvers.push(resolve);
      });
    }

    this.queue.push(item);
    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver(this.queue.shift()!);
    }
  }

  async get(): Promise<T> {
    if (this.closed && this.queue.length === 0) {
      throw new Error("Queue is closed");
    }

    if (this.queue.length === 0) {
      return new Promise<T>((resolve) => {
        this.resolvers.push(resolve);
      });
    }

    const item = this.queue.shift()!;
    const full_resolver = this.full_resolvers.shift();
    if (full_resolver) {
      full_resolver();
    }
    return item;
  }

  close(): void {
    this.closed = true;
    // Resolve all pending gets with an error
    this.resolvers.forEach((resolve) => {
      resolve(undefined as any);
    });
    this.resolvers = [];
  }
}
