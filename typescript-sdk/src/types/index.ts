import { SubscribeRequest, SubscribeUpdate } from "../grpc/geyser";
import { ControlCommand, ControlResponse } from "../grpc/fumarole";
import { AsyncQueue } from "../runtime/async-queue";

export interface FumaroleSubscribeConfig {
  concurrentDownloadLimit: number;
  commitInterval: number;
  maxFailedSlotDownloadAttempt: number;
  dataChannelCapacity: number;
  gcInterval: number;
  slotMemoryRetention: number;
}

export function getDefaultFumaroleSubscribeConfig(): FumaroleSubscribeConfig {
  return {
    concurrentDownloadLimit: DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
    commitInterval: DEFAULT_COMMIT_INTERVAL,
    maxFailedSlotDownloadAttempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
    dataChannelCapacity: DEFAULT_DRAGONSMOUTH_CAPACITY,
    gcInterval: DEFAULT_GC_INTERVAL,
    slotMemoryRetention: DEFAULT_SLOT_MEMORY_RETENTION
  };
}

// export class AsyncQueue<T> {
//   private queue: T[] = [];
//   private waitingResolvers: ((value: T) => void)[] = [];
//   private closed = false;

//   constructor(private maxSize: number) {}

//   async put(item: T): Promise<void> {
//     if (this.closed) {
//       throw new Error("Queue is closed");
//     }
//     if (this.waitingResolvers.length > 0) {
//       const resolve = this.waitingResolvers.shift()!;
//       resolve(item);
//     } else {
//       if (this.queue.length >= this.maxSize) {
//         await new Promise<void>((resolve) => {
//           this.waitingResolvers.push(() => resolve());
//         });
//       }
//       this.queue.push(item);
//     }
//   }

//   async get(): Promise<T> {
//     if (this.queue.length > 0) {
//       const item = this.queue.shift()!;
//       if (this.waitingResolvers.length > 0) {
//         const resolve = this.waitingResolvers.shift()!;
//         resolve(undefined as any);
//       }
//       return item;
//     }

//     if (this.closed) {
//       throw new Error("Queue shutdown");
//     }

//     return new Promise<T>((resolve) => {
//       this.waitingResolvers.push(resolve);
//     });
//   }

//   close() {
//     this.closed = true;
//     // Resolve all waiting getters with error
//     while (this.waitingResolvers.length > 0) {
//       const resolve = this.waitingResolvers.shift()!;
//       resolve(undefined as any);
//     }
//   }
// }

export interface DragonsmouthAdapterSession {
  /** Queue for sending subscribe requests */
  sink: AsyncQueue<SubscribeRequest>;
  /** Queue for receiving subscription updates */
  source: AsyncQueue<SubscribeUpdate | Error>;
  /** Handle for tracking the fumarole runtime */
  fumaroleHandle: Promise<void>;
}

// Constants
export const DEFAULT_DRAGONSMOUTH_CAPACITY = 10000;
export const DEFAULT_COMMIT_INTERVAL = 5.0; // seconds
export const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3;
export const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 10;
export const DEFAULT_GC_INTERVAL = 60; // seconds
export const DEFAULT_SLOT_MEMORY_RETENTION = 300; // seconds
