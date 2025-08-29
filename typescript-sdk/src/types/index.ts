import { SubscribeRequest, SubscribeUpdate } from "../grpc/geyser";
import { ControlCommand, ControlResponse } from "../grpc/fumarole";
import { Observable, ObservedValueOf, Observer } from "rxjs";

export interface FumaroleSubscribeConfig {
  concurrentDownloadLimit: number;
  commitInterval: number;
  maxFailedSlotDownloadAttempt: number;
  gcInterval: number;
  slotMemoryRetention: number;
}

export function getDefaultFumaroleSubscribeConfig(): FumaroleSubscribeConfig {
  return {
    concurrentDownloadLimit: DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
    commitInterval: DEFAULT_COMMIT_INTERVAL,
    maxFailedSlotDownloadAttempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
    gcInterval: DEFAULT_GC_INTERVAL,
    slotMemoryRetention: DEFAULT_SLOT_MEMORY_RETENTION
  };
}

export interface DragonsmouthAdapterSession {
  /** Queue for sending subscribe requests */
  sink: Observer<SubscribeRequest>;
  /** Queue for receiving subscription updates */
  source: Observable<SubscribeUpdate | Error>;
}

// Constants
export const DEFAULT_COMMIT_INTERVAL = 5000; // milliseconds
export const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3;
export const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 10;
export const DEFAULT_GC_INTERVAL = 100; // ticks
export const DEFAULT_SLOT_MEMORY_RETENTION = 1000; // seconds
