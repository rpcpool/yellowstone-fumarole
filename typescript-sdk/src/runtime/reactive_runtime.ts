/**
 * @module reactive_runtime
 *
 * This module provides a reactive Fumarole runtime that drives the {@link FumaroleSM} state machine using RxJS streams.
 *
 * It orchestrates the event loop and asynchronous event handling with RxJS primitives, enabling scalable and composable state management.
 *
 * Key concepts:
 * - **Observables**: Streams of data/events over time.
 * - **Observers**: Consumers that react to emitted values.
 * - **Subjects**: Both source and sink, multicasting values to many subscribers.
 *
 * RxJS streams are "hot": events are emitted even if no one is listening, so careful subscription management is required to avoid missing events.
 *
 * Comparison with other languages:
 * - Rust/Golang channels behave like queues; RxJS streams are not queues and can drop events if not subscribed.
 * - RxJS `Observer` is similar to a sender, `Observable` to a receiver, and `Subject` to a broadcast channel, but concurrency semantics differ.
 *
 * Note: Drawing parallels with other languages can be misleading due to differences in concurrency and execution models.
 *
 * For more information, see:
 * - RxJS documentation: https://rxjs.dev/guide/overview
 * 
 * 
 * Here's the execution graph of the runtime:
 * 
 * 
 */
import { defer, interval, map, Observable, Observer, Subject, Subscriber } from "rxjs";
import {
  ControlCommand,
  ControlResponse,
} from "../grpc/fumarole";
import {
  CommitmentLevel,
  SlotStatus,
  SubscribeRequest,
  SubscribeUpdate,
} from "../grpc/geyser";
import {
  FumaroleSM,
  FumeDownloadRequest,
  FumeShardIdx,
  FumeSlotStatus,
  Slot,
} from "./state-machine";
import { Runtime } from "inspector/promises";
import { LOGGER } from "../logging";

export class CompletedDownloadBlockTask {
  constructor(
    public slot: bigint,
    public blockUid: Uint8Array,
    public shardIdx: FumeShardIdx,
    public totalEventDownloaded: number
  ) {}
}

export type DownloadBlockErrorKind =
  | "Disconnected"
  | "OutletDisconnected"
  | "BlockShardNotFound"
  | "FailedDownload"
  | "Fatal";


export type DownloadBlockError = {
  kind: DownloadBlockErrorKind,
  message: any
}


export type DownloadTaskResultKind = "Ok" | "Err";

export class DownloadTaskResult {
  constructor(
    public kind: DownloadTaskResultKind,
    public completed?: CompletedDownloadBlockTask,
    public slot?: bigint,
    public err?: DownloadBlockError
  ) {}
}

export type DownloadTaskArgs = {
  // todo: should be renamed slotDownloadInfo
  downloadRequest: FumeDownloadRequest,
  subscribeRequest: SubscribeRequest,
}
// ...existing code...
export abstract class AsyncSlotDownloader {
  abstract runDownload(
    subscribeRequest: SubscribeRequest,
    spec: DownloadTaskArgs
  ): Promise<DownloadTaskResult>;
}

export type Tick = { };

export type ControlPlaneResp = { readonly response: ControlResponse }

export type DownloadTaskCompleted = { readonly result: DownloadTaskResult }

export type SubscribeRequestUpdate = { readonly new_subscribe_request: SubscribeRequest }

export type RuntimeEventKind =
  | 'tick'
  | 'subscribe_request_update'
  | 'download_completed'
  | 'control_plane_response';


export type RuntimeEvent =
  | { _kind: 'tick'; tick: Tick }
  | { _kind: 'subscribe_request_update'; subscribe_request_update: SubscribeRequestUpdate }
  | { _kind: 'download_completed'; download_completed: DownloadTaskResult }
  | { _kind: 'control_plane_response'; control_plane_response: ControlResponse };

/**
 * Arguments for creating a Fumarole runtime.
 */
export type FumaroleRuntimeArgs = {
  downloadTaskObserver: Observer<DownloadTaskArgs>,
  downloadTaskResultObservable: Observable<DownloadTaskResult>,
  controlPlaneObserver: Observer<ControlCommand>,
  dragonsmouthOutlet: Subject<SubscribeUpdate>,
  controlPlaneResponseObservable: Observable<ControlResponse>,
  sm: FumaroleSM,
  commitIntervalMillis: number,
  maxConcurrentDownload: number
  initialSubscribeRequest: SubscribeRequest,
}

/**
 * Execution context for the Fumarole runtime.
 */
type FumaroleRuntimeCtx = {
  /**
   * Current tick count for the runtime.
   */
  currentTick: number,
  /**
   * State machine for managing the runtime's state.
   */
  sm: FumaroleSM,
  /**
   * How many loop-tick interval before running runtime's GC routine.
   */
  gcInterval: number;
  /**
   * Maximum number of concurrent in-flight download tasks
   * 
   * `inflight_downloads` should never exceed this number
   */
  maxConcurrentDownload: number;
  /**
   * Time since epoch in milliseconds since last commit attempt has been made.
   */
  lastCommit: number;
  /**
   * In-flight download requests
   */
  inflightDownloads: Map<Slot, DownloadTaskArgs>,
  /**
   * Current subscribe request to apply during slot download.
   */
  subscribeRequest: SubscribeRequest,
  
  /**
   * {@link Observer} for control plane commands.
   */
  controlPlaneObserver: Observer<ControlCommand>,

  /**
   * {@link Observer} for download task results.
   */
  downloadTaskObserver: Observer<DownloadTaskArgs>

  /**
   * {@link Observer} for dragonsmouth subscribe updates.
   */
  dragonsmouthOutlet: Observer<SubscribeUpdate>,

  /**
   * Flag indicating whether a poll history request is in flight.
   */
  pollHistInflightFlag: boolean,

  /**
   * Flag indicating whether a commit offset request is in flight.
   */
  commitOffsetInflightFlag: boolean
}

/**
 * Handles control plane responses.
 * 
 * @param this `FumaroleRuntimeCtx` to use
 * @param resp `ControlResponse` response from Fumarole controle-plane.
 */
function onControlPlaneResponse(this: FumaroleRuntimeCtx, resp: ControlResponse) {
  if (resp.pollHist) {
    this.pollHistInflightFlag = false;
    const pollHist = resp.pollHist;
    if (pollHist.events.length > 0) {
      LOGGER.debug(`Received poll history ${pollHist.events.length} events`);
    }
    this.sm.queueBlockchainEvent(pollHist.events);
  } else if (resp.commitOffset) {
    const commitOffset = resp.commitOffset;
    this.commitOffsetInflightFlag = false;
    LOGGER.debug(`Received commit offset: ${JSON.stringify(commitOffset)}`);
    this.sm.updateCommittedOffset(commitOffset.offset);
  } else if (resp.pong) {
    LOGGER.debug("Received pong");
  } else {
    throw new Error("Unexpected control response");
  }
}

/**
 * Handles download completion events.
 *
 * @param this `FumaroleRuntimeCtx` to use
 * @param result `DownloadTaskResult` result of the download task
 */
function onDownloadCompleted(this: FumaroleRuntimeCtx, result: DownloadTaskResult) {
  LOGGER.debug("Download completed:", result);
  if (result.kind === "Ok") {
    const completed = result.completed!;
    LOGGER.debug(
      `Download completed for slot ${completed.slot}, shard ${completed.shardIdx}, ${completed.totalEventDownloaded} total events`
    );

    this.sm.makeSlotDownloadProgress(
      completed.slot,
      completed.shardIdx
    );

    if (!this.inflightDownloads.delete(completed.slot)) {
      LOGGER.warn(`Completed download for unknown slot ${completed.slot}`);
    }
  } else {
    const slot = result.slot;
    const err = result.err;
    throw new Error(`Failed to download slot ${slot}: ${err!.message}`);
  }
}

/**
 * Commits the offset if required.
 * This is what actually commits the offset to the control plane and make the persistent-subscriber progress
 * across gRPC sessions.
 * 
 * @param this `FumaroleRuntimeCtx` to use
 * @returns 
 */
function commitOffsetIfRequired(
  this: FumaroleRuntimeCtx
) {
  if (this.commitOffsetInflightFlag) {
    return;
  }
  if (this.sm.lastCommittedOffset < this.sm.committableOffset) {
    LOGGER.debug(
      `Committing offset ${this.sm.committableOffset}`
    );
    this.controlPlaneObserver.next(
      {
        commitOffset: {
          offset: this.sm.committableOffset,
          shardId: 0,
        }
      }
    )
    this.commitOffsetInflightFlag = true;
  }
  this.lastCommit = Date.now();
}

/**
 * Handles subscribe request updates.
 *
 * @param this `FumaroleRuntimeCtx` to use
 * @param update `SubscribeRequestUpdate` update to apply
 */
function onSubscribeRequestUpdate(this: FumaroleRuntimeCtx, update: SubscribeRequestUpdate) {
  LOGGER.debug("New subscribe request update");
  this.subscribeRequest = update.new_subscribe_request;
}

/**
 * Returns the commitment level for the given context.
 * @param ctx `FumaroleRuntimeCtx` to use
 * @returns `CommitmentLevel` for the context
 */
function ctxCommitmentLevel(ctx: FumaroleRuntimeCtx): CommitmentLevel {
  return ctx.subscribeRequest.commitment ?? CommitmentLevel.PROCESSED
}

/**
 * Schedules a download task if any.
 *
 * By "schedules" we mean we send a `DownloadTaskArgs` to `FumaroleRuntimeCtx.downloadTaskObserver`.
 *
 * Scheduling is ignored if the current number of inflight download is greater or equal to `maxConcurrentDownload`.
 *
 * @param this `FumaroleRuntimeCtx` to use
 */
function scheduleDownloadTaskIfAny(
  this: FumaroleRuntimeCtx,
) {
  while (true) {
    if (this.inflightDownloads.size >= this.maxConcurrentDownload) {
      break;
    }

    const downloadRequest = this.sm.popSlotToDownload(
      ctxCommitmentLevel(this)
    );

    if (!downloadRequest) {
      break;
    }

    if (!downloadRequest.blockchainId) {
      throw new Error("Download request must have a blockchain ID");
    }

    const downloadTaskArgs: DownloadTaskArgs = {
      downloadRequest,
      subscribeRequest: this.subscribeRequest,
    };

    // Track the promise alongside the request
    // Make sure to call `remove`, otherwise you could make the whole pipeline zombie a block forever.
    this.inflightDownloads.set(downloadRequest.slot, downloadTaskArgs);

    this.downloadTaskObserver.next(downloadTaskArgs);

    LOGGER.debug(
      `Scheduling download task for slot ${downloadRequest.slot}`
    );
  }
}

/**
 * Polls the history if needed.
 *
 * @param this `FumaroleRuntimeCtx`
 */
function pollHistoryIfNeeded(
  this: FumaroleRuntimeCtx
) {
  if (this.pollHistInflightFlag) {
    return;
  }
  // Poll the history if the state machine needs new events.
  if (this.sm.needNewBlockchainEvents()) {
    const cmd = {
      pollHist: {
        shardId: 0,
      },
    };
    LOGGER.debug("Polling history,", this.sm.unprocessedBlockchainEvent.size());
    this.controlPlaneObserver.next(cmd);
    this.pollHistInflightFlag = true;
  }
}

/**
 *
 * Fumarole guarantees {@link SlotStatus} are **always** sent after the entire block content ({@link SubscribeUpdateAccount}, {@link SubscribeUpdateTransaction}, {@link SubscribeUpdateEntry} and {@link SubscribeUpdateBlockMeta}).
 * This is to simplify buffering strategy for user code.
 * 
 * During download, The runtime tracks which slot have been "processed" (downlaoded and forwarded to the outlet).
 * Once a slot is "processed" we can down push (drain) {@link SlotStatus} of the slot.
 *
 * @param this {@link FumaroleRuntimeCtx}
 * @returns 
 */
function drainSlotStatusIfAny(
  this: FumaroleRuntimeCtx
) {
  const commitment = ctxCommitmentLevel(this);
  const slotStatusVec: FumeSlotStatus[] = [];

  let slotStatus: FumeSlotStatus | null;
  while ((slotStatus = this.sm.popNextSlotStatus())) {
    LOGGER.debug(`popping slot status: ${slotStatus.slot}`);
    slotStatusVec.push(slotStatus);
  }

  if (slotStatusVec.length === 0) {
    return;
  }

  LOGGER.debug(`Draining ${slotStatusVec.length} slot status`);

  for (const slotStatus of slotStatusVec) {
    const matchedFilters: string[] = [];

    for (const [filterName, filter] of Object.entries(
      this.subscribeRequest.slots
    )) {
      if (
        filter.filterByCommitment &&
        slotStatus.commitmentLevel === commitment
      ) {
        matchedFilters.push(filterName);
      } else if (!filter.filterByCommitment) {
        matchedFilters.push(filterName);
      }
    }

    if (matchedFilters.length > 0) {
      const update: SubscribeUpdate = {
        filters: matchedFilters,
        createdAt: undefined,
        slot: {
          slot: slotStatus.slot,
          parent: slotStatus.parentSlot,
          status: slotStatus.commitmentLevel as number as SlotStatus,
          deadError: slotStatus.deadError,
        },
      };
      this.dragonsmouthOutlet.next(update);
    }

    this.sm.markEventAsProcessed(slotStatus.sessionSequence);
  }

}

/**
 * 
 * Rxjs's {@link Observer}-like implementation.
 * In order to fit the {@link Observer} interface, one has to `bind` the `this` instance that will be used
 * in the closure.
 * 
 * @param this {@link FumaroleRuntimeCtx}
 * @param ev 
 */
function runtime_observer(this: FumaroleRuntimeCtx, ev: RuntimeEvent) {
  this.currentTick += 1;

  switch (ev._kind) {
    case 'tick':
      LOGGER.debug("Received tick event");
      commitOffsetIfRequired.call(this);
      break;
    case 'subscribe_request_update':
      onSubscribeRequestUpdate.call(this, ev.subscribe_request_update);
      break;
    case 'download_completed':
      onDownloadCompleted.call(this, ev.download_completed);
      break;
    case 'control_plane_response':
      onControlPlaneResponse.call(this, ev.control_plane_response);
      break;
  }
  pollHistoryIfNeeded.call(this);
  drainSlotStatusIfAny.call(this);
  scheduleDownloadTaskIfAny.call(this);

  if (this.currentTick % this.gcInterval === 0) {
    this.sm.gc();
  }
} 

/**
 * Creates a defered rxjs {@link Observable} for the fumarole runtime.
 * 
 * The actual runtime event-loop will bootstrap lazily on first {@link Subscriber} instance listening
 * to the {@link Observable}.
 *  
 *                                                                                                                                                      
 *                                                                        ┌────────────────┐                                                            
 *                                                                        │ ControlPlane   │                                                            
 *                                                                        │   Command      │                                                            
 *                                                                        │  Observer      │                                                            
 *                                                                        └────────────────┘                                                            
 *                        ┌─────────────────┐                                    ▲                                                                      
 *                        │ Interval Ticker │                                    │                                                                      
 *                        │     Observable  ◄──────Subscribe                     │                                                                      
 *                        └─────────────────┘           │                      (next)        (next: slot_status)                                        
 *                                                      │                        │               │                                                      
 *                                                      │                        │               │                                                      
 *                                                      │                        │               │                                                      
 * ┌───────────────┐               ┌────────────────────┴┐                ┌──────┴──────────┐    │      ┌───────────────┐          ┌───────────────────┐
 * │ ControlPlane  │               │ Fumarole Main Bus   ◄───(observe)────┤ runtimeObserver ├────┴────► │ Dragonsmouth  │  Observe │ User custom       │
 * │   Response  ◄─│───Subscribe───┤   Subject           │                │                 │           │    Subject    │◄─────────┼   Observer code   │
 * │  Observable   │               └───       ──────────┬┘                └───────┬─────────┘           └──────▲────────┘          └───────────────────┘
 * └───────────────┘                                    │                         │                            │                                        
 *                                                    Observe                   (next)                         │                                        
 *                                                      │                         │                            │                                        
 *                                                      │                         │                            │                                        
 *                                                      ▼                         ▼                            │                                        
 *                                             ┌───────────────────┐        ┌─────────────┐                    │                                        
 *                                             │DownloadTaskResult          │SlotDownloder│                    │                                        
 *                                             │   Observable      │        │ Observer    ┼───────(next)───────┘                                        
 *                                             └───────────────────┘        └─────────────┘      (data: account/tx/entry/blockmeta)                                                                       
 *  
 * @param args The arguments for the fumarole runtime.
 * @returns An observable that emits subscribe updates.
 */
export function fumaroleObservable(args: FumaroleRuntimeArgs): Observable<SubscribeUpdate> {
  const {
    downloadTaskObserver,
    controlPlaneObserver,
    downloadTaskResultObservable,
    dragonsmouthOutlet,
    controlPlaneResponseObservable,
    commitIntervalMillis,
    sm,
    maxConcurrentDownload,
    initialSubscribeRequest,
  } = args;

  // Defer all the stitching to wait at least one subscription otherwise we could lose event.
  return defer(() => {
    // Main bus will handle all runtime events
    const fumaroleMainBus = new Subject<RuntimeEvent>();

    // Plug download task result to the main bus
    downloadTaskResultObservable
    .pipe(
      map((result) => {
        return { _kind: 'download_completed', download_completed: result } as RuntimeEvent;
      })
    )
    .subscribe(fumaroleMainBus);
    LOGGER.debug("Plugged download task result");

    // Plug ticker
    interval(commitIntervalMillis).pipe(
      map((_) => {
        return { _kind: 'tick' } as RuntimeEvent;
      })
    ).subscribe(fumaroleMainBus);
    LOGGER.debug("Plugged ticker");

    // Plug control plane response
    controlPlaneResponseObservable
      .subscribe((response) => {
        fumaroleMainBus.next({ _kind: 'control_plane_response', control_plane_response: response });
      });
    LOGGER.debug("Plugged control plane response");

    const ctx: FumaroleRuntimeCtx = {
      currentTick: 0,
      sm,
      gcInterval: 1000,
      maxConcurrentDownload,
      lastCommit: Date.now(),
      inflightDownloads: new Map(),
      subscribeRequest: initialSubscribeRequest,
      controlPlaneObserver,
      downloadTaskObserver,
      dragonsmouthOutlet,
      pollHistInflightFlag: false,
      commitOffsetInflightFlag: false,
    }; 

    const runtimeObserver = runtime_observer.bind(ctx);

    fumaroleMainBus.subscribe(runtimeObserver);
    LOGGER.debug("Plugged runtime observer");
    return dragonsmouthOutlet.asObservable();
  });
}