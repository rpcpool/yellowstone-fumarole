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
  FumeOffset,
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
DownloadTaskResult
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

// export type RuntimeEvent = {
//   _kind: RuntimeEventKind,
//   tick: Tick | undefined,
//   subscribe_request_update: SubscribeRequestUpdate | undefined,
//   download_completed: DownloadTaskCompleted | undefined,
//   control_plane_response: ControlPlaneResp | undefined
// }

export type RuntimeEvent =
  | { _kind: 'tick'; tick: Tick }
  | { _kind: 'subscribe_request_update'; subscribe_request_update: SubscribeRequestUpdate }
  | { _kind: 'download_completed'; download_completed: DownloadTaskResult }
  | { _kind: 'control_plane_response'; control_plane_response: ControlResponse };


// export class FumeDragonsmouthRuntime {
//   public stateMachine: FumaroleSM;
//   public slotDownloader: AsyncSlotDownloader;
//   public subscribeRequest: SubscribeRequest;
//   public consumerGroupName: string;
//   public fumaroleEventBus: Observable<RuntimeEvent>;
//   public dragonsmouthOutlet: Observer<SubscribeUpdate | Error>;
//   public commitInterval: number; // in seconds
//   public gcInterval: number;
//   public maxConcurrentDownload: number;
//   public downloadTasks: Map<Promise<DownloadTaskResult>, FumeDownloadRequest>;
//   public lastCommit: number;

//   constructor(
//     stateMachine: FumaroleSM,
//     slotDownloader: AsyncSlotDownloader,
//     subscribeRequest: SubscribeRequest,
//     consumerGroupName: string,
//     fumaroleEventBus: Observable<RuntimeEvent>,
//     dragonsmouthOutlet: Observer<SubscribeUpdate | Error>,
//     commitInterval: number,
//     gcInterval: number,
//     maxConcurrentDownload: number = 10
//   ) {
//     this.stateMachine = stateMachine;
//     this.slotDownloader = slotDownloader;
//     this.subscribeRequest = subscribeRequest;
//     this.consumerGroupName = consumerGroupName;
//     this.fumaroleEventBus = fumaroleEventBus;
//     this.dragonsmouthOutlet = dragonsmouthOutlet;
//     this.commitInterval = commitInterval;
//     this.gcInterval = gcInterval;
//     this.maxConcurrentDownload = maxConcurrentDownload;
//     this.downloadTasks = new Map();
//     this.lastCommit = Date.now() / 1000; // seconds since epoch; to match python syntax
//   }

//   private buildPollHistoryCmd(fromOffset?: FumeOffset): ControlCommand {
//     // Build a command to poll the blockchain history
//     return {
//       pollHist: {
//         shardId: 0,
//         limit: undefined,
//       },
//     };
//   }

//   private buildCommitOffsetCmd(offset: FumeOffset): ControlCommand {
//     return {
//       commitOffset: {
//         offset,
//         shardId: 0,
//       },
//     };
//   }

//   private handleControlResponse(controlResponse: ControlResponse): void {
//     // Determine which oneof field is set
//     if (controlResponse.pollHist) {
//       const pollHist = controlResponse.pollHist;
//       LOGGER.debug(`Received poll history ${pollHist.events.length} events`);
//       this.stateMachine.queueBlockchainEvent(pollHist.events);
//     } else if (controlResponse.commitOffset) {
//       const commitOffset = controlResponse.commitOffset;
//       LOGGER.debug(`Received commit offset: ${JSON.stringify(commitOffset)}`);
//       this.stateMachine.updateCommittedOffset(commitOffset.offset);
//     } else if (controlResponse.pong) {
//       LOGGER.debug("Received pong");
//     } else {
//       throw new Error("Unexpected control response");
//     }
//   }

//   public get commitmentLevel(): CommitmentLevel | undefined {
//     return this.subscribeRequest.commitment;
//   }

//   public async pollHistoryIfNeeded(): Promise<void> {
//     // Poll the history if the state machine needs new events.
//     if (this.stateMachine.needNewBlockchainEvents()) {
//       const cmd = this.buildPollHistoryCmd(
//         this.stateMachine.committableOffset
//       );
//       // await this.controlPlaneObserver.next(cmd);
//     }
//   }

//   private scheduleDownloadTaskIfAny(): void {
//     while (true) {
//       LOGGER.debug("Checking for download tasks to schedule");

//       if (this.downloadTasks.size >= this.maxConcurrentDownload) {
//         break;
//       }

//       LOGGER.debug("Popping slot to download");
//       const downloadRequest = this.stateMachine.popSlotToDownload(
//         this.commitmentLevel
//       );
//       if (!downloadRequest) {
//         LOGGER.debug("No download request available");
//         break;
//       }

//       LOGGER.debug(`Download request for slot ${downloadRequest.slot} popped`);
//       if (!downloadRequest.blockchainId) {
//         throw new Error("Download request must have a blockchain ID");
//       }

//       const downloadTaskArgs: DownloadTaskArgs = {
//         downloadRequest,
//         subscribeRequest: this.subscribeRequest,
//       };

//       // In TS, calling async fn returns a Promise (like create_task)
//       const downloadTask = this.slotDownloader.runDownload(
//         this.subscribeRequest,
//         downloadTaskArgs
//       );

//       // Track the promise alongside the request
//       this.downloadTasks.set(downloadTask, downloadRequest);

//       LOGGER.debug(
//         `Scheduling download task for slot ${downloadRequest.slot}`
//       );
//     }
//   }

//   private handleDownloadResult(downloadResult: DownloadTaskResult): void {
//     /** Handles the result of a download task. */
//     if (downloadResult.kind === "Ok") {
//       const completed = downloadResult.completed!;
//       LOGGER.debug(
//         `Download completed for slot ${completed.slot}, shard ${completed.shardIdx}, ${completed.totalEventDownloaded} total events`
//       );

//       this.stateMachine.makeSlotDownloadProgress(
//         completed.slot,
//         completed.shardIdx
//       );
//     } else {
//       const slot = downloadResult.slot;
//       const err = downloadResult.err;
//       throw new Error(`Failed to download slot ${slot}: ${err!.message}`);
//     }
//   }

//   private async forceCommitOffset(): Promise<void> {
//     LOGGER.debug(
//       `Force committing offset ${this.stateMachine.committableOffset}`
//     );

//     // await this.controlPlaneObserver.next(
//     //   this.buildCommitOffsetCmd(this.stateMachine.committableOffset)
//     // );
//   }

//   private async commitOffset(): Promise<void> {
//     if (
//       this.stateMachine.lastCommittedOffset <
//       this.stateMachine.committableOffset
//     ) {
//       LOGGER.debug(
//         `Committing offset ${this.stateMachine.committableOffset}`
//       );
//       await this.forceCommitOffset();
//     }
//     this.lastCommit = Date.now() / 1000; // seconds since epoch; to match python syntax
//   }

//   private async drainSlotStatus(): Promise<void> {
//     const commitment = this.subscribeRequest.commitment;
//     const slotStatusVec: FumeSlotStatus[] = [];

//     let slotStatus: FumeSlotStatus | null;
//     while ((slotStatus = this.stateMachine.popNextSlotStatus())) {
//       slotStatusVec.push(slotStatus);
//     }

//     if (slotStatusVec.length === 0) {
//       return;
//     }

//     LOGGER.debug(`Draining ${slotStatusVec.length} slot status`);

//     for (const slotStatus of slotStatusVec) {
//       const matchedFilters: string[] = [];

//       for (const [filterName, filter] of Object.entries(
//         this.subscribeRequest.slots
//       )) {
//         if (
//           filter.filterByCommitment &&
//           slotStatus.commitmentLevel === commitment
//         ) {
//           matchedFilters.push(filterName);
//         } else if (!filter.filterByCommitment) {
//           matchedFilters.push(filterName);
//         }
//       }

//       if (matchedFilters.length > 0) {
//         const update: SubscribeUpdate = {
//           filters: matchedFilters,
//           createdAt: undefined,
//           slot: {
//             slot: slotStatus.slot,
//             parent: slotStatus.parentSlot,
//             status: slotStatus.commitmentLevel as number as SlotStatus,
//             deadError: slotStatus.deadError,
//           },
//         };

//         try {
//           this.dragonsmouthOutlet.next(update);
//         } catch (err) {
//           // TODO make proper error types
//           if (err === "Queue full") {
//             return;
//           }
//           throw err;
//         }
//       }

//       this.stateMachine.markEventAsProcessed(slotStatus.sessionSequence);
//     }
//   }

//   private async handleControlPlaneResp(
//     result: ControlResponse | Error
//   ): Promise<boolean> {
//     if (result instanceof Error) {
//       await this.dragonsmouthOutlet.next(result);
//       return false;
//     }

//     this.handleControlResponse(result);
//     return true;
//   }

//   public handleNewSubscribeRequest(subscribeRequest: SubscribeRequest) {
//     this.subscribeRequest = subscribeRequest;
//   }

//   public async run() {
//     LOGGER.debug("Fumarole runtime starting...");

//     const mainBus = new Subject<RuntimeEvent>();

//     // while (pending.size > 0) {
//     //   ticks += 1;
//     //   LOGGER.debug("Runtime loop tick");

//     //   if (ticks % this.gcInterval === 0) {
//     //     LOGGER.debug("Running garbage collection");
//     //     this.stateMachine.gc();
//     //     ticks = 0;
//     //   }

//     //   LOGGER.debug("Polling history if needed");
//     //   await this.pollHistoryIfNeeded();

//     //   LOGGER.debug("Scheduling download tasks if any");
//     //   this.scheduleDownloadTaskIfAny();
//     //   for (const [t] of this.downloadTasks.entries()) {
//     //     pending.add(t);
//     //     taskMap.set(t, "download_task");
//     //   }

//     //   const downloadTaskInflight = this.downloadTasks.size;
//     //   LOGGER.debug(
//     //     `Current download tasks in flight: ${downloadTaskInflight} / ${this.maxConcurrentDownload}`
//     //   );

//     //   // Wait for at least one task to finish
//     //   LOGGER.debug("UP UP");
//     //   // const { done, pending: newPending } = await Promise.race(pending);
//     //   const { done, pending: newPending } = await waitFirstCompleted(Array.from(pending));
//     //   LOGGER.debug("DOWN DOWN");
//     //   pending = new Set(newPending);

//     //   for (const t of done) {
//     //     const result = await t;
//     //     const name = taskMap.get(t)!;
//     //     taskMap.delete(t);

//     //     switch (name) {
//     //       case "dragonsmouth_bidi":
//     //         LOGGER.debug("Dragonsmouth subscribe request received");
//     //         this.handleNewSubscribeRequest(result);
//     //         const newTask1 = this.subscribeRequestUpdateQueue.get();
//     //         taskMap.set(newTask1, "dragonsmouth_bidi");
//     //         pending.add(newTask1);
//     //         break;

//     //       case "control_plane_rx":
//     //         LOGGER.debug("Control plane response received");
//     //         if (!(await this.handleControlPlaneResp(result))) {
//     //           LOGGER.debug("Control plane error");
//     //           return;
//     //         }
//     //         const newTask2 = this.controlPlaneReceiveQueue.get();
//     //         taskMap.set(newTask2, "control_plane_rx");
//     //         pending.add(newTask2);
//     //         break;

//     //       case "download_task":
//     //         LOGGER.debug("Download task result received");
//     //         this.downloadTasks.delete(t);
//     //         this.handleDownloadResult(result);
//     //         break;

//     //       case "commit_tick":
//     //         LOGGER.debug("Commit tick reached");
//     //         await this.commitOffset();
//     //         const newTask3 = new Interval(this.commitInterval).tick();
//     //         taskMap.set(newTask3, "commit_tick");
//     //         pending.add(newTask3);
//     //         break;

//     //       default:
//     //         throw new Error(`Unexpected task name: ${name}`);
//     //     }
//     //   }

//     //   await this.drainSlotStatus();
//     // }

//     LOGGER.debug("Fumarole runtime exiting");
//   }
// }


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
   * Observer for control plane commands.
   */
  controlPlaneObserver: Observer<ControlCommand>,

  /**
   * Observer for download task results.
   */
  downloadTaskObserver: Observer<DownloadTaskArgs>

  /**
   * Observer for dragonsmouth subscribe updates.
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
  } else {
    const slot = result.slot;
    const err = result.err;
    throw new Error(`Failed to download slot ${slot}: ${err!.message}`);
  }
}

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


function onSubscribeRequestUpdate(this: FumaroleRuntimeCtx, update: SubscribeRequestUpdate) {
  LOGGER.debug("New subscribe request update");
  this.subscribeRequest = update.new_subscribe_request;
}

function ctxCommitmentLevel(ctx: FumaroleRuntimeCtx): CommitmentLevel {
  return ctx.subscribeRequest.commitment ?? CommitmentLevel.PROCESSED
}

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
    this.inflightDownloads.set(downloadRequest.slot, downloadTaskArgs);

    this.downloadTaskObserver.next(downloadTaskArgs);

    LOGGER.debug(
      `Scheduling download task for slot ${downloadRequest.slot}`
    );
  }
}

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
        limit: undefined,
      },
    };
    LOGGER.debug("Polling history");
    this.controlPlaneObserver.next(cmd);
    this.pollHistInflightFlag = true;
  } else {
    LOGGER.debug("no need to poll history");
  }
}

function drainSlotStatusIfAny(
  this: FumaroleRuntimeCtx
) {
  const commitment = ctxCommitmentLevel(this);
  const slotStatusVec: FumeSlotStatus[] = [];

  let slotStatus: FumeSlotStatus | null;
  while ((slotStatus = this.sm.popNextSlotStatus())) {
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
  scheduleDownloadTaskIfAny.call(this);
  pollHistoryIfNeeded.call(this);
  drainSlotStatusIfAny.call(this);

  if (this.currentTick % this.gcInterval === 0) {
    this.sm.gc();
  }
} 

/**
 * Creates an observable for the fumarole runtime.
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