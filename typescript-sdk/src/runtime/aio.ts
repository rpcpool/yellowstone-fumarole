import { ChannelCredentials, ServiceError, status } from "@grpc/grpc-js";
import { Queue as AsyncQueue } from "./queue";
import { Interval } from "../utils/aio";
import {
  FumaroleSM,
  FumeDownloadRequest,
  FumeOffset,
  FumeShardIdx,
} from "./state-machine";
import {
  SubscribeRequest,
  SubscribeUpdate,
  SubscribeUpdateSlot,
  CommitmentLevel as ProtoCommitmentLevel,
} from "../grpc/geyser";
import {
  ControlCommand,
  PollBlockchainHistory,
  CommitOffset,
  ControlResponse,
  DownloadBlockShard,
  BlockFilters,
  FumaroleClient,
} from "../grpc/fumarole";

// Constants
export const DEFAULT_GC_INTERVAL = 5;
export const DEFAULT_SLOT_MEMORY_RETENTION = 10000;

// Types and Interfaces
export interface CompletedDownloadBlockTask {
  slot: number;
  blockUid: Uint8Array;
  shardIdx: FumeShardIdx;
  totalEventDownloaded: number;
}

export interface DownloadBlockError {
  kind:
    | "Disconnected"
    | "OutletDisconnected"
    | "BlockShardNotFound"
    | "FailedDownload"
    | "Fatal";
  message: string;
}

export interface DownloadTaskResult {
  kind: "Ok" | "Err";
  completed?: CompletedDownloadBlockTask;
  slot?: number;
  err?: DownloadBlockError;
}

export interface AsyncSlotDownloader {
  runDownload(
    subscribeRequest: SubscribeRequest,
    spec: DownloadTaskArgs
  ): Promise<DownloadTaskResult>;
}

const LOGGER = console;

export class AsyncioFumeDragonsmouthRuntime {
  private readonly sm: FumaroleSM;
  private readonly slotDownloader: AsyncSlotDownloader;
  private subscribeRequestUpdateQ: AsyncQueue<SubscribeRequest>;
  private subscribeRequest: SubscribeRequest;
  private readonly consumerGroupName: string;
  private readonly controlPlaneTx: AsyncQueue<ControlCommand>;
  private readonly controlPlaneRx: AsyncQueue<ControlResponse>;
  private readonly dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>;
  private readonly commitInterval: number;
  private readonly gcInterval: number;
  private readonly maxConcurrentDownload: number;
  private readonly downloadTasks: Map<
    Promise<DownloadTaskResult>,
    FumeDownloadRequest
  >;
  private lastCommit: number;

  constructor(
    sm: FumaroleSM,
    slotDownloader: AsyncSlotDownloader,
    subscribeRequestUpdateQ: AsyncQueue<SubscribeRequest>,
    subscribeRequest: SubscribeRequest,
    consumerGroupName: string,
    controlPlaneTxQ: AsyncQueue<ControlCommand>,
    controlPlaneRxQ: AsyncQueue<ControlResponse>,
    dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>,
    commitInterval: number,
    gcInterval: number,
    maxConcurrentDownload: number = 10
  ) {
    this.sm = sm;
    this.slotDownloader = slotDownloader;
    this.subscribeRequestUpdateQ = subscribeRequestUpdateQ;
    this.subscribeRequest = subscribeRequest;
    this.consumerGroupName = consumerGroupName;
    this.controlPlaneTx = controlPlaneTxQ;
    this.controlPlaneRx = controlPlaneRxQ;
    this.dragonsmouthOutlet = dragonsmouthOutlet;
    this.commitInterval = commitInterval;
    this.gcInterval = gcInterval;
    this.maxConcurrentDownload = maxConcurrentDownload;
    this.downloadTasks = new Map();
    this.lastCommit = Date.now();
  }

  private buildPollHistoryCmd(fromOffset?: FumeOffset): ControlCommand {
    return { pollHist: { shardId: 0 } } as ControlCommand;
  }

  private buildCommitOffsetCmd(offset: FumeOffset): ControlCommand {
    return { commitOffset: { offset, shardId: 0 } } as ControlCommand;
  }

  private handleControlResponse(controlResponse: ControlResponse): void {
    // Get first defined property from controlResponse
    const responseField = Object.keys(controlResponse).find(
      (key) => controlResponse[key] !== undefined && key !== "response"
    );

    if (!responseField) {
      throw new Error("Control response is empty");
    }

    switch (responseField) {
      case "pollHist": {
        const pollHist = controlResponse.pollHist!;
        LOGGER.debug(`Received poll history ${pollHist.events?.length} events`);
        this.sm.queueBlockchainEvent(pollHist.events);
        break;
      }
      case "commitOffset": {
        const commitOffset = controlResponse.commitOffset!;
        LOGGER.debug(`Received commit offset: ${commitOffset}`);
        this.sm.updateCommittedOffset(commitOffset.offset);
        break;
      }
      case "pong":
        LOGGER.debug("Received pong");
        break;
      default:
        throw new Error("Unexpected control response");
    }
  }

  private async pollHistoryIfNeeded(): Promise<void> {
    if (this.sm.needNewBlockchainEvents()) {
      const cmd = this.buildPollHistoryCmd(this.sm.committableOffset);
      await this.controlPlaneTx.put(cmd);
    }
  }

  private commitmentLevel(): number {
    return this.subscribeRequest.commitment || 0;
  }

  private scheduleDownloadTaskIfAny(): void {
    while (true) {
      LOGGER.debug("Checking for download tasks to schedule");
      if (this.downloadTasks.size >= this.maxConcurrentDownload) {
        break;
      }

      LOGGER.debug("Popping slot to download");
      const downloadRequest = this.sm.popSlotToDownload(this.commitmentLevel());
      if (!downloadRequest) {
        LOGGER.debug("No download request available");
        break;
      }

      LOGGER.debug(`Download request for slot ${downloadRequest.slot} popped`);
      if (!downloadRequest.blockchainId) {
        throw new Error("Download request must have a blockchain ID");
      }

      const downloadTaskArgs: DownloadTaskArgs = {
        downloadRequest,
        dragonsmouthOutlet: this.dragonsmouthOutlet,
      };

      const downloadPromise = this.slotDownloader.runDownload(
        this.subscribeRequest,
        downloadTaskArgs
      );
      this.downloadTasks.set(downloadPromise, downloadRequest);
      LOGGER.debug(`Scheduling download task for slot ${downloadRequest.slot}`);
    }
  }

  private handleDownloadResult(downloadResult: DownloadTaskResult): void {
    if (downloadResult.kind === "Ok") {
      const completed = downloadResult.completed!;
      LOGGER.debug(
        `Download completed for slot ${completed.slot}, shard ${completed.shardIdx}, ${completed.totalEventDownloaded} total events`
      );
      this.sm.makeSlotDownloadProgress(completed.slot, completed.shardIdx);
    } else {
      const slot = downloadResult.slot!;
      const err = downloadResult.err!;
      throw new Error(`Failed to download slot ${slot}: ${err.message}`);
    }
  }

  private async forceCommitOffset(): Promise<void> {
    LOGGER.debug(`Force committing offset ${this.sm.committableOffset}`);
    await this.controlPlaneTx.put(
      this.buildCommitOffsetCmd(this.sm.committableOffset)
    );
  }

  private async commitOffset(): Promise<void> {
    if (this.sm.lastCommittedOffset < this.sm.committableOffset) {
      LOGGER.debug(`Committing offset ${this.sm.committableOffset}`);
      await this.forceCommitOffset();
    }
    this.lastCommit = Date.now();
  }

  private async drainSlotStatus(): Promise<void> {
    const commitment = this.subscribeRequest.commitment || 0;
    const slotStatusVec: any[] = [];

    while (true) {
      const slotStatus = this.sm.popNextSlotStatus();
      if (!slotStatus) break;
      slotStatusVec.push(slotStatus);
    }

    if (!slotStatusVec.length) return;

    LOGGER.debug(`Draining ${slotStatusVec.length} slot status`);

    for (const slotStatus of slotStatusVec) {
      const matchedFilters: string[] = [];
      for (const [filterName, filter] of Object.entries(
        this.subscribeRequest.slots || {}
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

      if (matchedFilters.length) {
        const update: SubscribeUpdate = {
          filters: matchedFilters,
          createdAt: undefined,
          slot: {
            slot: slotStatus.slot,
            parent: slotStatus.parentSlot,
            status: slotStatus.commitmentLevel,
            deadError: slotStatus.deadError,
          } as SubscribeUpdateSlot,
        };

        try {
          await this.dragonsmouthOutlet.put(update);
        } catch (error) {
          if (error.message === "Queue full") return;
          throw error;
        }
      }

      this.sm.markEventAsProcessed(slotStatus.sessionSequence);
    }
  }

  private async handleControlPlaneResp(
    result: ControlResponse | Error
  ): Promise<boolean> {
    if (result instanceof Error) {
      // Create a slot update with the error information
      const errorUpdate: SubscribeUpdate = {
        filters: [],
        createdAt: undefined,
        slot: {
          slot: "0",
          parent: "0",
          status: 0, // Using 0 as default status for error case
          deadError: result.message,
        },
      };
      await this.dragonsmouthOutlet.put(errorUpdate);
      LOGGER.error(`Control plane error: ${result.message}`);
      return false;
    }
    this.handleControlResponse(result);
    return true;
  }

  public handleNewSubscribeRequest(subscribeRequest: SubscribeRequest): void {
    this.subscribeRequest = subscribeRequest;
  }

  public async run(): Promise<void> {
    LOGGER.debug("Fumarole runtime starting...");
    await this.controlPlaneTx.put(this.buildPollHistoryCmd());
    LOGGER.debug("Initial poll history command sent");
    await this.forceCommitOffset();
    LOGGER.debug("Initial commit offset command sent");
    let ticks = 0;

    const taskMap = new Map<Promise<any>, string>();

    // Initial tasks
    taskMap.set(this.subscribeRequestUpdateQ.get(), "dragonsmouth_bidi");
    taskMap.set(this.controlPlaneRx.get(), "control_plane_rx");
    taskMap.set(new Interval(this.commitInterval).tick(), "commit_tick");

    while (taskMap.size > 0) {
      ticks++;
      LOGGER.debug("Runtime loop tick");

      if (ticks % this.gcInterval === 0) {
        LOGGER.debug("Running garbage collection");
        this.sm.gc();
        ticks = 0;
      }

      LOGGER.debug("Polling history if needed");
      await this.pollHistoryIfNeeded();

      LOGGER.debug("Scheduling download tasks if any");
      this.scheduleDownloadTaskIfAny();

      for (const [task] of this.downloadTasks) {
        taskMap.set(task, "download_task");
      }

      const downloadTaskInFlight = this.downloadTasks.size;
      LOGGER.debug(
        `Current download tasks in flight: ${downloadTaskInFlight} / ${this.maxConcurrentDownload}`
      );

      const promises = Array.from(taskMap.keys());
      const done = await Promise.race(
        promises.map((p) => p.then((result) => ({ promise: p, result })))
      );

      const taskName = taskMap.get(done.promise);
      taskMap.delete(done.promise);

      switch (taskName) {
        case "dragonsmouth_bidi": {
          LOGGER.debug("Dragonsmouth subscribe request received");
          const result = done.result as SubscribeRequest;
          this.handleNewSubscribeRequest(result);
          const newTask = this.subscribeRequestUpdateQ.get();
          taskMap.set(newTask, "dragonsmouth_bidi");
          break;
        }
        case "control_plane_rx": {
          LOGGER.debug("Control plane response received");
          if (!(await this.handleControlPlaneResp(done.result))) {
            LOGGER.debug("Control plane error");
            return;
          }
          const newTask = this.controlPlaneRx.get();
          taskMap.set(newTask, "control_plane_rx");
          break;
        }
        case "download_task": {
          LOGGER.debug("Download task result received");
          this.downloadTasks.delete(done.promise);
          this.handleDownloadResult(done.result);
          break;
        }
        case "commit_tick": {
          LOGGER.debug("Commit tick reached");
          await this.commitOffset();
          const newTask = new Interval(this.commitInterval).tick();
          taskMap.set(newTask, "commit_tick");
          break;
        }
        default:
          throw new Error(`Unexpected task name: ${taskName}`);
      }

      await this.drainSlotStatus();
    }

    LOGGER.debug("Fumarole runtime exiting");
  }
}

export interface DownloadTaskRunnerChannels {
  downloadTaskQueueTx: AsyncQueue<any>;
  cncTx: AsyncQueue<any>;
  downloadResultRx: AsyncQueue<any>;
}

export interface DownloadTaskRunnerCommand {
  kind: string;
  subscribeRequest?: SubscribeRequest;
}

export interface DownloadTaskArgs {
  downloadRequest: FumeDownloadRequest;
  dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>;
}

export class GrpcSlotDownloader implements AsyncSlotDownloader {
  private client: FumaroleClient;

  constructor(client: FumaroleClient) {
    this.client = client;
  }

  public async runDownload(
    subscribeRequest: SubscribeRequest,
    spec: DownloadTaskArgs
  ): Promise<DownloadTaskResult> {
    const downloadTask = new GrpcDownloadBlockTaskRun(
      spec.downloadRequest,
      this.client,
      {
        accounts: subscribeRequest.accounts,
        transactions: subscribeRequest.transactions,
        entries: subscribeRequest.entry,
        blocksMeta: subscribeRequest.blocksMeta,
      } as BlockFilters,
      spec.dragonsmouthOutlet
    );

    LOGGER.debug(`Running download task for slot ${spec.downloadRequest.slot}`);
    return await downloadTask.run();
  }
}

export class GrpcDownloadBlockTaskRun {
  private downloadRequest: FumeDownloadRequest;
  private client: FumaroleClient;
  private filters: BlockFilters;
  private dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>;

  constructor(
    downloadRequest: FumeDownloadRequest,
    client: FumaroleClient,
    filters: BlockFilters,
    dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>
  ) {
    this.downloadRequest = downloadRequest;
    this.client = client;
    this.filters = filters;
    this.dragonsmouthOutlet = dragonsmouthOutlet;
  }

  private mapTonicErrorCodeToDownloadBlockError(
    error: ServiceError
  ): DownloadBlockError {
    switch (error.code) {
      case status.NOT_FOUND:
        return {
          kind: "BlockShardNotFound",
          message: "Block shard not found",
        };
      case status.UNAVAILABLE:
        return {
          kind: "Disconnected",
          message: "Disconnected",
        };
      case status.INTERNAL:
      case status.ABORTED:
      case status.DATA_LOSS:
      case status.RESOURCE_EXHAUSTED:
      case status.UNKNOWN:
      case status.CANCELLED:
      case status.DEADLINE_EXCEEDED:
        return {
          kind: "FailedDownload",
          message: "Failed download",
        };
      case status.INVALID_ARGUMENT:
        throw new Error("Invalid argument");
      default:
        return {
          kind: "Fatal",
          message: `Unknown error: ${error.code}`,
        };
    }
  }

  public async run(): Promise<DownloadTaskResult> {
    const request = {
      blockchainId: this.downloadRequest.blockchainId,
      blockUid: this.downloadRequest.blockUid,
      shardIdx: 0,
      blockFilters: this.filters,
    } as DownloadBlockShard;

    try {
      LOGGER.debug(
        `Requesting download for block ${Buffer.from(
          this.downloadRequest.blockUid
        ).toString("hex")} at slot ${this.downloadRequest.slot}`
      );

      let totalEventDownloaded = 0;
      const stream = this.client.downloadBlock(request);

      return new Promise((resolve, reject) => {
        stream.on("data", async (data: any) => {
          const kind = Object.keys(data).find(
            (k) => data[k] !== undefined && k !== "response"
          );
          if (!kind) return;

          switch (kind) {
            case "update": {
              const update = data.update;
              if (!update) throw new Error("Update is null");
              totalEventDownloaded++;
              try {
                await this.dragonsmouthOutlet.put(update);
              } catch (error) {
                if (error.message === "Queue shutdown") {
                  LOGGER.error("Dragonsmouth outlet is disconnected");
                  resolve({
                    kind: "Err",
                    slot: this.downloadRequest.slot,
                    err: {
                      kind: "OutletDisconnected",
                      message: "Outlet disconnected",
                    },
                  });
                }
              }
              break;
            }
            case "blockShardDownloadFinish":
              LOGGER.debug(
                `Download finished for block ${Buffer.from(
                  this.downloadRequest.blockUid
                ).toString("hex")} at slot ${this.downloadRequest.slot}`
              );
              resolve({
                kind: "Ok",
                completed: {
                  slot: this.downloadRequest.slot,
                  blockUid: this.downloadRequest.blockUid,
                  shardIdx: 0,
                  totalEventDownloaded,
                },
              });
              break;
            default:
              reject(new Error(`Unexpected response kind: ${kind}`));
          }
        });

        stream.on("error", (error: ServiceError) => {
          LOGGER.error(`Download block error: ${error}`);
          resolve({
            kind: "Err",
            slot: this.downloadRequest.slot,
            err: this.mapTonicErrorCodeToDownloadBlockError(error),
          });
        });

        stream.on("end", () => {
          resolve({
            kind: "Err",
            slot: this.downloadRequest.slot,
            err: {
              kind: "FailedDownload",
              message: "Failed download",
            },
          });
        });
      });
    } catch (error) {
      LOGGER.error(`Download block error: ${error}`);
      return {
        kind: "Err",
        slot: this.downloadRequest.slot,
        err: this.mapTonicErrorCodeToDownloadBlockError(error as ServiceError),
      };
    }
  }
}
