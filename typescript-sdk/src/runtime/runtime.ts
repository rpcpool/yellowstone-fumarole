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
import { Interval } from "../utils/interval";
import { AsyncQueue } from "./async-queue";
import {
  AsyncSlotDownloader,
  DownloadTaskArgs,
  DownloadTaskResult,
} from "./grpc-slot-downloader";
import {
  FumaroleSM,
  FumeDownloadRequest,
  FumeOffset,
  FumeSlotStatus,
} from "./state-machine";
import { waitFirstCompleted } from "./wait-first-completed";

type TaskName =
  | "dragonsmouth_bidi"
  | "control_plane_rx"
  | "download_task"
  | "commit_tick";

export class FumeDragonsmouthRuntime {
  public stateMachine: FumaroleSM;
  public slotDownloader: AsyncSlotDownloader;
  public subscribeRequestUpdateQueue: AsyncQueue<{}>;
  public subscribeRequest: SubscribeRequest;
  public consumerGroupName: string;
  public controlPlaneTransmitQueue: AsyncQueue<{}>;
  public controlPlaneReceiveQueue: AsyncQueue<{}>;
  public dragonsmouthOutlet: AsyncQueue<SubscribeUpdate | Error>;
  public commitInterval: number; // in seconds
  public gcInterval: number;
  public maxConcurrentDownload: number;
  public downloadTasks: Map<Promise<DownloadTaskResult>, FumeDownloadRequest>;
  public innerRuntimeChannel: AsyncQueue<{}>;
  public lastCommit: number;

  constructor(
    stateMachine: FumaroleSM,
    slotDownloader: AsyncSlotDownloader,
    subscribeRequestUpdateQueue: AsyncQueue<{}>,
    subscribeRequest: SubscribeRequest,
    consumerGroupName: string,
    controlPlaneTransmitQueue: AsyncQueue<{}>,
    controlPlaneReceiveQueue: AsyncQueue<{}>,
    dragonsmouthOutlet: AsyncQueue<SubscribeUpdate | Error>,
    commitInterval: number,
    gcInterval: number,
    maxConcurrentDownload: number = 10
  ) {
    this.stateMachine = stateMachine;
    this.slotDownloader = slotDownloader;
    this.subscribeRequestUpdateQueue = subscribeRequestUpdateQueue;
    this.subscribeRequest = subscribeRequest;
    this.consumerGroupName = consumerGroupName;
    this.controlPlaneTransmitQueue = controlPlaneTransmitQueue;
    this.controlPlaneReceiveQueue = controlPlaneReceiveQueue;
    this.dragonsmouthOutlet = dragonsmouthOutlet;
    this.commitInterval = commitInterval;
    this.gcInterval = gcInterval;
    this.maxConcurrentDownload = maxConcurrentDownload;
    this.downloadTasks = new Map();
    this.innerRuntimeChannel = new AsyncQueue<{}>();
    this.lastCommit = Date.now() / 1000; // seconds since epoch; to match python syntax
  }

  private buildPollHistoryCmd(fromOffset?: FumeOffset): ControlCommand {
    // Build a command to poll the blockchain history
    return {
      pollHist: {
        shardId: 0,
        limit: undefined,
      },
    };
  }

  private buildCommitOffsetCmd(offset: FumeOffset): ControlCommand {
    return {
      commitOffset: {
        offset,
        shardId: 0,
      },
    };
  }

  private handleControlResponse(controlResponse: ControlResponse): void {
    // Determine which oneof field is set
    if (controlResponse.pollHist) {
      const pollHist = controlResponse.pollHist;
      console.log(`Received poll history ${pollHist.events.length} events`);
      this.stateMachine.queueBlockchainEvent(pollHist.events);
    } else if (controlResponse.commitOffset) {
      const commitOffset = controlResponse.commitOffset;
      console.log(`Received commit offset: ${JSON.stringify(commitOffset)}`);
      this.stateMachine.updateCommittedOffset(commitOffset.offset);
    } else if (controlResponse.pong) {
      console.log("Received pong");
    } else {
      throw new Error("Unexpected control response");
    }
  }

  public get commitmentLevel(): CommitmentLevel | undefined {
    return this.subscribeRequest.commitment;
  }

  public async pollHistoryIfNeeded(): Promise<void> {
    // Poll the history if the state machine needs new events.
    if (this.stateMachine.needNewBlockchainEvents()) {
      const cmd = this.buildPollHistoryCmd(
        this.stateMachine.committableOffset
      );
      await this.controlPlaneTransmitQueue.put(cmd);
    }
  }

  private scheduleDownloadTaskIfAny(): void {
    while (true) {
      console.log("Checking for download tasks to schedule");

      if (this.downloadTasks.size >= this.maxConcurrentDownload) {
        break;
      }

      console.log("Popping slot to download");
      const downloadRequest = this.stateMachine.popSlotToDownload(
        this.commitmentLevel
      );
      if (!downloadRequest) {
        console.log("No download request available");
        break;
      }

      console.log(`Download request for slot ${downloadRequest.slot} popped`);
      if (!downloadRequest.blockchainId) {
        throw new Error("Download request must have a blockchain ID");
      }

      const downloadTaskArgs: DownloadTaskArgs = {
        downloadRequest,
        dragonsmouthOutlet: this.dragonsmouthOutlet,
      };

      // In TS, calling async fn returns a Promise (like create_task)
      const downloadTask = this.slotDownloader.runDownload(
        this.subscribeRequest,
        downloadTaskArgs
      );

      // Track the promise alongside the request
      this.downloadTasks.set(downloadTask, downloadRequest);

      console.log(
        `Scheduling download task for slot ${downloadRequest.slot}`
      );
    }
  }

  private handleDownloadResult(downloadResult: DownloadTaskResult): void {
    /** Handles the result of a download task. */
    if (downloadResult.kind === "Ok") {
      const completed = downloadResult.completed!;
      console.log(
        `Download completed for slot ${completed.slot}, shard ${completed.shardIdx}, ${completed.totalEventDownloaded} total events`
      );

      this.stateMachine.makeSlotDownloadProgress(
        completed.slot,
        completed.shardIdx
      );
    } else {
      const slot = downloadResult.slot;
      const err = downloadResult.err;
      throw new Error(`Failed to download slot ${slot}: ${err!.message}`);
    }
  }

  private async forceCommitOffset(): Promise<void> {
    console.log(
      `Force committing offset ${this.stateMachine.committableOffset}`
    );

    await this.controlPlaneTransmitQueue.put(
      this.buildCommitOffsetCmd(this.stateMachine.committableOffset)
    );
  }

  private async commitOffset(): Promise<void> {
    if (
      this.stateMachine.lastCommittedOffset <
      this.stateMachine.committableOffset
    ) {
      console.log(
        `Committing offset ${this.stateMachine.committableOffset}`
      );
      await this.forceCommitOffset();
    }
    this.lastCommit = Date.now() / 1000; // seconds since epoch; to match python syntax
  }

  private async drainSlotStatus(): Promise<void> {
    const commitment = this.subscribeRequest.commitment;
    const slotStatusVec: FumeSlotStatus[] = [];

    let slotStatus: FumeSlotStatus | null;
    while ((slotStatus = this.stateMachine.popNextSlotStatus())) {
      slotStatusVec.push(slotStatus);
    }

    if (slotStatusVec.length === 0) {
      return;
    }

    console.log(`Draining ${slotStatusVec.length} slot status`);

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

        try {
          await this.dragonsmouthOutlet.put(update);
        } catch (err) {
          // TODO make proper error types
          if (err === "Queue full") {
            return;
          }
          throw err;
        }
      }

      this.stateMachine.markEventAsProcessed(slotStatus.sessionSequence);
    }
  }

  private async handleControlPlaneResp(
    result: ControlResponse | Error
  ): Promise<boolean> {
    if (result instanceof Error) {
      await this.dragonsmouthOutlet.put(result);
      return false;
    }

    this.handleControlResponse(result);
    return true;
  }

  public handleNewSubscribeRequest(subscribeRequest: SubscribeRequest) {
    this.subscribeRequest = subscribeRequest;
  }

  public async run() {
    console.log("Fumarole runtime starting...");

    await this.controlPlaneTransmitQueue.put(
      this.buildPollHistoryCmd(undefined)
    );
    console.log("Initial poll history command sent");

    await this.forceCommitOffset();
    console.log("Initial commit offset command sent");

    let ticks = 0;
    let pending = new Set<Promise<any>>();
    let taskMap = new Map<Promise<any>, TaskName>();

    // Initial tasks
    const task1 = this.subscribeRequestUpdateQueue.get();
    taskMap.set(task1, "dragonsmouth_bidi");
    pending.add(task1);

    const task2 = this.controlPlaneReceiveQueue.get();
    taskMap.set(task2, "control_plane_rx");
    pending.add(task2);

    const task3 = new Interval(this.commitInterval).tick();
    taskMap.set(task3, "commit_tick");
    pending.add(task3);

    while (pending.size > 0) {
      ticks += 1;
      console.log("Runtime loop tick");

      if (ticks % this.gcInterval === 0) {
        console.log("Running garbage collection");
        this.stateMachine.gc();
        ticks = 0;
      }

      console.log("Polling history if needed");
      await this.pollHistoryIfNeeded();

      console.log("Scheduling download tasks if any");
      this.scheduleDownloadTaskIfAny();
      for (const [t] of this.downloadTasks.entries()) {
        pending.add(t);
        taskMap.set(t, "download_task");
      }

      const downloadTaskInflight = this.downloadTasks.size;
      console.log(
        `Current download tasks in flight: ${downloadTaskInflight} / ${this.maxConcurrentDownload}`
      );

      // Wait for at least one task to finish
      console.log("UP UP");
      
      // const { done, pending: newPending } = await Promise.race(pending);
      const { done, pending: newPending } = await waitFirstCompleted(Array.from(pending));
      console.log("DOWN DOWN");
      pending = new Set(newPending);

      for (const t of done) {
        const result = await t;
        const name = taskMap.get(t)!;
        taskMap.delete(t);

        switch (name) {
          case "dragonsmouth_bidi":
            console.log("Dragonsmouth subscribe request received");
            this.handleNewSubscribeRequest(result);
            const newTask1 = this.subscribeRequestUpdateQueue.get();
            taskMap.set(newTask1, "dragonsmouth_bidi");
            pending.add(newTask1);
            break;

          case "control_plane_rx":
            console.log("Control plane response received");
            if (!(await this.handleControlPlaneResp(result))) {
              console.log("Control plane error");
              return;
            }
            const newTask2 = this.controlPlaneReceiveQueue.get();
            taskMap.set(newTask2, "control_plane_rx");
            pending.add(newTask2);
            break;

          case "download_task":
            console.log("Download task result received");
            this.downloadTasks.delete(t);
            this.handleDownloadResult(result);
            break;

          case "commit_tick":
            console.log("Commit tick reached");
            await this.commitOffset();
            const newTask3 = new Interval(this.commitInterval).tick();
            taskMap.set(newTask3, "commit_tick");
            pending.add(newTask3);
            break;

          default:
            throw new Error(`Unexpected task name: ${name}`);
        }
      }

      await this.drainSlotStatus();
    }

    console.log("Fumarole runtime exiting");
  }
}
