import { ClientReadableStream, ServiceError, status } from "@grpc/grpc-js";
import {
  BlockFilters,
  DataResponse,
  DownloadBlockShard,
  FumaroleClient,
} from "../grpc/fumarole";
import { SubscribeRequest, SubscribeUpdate } from "../grpc/geyser";
import { AsyncQueue } from "./async-queue";
import { FumeDownloadRequest, FumeShardIdx } from "./state-machine";

// Constants
const DEFAULT_GC_INTERVAL = 5;

const DEFAULT_SLOT_MEMORY_RETENTION = 10000;

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

export class DownloadBlockError {
  constructor(public kind: DownloadBlockErrorKind, public message: string) {}
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

export class DownloadTaskArgs {
  constructor(
    public downloadRequest: FumeDownloadRequest,
    // TODO: figure out a type for this
    public dragonsmouthOutlet: AsyncQueue<SubscribeUpdate| Error>
  ) {}
}

export abstract class AsyncSlotDownloader {
  abstract runDownload(
    subscribeRequest: SubscribeRequest,
    spec: DownloadTaskArgs
  ): Promise<DownloadTaskResult>;
}

export class GrpcDownloadBlockTaskRun {
  public downloadRequest: FumeDownloadRequest;
  public client: FumaroleClient;
  public filters?: BlockFilters | null;
  public dragonsmouthOutlet: AsyncQueue<SubscribeUpdate | Error>;

  constructor(
    downloadRequest: FumeDownloadRequest,
    client: FumaroleClient,
    filters: BlockFilters | null,
    dragonsmouthOutlet: AsyncQueue<SubscribeUpdate | Error>
  ) {
    this.downloadRequest = downloadRequest;
    this.client = client;
    this.filters = filters;
    this.dragonsmouthOutlet = dragonsmouthOutlet;
  }

  public mapTonicErrorCodeToDownloadBlockError(
    e: ServiceError
  ): DownloadBlockError {
    const code = e.code;

    if (code === status.NOT_FOUND) {
      return new DownloadBlockError(
        "BlockShardNotFound",
        "Block shard not found"
      );
    } else if (code === status.UNAVAILABLE) {
      return new DownloadBlockError("Disconnected", "Disconnected");
    } else if (
      code === status.INTERNAL ||
      code === status.ABORTED ||
      code === status.DATA_LOSS ||
      code === status.RESOURCE_EXHAUSTED ||
      code === status.UNKNOWN ||
      code === status.CANCELLED ||
      code === status.DEADLINE_EXCEEDED
    ) {
      return new DownloadBlockError("FailedDownload", "Failed download");
    } else if (code === status.INVALID_ARGUMENT) {
      throw new Error("Invalid argument");
    } else {
      return new DownloadBlockError("Fatal", `Unknown error: ${code}`);
    }
  }

  public async run(): Promise<DownloadTaskResult> {
    const downloadRequest: DownloadBlockShard = {
      blockchainId: this.downloadRequest.blockchainId,
      blockUid: this.downloadRequest.blockUid,
      shardIdx: 0,
      blockFilters: this.filters === null ? undefined : this.filters,
    };

    let downloadResponse: ClientReadableStream<DataResponse>;

    try {
      console.log(
        `Requesting download for block ${this.downloadRequest.blockUid.toString()} at slot ${
          this.downloadRequest.slot
        }`
      );

      downloadResponse = this.client.downloadBlock(downloadRequest);
    } catch (e: any) {
      console.log(`Download block error ${e}`);
      return {
        kind: "Err",
        slot: this.downloadRequest.slot,
        err: this.mapTonicErrorCodeToDownloadBlockError(e),
      };
    }

    let totalEventDownloaded = 0;

    // Wrap the event-driven API in a Promise
    return new Promise<DownloadTaskResult>((resolve, reject) => {
      downloadResponse.on("data", async (data: DataResponse) => {
        try {
          if (data.update) {
            // === case: update ===
            const update = data.update;
            totalEventDownloaded += 1;

            try {
              await this.dragonsmouthOutlet.put(update);
            } catch (err: any) {
              // TODO: figure out a type for this
              console.error("Dragonsmouth outlet is disconnected");
              resolve({
                kind: "Err",
                slot: this.downloadRequest.slot,
                err: {
                  kind: "OutletDisconnected",
                  message: "Outlet disconnected",
                },
              });
            }
          } else if (data.blockShardDownloadFinish) {
            // === case: block_shard_download_finish ===
            console.debug(
              `Download finished for block ${this.downloadRequest.blockUid.toString()} at slot ${
                this.downloadRequest.slot
              }`
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
          } else {
            // === unexpected ===
            reject(
              new Error(`Unexpected DataResponse: ${JSON.stringify(data)}`)
            );
          }
        } catch (err) {
          reject(err);
        }
      });

      // TODO: figure out a type for this
      downloadResponse.on("error", (err: any) => {
        console.error("stream error", err);
        reject(this.mapTonicErrorCodeToDownloadBlockError(err));
      });

      downloadResponse.on("end", () => {
        console.log("stream ended without blockShardDownloadFinish");
        resolve({
          kind: "Err",
          slot: this.downloadRequest.slot,
          err: {
            kind: "FailedDownload",
            message: "Stream ended unexpectedly",
          },
        });
      });
    });
  }
}

export class GrpcSlotDownloader extends AsyncSlotDownloader {
  public fumaroleClient: FumaroleClient;

  constructor(fumaroleClient: FumaroleClient) {
    super();
    this.fumaroleClient = fumaroleClient;
  }

  public async runDownload(
    subscribeRequest: SubscribeRequest,
    spec: DownloadTaskArgs
  ): Promise<DownloadTaskResult> {
    const downloadTask = new GrpcDownloadBlockTaskRun(
      spec.downloadRequest,
      this.fumaroleClient,
      {
        accounts: subscribeRequest.accounts,
        transactions: subscribeRequest.transactions,
        entries: subscribeRequest.entry,
        blocksMeta: subscribeRequest.blocksMeta,
      },
      spec.dragonsmouthOutlet
    );

    console.log(`Running download task for slot ${spec.downloadRequest.slot}`);

    return await downloadTask.run();
  }
}

export class DownloadTaskRunnerCommand {
  kind: string;
  subscribeRequest?: SubscribeRequest;

  private constructor(kind: string, subscribeRequest?: SubscribeRequest) {
    this.kind = kind;
    this.subscribeRequest = subscribeRequest;
  }

  static UpdateSubscribeRequest(subscribeRequest: SubscribeRequest): DownloadTaskRunnerCommand {
    return new DownloadTaskRunnerCommand("UpdateSubscribeRequest", subscribeRequest);
  }
}

export class DownloadTaskRunnerChannels {
  constructor(
    public downloadTaskQueueTx: AsyncQueue<{}>,
    public cncTx: AsyncQueue<{}>,
    public downloadResultRx: AsyncQueue<{}>
  ) {}
}

