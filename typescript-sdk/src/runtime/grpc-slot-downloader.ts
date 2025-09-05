import {
  ClientReadableStream,
  Metadata,
  ServiceError,
  status,
} from "@grpc/grpc-js";
import {
  DataResponse,
  DownloadBlockShard,
  FumaroleClient,
} from "../grpc/fumarole";
import {
  DownloadBlockErrorKind,
  DownloadTaskArgs,
  DownloadTaskResult,
} from "./reactive_runtime";
import { Observer } from "rxjs";
import { LOGGER } from "../logging";

function mapTonicErrorCodeToDownloadBlockError(
  e: ServiceError,
): DownloadBlockErrorKind {
  const code = e.code;

  if (code === status.NOT_FOUND) {
    return "BlockShardNotFound";
  } else if (code === status.UNAVAILABLE) {
    return "Disconnected";
  } else if (
    code === status.INTERNAL ||
    code === status.ABORTED ||
    code === status.DATA_LOSS ||
    code === status.RESOURCE_EXHAUSTED ||
    code === status.UNKNOWN ||
    code === status.CANCELLED ||
    code === status.DEADLINE_EXCEEDED
  ) {
    return "FailedDownload";
  } else if (code === status.INVALID_ARGUMENT) {
    return "Fatal";
  } else {
    return "Fatal";
  }
}

/**
 * gRPC slot downloader context.
 */
export type GrpcSlotDownloader = {
  /**
   * The gRPC client for downloading slots.
   */
  client: FumaroleClient;
  /**
   * The metadata for the gRPC client.
   */
  client_metadata: Metadata;
  /**
   * Observer to send download task results too.
   */
  downloadTaskResultObserver: Observer<DownloadTaskResult>;
  /**
   * Maximum number of download attempts.
   * Must be greater than 0
   */
  maxDownloadAttempt: number;

  /**
   * Total number of slots downloaded so far.
   */
  totalDownloadedSlot: number;
};

function do_download(this: GrpcSlotDownloader, args: DownloadTaskArgs) {
  const blockFilters = {
    accounts: args.subscribeRequest.accounts,
    transactions: args.subscribeRequest.transactions,
    entries: args.subscribeRequest.entry,
    blocksMeta: args.subscribeRequest.blocksMeta,
  };
  LOGGER.debug(`Starting download for block ${JSON.stringify(blockFilters)}`);
  const request: DownloadBlockShard = {
    blockchainId: args.downloadRequest.blockchainId,
    blockUid: args.downloadRequest.blockUid,
    shardIdx: 0,
    blockFilters: blockFilters,
  };
  const outlet = args.outlet;
  const downloadResponse: ClientReadableStream<DataResponse> =
    this.client.downloadBlock(request, this.client_metadata);
  let totalEventDownloaded = 0;
  let finish = false;
  let startedAt = Date.now();
  downloadResponse.on("data", (data: DataResponse) => {
    if (finish) {
      return;
    }
    if (data.update) {
      totalEventDownloaded++;
      outlet.next(data.update);
    } else if (data.blockShardDownloadFinish) {
      const e = Date.now() - startedAt;
      LOGGER.info(
        `Finished download for slot ${args.downloadRequest.slot}, total events downloaded: ${totalEventDownloaded} in ${e} ms`,
      );
      // cancel can trigger an error later in stream.
      finish = true;
      this.totalDownloadedSlot += 1;
      this.downloadTaskResultObserver.next({
        kind: "Ok",
        completed: {
          slot: args.downloadRequest.slot,
          blockUid: args.downloadRequest.blockUid,
          shardIdx: 0,
          totalEventDownloaded,
        },
      });
    }
  });

  downloadResponse.on("error", (err: any) => {
    if (finish) {
      return;
    }
    finish = true;
    downloadResponse.cancel();
    const err_kind = mapTonicErrorCodeToDownloadBlockError(err);
    if (
      err_kind === "FailedDownload" &&
      args.downloadAttempt < this.maxDownloadAttempt
    ) {
      LOGGER.error("Download failed, retrying...");
      const args2 = { ...args };
      args2.downloadAttempt += 1;
      do_download.call(this, args2);
    } else {
      const result: DownloadTaskResult = {
        kind: "Err",
        err: {
          kind: err_kind,
          message: err,
        },
        slot: args.downloadRequest.slot,
      };
      this.downloadTaskResultObserver.next(result);
    }
  });
}

/**
 * Creates an observer for downloading slots.
 *
 * @param ctx The gRPC slot downloader context.
 * @returns An observer for download task arguments.
 */
export function downloadSlotObserverFactory(
  ctx: GrpcSlotDownloader,
): Observer<DownloadTaskArgs> {
  const download_fn = do_download.bind(ctx);
  if (ctx.maxDownloadAttempt <= 0) {
    throw new Error("maxDownloadAttempt must be greater than 0");
  }
  return {
    next: (args: DownloadTaskArgs) => {
      download_fn(args);
    },
    error: (err: Error) => {
      LOGGER.error(err);
      ctx.client.close();
    },
    complete: () => {
      ctx.client.close();
    },
  };
}

/**
 * (for testing only)
 * Creates an observer that always fails the download.
 *
 * @param ctx The gRPC slot downloader context.
 * @returns An observer for download task arguments.
 */
export function failingDownloadSlotObserverFactory(
  ctx: GrpcSlotDownloader,
): Observer<DownloadTaskArgs> {
  return {
    next: (args: DownloadTaskArgs) => {
      LOGGER.error("Download failed");
      ctx.downloadTaskResultObserver.next({
        kind: "Err",
        err: {
          kind: "FailedDownload",
          message: "Download failed",
        },
        slot: args.downloadRequest.slot,
      });
    },
    error: (err: Error) => {
      ctx.client.close();
    },
    complete: () => {
      ctx.client.close();
    },
  };
}
