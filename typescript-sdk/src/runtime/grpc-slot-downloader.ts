import { ClientReadableStream, Metadata, ServiceError, status } from "@grpc/grpc-js";
import {
  DataResponse,
  DownloadBlockShard,
  FumaroleClient,
} from "../grpc/fumarole";
import { SubscribeUpdate } from "../grpc/geyser";
import { DownloadBlockError, DownloadBlockErrorKind, DownloadTaskArgs, DownloadTaskResult } from "./runtime";
import { Observer } from "rxjs";


function mapTonicErrorCodeToDownloadBlockError(
    e: ServiceError
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
  client: FumaroleClient,
  /**
   * The metadata for the gRPC client.
   */
  client_metadata: Metadata,
  /**
   * The outlet for dragonsmouth updates.
   */
  dragonsmouthOutlet: Observer<SubscribeUpdate>,
  /**
   * Observer to send download task results too.
   */
  downloadTaskResultObserver: Observer<DownloadTaskResult>,
}


function do_download(
  this: GrpcSlotDownloader,
  args: DownloadTaskArgs
) {
  const blockFilters = {
    accounts: args.subscribeRequest.accounts,
    transactions: args.subscribeRequest.transactions,
    entries: args.subscribeRequest.entry,
    blocksMeta: args.subscribeRequest.blocksMeta,
  };

  const request: DownloadBlockShard = {
    blockchainId: args.downloadRequest.blockchainId,
    blockUid: args.downloadRequest.blockUid,
    shardIdx: 0,
    blockFilters: blockFilters,
  };
  const downloadResponse: ClientReadableStream<DataResponse> = this.client.downloadBlock(request, this.client_metadata);
  let totalEventDownloaded = 0;
  downloadResponse.on("data", (data: DataResponse) => {
    if (data.update) {
      totalEventDownloaded++;
      this.dragonsmouthOutlet.next(data.update);
    } else if (data.blockShardDownloadFinish) {
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
    const result: DownloadTaskResult = {
      kind: 'Err',
      err: {
        kind: mapTonicErrorCodeToDownloadBlockError(err),
        message: err,
      },
      slot: args.downloadRequest.slot,
    };
    this.downloadTaskResultObserver.next(result);
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
  return {
    next: (args: DownloadTaskArgs) => {
      download_fn(args);
    },
    error: (err: Error) => {
      console.error(err);
    },
    complete: () => { }
  };
}