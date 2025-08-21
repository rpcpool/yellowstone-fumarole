import { jest } from "@jest/globals";


import { EventEmitter } from "events";
import { status, ServiceError } from "@grpc/grpc-js";
import {
  GrpcDownloadBlockTaskRun,
  DownloadBlockError,
  DownloadTaskResult,
  GrpcSlotDownloader,
  DownloadTaskArgs,
} from "../runtime/grpc-slot-downloader";
import { FumaroleClient } from "../grpc/fumarole";
import { AsyncQueue } from "../runtime/async-queue";
import { CommitmentLevel, SubscribeRequest, SubscribeUpdate } from "../grpc/geyser";

// === helpers ===
class MockReadableStream<T> extends EventEmitter {
  cancel = jest.fn();
}

// mock AsyncQueue that can simulate disconnection
class MockAsyncQueue<T> extends AsyncQueue<T> {
  public put = jest.fn(async (_: T) => {});
}

describe("GrpcDownloadBlockTaskRun", () => {
  let mockClient: jest.Mocked<FumaroleClient>;
  let mockQueue: MockAsyncQueue<SubscribeUpdate>;
  let downloadRequest: any;

  beforeEach(() => {
    mockClient = {
      downloadBlock: jest.fn(),
    } as any;

    mockQueue = new MockAsyncQueue<SubscribeUpdate>();

    downloadRequest = {
      blockchainId: 0,
      blockUid: new Uint8Array([1, 2, 3]),
      slot: BigInt(42),
    };
  });

  test("mapTonicErrorCodeToDownloadBlockError returns correct mapping", () => {
    const task = new GrpcDownloadBlockTaskRun(
      downloadRequest,
      mockClient,
      null,
      mockQueue
    );

    const codes: [number, string][] = [
      [status.NOT_FOUND, "BlockShardNotFound"],
      [status.UNAVAILABLE, "Disconnected"],
      [status.INTERNAL, "FailedDownload"],
      [status.INVALID_ARGUMENT, "throw"],
      [12345, "Fatal"],
    ];

    for (const [code, expected] of codes) {
      const err = { code } as ServiceError;
      if (expected === "throw") {
        expect(() => task.mapTonicErrorCodeToDownloadBlockError(err)).toThrow(
          "Invalid argument"
        );
      } else {
        const mapped = task.mapTonicErrorCodeToDownloadBlockError(err);
        expect(mapped.kind).toBe(expected);
      }
    }
  });

  test("run resolves with Ok when blockShardDownloadFinish is received", async () => {
    const stream = new MockReadableStream();
    mockClient.downloadBlock.mockReturnValue(stream as any);

    const task = new GrpcDownloadBlockTaskRun(
      downloadRequest,
      mockClient,
      null,
      mockQueue
    );

    const promise = task.run();

    // emit update first
    stream.emit("data", { update: { foo: "bar" } });
    // emit finish
    stream.emit("data", { blockShardDownloadFinish: true });

    const result = await promise;
    expect(result.kind).toBe("Ok");
    expect(result.completed?.slot).toBe(BigInt(42));
    expect(mockQueue.put).toHaveBeenCalled();
  });

  test("run resolves with Err when outlet is disconnected", async () => {
    const stream = new MockReadableStream();
    mockClient.downloadBlock.mockReturnValue(stream as any);

    // simulate queue.put throwing
    mockQueue.put.mockRejectedValueOnce(new Error("disconnected"));

    const task = new GrpcDownloadBlockTaskRun(
      downloadRequest,
      mockClient,
      null,
      mockQueue
    );

    const promise = task.run();

    stream.emit("data", { update: { foo: "bar" } });

    const result = await promise;
    expect(result.kind).toBe("Err");
    expect(result.err?.kind).toBe("OutletDisconnected");
  });

  test("run rejects when stream emits error", async () => {
    const stream = new MockReadableStream();
    mockClient.downloadBlock.mockReturnValue(stream as any);

    const task = new GrpcDownloadBlockTaskRun(
      downloadRequest,
      mockClient,
      null,
      mockQueue
    );

    const promise = task.run();

    const err = { code: status.UNAVAILABLE } as ServiceError;
    stream.emit("error", err);

    await expect(promise).rejects.toBeInstanceOf(DownloadBlockError);
  });

  test("run resolves Err when stream ends without finish", async () => {
    const stream = new MockReadableStream();
    mockClient.downloadBlock.mockReturnValue(stream as any);

    const task = new GrpcDownloadBlockTaskRun(
      downloadRequest,
      mockClient,
      null,
      mockQueue
    );

    const promise = task.run();

    stream.emit("end");

    const result = await promise;
    expect(result.kind).toBe("Err");
    expect(result.err?.kind).toBe("FailedDownload");
  });
});

describe("GrpcSlotDownloader", () => {
  test("runDownload delegates to GrpcDownloadBlockTaskRun", async () => {
    const mockClient = {
      downloadBlock: jest.fn(() => new MockReadableStream()),
    } as any as FumaroleClient;

    const downloader = new GrpcSlotDownloader(mockClient);

    const spec = new DownloadTaskArgs(
      {
        blockchainId: new Uint8Array([0,0,0]),
        blockUid: new Uint8Array([1, 2, 3]),
        slot: BigInt(42),
        numShards: 1,
        commitmentLevel: CommitmentLevel.CONFIRMED
      },
      new MockAsyncQueue()
    );

    const subscribeRequest: any = {
      accounts: {},
      transactions: {},
      entry: {},
      blocksMeta: {},
    };

    // stub GrpcDownloadBlockTaskRun.run
    const runSpy = jest
      .spyOn(GrpcDownloadBlockTaskRun.prototype, "run")
      .mockResolvedValue({ kind: "Ok" } as DownloadTaskResult);

    const result = await downloader.runDownload(subscribeRequest, spec);

    expect(runSpy).toHaveBeenCalled();
    expect(result.kind).toBe("Ok");
  });
});
