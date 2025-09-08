import { Metadata, ServiceError, ClientDuplexStream } from "@grpc/grpc-js";
import { FumaroleConfig } from "./config";
import { FumaroleClient as GrpcClient } from "./grpc/fumarole";
import { FumaroleGrpcConnector } from "./connectivity";
import {
  LOGGER,
  setCustomFumaroleLogger,
  setDefaultFumaroleLogger,
} from "./logging";
import {
  VersionRequest,
  VersionResponse,
  ControlResponse,
  JoinControlPlane,
  ControlCommand,
  ListConsumerGroupsRequest,
  ListConsumerGroupsResponse,
  GetConsumerGroupInfoRequest,
  ConsumerGroupInfo,
  DeleteConsumerGroupRequest,
  DeleteConsumerGroupResponse,
  CreateConsumerGroupRequest,
  CreateConsumerGroupResponse,
  InitialOffsetPolicy,
} from "./grpc/fumarole";
import {
  SubscribeRequest,
  SubscribeUpdate,
  CommitmentLevel,
} from "./grpc/geyser";
import { FumaroleSM } from "./runtime/state-machine";
import {
  downloadSlotObserverFactory,
  failingDownloadSlotObserverFactory,
  GrpcSlotDownloader,
} from "./runtime/grpc-slot-downloader";
import {
  DownloadTaskArgs,
  DownloadTaskResult,
  fumaroleObservable,
  FumaroleRuntimeArgs,
} from "./runtime/reactive_runtime";
import { finalize, Observable, Observer, Subject } from "rxjs";
import { makeObservable } from "./utils/grpc_ext";
export interface FumaroleSubscribeConfig {
  /**
   * The maximum number of concurrent downloads allowed per TCP connection.
   * 
   * NOTE: Since grpc-js is pure javascript, the grpc handling is really slow and underperforming compared
   * to other programming languages. Suggested value: `1`
   */
  concurrentDownloadLimit: number;
  /**
   * The interval at which to commit offsets.
   * 
   * Fumarole keeps track of which slot have been processed at all time.
   * The runtime will, at regular interval, commit the new "offset" to the Fumarole Backend service.
   */
  commitInterval: number;
  /**
   * The maximum number of failed slot download attempts before giving up.
   */
  maxFailedSlotDownloadAttempt: number;
  /**
   * The interval at which to run the runtime garbage collection.
   */
  gcInterval: number;
  /**
   * The duration for which to retain slot memory.
   */
  slotMemoryRetention: number;
}

// Constants
export const DEFAULT_COMMIT_INTERVAL = 5000; // milliseconds
export const DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3;
export const DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 1;
export const DEFAULT_GC_INTERVAL = 100; // ticks
export const DEFAULT_SLOT_MEMORY_RETENTION = 1000; // seconds

/**
 * Get the default configuration for Fumarole subscriptions.
 *
 * @returns The default Fumarole subscribe configuration.
 */
export function getDefaultFumaroleSubscribeConfig(): FumaroleSubscribeConfig {
  return {
    concurrentDownloadLimit: DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
    commitInterval: DEFAULT_COMMIT_INTERVAL,
    maxFailedSlotDownloadAttempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
    gcInterval: DEFAULT_GC_INTERVAL,
    slotMemoryRetention: DEFAULT_SLOT_MEMORY_RETENTION,
  };
}

/**
 * Adapter that allows to bridge Fumarole protocol to Dragonsmouth-like consumer API.
 */
export interface DragonsmouthAdapterSession {
  /** Emits {@link SubscribeRequest} update to the current Fumarole subscribe session */
  sink: Observer<SubscribeRequest>;

  /** An {@link Observable} of {@link SubscribeUpdate} */
  source: Observable<SubscribeUpdate>;
}

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

/**
 *
 * Fumarole Client class.
 *
 * # Examples
 *
 * ## Subscribe
 *
 * Subscribe to {@link SubscribeUpdate}
 *
 * ```ts
 * const config = {
 *   endpoint: FUMAROLE_ENDPOINT,
 *   xToken: FUMAROLE_X_TOKEN,
 *   maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
 *   xMetadata: {},
 * };
 * client = await FumaroleClient.connect(config);
 *
 * const request: SubscribeRequest = {
 *   commitment: CommitmentLevel.CONFIRMED,
 *   accounts: { },
 *   transactions: {
 *     token: {
 *       accountInclude: [TOKEN_ADDRESS],
 *       accountExclude: [],
 *       accountRequired: [],
 *     }
 *   },
 *   slots: {
 *     test: {
 *       filterByCommitment: true,
 *     }
 *   },
 *   transactionsStatus: {},
 *   blocks: {},
 *   blocksMeta: {},
 *   entry: {},
 *   ping: { id: Date.now() },
 *   accountsDataSlice: [],
 *   fromSlot: undefined,
 * };
 *
 * const {
 *  sink,
 *  source
 * } = await client.dragonsmouthSubscribe("example", request);
 * ```
 *
 * ## Update `SubscribeRequest` during session
 *
 * To update the {@link SubscribeRequest}, use the `sink` observer:
 *
 * ```ts
 * const {
 *  sink,
 *  source
 * } = await client.dragonsmouthSubscribe("example", request);
 *
 * sink.next(my_new_request);
 * ```
 *
 * ## for-await supports
 *
 * To consume `source`, you can use all {@link Observable} operators, or use `for await` with `rxjs-for-await` lib:
 *
 * ```ts
 * for await (const update of eachValueFrom(source)) {
 *   console.log("Received update:", update);
 * }
 * ```
 *
 * ## Observable's Subscription handling
 *
 * ```ts
 * const sub = source.subscribe((update) => {
 *  console.log(`new subscribe update: ${update}`);
 * });
 *
 * ...
 *
 * // Don't forget to `unsubscribe()`
 * sub.unsubscribe();
 * ```
 *
 * ## Observable to Promise using `forEach`
 *
 * You can also use `forEach` which transforms {@link Observable} into a {@link Promise}:
 *
 * ```ts
 * await source.forEach((update) => {
 *   console.log(`new subscribe update: ${update}`);
 * });
 * ```
 */
export class FumaroleClient {
  private readonly connector: FumaroleGrpcConnector;
  private readonly stub: GrpcClient;

  private static safeStringify(obj: unknown): string {
    return JSON.stringify(obj, (_, v) =>
      typeof v === "bigint" ? v.toString() : v,
    );
  }

  constructor(connector: FumaroleGrpcConnector, stub: GrpcClient) {
    this.connector = connector;
    this.stub = stub;
  }

  static async connect(config: FumaroleConfig): Promise<FumaroleClient> {
    const endpoint = config.endpoint;
    const connector = new FumaroleGrpcConnector(config, endpoint);

    LOGGER.debug(`Connecting to ${endpoint}`);
    LOGGER.debug(
      "Connection config:",
      FumaroleClient.safeStringify({
        endpoint: config.endpoint,
        xToken: config.xToken ? "***" : "none",
        maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
      }),
    );

    const client = await connector.connect();
    LOGGER.debug(`Connected to ${endpoint}, testing stub...`);

    // Wait for client to be ready
    await new Promise((resolve, reject) => {
      const deadline = new Date().getTime() + 5000; // 5 second timeout
      client.waitForReady(deadline, (error) => {
        if (error) {
          LOGGER.error(
            "Client failed to become ready:",
            FumaroleClient.safeStringify(error),
          );
          reject(error);
        } else {
          LOGGER.debug("Client is ready");
          resolve(undefined);
        }
      });
    });

    // Verify client methods
    if (!client || typeof client.listConsumerGroups !== "function") {
      const methods = client
        ? Object.getOwnPropertyNames(Object.getPrototypeOf(client))
        : [];
      LOGGER.error("Available methods:", FumaroleClient.safeStringify(methods));
      throw new Error("gRPC client or listConsumerGroups method not available");
    }

    LOGGER.debug("gRPC client initialized successfully");
    return new FumaroleClient(connector, client);
  }

  async version(): Promise<VersionResponse> {
    LOGGER.debug("Sending version request");
    const request = {} as VersionRequest;
    return new Promise((resolve, reject) => {
      this.stub.version(request, (error, response) => {
        if (error) {
          LOGGER.error(
            "Version request failed:",
            FumaroleClient.safeStringify(error),
          );
          reject(error);
        } else {
          LOGGER.debug(
            "Version response:",
            FumaroleClient.safeStringify(response),
          );
          resolve(response);
        }
      });
    });
  }

  /**
   * Establish a Dragonsouth-like consumption stream from a persistent subscriber.
   * See {@link FumaroleClient.dragonsmouthSubscribeWithConfig} for more details.
   *
   * @param persistentSubscriberName The name of the persistent subscriber to connect to.
   * @param request the initial `SubscribeRequest` to use.
   *
   * @returns an {@link DragonsmouthAdapterSession}
   *
   */
  async dragonsmouthSubscribe(
    persistentSubscriberName: string,
    request: SubscribeRequest,
  ): Promise<DragonsmouthAdapterSession> {
    return this.dragonsmouthSubscribeWithConfig(
      persistentSubscriberName,
      request,
      getDefaultFumaroleSubscribeConfig(),
    );
  }

  /**
   * Establish a Dragonsouth-like consumption stream from a persistent subscriber.
   *
   * @param persistentSubscriberName The name of the persistent subscriber to connect to.
   * @param initialSubscribeRequest The initial `SubscribeRequest` to use.
   * @param config An instance of `FumaroleSubscribeConfig` configuration options for the subscription.
   * @returns an {@link DragonsmouthAdapterSession}
   */
  public async dragonsmouthSubscribeWithConfig(
    persistentSubscriberName: string,
    initialSubscribeRequest: SubscribeRequest,
    config: FumaroleSubscribeConfig,
  ): Promise<DragonsmouthAdapterSession> {
    const initialJoin: JoinControlPlane = {
      consumerGroupName: persistentSubscriberName,
    };
    const initialJoinCommand: ControlCommand = { initialJoin };
    const controlPlaneCommandSubject = new Subject<ControlCommand>();
    const metadata = new Metadata();

    // Create duplex stream
    const fumeControlPlaneDuplex = this.stub.subscribe(
      metadata,
      {},
    ) as ClientDuplexStream<ControlCommand, ControlResponse>;

    controlPlaneCommandSubject
      .pipe(
        finalize(() => {
          fumeControlPlaneDuplex.end();
        }),
      )
      .subscribe(async (command) => {
        await new Promise<void>((resolve, reject) => {
          fumeControlPlaneDuplex.write(command, (error: any) => {
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          });
        });
      });

    const waitInitCtrlMsg: Promise<ControlResponse> = new Promise(
      (resolve, reject) => {
        let has_resolved = false;
        fumeControlPlaneDuplex.once("data", (msg: ControlResponse) => {
          has_resolved = true;
          resolve(msg);
        });
        // I make sure I marked as resolve since data and error are independent
        // and we cannot remove listener once they are called.
        fumeControlPlaneDuplex.once("error", (err: any) => {
          if (!has_resolved) {
            reject(err);
          }
        });
      },
    );

    controlPlaneCommandSubject.next(initialJoinCommand);
    const controlResponse = await waitInitCtrlMsg;

    const init = (controlResponse as ControlResponse).init;
    if (!init)
      throw new Error(`Unexpected initial response: ${controlResponse}`);
    LOGGER.debug(`Control response:`, controlResponse);

    const lastCommittedOffset = init.lastCommittedOffsets[0];
    if (lastCommittedOffset == null)
      throw new Error("No last committed offset");

    const ctrlPlaneResponseObservable: Observable<ControlResponse> =
      makeObservable(fumeControlPlaneDuplex);
    // Initialize state machine and queues
    const sm = new FumaroleSM(lastCommittedOffset, config.slotMemoryRetention);

    const downloadTaskResultSubject = new Subject<DownloadTaskResult>();
    // // Connect data plane and create slot downloader
    const dataPlaneClient = await this.connector.connect();
    const grpcSlotDownloadCtx: GrpcSlotDownloader = {
      client: dataPlaneClient,
      client_metadata: metadata,
      downloadTaskResultObserver: downloadTaskResultSubject,
      maxDownloadAttempt: config.maxFailedSlotDownloadAttempt,
      totalDownloadedSlot: 0,
    };

    const grpcSlotDownloader: Observer<DownloadTaskArgs> =
      downloadSlotObserverFactory(grpcSlotDownloadCtx);

    const subscribeRequestSub = new Subject<SubscribeRequest>();

    const runtimeArgs: FumaroleRuntimeArgs = {
      downloadTaskObserver: grpcSlotDownloader,
      downloadTaskResultObservable: downloadTaskResultSubject.asObservable(),
      controlPlaneObserver: controlPlaneCommandSubject,
      controlPlaneResponseObservable: ctrlPlaneResponseObservable,
      sm,
      commitIntervalMillis: config.commitInterval,
      maxConcurrentDownload: config.concurrentDownloadLimit,
      initialSubscribeRequest,
      subscribeRequestObservable: subscribeRequestSub.asObservable(),
    };

    return {
      sink: subscribeRequestSub,
      source: fumaroleObservable(runtimeArgs),
    };
  }

  /**
   * List all persistent subscribers under your account.
   *
   * @returns
   */
  async listPersistentSubscribers(): Promise<ListConsumerGroupsResponse> {
    if (!this.stub) {
      throw new Error("gRPC stub not initialized");
    }
    if (!this.stub.listConsumerGroups) {
      throw new Error("listConsumerGroups method not available on stub");
    }

    LOGGER.debug("Preparing listConsumerGroups request");
    const request = {} as ListConsumerGroupsRequest;
    const metadata = new Metadata();

    return new Promise((resolve, reject) => {
      let hasResponded = false;
      const timeout = setTimeout(() => {
        if (!hasResponded) {
          LOGGER.error("ListConsumerGroups timeout after 30s");
          if (call) {
            try {
              call.cancel();
            } catch (e) {
              LOGGER.error("Error cancelling call:", e);
            }
          }
          reject(new Error("gRPC call timed out after 30 seconds"));
        }
      }, 30000); // 30 second timeout

      let call: any;
      try {
        LOGGER.debug("Starting gRPC listConsumerGroups call");
        call = this.stub.listConsumerGroups(
          request,
          metadata,
          {
            deadline: Date.now() + 30000, // 30 second deadline
          },
          (
            error: ServiceError | null,
            response: ListConsumerGroupsResponse,
          ) => {
            hasResponded = true;
            clearTimeout(timeout);

            if (error) {
              const errorDetails = {
                code: error.code,
                details: error.details,
                metadata: error.metadata?.getMap(),
                stack: error.stack,
                message: error.message,
                name: error.name,
              };
              LOGGER.error("ListConsumerGroups error:", errorDetails);
              reject(error);
            } else {
              LOGGER.debug(
                "ListConsumerGroups success - Response:",
                FumaroleClient.safeStringify(response),
              );
              resolve(response);
            }
          },
        );

        // Monitor call state
        if (call) {
          call.on("metadata", (metadata: Metadata) => {
            LOGGER.debug("Received metadata:", metadata.getMap());
          });

          call.on("status", (status: any) => {
            LOGGER.debug("Call status:", status);
          });

          call.on("error", (error: Error) => {
            LOGGER.error("Call stream error:", error);
            if (!hasResponded) {
              hasResponded = true;
              clearTimeout(timeout);
              reject(error);
            }
          });
        } else {
          LOGGER.error("Failed to create gRPC call object");
          hasResponded = true;
          clearTimeout(timeout);
          reject(new Error("Failed to create gRPC call"));
        }
      } catch (setupError) {
        hasResponded = true;
        clearTimeout(timeout);
        LOGGER.error("Error setting up gRPC call:", setupError);
        reject(setupError);
      }
    });
  }

  /**
   * Get information about a persistent subscriber.
   *
   * @param persistentSubscriberName The name of the persistent subscriber.
   * @returns The information about the persistent subscriber or null if not found.
   */
  async getPersistentSubscriberInfo(
    persistentSubscriberName: string,
  ): Promise<ConsumerGroupInfo | null> {
    LOGGER.debug(
      "Sending getConsumerGroupInfo request:",
      persistentSubscriberName,
    );
    const request = {
      consumerGroupName: persistentSubscriberName,
    } as GetConsumerGroupInfoRequest;
    return new Promise((resolve, reject) => {
      this.stub.getConsumerGroupInfo(
        request,
        (error: ServiceError | null, response: ConsumerGroupInfo) => {
          if (error) {
            if (error.code === 14) {
              // grpc.status.NOT_FOUND
              LOGGER.debug(
                "Consumer group not found:",
                persistentSubscriberName,
              );
              resolve(null);
            } else {
              LOGGER.error("GetConsumerGroupInfo error:", error);
              reject(error);
            }
          } else {
            LOGGER.debug("GetConsumerGroupInfo response:", response);
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Delete a persistent subscriber.
   *
   * @param persistentSubscriberName The name of the persistent subscriber.
   * @returns The response from the delete operation.
   */
  async deletePersistentSubscriber(
    persistentSubscriberName: string,
  ): Promise<DeleteConsumerGroupResponse> {
    LOGGER.debug(
      "Sending deleteConsumerGroup request:",
      persistentSubscriberName,
    );
    const request = {
      consumerGroupName: persistentSubscriberName,
    } as DeleteConsumerGroupRequest;
    return new Promise((resolve, reject) => {
      this.stub.deleteConsumerGroup(
        request,
        (error: ServiceError | null, response: DeleteConsumerGroupResponse) => {
          if (error) {
            LOGGER.error("DeleteConsumerGroup error:", error);
            reject(error);
          } else {
            LOGGER.debug("DeleteConsumerGroup response:", response);
            resolve(response);
          }
        },
      );
    });
  }

  /**
   * Delete all persistent subscribers under you account.
   */
  async deleteAllPersistentSubscribers(): Promise<void> {
    const response = await this.listPersistentSubscribers();
    const deletePromises = response.consumerGroups.map((group) =>
      this.deletePersistentSubscriber(group.consumerGroupName),
    );

    const results = await Promise.all(deletePromises);

    // Check for any failures
    const failures = results.filter((result) => !result.success);
    if (failures.length > 0) {
      throw new Error(
        `Failed to delete some consumer groups: ${FumaroleClient.safeStringify(
          failures,
        )}`,
      );
    }
  }

  /**
   * Create a new persistent subscriber.
   *
   * @param request The request object containing the subscriber details.
   * @returns The response from the create operation.
   */
  async createPersistentSubscriber(
    name: string,
    fromSlot?: bigint
  ): Promise<CreateConsumerGroupResponse> {
    const initialOffsetPolicy = fromSlot ? InitialOffsetPolicy.FROM_SLOT : InitialOffsetPolicy.LATEST;
    let request  = {
      consumerGroupName: String(name),
      initialOffsetPolicy: initialOffsetPolicy,
      fromSlot: fromSlot
    }
    return new Promise((resolve, reject) => {
      this.stub.createConsumerGroup(
        request,
        (error: ServiceError | null, response: CreateConsumerGroupResponse) => {
          if (error) {
            LOGGER.error("CreateConsumerGroup error:", error);
            reject(error);
          } else {
            LOGGER.debug("CreateConsumerGroup response:", response);
            resolve(response);
          }
        },
      );
    });
  }
}

export {
  FumaroleConfig,
  InitialOffsetPolicy,
  CommitmentLevel,
  SubscribeRequest,
  SubscribeUpdate,
  setCustomFumaroleLogger,
  setDefaultFumaroleLogger,
};
