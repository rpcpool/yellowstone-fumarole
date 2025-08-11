import { Metadata, ServiceError } from "@grpc/grpc-js";
import { FumaroleConfig } from "./config/config";
import { FumaroleClient as GrpcClient } from "./grpc/fumarole";
import { FumaroleGrpcConnector } from "./connectivity";
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
} from "./grpc/fumarole";
import { SubscribeRequest, SubscribeUpdate } from "./grpc/geyser";
import type {
  DragonsmouthAdapterSession,
  FumaroleSubscribeConfig,
} from "./types";
import {
  AsyncQueue,
  DEFAULT_DRAGONSMOUTH_CAPACITY,
  DEFAULT_COMMIT_INTERVAL,
  DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
  DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
  DEFAULT_GC_INTERVAL,
  DEFAULT_SLOT_MEMORY_RETENTION,
} from "./types";

export class FumaroleClient {
  private static readonly logger = console;
  private readonly connector: FumaroleGrpcConnector;
  private readonly stub: GrpcClient;

  constructor(connector: FumaroleGrpcConnector, stub: GrpcClient) {
    this.connector = connector;
    this.stub = stub;
  }

  static async connect(config: FumaroleConfig): Promise<FumaroleClient> {
    const endpoint = config.endpoint;
    const connector = new FumaroleGrpcConnector(config, endpoint);

    FumaroleClient.logger.debug(`Connecting to ${endpoint}`);
    FumaroleClient.logger.debug("Connection config:", {
      endpoint: config.endpoint,
      xToken: config.xToken ? "***" : "none",
      maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
    });

    const client = await connector.connect();
    FumaroleClient.logger.debug(`Connected to ${endpoint}, testing stub...`);

    // Wait for client to be ready
    await new Promise((resolve, reject) => {
      const deadline = new Date().getTime() + 5000; // 5 second timeout
      client.waitForReady(deadline, (error) => {
        if (error) {
          FumaroleClient.logger.error("Client failed to become ready:", error);
          reject(error);
        } else {
          FumaroleClient.logger.debug("Client is ready");
          resolve(undefined);
        }
      });
    });

    // Verify client methods
    if (!client || typeof client.listConsumerGroups !== "function") {
      const methods = client
        ? Object.getOwnPropertyNames(Object.getPrototypeOf(client))
        : [];
      FumaroleClient.logger.error("Available methods:", methods);
      throw new Error("gRPC client or listConsumerGroups method not available");
    }

    FumaroleClient.logger.debug("gRPC client initialized successfully");
    return new FumaroleClient(connector, client);
  }

  async version(): Promise<VersionResponse> {
    FumaroleClient.logger.debug("Sending version request");
    const request = {} as VersionRequest;
    return new Promise((resolve, reject) => {
      this.stub.version(request, (error, response) => {
        if (error) {
          FumaroleClient.logger.error("Version request failed:", error);
          reject(error);
        } else {
          FumaroleClient.logger.debug("Version response:", response);
          resolve(response);
        }
      });
    });
  }

  async dragonsmouthSubscribe(
    consumerGroupName: string,
    request: SubscribeRequest
  ): Promise<DragonsmouthAdapterSession> {
    return this.dragonsmouthSubscribeWithConfig(consumerGroupName, request, {});
  }

  async dragonsmouthSubscribeWithConfig(
    consumerGroupName: string,
    request: SubscribeRequest,
    config: FumaroleSubscribeConfig
  ): Promise<DragonsmouthAdapterSession> {
    const finalConfig = {
      concurrentDownloadLimit: DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
      commitInterval: DEFAULT_COMMIT_INTERVAL,
      maxFailedSlotDownloadAttempt: DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
      dataChannelCapacity: DEFAULT_DRAGONSMOUTH_CAPACITY,
      gcInterval: DEFAULT_GC_INTERVAL,
      slotMemoryRetention: DEFAULT_SLOT_MEMORY_RETENTION,
      ...config,
    };

    const dragonsmouthOutlet = new AsyncQueue<SubscribeUpdate>(
      finalConfig.dataChannelCapacity
    );
    const fumeControlPlaneQ = new AsyncQueue<ControlCommand>(100);

    const initialJoin = { consumerGroupName } as JoinControlPlane;
    const initialJoinCommand = { initialJoin } as ControlCommand;
    await fumeControlPlaneQ.put(initialJoinCommand);

    FumaroleClient.logger.debug(
      `Sent initial join command: ${JSON.stringify(initialJoinCommand)}`
    );

    const controlPlaneStream = this.stub.subscribe();
    const subscribeRequestQueue = new AsyncQueue<SubscribeRequest>(100);
    const fumeControlPlaneRxQ = new AsyncQueue<ControlResponse>(100);

    // Start the control plane source task
    const controlPlaneSourceTask = (async () => {
      try {
        for await (const update of controlPlaneStream) {
          await fumeControlPlaneRxQ.put(update);
        }
      } catch (error) {
        if (error.code !== "CANCELLED") {
          throw error;
        }
      }
    })();

    // Read the initial response
    const controlResponse =
      (await fumeControlPlaneRxQ.get()) as ControlResponse;
    const init = controlResponse.init;
    if (!init) {
      throw new Error(
        `Unexpected initial response: ${JSON.stringify(controlResponse)}`
      );
    }

    FumaroleClient.logger.debug(
      `Control response: ${JSON.stringify(controlResponse)}`
    );

    const lastCommittedOffsetStr = init.lastCommittedOffsets?.[0];
    if (!lastCommittedOffsetStr) {
      throw new Error("No last committed offset");
    }
    const lastCommittedOffset = BigInt(lastCommittedOffsetStr);

    // Create the runtime
    const dataPlaneClient = await this.connector.connect();

    // Start the runtime task
    const runtimeTask = this.startRuntime(
      subscribeRequestQueue,
      fumeControlPlaneQ,
      fumeControlPlaneRxQ,
      dragonsmouthOutlet,
      request,
      consumerGroupName,
      lastCommittedOffset,
      finalConfig,
      dataPlaneClient
    );

    FumaroleClient.logger.debug(`Fumarole handle created: ${runtimeTask}`);

    return {
      sink: subscribeRequestQueue,
      source: dragonsmouthOutlet,
      fumaroleHandle: runtimeTask,
    };
  }

  private async startRuntime(
    subscribeRequestQueue: AsyncQueue<SubscribeRequest>,
    controlPlaneTxQ: AsyncQueue<ControlCommand>,
    controlPlaneRxQ: AsyncQueue<ControlResponse>,
    dragonsmouthOutlet: AsyncQueue<SubscribeUpdate>,
    request: SubscribeRequest,
    consumerGroupName: string,
    lastCommittedOffset: bigint,
    config: Required<FumaroleSubscribeConfig>,
    dataPlaneClient: GrpcClient
  ): Promise<void> {
    // Implementation of runtime task here
    // This would be equivalent to AsyncioFumeDragonsmouthRuntime in Python
    // For brevity, this is a placeholder implementation
    return Promise.resolve();
  }

  async listConsumerGroups(): Promise<ListConsumerGroupsResponse> {
    if (!this.stub) {
      throw new Error("gRPC stub not initialized");
    }
    if (!this.stub.listConsumerGroups) {
      throw new Error("listConsumerGroups method not available on stub");
    }

    FumaroleClient.logger.debug("Preparing listConsumerGroups request");
    const request = {} as ListConsumerGroupsRequest;
    const metadata = new Metadata();

    return new Promise((resolve, reject) => {
      let hasResponded = false;
      const timeout = setTimeout(() => {
        if (!hasResponded) {
          FumaroleClient.logger.error("ListConsumerGroups timeout after 30s");
          if (call) {
            try {
              call.cancel();
            } catch (e) {
              FumaroleClient.logger.error("Error cancelling call:", e);
            }
          }
          reject(new Error("gRPC call timed out after 30 seconds"));
        }
      }, 30000); // 30 second timeout

      let call: any;
      try {
        FumaroleClient.logger.debug("Starting gRPC listConsumerGroups call");
        call = this.stub.listConsumerGroups(
          request,
          metadata,
          {
            deadline: Date.now() + 30000, // 30 second deadline
          },
          (
            error: ServiceError | null,
            response: ListConsumerGroupsResponse
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
              FumaroleClient.logger.error(
                "ListConsumerGroups error:",
                errorDetails
              );
              reject(error);
            } else {
              FumaroleClient.logger.debug(
                "ListConsumerGroups success - Response:",
                JSON.stringify(response, null, 2)
              );
              resolve(response);
            }
          }
        );

        // Monitor call state
        if (call) {
          call.on("metadata", (metadata: Metadata) => {
            FumaroleClient.logger.debug(
              "Received metadata:",
              metadata.getMap()
            );
          });

          call.on("status", (status: any) => {
            FumaroleClient.logger.debug("Call status:", status);
          });

          call.on("error", (error: Error) => {
            FumaroleClient.logger.error("Call stream error:", error);
            if (!hasResponded) {
              hasResponded = true;
              clearTimeout(timeout);
              reject(error);
            }
          });
        } else {
          FumaroleClient.logger.error("Failed to create gRPC call object");
          hasResponded = true;
          clearTimeout(timeout);
          reject(new Error("Failed to create gRPC call"));
        }
      } catch (setupError) {
        hasResponded = true;
        clearTimeout(timeout);
        FumaroleClient.logger.error("Error setting up gRPC call:", setupError);
        reject(setupError);
      }
    });
  }

  async getConsumerGroupInfo(
    consumerGroupName: string
  ): Promise<ConsumerGroupInfo | null> {
    FumaroleClient.logger.debug(
      "Sending getConsumerGroupInfo request:",
      consumerGroupName
    );
    const request = { consumerGroupName } as GetConsumerGroupInfoRequest;
    return new Promise((resolve, reject) => {
      this.stub.getConsumerGroupInfo(
        request,
        (error: ServiceError | null, response: ConsumerGroupInfo) => {
          if (error) {
            if (error.code === 14) {
              // grpc.status.NOT_FOUND
              FumaroleClient.logger.debug(
                "Consumer group not found:",
                consumerGroupName
              );
              resolve(null);
            } else {
              FumaroleClient.logger.error("GetConsumerGroupInfo error:", error);
              reject(error);
            }
          } else {
            FumaroleClient.logger.debug(
              "GetConsumerGroupInfo response:",
              response
            );
            resolve(response);
          }
        }
      );
    });
  }

  async deleteConsumerGroup(
    consumerGroupName: string
  ): Promise<DeleteConsumerGroupResponse> {
    FumaroleClient.logger.debug(
      "Sending deleteConsumerGroup request:",
      consumerGroupName
    );
    const request = { consumerGroupName } as DeleteConsumerGroupRequest;
    return new Promise((resolve, reject) => {
      this.stub.deleteConsumerGroup(
        request,
        (error: ServiceError | null, response: DeleteConsumerGroupResponse) => {
          if (error) {
            FumaroleClient.logger.error("DeleteConsumerGroup error:", error);
            reject(error);
          } else {
            FumaroleClient.logger.debug(
              "DeleteConsumerGroup response:",
              response
            );
            resolve(response);
          }
        }
      );
    });
  }

  async deleteAllConsumerGroups(): Promise<void> {
    const response = await this.listConsumerGroups();
    const deletePromises = response.consumerGroups.map((group) =>
      this.deleteConsumerGroup(group.consumerGroupName)
    );

    const results = await Promise.all(deletePromises);

    // Check for any failures
    const failures = results.filter((result) => !result.success);
    if (failures.length > 0) {
      throw new Error(
        `Failed to delete some consumer groups: ${JSON.stringify(failures)}`
      );
    }
  }

  async createConsumerGroup(
    request: CreateConsumerGroupRequest
  ): Promise<CreateConsumerGroupResponse> {
    FumaroleClient.logger.debug(
      "Sending createConsumerGroup request:",
      request
    );
    return new Promise((resolve, reject) => {
      this.stub.createConsumerGroup(
        request,
        (error: ServiceError | null, response: CreateConsumerGroupResponse) => {
          if (error) {
            FumaroleClient.logger.error("CreateConsumerGroup error:", error);
            reject(error);
          } else {
            FumaroleClient.logger.debug(
              "CreateConsumerGroup response:",
              response
            );
            resolve(response);
          }
        }
      );
    });
  }
}

export {
  FumaroleConfig,
  DEFAULT_DRAGONSMOUTH_CAPACITY,
  DEFAULT_COMMIT_INTERVAL,
  DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
  DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
};

export type { DragonsmouthAdapterSession, FumaroleSubscribeConfig };
