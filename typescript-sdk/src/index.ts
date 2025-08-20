import { Metadata, ServiceError, MetadataValue, status } from "@grpc/grpc-js";
import { FumaroleConfig } from "./config/config";
import { FumaroleClient as GrpcClient } from "./grpc/fumarole";
import { FumaroleGrpcConnector } from "./connectivity";

const X_TOKEN_HEADER = "x-token";
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
  getDefaultFumaroleSubscribeConfig,
} from "./types";

export class FumaroleClient {
  private static readonly logger = console;
  private readonly connector: FumaroleGrpcConnector;
  private readonly stub: GrpcClient;

  private static safeStringify(obj: unknown): string {
    return JSON.stringify(obj, (_, v) =>
      typeof v === "bigint" ? v.toString() : v
    );
  }

  constructor(connector: FumaroleGrpcConnector, stub: GrpcClient) {
    this.connector = connector;
    this.stub = stub;
  }

  static async connect(config: FumaroleConfig): Promise<FumaroleClient> {
    const endpoint = config.endpoint;
    const connector = new FumaroleGrpcConnector(config, endpoint);

    FumaroleClient.logger.debug(`Connecting to ${endpoint}`);
    FumaroleClient.logger.debug(
      "Connection config:",
      FumaroleClient.safeStringify({
        endpoint: config.endpoint,
        xToken: config.xToken ? "***" : "none",
        maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
      })
    );

    const client = await connector.connect();
    FumaroleClient.logger.debug(`Connected to ${endpoint}, testing stub...`);

    // Wait for client to be ready
    await new Promise((resolve, reject) => {
      const deadline = new Date().getTime() + 5000; // 5 second timeout
      client.waitForReady(deadline, (error) => {
        if (error) {
          FumaroleClient.logger.error(
            "Client failed to become ready:",
            FumaroleClient.safeStringify(error)
          );
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
      FumaroleClient.logger.error(
        "Available methods:",
        FumaroleClient.safeStringify(methods)
      );
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
          FumaroleClient.logger.error(
            "Version request failed:",
            FumaroleClient.safeStringify(error)
          );
          reject(error);
        } else {
          FumaroleClient.logger.debug(
            "Version response:",
            FumaroleClient.safeStringify(response)
          );
          resolve(response);
        }
      });
    });
  }

  // async dragonsmouthSubscribe(
  //   consumerGroupName: string,
  //   request: SubscribeRequest
  // ): Promise<DragonsmouthAdapterSession> {
  //   return this.dragonsmouthSubscribeWithConfig(
  //     consumerGroupName,
  //     request,
  //     getDefaultFumaroleSubscribeConfig()
  //   );
  // }

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
                FumaroleClient.safeStringify(response)
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
        `Failed to delete some consumer groups: ${FumaroleClient.safeStringify(
          failures
        )}`
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
  InitialOffsetPolicy,
  CommitmentLevel,
  SubscribeRequest,
  SubscribeUpdate,
  DEFAULT_DRAGONSMOUTH_CAPACITY,
  DEFAULT_COMMIT_INTERVAL,
  DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
  DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
};

export type { DragonsmouthAdapterSession, FumaroleSubscribeConfig };
