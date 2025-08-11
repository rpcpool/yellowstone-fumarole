import { ServiceError } from "@grpc/grpc-js";
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
import {
  AsyncQueue,
  DragonsmouthAdapterSession,
  FumaroleSubscribeConfig,
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
    const client = await connector.connect();
    FumaroleClient.logger.debug(`Connected to ${endpoint}`);
    return new FumaroleClient(connector, client);
  }

  async version(): Promise<VersionResponse> {
    const request = {} as VersionRequest;
    return new Promise((resolve, reject) => {
      this.stub.version(
        request,
        (error: ServiceError | null, response: VersionResponse) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        }
      );
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
    const request = {} as ListConsumerGroupsRequest;
    return new Promise((resolve, reject) => {
      this.stub.listConsumerGroups(
        request,
        (error: ServiceError | null, response: ListConsumerGroupsResponse) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        }
      );
    });
  }

  async getConsumerGroupInfo(
    consumerGroupName: string
  ): Promise<ConsumerGroupInfo | null> {
    const request = { consumerGroupName } as GetConsumerGroupInfoRequest;
    return new Promise((resolve, reject) => {
      this.stub.getConsumerGroupInfo(
        request,
        (error: ServiceError | null, response: ConsumerGroupInfo) => {
          if (error) {
            if (error.code === 14) {
              // grpc.status.NOT_FOUND
              resolve(null);
            } else {
              reject(error);
            }
          } else {
            resolve(response);
          }
        }
      );
    });
  }

  async deleteConsumerGroup(
    consumerGroupName: string
  ): Promise<DeleteConsumerGroupResponse> {
    const request = { consumerGroupName } as DeleteConsumerGroupRequest;
    return new Promise((resolve, reject) => {
      this.stub.deleteConsumerGroup(
        request,
        (error: ServiceError | null, response: DeleteConsumerGroupResponse) => {
          if (error) {
            reject(error);
          } else {
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
    return new Promise((resolve, reject) => {
      this.stub.createConsumerGroup(
        request,
        (error: ServiceError | null, response: CreateConsumerGroupResponse) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        }
      );
    });
  }
}

export {
  FumaroleConfig,
  FumaroleSubscribeConfig,
  DragonsmouthAdapterSession,
  DEFAULT_DRAGONSMOUTH_CAPACITY,
  DEFAULT_COMMIT_INTERVAL,
  DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT,
  DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP,
};
