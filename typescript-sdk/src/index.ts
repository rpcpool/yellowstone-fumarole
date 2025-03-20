/**
 * @fileoverview Fumarole TypeScript SDK for streaming Solana account and transaction data
 *
 * Fumarole provides:
 * - High availability through multi-node data collection
 * - Persistent storage of historical state
 * - Horizontal scalability via consumer groups
 *
 * @see https://github.com/rpcpool/yellowstone-fumarole
 */
import {
  ChannelCredentials,
  credentials,
  ChannelOptions,
  Metadata,
} from "@grpc/grpc-js";
import {
  ConsumerGroupInfo,
  CreateStaticConsumerGroupRequest,
  CreateStaticConsumerGroupResponse,
  DeleteConsumerGroupRequest,
  DeleteConsumerGroupResponse,
  FumaroleClient,
  GetConsumerGroupInfoRequest,
  GetOldestSlotRequest,
  GetOldestSlotResponse,
  GetSlotLagInfoRequest,
  GetSlotLagInfoResponse,
  ListAvailableCommitmentLevelsRequest,
  ListAvailableCommitmentLevelsResponse,
  ListConsumerGroupsResponse,
  SubscribeRequest,
} from "./grpc/fumarole";

export type FumaroleSubscribeRequest = SubscribeRequest;

/**
 * Configuration options for Fumarole subscription
 * @example
 * ```typescript
 * const stream = await client.subscribe({ compression: "gzip" });
 * ```
 */
export type SubscribeConfig = {
  /** Enable gzip compression for reduced bandwidth usage */
  compression?: "gzip";
};

/**
 * Main client for interacting with the Fumarole service
 */
export default class Client {
  _client: FumaroleClient;
  _insecureXToken: string | undefined;

  /**
   * Creates a new Fumarole client instance
   *
   * @param endpoint - The Fumarole service endpoint URL
   * @param xToken - Authentication token provided by Triton
   * @param channelOptions - Additional gRPC channel options
   */
  constructor(
    endpoint: string,
    xToken: string | undefined,
    channelOptions: ChannelOptions | undefined
  ) {
    let creds: ChannelCredentials;

    const endpointURL = new URL(endpoint);
    let port = endpointURL.port;
    if (!port) {
      switch (endpointURL.protocol) {
        case "https:":
          port = "443";
          break;
        case "http:":
          port = "80";
          break;
      }
    }

    // Check if we need to use TLS.
    if (endpointURL.protocol.startsWith("https:")) {
      creds = credentials.combineChannelCredentials(
        credentials.createSsl(),
        credentials.createFromMetadataGenerator((_params, callback) => {
          const metadata = new Metadata();
          if (xToken !== undefined) {
            metadata.add("x-token", xToken);
          }
          return callback(null, metadata);
        })
      );
    } else {
      creds = ChannelCredentials.createInsecure();
      if (xToken !== undefined) {
        this._insecureXToken = xToken;
      }
    }

    this._client = new FumaroleClient(
      `${endpointURL.hostname}:${port}`,
      creds,
      channelOptions
    );
  }

  private _getInsecureMetadata(): Metadata {
    const metadata = new Metadata();
    if (this._insecureXToken) {
      metadata.add("x-token", this._insecureXToken);
    }
    return metadata;
  }

  /**
   * Creates a new static consumer group for horizontal scaling
   *
   * @example
   * ```typescript
   * const group = await client.createStaticConsumerGroup({
   *   commitmentLevel: CommitmentLevel.CONFIRMED,
   *   consumerGroupLabel: "my-group",
   *   eventSubscriptionPolicy: EventSubscriptionPolicy.BOTH,
   *   initialOffsetPolicy: InitialOffsetPolicy.LATEST,
   * });
   * ```
   */
  async createStaticConsumerGroup(
    request: CreateStaticConsumerGroupRequest
  ): Promise<CreateStaticConsumerGroupResponse> {
    return await new Promise((resolve, reject) => {
      this._client.createStaticConsumerGroup(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Lists all available consumer groups
   *
   * @param request - List request parameters
   * @returns Promise resolving to list of consumer groups
   */
  async listConsumerGroups(
    request: ListAvailableCommitmentLevelsRequest
  ): Promise<ListConsumerGroupsResponse> {
    return await new Promise((resolve, reject) => {
      this._client.listConsumerGroups(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Gets detailed information about a specific consumer group
   *
   * @param request - Consumer group info request
   * @returns Promise resolving to consumer group details
   */
  async getConsumerGroupInfo(
    request: GetConsumerGroupInfoRequest
  ): Promise<ConsumerGroupInfo> {
    return await new Promise((resolve, reject) => {
      this._client.getConsumerGroupInfo(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Deletes an existing consumer group
   *
   * @param request - Delete request parameters
   * @returns Promise resolving when deletion is complete
   */
  async deleteConsumerGroup(
    request: DeleteConsumerGroupRequest
  ): Promise<DeleteConsumerGroupResponse> {
    return await new Promise((resolve, reject) => {
      this._client.deleteConsumerGroup(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Gets information about slot lag for a subscription
   *
   * @param request - Slot lag info request
   * @returns Promise resolving to slot lag details
   */
  async getSlotLagInfo(
    request: GetSlotLagInfoRequest
  ): Promise<GetSlotLagInfoResponse> {
    return await new Promise<GetSlotLagInfoResponse>((resolve, reject) => {
      this._client.getSlotLagInfo(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Gets the oldest available slot in the persistence store
   *
   * @param request - Oldest slot request parameters
   * @returns Promise resolving to oldest slot information
   */
  async getOldestSlot(
    request: GetOldestSlotRequest
  ): Promise<GetOldestSlotResponse> {
    return await new Promise((resolve, reject) => {
      this._client.getOldestSlot(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Lists available commitment levels for subscriptions
   *
   * @param request - List commitment levels request
   * @returns Promise resolving to available commitment levels
   */
  async listAvailableCommitmentLevels(
    request: ListAvailableCommitmentLevelsRequest
  ): Promise<ListAvailableCommitmentLevelsResponse> {
    return await new Promise((resolve, reject) => {
      this._client.listAvailableCommitmentLevels(request, (error, response) => {
        if (error === null || error === undefined) {
          resolve(response);
        } else {
          reject(error);
        }
      });
    });
  }

  /**
   * Subscribes to account and transaction updates
   *
   * @example
   * ```typescript
   * const stream = await client.subscribe({ compression: "gzip" });
   *
   * stream.on('data', (data) => console.log(data));
   * stream.write({
   *   accounts: {
   *     tokenKeg: {
   *       account: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
   *       filters: [],
   *       owner: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
   *     }
   *   },
   *   consumerGroupLabel: "my-group"
   * });
   * ```
   */
  async subscribe(config?: SubscribeConfig) {
    const options: any = {};
    if (config) {
      if (config.compression) {
        switch (config.compression) {
          case "gzip":
            options["grpc.default_compression_algorithm"] = 2; // set compression to: gzip
            break;
          default:
            options["grpc.default_compression_algorithm"] = 0; // set compression to: none
            break;
        }
      }
    }
    return await this._client.subscribe(this._getInsecureMetadata(), options);
  }
}
