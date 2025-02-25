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

export default class Client {
  _client: FumaroleClient;
  _insecureXToken: string | undefined;

  constructor(
    endpoint: string,
    xToken: string | undefined,
    channelOptions: ChannelOptions | undefined
  ) {
    let creds: ChannelCredentials;

    const endpointURL = new URL(endpoint);
    let port = endpointURL.port;
    if (port == "") {
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
    if (endpointURL.protocol === "https:") {
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

  async subscribe() {
    return await this._client.subscribe(this._getInsecureMetadata());
  }
}
