import {
  ChannelCredentials,
  credentials,
  Metadata,
  ServiceError,
} from "@grpc/grpc-js";
import { FumaroleClient } from "./grpc/fumarole";
import { LOGGER } from "./logging";

export const X_TOKEN_HEADER = "x-token";

interface FumaroleConfig {
  xToken?: string;
  xMetadata?: Record<string, string>;
}

export class FumaroleGrpcConnector {
  private readonly config: FumaroleConfig;
  private readonly endpoint: string;

  constructor(config: FumaroleConfig, endpoint: string) {
    this.config = config;
    this.endpoint = endpoint;
  }

  private createMetadata(): Metadata {
    const metadata = new Metadata();
    if (this.config.xMetadata) {
      Object.entries(this.config.xMetadata).forEach(([key, value]) => {
        metadata.set(key, value);
      });
    }
    return metadata;
  }

  async connect(
    grpcOptions: { [key: string]: any }[] = []
  ): Promise<FumaroleClient> {
    LOGGER.debug(`Connecting to endpoint: ${this.endpoint}`);

    const defaultOptions: { [key: string]: any } = {
      "grpc.max_receive_message_length": 111111110,
      "grpc.keepalive_time_ms": 10000,
      "grpc.keepalive_timeout_ms": 5000,
      "grpc.http2.min_time_between_pings_ms": 10000,
      "grpc.keepalive_permit_without_calls": 1,
      "grpc.initial_reconnect_backoff_ms": 1000,
      "grpc.max_reconnect_backoff_ms": 10000,
      "grpc.service_config": JSON.stringify({
        loadBalancingConfig: [{ round_robin: {} }],
        methodConfig: [
          {
            name: [{ service: "fumarole.Fumarole" }],
            retryPolicy: {
              maxAttempts: 5,
              initialBackoff: "1s",
              maxBackoff: "10s",
              backoffMultiplier: 2,
              retryableStatusCodes: ["UNAVAILABLE"],
            },
          },
        ],
      }),
    };

    const channelOptions: { [key: string]: any } = {
      ...defaultOptions,
    };

    // Add additional options
    grpcOptions.forEach((opt) => {
      Object.entries(opt).forEach(([key, value]) => {
        LOGGER.debug(`Setting channel option: ${key} = ${value}`);
        channelOptions[key] = value;
      });
    });

    let channelCredentials: ChannelCredentials;

    try {
      const endpointURL = new URL(this.endpoint);
      LOGGER.debug(
        `Parsed URL - protocol: ${endpointURL.protocol}, hostname: ${endpointURL.hostname}, port: ${endpointURL.port}`
      );

      let port = endpointURL.port;
      if (port === "") {
        switch (endpointURL.protocol) {
          case "https:":
            port = "443";
            break;
          case "http:":
            port = "80";
            break;
        }
        LOGGER.debug(`No port specified, using default port: ${port}`);
      }

      // Check if we need to use TLS.
      if (endpointURL.protocol === "https:") {
        LOGGER.debug("HTTPS detected, setting up SSL credentials");
        const sslCreds = credentials.createSsl();
        LOGGER.debug("SSL credentials created");

        const callCreds = credentials.createFromMetadataGenerator(
          (_params, callback) => {
            const metadata = new Metadata();
            if (this.config.xToken !== undefined) {
              LOGGER.debug("Adding x-token to metadata");
              metadata.add(X_TOKEN_HEADER, this.config.xToken);
              // TODO remove this
              metadata.add("x-subscription-id", this.config.xToken);
            }
            return callback(null, metadata);
          }
        );
        LOGGER.debug("Call credentials created");

        channelCredentials = credentials.combineChannelCredentials(
          sslCreds,
          callCreds
        );
        LOGGER.debug("Using secure channel with x-token authentication");
      } else {
        channelCredentials = credentials.createInsecure();
        LOGGER.debug("Using insecure channel without authentication");
      }

      const finalEndpoint = `${endpointURL.hostname}:${port}`;
      LOGGER.debug(`Creating gRPC client with endpoint: ${finalEndpoint}`);

      const client = new FumaroleClient(
        finalEndpoint,
        channelCredentials,
        channelOptions
      );

      LOGGER.debug(`gRPC client created, waiting for ready state...`);

      // Wait for the client to be ready with a longer timeout
      await new Promise((resolve, reject) => {
        const deadline = new Date().getTime() + 30000; // 30 second timeout
        client.waitForReady(deadline, (error) => {
          if (error) {
            LOGGER.debug(
              `Client failed to become ready: ${error.message}`
            );
            const grpcError = error as ServiceError;
            if (grpcError.code !== undefined)
              LOGGER.debug(`Error code: ${grpcError.code}`);
            if (grpcError.details)
              LOGGER.debug(`Error details: ${grpcError.details}`);
            if (grpcError.metadata)
              LOGGER.debug(`Error metadata: ${grpcError.metadata}`);
            reject(error);
          } else {
            LOGGER.debug(`Client is ready`);
            resolve(undefined);
          }
        });
      });

      LOGGER.debug(
        `gRPC client created successfully for ${finalEndpoint}`
      );
      return client;
    } catch (error) {
      LOGGER.debug(
        `Error during connection setup: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      throw error;
    }
  }
}