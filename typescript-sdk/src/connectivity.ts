import { ChannelCredentials, Metadata, credentials } from "@grpc/grpc-js";
import { FumaroleClient } from "./grpc/fumarole";
import { FumaroleConfig } from "./config/config";

const X_TOKEN_HEADER = "x-token";

class TritonAuthMetadataGenerator {
  constructor(private readonly xToken: string) {}

  generateMetadata(): Promise<Metadata> {
    const metadata = new Metadata();
    metadata.set(X_TOKEN_HEADER, this.xToken);
    return Promise.resolve(metadata);
  }
}

interface CallMetadataOptions {
  metadata?: Metadata;
}

class MetadataProvider {
  private metadata: Metadata;

  constructor(metadata: Record<string, string>) {
    this.metadata = new Metadata();
    Object.entries(metadata).forEach(([key, value]) => {
      this.metadata.set(key, value);
    });
  }

  getMetadata(): Promise<Metadata> {
    return Promise.resolve(this.metadata);
  }
}

export class FumaroleGrpcConnector {
  private static readonly logger = console;

  constructor(
    private readonly config: FumaroleConfig,
    private readonly endpoint: string
  ) {}

  async connect(
    grpcOptions: Record<string, any> = {}
  ): Promise<FumaroleClient> {
    const options = {
      "grpc.max_receive_message_length": 111111110,
      ...grpcOptions,
    };

    let channelCredentials: ChannelCredentials;
    let insecureXToken: string | undefined;

    // Parse endpoint properly
    const endpointURL = new URL(this.endpoint);
    let port = endpointURL.port;
    if (port === "") {
      port = endpointURL.protocol === "https:" ? "443" : "80";
    }
    const address = `${endpointURL.hostname}:${port}`;

    // Handle credentials based on protocol
    if (endpointURL.protocol === "https:") {
      channelCredentials = credentials.combineChannelCredentials(
        credentials.createSsl(),
        credentials.createFromMetadataGenerator((_params, callback) => {
          const metadata = new Metadata();
          if (this.config.xToken) {
            metadata.add("x-token", this.config.xToken);
          }
          if (this.config.xMetadata) {
            Object.entries(this.config.xMetadata).forEach(([key, value]) => {
              metadata.add(key, value);
            });
          }
          callback(null, metadata);
        })
      );
    } else {
      channelCredentials = credentials.createInsecure();
      if (this.config.xToken) {
        insecureXToken = this.config.xToken;
      }
    }

    // Create the client options with simpler settings
    const clientOptions = {
      ...options,
      "grpc.enable_http_proxy": 0,
      // Basic keepalive settings
      "grpc.keepalive_time_ms": 20000,
      "grpc.keepalive_timeout_ms": 10000,
      "grpc.http2.min_time_between_pings_ms": 10000,
      // Connection settings
      "grpc.initial_reconnect_backoff_ms": 100,
      "grpc.max_reconnect_backoff_ms": 3000,
      "grpc.min_reconnect_backoff_ms": 100,
      // Enable retries
      "grpc.enable_retries": 1,
      "grpc.service_config": JSON.stringify({
        methodConfig: [
          {
            name: [{}], // Apply to all methods
            retryPolicy: {
              maxAttempts: 5,
              initialBackoff: "0.1s",
              maxBackoff: "3s",
              backoffMultiplier: 2,
              retryableStatusCodes: ["UNAVAILABLE", "DEADLINE_EXCEEDED"],
            },
          },
        ],
      }),
    };

    // Create the client with credentials and options
    const client = new FumaroleClient(
      address,
      channelCredentials,
      clientOptions
    );

    // Do a simple connection check
    try {
      await new Promise<void>((resolve, reject) => {
        const deadline = Date.now() + 5000; // 5 second timeout
        client.waitForReady(deadline, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    } catch (error) {
      throw error;
    }

    return client;
  }
}
