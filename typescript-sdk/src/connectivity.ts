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
    const metadataProvider = new MetadataProvider(this.config.xMetadata);
    const callCredentials = credentials.createFromMetadataGenerator(
      metadataProvider.getMetadata.bind(metadataProvider)
    );

    if (this.config.xToken) {
      // SSL credentials for HTTPS endpoint
      const sslCreds = credentials.createSsl();

      // Create call credentials with token
      const authGenerator = new TritonAuthMetadataGenerator(this.config.xToken);
      const callCreds = credentials.createFromMetadataGenerator(
        authGenerator.generateMetadata.bind(authGenerator)
      );

      // Combine credentials
      channelCredentials = credentials.combineChannelCredentials(
        sslCreds,
        callCreds
      );
      FumaroleGrpcConnector.logger.debug(
        "Using secure channel with x-token authentication"
      );
    } else {
      channelCredentials = credentials.createInsecure();
      FumaroleGrpcConnector.logger.debug(
        "Using insecure channel without authentication"
      );
    }

    // Create the client with credentials and options
    const client = new FumaroleClient(this.endpoint, channelCredentials, {
      ...options,
    });

    return client;
  }
}
