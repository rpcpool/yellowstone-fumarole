import * as yaml from "js-yaml";

export interface FumaroleConfigOptions {
  endpoint: string;
  xToken?: string;
  maxDecodingMessageSizeBytes?: number;
  xMetadata?: Record<string, string>;
}

export class FumaroleConfig {
  /**
   * The endpoint URL for the Fumarole service.
   */
  endpoint: string;
  /**
   * The token used for authentication with the Fumarole service.
   */
  xToken?: string;
  /**
   * The maximum size of a message that can be decoded by the Fumarole service.
   */
  maxDecodingMessageSizeBytes: number;
  /**
   * Optional metadata to include with each request to the Fumarole service.
   */
  xMetadata?: Record<string, string>;

  static readonly DEFAULT_MAX_DECODING_MESSAGE_SIZE = 512_000_000;

  constructor(options: FumaroleConfigOptions) {
    this.endpoint = options.endpoint;
    this.xToken = options.xToken;
    this.maxDecodingMessageSizeBytes =
      options.maxDecodingMessageSizeBytes ??
      FumaroleConfig.DEFAULT_MAX_DECODING_MESSAGE_SIZE;
    this.xMetadata = options.xMetadata ?? {};
  }

  /**
   * Creates a FumaroleConfig instance from a YAML string.
   *
   * @param yamlContent The YAML string to parse.
   * @returns A FumaroleConfig instance.
   */
  static fromYaml(yamlContent: string): FumaroleConfig {
    const data = yaml.load(yamlContent) as Record<string, any>;

    return new FumaroleConfig({
      endpoint: data.endpoint,
      xToken: data["x-token"] || data.x_token,
      maxDecodingMessageSizeBytes:
        data.max_decoding_message_size_bytes ??
        FumaroleConfig.DEFAULT_MAX_DECODING_MESSAGE_SIZE,
      xMetadata: data["x-metadata"] ?? {},
    });
  }
}
