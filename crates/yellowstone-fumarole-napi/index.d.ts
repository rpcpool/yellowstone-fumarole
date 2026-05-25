/// <reference types="node" />

// ─── Config ──────────────────────────────────────────────────────────────────

export interface FumaroleConfigOptions {
  endpoint: string
  xToken?: string
  /** Default: 512 MB */
  maxDecodingMessageSizeBytes?: number
}

export interface FumaroleSubscribeConfigOptions {
  /** Number of parallel data-plane TCP connections. Default: 1 */
  numDataPlaneTcpConnections?: number
  /** Max concurrent downloads per TCP connection. Default: 2 */
  concurrentDownloadLimitPerTcp?: number
  /** Offset commit interval in milliseconds. Default: 10 000 */
  commitIntervalMs?: number
  /** Max consecutive failed slot downloads before the session fails. Default: 3 */
  maxFailedSlotDownloadAttempt?: number
  /** GC tick interval. Default: 100 */
  gcInterval?: number
  /** Number of slots kept in the dedup window. Default: 1 000 */
  slotMemoryRetention?: number
  /** Disable offset commits entirely. */
  noCommit?: boolean
  /** Automatically commit progress after each event. */
  autoCommit?: boolean
}

// ─── Event ───────────────────────────────────────────────────────────────────

export interface FumaroleEvent {
  slot: bigint
  /** `true` when the slot has finished streaming. */
  isSlotEnded: boolean
  /**
   * Protobuf-encoded `geyser.SubscribeUpdate` bytes.
   * Only present when `isSlotEnded` is `false`.
   */
  update?: Buffer
}

// ─── FumaroleSubscription ────────────────────────────────────────────────────

/**
 * An active Fumarole subscription.
 *
 * Implements `AsyncIterable<FumaroleEvent>`:
 *
 * ```ts
 * for await (const event of subscription) {
 *   if (event.isSlotEnded) continue
 *   const update = SubscribeUpdate.decode(event.update!)
 * }
 * ```
 */
export declare class FumaroleSubscription implements AsyncIterable<FumaroleEvent> {
  /**
   * Returns the next event, or `null` when the stream is exhausted.
   */
  next(): Promise<FumaroleEvent | null>

  /**
   * Update active subscription filters.
   *
   * @param request  Protobuf-encoded `geyser.SubscribeRequest`
   */
  send(request: Buffer): Promise<void>

  [Symbol.asyncIterator](): AsyncGenerator<FumaroleEvent, void, unknown>
}

// ─── FumaroleClient ──────────────────────────────────────────────────────────

export declare class FumaroleClient {
  /**
   * Connect to a Fumarole service.
   */
  static connect(config: FumaroleConfigOptions): Promise<FumaroleClient>

  /**
   * Returns the service version as a protobuf-encoded `VersionResponse` Buffer.
   * Decode with `VersionResponse.decode(buffer)` from the generated proto types.
   */
  version(): Promise<Buffer>

  /**
   * Subscribe to a stream of updates using default configuration.
   *
   * @param subscriberName  Persistent subscriber name
   * @param request         Protobuf-encoded `geyser.SubscribeRequest`
   */
  subscribe(subscriberName: string, request: Buffer): Promise<FumaroleSubscription>

  /**
   * Subscribe to a stream of updates with custom tuning.
   *
   * @param subscriberName  Persistent subscriber name
   * @param request         Protobuf-encoded `geyser.SubscribeRequest`
   * @param config          Tuning options
   */
  subscribeWithConfig(
    subscriberName: string,
    request: Buffer,
    config: FumaroleSubscribeConfigOptions
  ): Promise<FumaroleSubscription>

  /**
   * @param request  Protobuf-encoded `ListConsumerGroupsRequest`
   * @returns        Protobuf-encoded `ListConsumerGroupsResponse`
   */
  listConsumerGroups(request: Buffer): Promise<Buffer>

  /**
   * @param request  Protobuf-encoded `GetConsumerGroupInfoRequest`
   * @returns        Protobuf-encoded `ConsumerGroupInfo`
   */
  getConsumerGroupInfo(request: Buffer): Promise<Buffer>

  /**
   * @param request  Protobuf-encoded `DeleteConsumerGroupRequest`
   * @returns        Protobuf-encoded `DeleteConsumerGroupResponse`
   */
  deleteConsumerGroup(request: Buffer): Promise<Buffer>

  /**
   * @param request  Protobuf-encoded `CreateConsumerGroupRequest`
   * @returns        Protobuf-encoded `CreateConsumerGroupResponse`
   */
  createConsumerGroup(request: Buffer): Promise<Buffer>

  /**
   * @param request  Protobuf-encoded `GetChainTipRequest`
   * @returns        Protobuf-encoded `GetChainTipResponse`
   */
  getChainTip(request: Buffer): Promise<Buffer>

  /**
   * @returns  Protobuf-encoded `GetSlotRangeResponse`
   */
  getSlotRange(): Promise<Buffer>
}
