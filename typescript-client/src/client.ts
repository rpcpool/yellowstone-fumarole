import { FumaroleClient as NapiClient } from '@triton-one/yellowstone-fumarole-napi'
import type { FumaroleConfigOptions, FumaroleSubscribeConfigOptions } from './config.js'
import { FumaroleSubscription } from './subscription.js'
import {
  ConsumerGroupInfo,
  CreateConsumerGroupRequest,
  CreateConsumerGroupResponse,
  DeleteConsumerGroupRequest,
  DeleteConsumerGroupResponse,
  GetChainTipRequest,
  GetChainTipResponse,
  GetConsumerGroupInfoRequest,
  GetSlotRangeResponse,
  InitialOffsetPolicy,
  ListConsumerGroupsRequest,
  ListConsumerGroupsResponse,
  VersionResponse,
} from './grpc/fumarole.js'
import { SubscribeRequest } from './grpc/geyser.js'

export class FumaroleClient {
  readonly #inner: NapiClient

  private constructor(inner: NapiClient) {
    this.#inner = inner
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  /**
   * Connect to a Fumarole service.
   *
   * ```ts
   * const client = await FumaroleClient.connect({
   *   endpoint: 'https://fumarole.triton.one',
   *   xToken: process.env.FUMAROLE_TOKEN,
   * })
   * ```
   */
  static async connect(config: FumaroleConfigOptions): Promise<FumaroleClient> {
    const inner = await NapiClient.connect({
      endpoint: config.endpoint,
      xToken: config.xToken,
      maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
    })
    return new FumaroleClient(inner)
  }

  // ─── Version ──────────────────────────────────────────────────────────────

  /** Returns the current version of the connected Fumarole service. */
  async version(): Promise<VersionResponse> {
    const buf = await this.#inner.version()
    return VersionResponse.decode(buf)
  }

  // ─── Subscribe ────────────────────────────────────────────────────────────

  /**
   * Subscribe to a stream of updates using default configuration.
   *
   * ```ts
   * const sub = await client.subscribe('my-subscriber', {
   *   commitment: CommitmentLevel.CONFIRMED,
   *   slots: { all: {} },
   *   accounts: {},
   *   transactions: {},
   *   transactionsStatus: {},
   *   blocks: {},
   *   blocksMeta: {},
   *   entry: {},
   *   accountsDataSlice: [],
   * })
   *
   * for await (const event of sub) {
   *   if (event.type === 'data') console.log(event.update)
   * }
   * ```
   *
   * @param subscriberName  Persistent subscriber name (identifies the offset group)
   * @param request         Geyser filter configuration
   */
  async subscribe(
    subscriberName: string,
    request: SubscribeRequest,
  ): Promise<FumaroleSubscription> {
    const encoded = Buffer.from(SubscribeRequest.encode(request).finish())
    const napiSub = await this.#inner.subscribe(subscriberName, encoded)
    return new FumaroleSubscription(napiSub)
  }

  /**
   * Subscribe with custom tuning options.
   *
   * @param subscriberName  Persistent subscriber name
   * @param request         Geyser filter configuration
   * @param config          Tuning options (parallelism, commit interval, etc.)
   */
  async subscribeWithConfig(
    subscriberName: string,
    request: SubscribeRequest,
    config: FumaroleSubscribeConfigOptions,
  ): Promise<FumaroleSubscription> {
    const encoded = Buffer.from(SubscribeRequest.encode(request).finish())
    const napiSub = await this.#inner.subscribeWithConfig(subscriberName, encoded, {
      numDataPlaneTcpConnections: config.numDataPlaneTcpConnections,
      concurrentDownloadLimitPerTcp: config.concurrentDownloadLimitPerTcp,
      commitIntervalMs: config.commitIntervalMs,
      maxFailedSlotDownloadAttempt: config.maxFailedSlotDownloadAttempt,
      gcInterval: config.gcInterval,
      slotMemoryRetention: config.slotMemoryRetention,
      noCommit: config.noCommit,
      autoCommit: config.autoCommit,
    })
    return new FumaroleSubscription(napiSub)
  }

  // ─── Consumer group management ────────────────────────────────────────────

  /** Returns all consumer groups on the account. */
  async listConsumerGroups(): Promise<ListConsumerGroupsResponse> {
    const req = ListConsumerGroupsRequest.encode({}).finish()
    const buf = await this.#inner.listConsumerGroups(Buffer.from(req))
    return ListConsumerGroupsResponse.decode(buf)
  }

  /**
   * Returns info for a specific consumer group, or `null` if it does not exist.
   *
   * @param name  Consumer group name
   */
  async getConsumerGroupInfo(name: string): Promise<ConsumerGroupInfo | null> {
    const req: GetConsumerGroupInfoRequest = { consumerGroupName: name }
    try {
      const buf = await this.#inner.getConsumerGroupInfo(
        Buffer.from(GetConsumerGroupInfoRequest.encode(req).finish()),
      )
      return ConsumerGroupInfo.decode(buf)
    } catch (err: unknown) {
      // Treat NOT_FOUND (code 5) or UNAVAILABLE (code 14) as null
      if (isGrpcNotFound(err)) return null
      throw err
    }
  }

  /**
   * Delete a consumer group by name.
   *
   * @param name  Consumer group name
   */
  async deleteConsumerGroup(name: string): Promise<DeleteConsumerGroupResponse> {
    const req: DeleteConsumerGroupRequest = { consumerGroupName: name }
    const buf = await this.#inner.deleteConsumerGroup(
      Buffer.from(DeleteConsumerGroupRequest.encode(req).finish()),
    )
    return DeleteConsumerGroupResponse.decode(buf)
  }

  /**
   * Delete all consumer groups on the account.
   */
  async deleteAllConsumerGroups(): Promise<void> {
    const { consumerGroups } = await this.listConsumerGroups()
    await Promise.all(consumerGroups.map((g) => this.deleteConsumerGroup(g.consumerGroupName)))
  }

  /**
   * Create a new consumer group.
   *
   * @param name      Consumer group name
   * @param fromSlot  Start from this slot (omit to start from latest)
   */
  async createConsumerGroup(
    name: string,
    fromSlot?: bigint,
  ): Promise<CreateConsumerGroupResponse> {
    const req: CreateConsumerGroupRequest = {
      consumerGroupName: name,
      initialOffsetPolicy: fromSlot !== undefined ? InitialOffsetPolicy.FROM_SLOT : InitialOffsetPolicy.LATEST,
      fromSlot,
    }
    const buf = await this.#inner.createConsumerGroup(
      Buffer.from(CreateConsumerGroupRequest.encode(req).finish()),
    )
    return CreateConsumerGroupResponse.decode(buf)
  }

  // ─── Chain info ───────────────────────────────────────────────────────────

  /** Returns the current chain tip (max committed offsets per shard). */
  async getChainTip(): Promise<GetChainTipResponse> {
    const req: GetChainTipRequest = { blockchainId: new Uint8Array(16) }
    const buf = await this.#inner.getChainTip(
      Buffer.from(GetChainTipRequest.encode(req).finish()),
    )
    return GetChainTipResponse.decode(buf)
  }

  /** Returns the available slot range on the service. */
  async getSlotRange(): Promise<GetSlotRangeResponse> {
    const buf = await this.#inner.getSlotRange()
    return GetSlotRangeResponse.decode(buf)
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function isGrpcNotFound(err: unknown): boolean {
  if (err == null || typeof err !== 'object') return false
  const code = (err as Record<string, unknown>).code
  return code === 5 || code === 14
}
