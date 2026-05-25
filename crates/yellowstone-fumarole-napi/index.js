'use strict'

// Load the platform-specific native addon.
// After `napi build`, the @napi-rs/cli regenerates this file with full
// platform/architecture detection. Until then, try the direct .node path.
let binding
try {
  binding = require('./yellowstone-fumarole-napi.node')
} catch {
  throw new Error(
    'yellowstone-fumarole-napi: native module not found. ' +
      'Run `npm run build` inside crates/yellowstone-fumarole-napi first.'
  )
}

const { FumaroleClient: _FumaroleClient, FumaroleSubscription: _FumaroleSubscription } = binding

/**
 * An active Fumarole subscription.
 *
 * Implements the async iterator protocol so you can consume events with
 * `for await`:
 *
 * ```js
 * for await (const event of subscription) {
 *   if (event.isSlotEnded) continue
 *   const update = SubscribeUpdate.decode(event.update)
 *   // …
 * }
 * ```
 */
class FumaroleSubscription {
  /** @type {InstanceType<typeof _FumaroleSubscription>} */
  #inner

  /** @param {InstanceType<typeof _FumaroleSubscription>} inner */
  constructor(inner) {
    this.#inner = inner
  }

  /**
   * Returns the next event, or `null` when the stream is exhausted.
   *
   * @returns {Promise<{slot: bigint, isSlotEnded: boolean, update?: Buffer} | null>}
   */
  next() {
    return this.#inner.next()
  }

  /**
   * Update active subscription filters.
   *
   * @param {Buffer} request  Protobuf-encoded `geyser.SubscribeRequest`
   * @returns {Promise<void>}
   */
  send(request) {
    return this.#inner.send(request)
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      const event = await this.next()
      if (event === null) break
      yield event
    }
  }
}

/**
 * A client connected to a Fumarole service.
 *
 * ```js
 * const client = await FumaroleClient.connect({ endpoint: 'https://…', xToken: '…' })
 * const sub = await client.subscribe('my-subscriber', encodeSubscribeRequest(…))
 * for await (const event of sub) {
 *   // …
 * }
 * ```
 */
class FumaroleClient {
  /** @type {InstanceType<typeof _FumaroleClient>} */
  #inner

  /** @param {InstanceType<typeof _FumaroleClient>} inner */
  constructor(inner) {
    this.#inner = inner
  }

  /**
   * Connect to a Fumarole service.
   *
   * @param {{ endpoint: string, xToken?: string, maxDecodingMessageSizeBytes?: number }} config
   * @returns {Promise<FumaroleClient>}
   */
  static async connect(config) {
    const inner = await _FumaroleClient.connect({
      endpoint: config.endpoint,
      xToken: config.xToken,
      maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
    })
    return new FumaroleClient(inner)
  }

  /**
   * Returns a protobuf-encoded `VersionResponse` Buffer.
   * @returns {Promise<Buffer>}
   */
  version() {
    return this.#inner.version()
  }

  /**
   * Subscribe with default configuration.
   *
   * @param {string} subscriberName  Persistent subscriber name
   * @param {Buffer} request         Protobuf-encoded `geyser.SubscribeRequest`
   * @returns {Promise<FumaroleSubscription>}
   */
  async subscribe(subscriberName, request) {
    const inner = await this.#inner.subscribe(subscriberName, request)
    return new FumaroleSubscription(inner)
  }

  /**
   * Subscribe with custom tuning.
   *
   * @param {string} subscriberName  Persistent subscriber name
   * @param {Buffer} request         Protobuf-encoded `geyser.SubscribeRequest`
   * @param {{ numDataPlaneTcpConnections?: number, concurrentDownloadLimitPerTcp?: number, commitIntervalMs?: number, maxFailedSlotDownloadAttempt?: number, gcInterval?: number, slotMemoryRetention?: number, noCommit?: boolean, autoCommit?: boolean }} config
   * @returns {Promise<FumaroleSubscription>}
   */
  async subscribeWithConfig(subscriberName, request, config) {
    const inner = await this.#inner.subscribeWithConfig(subscriberName, request, {
      numDataPlaneTcpConnections: config.numDataPlaneTcpConnections,
      concurrentDownloadLimitPerTcp: config.concurrentDownloadLimitPerTcp,
      commitIntervalMs: config.commitIntervalMs,
      maxFailedSlotDownloadAttempt: config.maxFailedSlotDownloadAttempt,
      gcInterval: config.gcInterval,
      slotMemoryRetention: config.slotMemoryRetention,
      noCommit: config.noCommit,
      autoCommit: config.autoCommit,
    })
    return new FumaroleSubscription(inner)
  }

  /**
   * @param {Buffer} request  Protobuf-encoded `ListConsumerGroupsRequest`
   * @returns {Promise<Buffer>}  Protobuf-encoded `ListConsumerGroupsResponse`
   */
  listConsumerGroups(request) {
    return this.#inner.listConsumerGroups(request)
  }

  /**
   * @param {Buffer} request  Protobuf-encoded `GetConsumerGroupInfoRequest`
   * @returns {Promise<Buffer>}  Protobuf-encoded `ConsumerGroupInfo`
   */
  getConsumerGroupInfo(request) {
    return this.#inner.getConsumerGroupInfo(request)
  }

  /**
   * @param {Buffer} request  Protobuf-encoded `DeleteConsumerGroupRequest`
   * @returns {Promise<Buffer>}  Protobuf-encoded `DeleteConsumerGroupResponse`
   */
  deleteConsumerGroup(request) {
    return this.#inner.deleteConsumerGroup(request)
  }

  /**
   * @param {Buffer} request  Protobuf-encoded `CreateConsumerGroupRequest`
   * @returns {Promise<Buffer>}  Protobuf-encoded `CreateConsumerGroupResponse`
   */
  createConsumerGroup(request) {
    return this.#inner.createConsumerGroup(request)
  }

  /**
   * @param {Buffer} request  Protobuf-encoded `GetChainTipRequest`
   * @returns {Promise<Buffer>}  Protobuf-encoded `GetChainTipResponse`
   */
  getChainTip(request) {
    return this.#inner.getChainTip(request)
  }

  /**
   * @returns {Promise<Buffer>}  Protobuf-encoded `GetSlotRangeResponse`
   */
  getSlotRange() {
    return this.#inner.getSlotRange()
  }
}

module.exports = { FumaroleClient, FumaroleSubscription }
