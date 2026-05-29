import type { FumaroleSubscription as NapiSubscription } from '@triton-one/yellowstone-fumarole-napi'
import { SubscribeRequest, SubscribeUpdate } from './grpc/geyser.js'

/** A decoded event from a live Fumarole subscription. */
export type FumaroleEvent =
  | { type: 'data'; slot: bigint; update: SubscribeUpdate }
  | { type: 'slotEnded'; slot: bigint }

/**
 * An active Fumarole subscription backed by the native Rust client.
 *
 * Implements `AsyncIterable<FumaroleEvent>` so you can consume events with
 * `for await`:
 *
 * ```ts
 * for await (const event of subscription) {
 *   if (event.type === 'slotEnded') continue
 *   console.log(event.slot, event.update)
 * }
 * ```
 */
export class FumaroleSubscription implements AsyncIterable<FumaroleEvent> {
  readonly #inner: NapiSubscription

  constructor(inner: NapiSubscription) {
    this.#inner = inner
  }

  /**
   * Returns the next decoded event, or `null` when the stream is exhausted.
   */
  async next(): Promise<FumaroleEvent | null> {
    const raw = await this.#inner.next()
    if (raw === null) return null

    if (raw.isSlotEnded) {
      return { type: 'slotEnded', slot: raw.slot }
    }

    const update = SubscribeUpdate.decode(raw.update!)
    return { type: 'data', slot: raw.slot, update }
  }

  /**
   * Update the active subscription filters while the stream is live.
   *
   * @param request  New filter configuration
   */
  async updateFilters(request: SubscribeRequest): Promise<void> {
    const encoded = SubscribeRequest.encode(request).finish()
    await this.#inner.send(Buffer.from(encoded))
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<FumaroleEvent, void, unknown> {
    while (true) {
      const event = await this.next()
      if (event === null) break
      yield event
    }
  }
}
