# @triton-one/yellowstone-fumarole-client

TypeScript client for the [Yellowstone Fumarole](https://github.com/rpcpool/yellowstone-fumarole) service — a high-throughput Solana data streaming platform built on top of Geyser.

## Installation

```sh
npm install @triton-one/yellowstone-fumarole-client
```

Requires Node.js >= 22. The native binary is installed automatically via an optional dependency for your platform.

### Supported platforms

Prebuilt native binaries are currently published for `linux-x64-gnu`, `darwin-arm64`, and `win32-x64-msvc` only. `linux-x64-musl` (e.g. `node:*-alpine` images) and `linux-arm64` are not published yet — use a glibc-based base image (e.g. `node:22-slim`, `node:22-bookworm`) or an x64 host in the meantime.

### TLS / CA certificates

The native client uses [`rustls`](https://github.com/rustls/rustls) with the **OS's trust store**, unlike `@grpc/grpc-js`, which bundles Mozilla's root CAs. On a minimal base image without system CA certificates installed, connecting fails with `failed to configure TLS`. On a Debian/Ubuntu-based image (e.g. `node:22-slim`), install them explicitly:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
```

## Quick start

```ts
import {
  FumaroleClient,
  CommitmentLevel,
  SubscribeRequest,
} from '@triton-one/yellowstone-fumarole-client'

const client = await FumaroleClient.connect({
  endpoint: 'https://fumarole.triton.one',
  xToken: process.env.FUMAROLE_X_TOKEN,
})

const request: SubscribeRequest = {
  commitment: CommitmentLevel.CONFIRMED,
  accounts: {},
  transactions: {
    all: { accountInclude: [], accountExclude: [], accountRequired: [] },
  },
  slots: { all: { filterByCommitment: true } },
  transactionsStatus: {},
  blocks: {},
  blocksMeta: {},
  entry: {},
  accountsDataSlice: [],
}

const subscription = await client.subscribe('my-subscriber', request)

for await (const event of subscription) {
  if (event.type === 'slotEnded') {
    console.log('slot completed:', event.slot)
    continue
  }
  // event.type === 'data'
  console.log('slot:', event.slot, 'update:', event.update)
}
```

## Connecting

```ts
const client = await FumaroleClient.connect({
  endpoint: 'https://fumarole.triton.one', // required
  xToken: 'your-token',                    // optional auth token
  maxDecodingMessageSizeBytes: 512 * 1024 * 1024, // optional, default 512 MB
})
```

## Subscribing

### Basic subscribe

```ts
const subscription = await client.subscribe('my-subscriber', request)
```

The `subscriberName` is a persistent identifier — Fumarole tracks your offset under this name so you can resume from where you left off after a restart.

### Subscribe with tuning options

```ts
const subscription = await client.subscribeWithConfig('my-subscriber', request, {
  numDataPlaneTcpConnections: 1,    // parallel TCP connections (default: 1)
  commitIntervalMs: 10_000,         // offset commit interval in ms (default: 10 000)
  maxFailedSlotDownloadAttempt: 3,  // failures before session fails (default: 3)
  gcInterval: 100,                  // GC tick interval (default: 100)
  slotMemoryRetention: 1_000,       // dedup window size in slots (default: 1 000)
  noCommit: false,                  // disable offset commits
  autoCommit: true,                 // commit after every event
})
```

### Consuming events

```ts
for await (const event of subscription) {
  if (event.type === 'slotEnded') {
    // All updates for this slot have been delivered
    console.log('slot done:', event.slot)
    continue
  }

  // event.type === 'data'
  const { slot, update } = event

  if (update.account)     console.log('account update', update.account)
  if (update.transaction) console.log('transaction',    update.transaction)
  if (update.slot)        console.log('slot update',    update.slot)
  if (update.block)       console.log('block',          update.block)
  if (update.blockMeta)   console.log('block meta',     update.blockMeta)
  if (update.entry)       console.log('entry',          update.entry)
}
```

### Updating filters on a live subscription

```ts
await subscription.updateFilters({
  ...request,
  transactions: {
    specific: {
      accountInclude: ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
      accountExclude: [],
      accountRequired: [],
    },
  },
})
```

### Closing a subscription

```ts
subscription.close() // or subscription.cancel() — same thing
```

Tears the subscription down deterministically: any in-flight or future call to `next()` resolves
to `null` right away instead of hanging or erroring. Useful for reconnect logic that needs to give
up on a stalled subscription without waiting on the server.

## Consumer group management

Fumarole tracks your read position server-side under a named consumer group. The `subscriberName` you pass to `subscribe` is the consumer group name.

```ts
// List all consumer groups on the account
const { consumerGroups } = await client.listConsumerGroups()

// Get info for a specific group (returns null if not found)
const info = await client.getConsumerGroupInfo('my-subscriber')

// Create a group starting from the latest offset
await client.createConsumerGroup('my-subscriber')

// Create a group starting from a specific slot
await client.createConsumerGroup('my-subscriber', 350_000_000n)

// Delete a specific group (resets the offset)
await client.deleteConsumerGroup('my-subscriber')

// Delete all groups on the account
await client.deleteAllConsumerGroups()
```

## Miscellaneous

```ts
// Service version
const { version } = await client.version()

// Available slot range on the service
const range = await client.getSlotRange()
```

## Error handling

RPC failures reject with a plain `Error`. Because they cross a native (Rust/NAPI) boundary,
`.code` is always `'GenericFailure'` — it is **not** the gRPC status code. The gRPC status is
instead embedded in the message as a stable, documented prefix; use `getGrpcStatusCode` (or
`parseGrpcError` for the full breakdown) instead of matching on the message text directly:

```ts
import { getGrpcStatusCode, GrpcStatus } from '@triton-one/yellowstone-fumarole-client'

try {
  await client.deleteConsumerGroup('my-subscriber')
} catch (err) {
  if (getGrpcStatusCode(err) === GrpcStatus.NOT_FOUND) {
    // already deleted
  } else {
    throw err
  }
}
```

## Subscription filters reference

All fields of `SubscribeRequest` are optional except `commitment`. Set a field to an empty object `{}` to include it with no filter, or omit it entirely to exclude that update type.

| Field | Type | Description |
|---|---|---|
| `commitment` | `CommitmentLevel` | `PROCESSED`, `CONFIRMED`, or `FINALIZED` |
| `accounts` | `Record<string, SubscribeRequestFilterAccounts>` | Account updates |
| `transactions` | `Record<string, SubscribeRequestFilterTransactions>` | Transaction updates |
| `transactionsStatus` | `Record<string, SubscribeRequestFilterTransactions>` | Transaction status only |
| `slots` | `Record<string, SubscribeRequestFilterSlots>` | Slot updates |
| `blocks` | `Record<string, SubscribeRequestFilterBlocks>` | Full block updates |
| `blocksMeta` | `Record<string, SubscribeRequestFilterBlocksMeta>` | Block metadata only |
| `entry` | `Record<string, SubscribeRequestFilterEntry>` | Entry updates |
| `accountsDataSlice` | `SubscribeRequestAccountsDataSlice[]` | Slice account data |

## Examples

See the [examples/](examples/) directory:

- [`subscribe-firehose.ts`](examples/src/subscribe-firehose.ts) — subscribe to all accounts and transactions
- [`subscribe-token-transactions.ts`](examples/src/subscribe-token-transactions.ts) — filter transactions by program
- [`list-consumer-groups-with-group-info.ts`](examples/src/list-consumer-groups-with-group-info.ts) — list and inspect consumer groups

To run an example:

```sh
cd examples
cp .env.example .env   # fill in FUMAROLE_ENDPOINT and FUMAROLE_X_TOKEN
npm install
npm run subscribe-firehose
```
