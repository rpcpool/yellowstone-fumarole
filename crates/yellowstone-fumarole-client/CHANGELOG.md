# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

### Fixes

## [0.9.1]

### Fixes

- Sharded downloads no longer drop distinct updates while deduplicating retried shards:
  - account writes are told apart by `write_version` instead of the transaction signature, so an account written several times in one block outside any transaction (the Clock sysvar, the leader's vote account, EpochRewards at an epoch boundary) is no longer cut down to its first write;
  - a `TransactionStatus` and the `Transaction` with the same index are both delivered when a request sets both the transactions and transactions-status filters;
  - `Slot`, `Block` and `EntryUpdateParent` updates received in a shard are delivered once instead of always dropped, and Ping/Pong always pass through.
- Corrected the `BlockStream` docs: block metadata is delivered inside the `Block` event, not as a separate event.

## [0.9.0]

### Features

- Alpenglow block footer support: add a `block_footer` filter to the subscribe request to receive each block's footer. It is emitted once every shard of the block is downloaded, in the order footer, block meta, `SlotEnded`. Certificates are stripped unless a filter sets `include_certificates`. A block whose footer never arrives still completes.

### Breaking

- Moved to `yellowstone-grpc-proto` 14 and `yellowstone-grpc-client` 15. `SubscribeRequest` is part of the public API, so callers must move to `yellowstone-grpc-proto` 14 as well.

## [0.8.0]

### Features

- Fork awareness: downloads and commitment progress are tracked per bank `(slot, blockhash)` instead of per slot. Two forks of the same slot are each downloaded once, get their own slot statuses, and are never deduplicated against each other. Dead slots are never downloaded.

### Breaking

- `FumaroleEvent::Data` and `FumaroleEvent::SlotEnded` carry the bank's `blockhash` (`None` when the backend does not report it) and `SlotEnded` is now a struct variant: match it as `SlotEnded { slot, .. }`. `FumaroleBlockEvent` and the slot status events carry the blockhash too.
- Moved to Solana 4 crates (`solana-clock`/`solana-pubkey` 4), `yellowstone-grpc-proto` 13 and `yellowstone-grpc-client` 14. The version no longer carries the `+solana.N` suffix.

## [0.7.1+solana.3]

### Fixes

- Classify h2 stream truncation (`Code::Unknown`, plus `Aborted`/`ResourceExhausted`/`Cancelled`/`DeadlineExceeded`/`transport::Error` sources) as a recoverable control plane disconnect instead of a fatal application error, so proxy/load-balancer-truncated streams go through the existing rejoin-with-backoff logic.
- Control plane write failures (history poll, offset commit) now trigger the same rejoin-with-backoff path as read failures instead of panicking.
- Fixed head-of-line blocking in the sharded download orchestrator's pending-download drain, where one congested downloader lane could stall intake for every other lane.

## [0.7.0+solana.3]

### Features

- Support the `transactions_status` filter in the subscribe request.

### Fixes

- The data channel now uses the configured `data_channel_capacity` instead of a fixed default.

## [0.6.0+solana.3]

### Breaking

- A subscription's commitment level is now a minimum: a slot is delivered once it reaches at least the requested commitment, instead of only when its status equals it exactly.

### Features

- The new `yellowstone-fumarole-napi` crate exposes this client to Node.js; the TypeScript client is built on it.

## [0.5.0+solana.3]

### Features

- New stream API: `BlockStream`, `SlotSequentialStream` and `DragonsmouthLike` adapters over the subscription.
- Manual offset commits.

### Breaking

- The stream API was redesigned around the adapters above.

## [0.4.0+solana.3]

### Features

- Sharded block download and zstd compression are enabled by default.
- Upgraded to `yellowstone-grpc` 12.

## [0.3.2+solana.3]

### Features

- Shard download requests carry the slot as a hint to the backend.

## [0.3.1+solana.3]

### Fixes

- The legacy downloader's concurrent download limit now scales with the number of data plane connections. Defaults changed to 4 parallel data streams and 2 concurrent downloads per connection.

## [0.3.0+solana.3]

### Breaking

- Upgraded dependencies to Solana 3.

## [0.2.3+solana.2]

### Features

- Experimental sharded block download, enabled with `experimental_enable_sharded_block_download`.

### Fixes

- A block the backend reports as not found is skipped instead of failing the subscription.

## [0.2.2+solana.2]

- Removed panic when the grpc download task schedule closed due to organic reason.

## [0.2.1+solana.2]

### Fixes

- Add missing `mark_event_as_processed` in `pop_next_slot_status`


