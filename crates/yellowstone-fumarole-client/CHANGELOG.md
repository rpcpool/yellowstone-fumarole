# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

### Fixes

## [0.7.1+solana.3]

### Fixes

- Classify h2 stream truncation (`Code::Unknown`, plus `Aborted`/`ResourceExhausted`/`Cancelled`/`DeadlineExceeded`/`transport::Error` sources) as a recoverable control plane disconnect instead of a fatal application error, so proxy/load-balancer-truncated streams go through the existing rejoin-with-backoff logic.
- Control plane write failures (history poll, offset commit) now trigger the same rejoin-with-backoff path as read failures instead of panicking.
- Fixed head-of-line blocking in the sharded download orchestrator's pending-download drain, where one congested downloader lane could stall intake for every other lane.

## [0.2.2+solana.2]

- Removed panic when the grpc download task schedule closed due to organic reason.

## [0.2.1+solana.2]

### Fixes

- Add missing `mark_event_as_processed` in `pop_next_slot_status`


