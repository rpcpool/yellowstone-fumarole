# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

### Fixes

### Breaking

## [0.2.0]

### Features

- `yellowstone_fumarole_client.DragonsmouthAdapterSession` implements async contextmanager [#30](https://github.com/rpcpool/yellowstone-fumarole/issues/30)
- Supports for gzip compression in gRPC response [#21](https://github.com/rpcpool/yellowstone-fumarole/issues/21)
- Exposes low-level metrics about fumarole session [#32](https://github.com/rpcpool/yellowstone-fumarole/issues/32)

### Fixes

- Added missing `asyncio.Queue.shutdown` calls to all queue managed by `yellowstone_fumarole_client.runtime.aio` module.
- Fixed protobuf definition [#28][https://github.com/rpcpool/yellowstone-fumarole/issues/28]

### Breaking

- `yellowstone_fumarole_client.DragonsmouthAdapterSession.source` returns `AsyncGenerator` instead of `asyncio.Queue`. [#31](https://github.com/rpcpool/yellowstone-fumarole/issues/31)
- Migrated `protobuf` from `^5` to `^6.32.0` 

## [0.1.0]

Initial release