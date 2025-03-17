# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Breaking Changes

### Features

### Fixes

## [0.2.0]

### Fixes

- Fixed blocking Ctlr+C where subprocess would block on `next(subscriber_stream)` until it got timeout by gRPC. Now Ctrl+C gets handled via a `sigint_handler` to interrupt gRPC stream.

### Features

- Enable gRPC GZIP compression by supports via `toml` configuration `compression` which can either be `gzip` or `none` (default).

## [0.1.0]

Initial release