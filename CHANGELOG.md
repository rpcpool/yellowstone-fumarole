# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Features

### Fixes

## [0.1.1+solana.2.1.11]

### Fixes

- Fixed build error when download from crates.io: required proto folders during compilation are now included during `cargo package` using symlinks.

### Features

- Supports Gzip compression
- Support custom metadata headers in yaml configuration file through `x-metadata` mapping.
- Supports both `x_token` and `x-token` field in the configuration format.

## [0.10.0+solana.2.1.11]

Initial beta release of Fumarole
