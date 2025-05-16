# Yellowstone-Fumarole

Public repo for Yellowstone-Fumarole

## Fume CLI


```bash
cargo install yellowstone-fumarole-cli
```

See [yellowstone fumarole cli documentation](apps/yellowstone-fumarole-cli/README.md)

## Rust client

We offer a simple fumarole Rust client crate in `crates/yellowstone-fumarole-client`.

An example can be found in `examples/rust/client.rs`.
See rust example [README](examples/rust/README.md) for more details.

## Examples

See [examples/rust folder](examples/rust).

## Dragonsmouth vs Fumarole

Here's a comparison table between the Two

|| gRPC | Persisted | Stateful | 
|-------|------|-----------|-----------|
| Fumarole | ✅ | ✅ | ✅ |
| Dragonsmouth | ✅ | ❌ | ❌ |  


## Target audience

Wallet Apps, dApps, indexer.

Fumarole puts more emphasis on reliability and availability.
Compare to [yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc), slot latency will be higher.

We are aiming at three (3) slot behind on avg for Fumarole as its main purpose is to provide more reliable and forgiving geyser data source.


**NOTE**: slot latency exclude client side latency to download and process the whole slot.
slot latency refers to the difference between the chain-tip and what Fumarole has register internally so-far.