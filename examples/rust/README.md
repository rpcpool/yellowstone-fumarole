# Fumarole rust examples

## Consumer groups

Fumarole supports consumer groups, designed to scale reads similarly to Kafka consumer groups. However, Fumarole specifically implements static consumer groups.

When creating a consumer group, you can define the number of parallel readers, with a maximum limit of six. In a group with six members, each member is assigned a subset of blockchain event partitions. Every member maintains its own offset tracker, which records its position in the Fumarole log.

To ensure proper operation, all members must remain active. If any member becomes stale, the entire consumer group is considered stale.


## Configuration file

Inside a YAML file, provide the following configuration:

```yaml
endpoint: https://fumarole.endpoint.com
x_token: <YOUR X_TOKEN>
```

## Create a consumre group

Install [fume](../../fume/README.md).

Use the following `fume` command:

```sh
fume --config <PATH/TO/CONFIG.YAML> create-cg --name test --size 1
```
Here `--size` indicates how many members is in the group.
Size of `1` means only one TCP connection can exist at all time to Fumarole.
Size of `1` means only one member exists in the group, therefore all partitions are assigned to it.

**NOTE**: When you connect to fumarole, a distributed lock is acquire by your TCP connection, preventing you
reading from the same member twice.

## Subscribe

```sh
cargo run --bin client -- --config <PATH/TO/CONFIG YAML> subscribe --cg-name <CONSUMER-GROUP-NAME>
```

## Dragonsmouth vs Fumarole


|| gRPC | Persisted | Redundant | 
|-------|------|-----------|-----------|
| Fumarole | ✅ | ✅ | ✅ |
| Dragonsmouth | ✅ | ❌ | ❌ |  


**Persisted** : If you drop your connection with Fumarole and reconnect within a reasonnable amount of
time, you won't loose any data. You restart right where you left off.

**Redundant** : Fumarole backend is driven by multiple RPC Nodes and partition Geyser event so it scale
horizontally both for reads and writes.

**gRPC**: Fumarole subscribe stream outs _Dragonsmouth_ `SubscribeUpdate` object so the learning curve
for fumarole stays low and can integrate easily into your code without too much changes.


**Note**: You don't have to do anything to benefits from redundancy and persistence. It is done
in the backend for you.

## Breaking changes with Dragonsmouth

Fumarole does not support the same filter API as Dragonsmouth yet.

Uses the `yellowstone_fumarole_client::SubscribeRequestBuilder` to build custom subscribe request.

Fumarole supports the following filters:

- `AccountUpdate` pubkey set filter
- `AccountUpdate` owner set filter
- `TransactionUpdate` reference accounts keys filter

Here's a Rust example to filter all accounts update where the owner is the tokenkeg account:

```rust

let tokenkeg = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
    .with_owners(Some(vec![tokenkeg]))
    .build();
```

Here's a how to subscribe to account updates whose pubkey is contains in a pubkey set.

```rust

let pubkey1: Pubkey = ...;
let pubkey2: Pubkey = ...;
let pubkey3: Pubkey = ...;

let pubkeyset = vec![pubkey1, pubkey2, pubkey3];

let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
    .with_accounts(Some(pubkeyset))
    .build();
```

Here's how to subscribe to transaction that contains a list of pubkeys:

```rust
let pubkey1: Pubkey = ...;
let pubkey2: Pubkey = ...;
let pubkey3: Pubkey = ...;

let pubkeyset = vec![pubkey1, pubkey2, pubkey3];

let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
    .with_tx_accounts(Some(pubkeyset))
    .build();
```


You can mix-and-match any of those:

```rust
let pubkey1: Pubkey = ...;
let pubkey2: Pubkey = ...;
let pubkey3: Pubkey = ...;

let tokenkeg = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
let pubkeyset = vec![pubkey1, pubkey2, pubkey3];

let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
    .with_owners(Some(vec![tokenkeg]))
    .with_tx_accounts(Some(pubkeyset))
    .build();
```


