# yellowstone-fumarole

Public repo for Yellowstone-Fumarole

## Fume CLI

See more details in fume [README](fume/README.md)

## Rust client

We offer a simple fumarole Rust client crate in `crates/yellowstone-fumarole-client`.

An example can be found in `examples/rust/client.rs`.
See rust example [README](examples/rust/README.md) for more details.


## How Fumarole Works

Fumarole collects and stores geyser events from multiple RPC nodes, creating a unified log where each geyser event is stored with a unique ever increasing offset.

To enhance scalability for both reads and writes, Fumarole distributes data across multiple partitions. 

Every shard has its private sequence generator that assigned unique offset to
each geyser event.

### Consumer groups

Fumarole supports consumer groups, designed to scale reads similarly to Kafka consumer groups. However, Fumarole specifically implements static consumer groups.

When creating a consumer group, you can define the number of parallel readers, with a maximum limit of six. In a group with six members, each member is assigned a subset of blockchain event partitions. Every member maintains its own offset tracker, which records its position in the Fumarole log.

To ensure proper operation, all members must remain active. If any member becomes stale, the entire consumer group is considered stale.

Consumer group are bound to a commitment level: you must decide if you want to listen on 
`PROCESSED`, `CONFIRMED` or `FINALIZED` data.

### Parallel streaming

Consumer group with size greater than `1` allows you to streaming geyser event in paralle on two distinct TCP connection.

Each TCP connections returns different geyser event.

Each member of your consumer group gets exclusive access on a equal amount of partitions in Fumarole.

**Note**: If you create a consumer group of size > `1` you need to make sure
to consume from every member, otherwise you will eventually make your consumer group stale.

### Stale consumer group

Fumarole has a retention policy of two (`2`) hours.

If you create a consumer group and one of the member still points to stale offset, the whole consumer group will become stale.

Once a consumer group is stale you cannot use it anymore and must delete it using [fume](fume).

### Consumer group offset commitment

Fumarole uses automatic offset commitment and stores your offset every `500ms`.

Later version of Fumarole will support manual offset commit which better precision and
removes the risk of skipping data during transmission failure.

### Creating a consumer group

When you crate a consumer group you must provide the following information:

1. The name of the group
2. The size of the group: maximum of `6`
3. What you want to listen to: `account`, `tx` or `both`
4. Initial offset:
    - LATEST: start at the peek of the log
    - EARLIEST: start at the beginning of the log
    - SLOT: You provide a slot number where you want to start at. Fumarole will clip you closest to that slot.

As of right now, every customer account is limited to 15 consumer groups.

To create a consumer group, use [fume create-cg](fume) command:

```sh
fume --config <PATH/TO/CONFIG.YAML> create-cg --help
Usage: fume create-cg [OPTIONS]

  Creates a consumer group

Options:
  --name TEXT                     Consumer group name to subscribe to, if none
                                  provided a random name will be generated
                                  following the pattern
                                  'fume-<random-6-character>'.
  --size INTEGER                  Size of the consumer group
  --commitment [processed|confirmed|finalized]
                                  Commitment level  [default: confirmed]
  --include [all|account|tx]      Include option  [default: all]
  --seek [earliest|latest|slot]   Seek option  [default: latest]
  --help                          Show this message and exit.
```

### Consumer group size recommandation

You don't have to over provisionned your consumer group.
Bigger consumer group can be more complex to manager and higher risk of having staleness.

Whatever you want to consume always start with a consumer group of size `1`
and increase as you need.

The most important criteria to consume data is still your bandwidth capacity and network latency with our datacenters.


### Consumer groups limitations

- Maximum group size : 6
- Number of consumer groups per customer account: 15
- Event you can subscribe too: account updates and transactions
- Consumer group can not change commitment level once created.
- If one member of the consumer group become stale, the entire consumer group become stale.
- Stale consumer group cannot be recuperate
- Time before stale : TBD
- One TCP connection per member
- Because of partitionning, streaming geyser event are not sorted by slot.
- Fumarole deliver at-least one semantics

## Dragonsmouth vs Fumarole

|| gRPC | Persisted | Redundant | 
|-------|------|-----------|-----------|
| Fumarole | ✅ | ✅ | ✅ |
| Dragonsmouth | ✅ | ❌ | ❌ |  


**Persisted** : If you drop your connection with Fumarole and reconnect within a reasonnable amount of
time, you won't loose any data. You restart right where you left off.

**Redundant** : Fumarole backend is fed by multiple RPC Nodes and data is stored across multiple servers
allowing redundancy and better read/write scalability.

**gRPC**: Fumarole subscribe stream outs _Dragonsmouth_ `SubscribeUpdate` object so the learning curve
for fumarole stays low and can integrate easily into your code without too much changes.

**Note**: You don't have to do anything to benefits from redundancy and persistence. It is done
in the backend for you.

### Coding examples

To see the difference between Dragonsmouth and fumarole compare two files [dragonsmouth.rs](examples/rust/src/bin/dragonsmouth.rs) and
[client.rs](examples/rust/src/bin/client.rs). 

More precisely the only difference between the two is how you subscribe.


Here is Dragonsmouth:

```rust
let endpoint = config.endpoint.clone();

let mut geyser = GeyserGrpcBuilder::from_shared(endpoint)
    .expect("Failed to parse endpoint")
    .x_token(config.x_token)
    .expect("x_token")
    .tls_config(ClientTlsConfig::new().with_native_roots())
    .expect("tls_config")
    .connect()
    .await
    .expect("Failed to connect to geyser");

// This request listen for all account updates and transaction updates
let request = SubscribeRequest {
    accounts: HashMap::from(
        [("f1".to_owned(), SubscribeRequestFilterAccounts::default())]
    ),
    transactions: HashMap::from(
        [("f1".to_owned(), SubscribeRequestFilterTransactions::default())]
    ),
    ..Default::default()
};
let (_sink, mut rx) = geyser.subscribe_with_request(Some(request)).await.expect("Failed to subscribe");
```


And here's Fumarole version:


```rust
let requests = yellowstone_fumarole_client::SubscribeRequestBuilder::default()
    .build(args.cg_name);

let fumarole = FumaroleClientBuilder::connect(config)
let rx = fumarole
    .subscribe_with_request(request)
    .await
    .expect("Failed to subscribe to Fumarole service");
```


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

For more example on how to filter please look at the [proto definition](https://github.com/rpcpool/yellowstone-api/blob/main/proto/fumarole.proto)