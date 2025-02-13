
# Yellowstone-Fume

Fumarole CLI tool

## Install


```sh
$ pip install triton-fume
```

## Usage

### Configuration file

Fumarole CLI look for a file in `~/.config/fume/config.toml` by default, you can change the path location by using `fume --config <PATH>`.

Here's how to configure your config file:

```toml
[fumarole]
endpoints = ["https://fumarole.endpoint.rpcpool.com"]
x-token = "<YOUR X-TOKEN secret here>"
```

You can test your configuration file with `test-config` subcommand:

```sh
$ fume test-config
```

or with custom config path:

```sh
$ fume --config path/to/config.toml test-config
```

### Create consumer group

To create a consumer group that at the end of the log, that stream only "confirmed" commitment level transaction:

```sh
$ fume create-cg --name helloworld-1 \
--commitment confirmed \
--seek latest \
--include tx
```

To do the same but for account update

```sh
$ fume create-cg --name helloworld-2 \
--commitment confirmed \
--seek latest \
--include account
```

More usage can be find using the `--help` options:

```sh
$ fume create-cg --help
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

### Consumer Group Staleness

Consumer groups can become stale if you are ingesting too slowly.

Fumarole is a distributed log of blockchain event where each new blockchain event gets appended to.

As Solana emits a lot of event in one hour, we cannot keep every blockchain event forever.

Fumarole evicts fragment of the log as they age old enough.

Depending of the Fumarole cluster you are connected to this time may vary. Connect Triton-One team to learn more.

When creating a Consumer Group, you must ingest what you are capable of. Otherwise your consumer group is destined to become stale.

A stale consumer group is a consumer group that haven't yet ingested blockchain event that had already been evicted by Fumarole vacuum process.


### Consumer Group Size and performance guideline

Consumer group size allow you to shard a fumarole stream into multiple consumer group member.
Sharded consumer group follow similar semantics as [Kafka Static Consumer membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances).

Here's a quick-recap of static group membership:

- The Fumarole log is already sharded in multiple partitions.
- When you create a consumer group with `--size N`, it creates `N` member with each `# total fumarole partition / N` partitions.
- Each member of the cnsumer group advance **at its own pace**.
- Your consumer group becomes stale as soon as **one membership is stale**.


As of this writing the maximum size of a consumer group is `6`.

Each member can have their own dedicated TCP connection which offer better performance.

The processing you do in reception, your internet speed, network bandwidth and location will impact the size of the consumer group.

Ingesting everything Fumarole can output requires you to be in the same region as your assigned Fumarole cluster and multiple Gbits for internet Bandwidth, otherwise you will fall behind and become stale.

Limit your subscription feed by using the various filters over the accounts and transactions we offer.

As for the consumer group size goes, starting with a size of `1` is the simplest approach.

If you are falling behind because your receiving code adds too much processing overhead, you can try
`2`, `3` and so forth.

Fumarole is already redundant and load balanced inside our Data centers, increasing `--size` does not inherently add more redundancy. It is a tool for you to scale your read operation in case on instance is not sufficient.

To create a consumer group with `2` members you just have to provided `--size` options:

```sh
$ fume create-cg --name example --size 2
```

### List all consumer groups

```sh
$ fume list-cg
```

### Delete a consumer groups

```sh
$ fume delete-cg --name helloworld
```

### Delete all consumer groups

```sh
$ fume delete-all-cg
```

### Stream summary on terminal

To stream out from the CLI, you can use the `stream` command and its various features!

```sh
$ fume stream --name helloworld
```

You can filter the stream content by adding one or multiple occurrence of the following options:

- `--tx-account <base58 pubkey>` : filter transaction by account keys.
- `--owner <base58 pubkey>` : filter account update based on its owner
- `--account <base58 pubkey>` : filter account update based on accout key. 

Here is an example to get all account updates owned by Token SPL program:

```sh
$ fume stream --name helloworld \
--owner TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

Here is how to chain multiple filters together:
 
```sh
$ fume stream --name helloworld \
--owner metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s \
--owner TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb \
--owner TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA \
--owner ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL \
--owner BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY \
--owner CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d \
--tx-account BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY
```

The above command stream all data required by [DAS](https://github.com/rpcpool/digital-asset-validator-plugin).

**Note**: This command serves more as a testing tool/playground for you to try it out as it only prints summarized data.

## Development mode

First git clone:

```sh
git clone --recursive https://github.com/rpcpool/triton-fume.git
```

Initialize poetry project:

```sh
$ poetry init
```

Install in dev mode:

```sh
poetry install
```

Test in fume CLI works by printing its version:

```sh
$ poetry run fume version
0.1.0
```

