
# Yellowstone Fumarole CLI

Fumarole CLI tool

## Install

```sh
$ cargo install yellowstone-fumarole-cli
```

## Usage

```sh
fume --help

Yellowstone Fumarole CLI

Usage: fume [OPTIONS] --config <CONFIG> <COMMAND>

Commands:
  test-config  Test the connection to the fumarole service
  get-info     Get Persistent Subscriber Info
  create       Create Persistent Subscriber
  delete       Delete a Persistent Subscriber
  list         List all persistent subscribers
  delete-all   Delete all persistent subscribers
  subscribe    Subscribe to fumarole events
  help         Print this message or the help of the given subcommand(s)

Options:
      --config <CONFIG>  Path to static config file
  -v, --verbose...       Increase logging verbosity
  -q, --quiet...         Decrease logging verbosity
  -h, --help             Print help
  -V, --version          Print version
```


### Configuration file

Here's how to configure your config file:

```toml
[fumarole]
endpoints = ["https://fumarole.endpoint.rpcpool.com"]
x-token = "00000000-0000-0000-0000-000000000000"
```

You can test your configuration file with `test-config` subcommand:

```sh
$ fume --config path/to/config.toml test-config
```

or with custom config path:

```sh
$ fume --config path/to/config.toml test-config
```

### Create a Persistent Subscriber


```sh
$ fume create --name helloworld-1 \
```

### List all persistent subscribers

```sh
$ fume list
```

### Delete a persistent subscribers

```sh
$ fume delete --name helloworld
```

### Delete all persistent subscribers

```sh
$ fume delete-all
```

### Stream summary on terminal

To stream out from the CLI, you can use the `stream` command and its various features!

```sh
$ fume subscribe --name helloworld
```

You can filter the stream content by adding one or multiple occurrence of the following options:

- `--tx-pubkey <base58 pubkey>` : filter transaction by account keys.
- `--owner <base58 pubkey>` : filter account update based on its owner
- `--account <base58 pubkey>` : filter account update based on accout key. 

Here is an example to get all account updates owned by Token SPL program:

```sh
$ fume subscribe --name helloworld \
--owner TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

Here is how to chain multiple filters together:
 
```sh
$ fume subscribe --cg-name helloworld \
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