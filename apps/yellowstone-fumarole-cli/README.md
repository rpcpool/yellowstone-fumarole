
# Yellowstone Fumarole CLI

Fumarole CLI tool

## Install

```sh
$ cargo install yellowstone-fumarole-cli
```

## Usage

```sh
fume help
Yellowstone Fumarole CLI

Usage: fume [OPTIONS] <COMMAND>

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
      --config <CONFIG>  Path to the config file. If not specified, the default config file will be used. The default config file is ~/.fumarole/config.yaml. You can also set the FUMAROLE_CONFIG environment variable to specify the config file. If the config file is not found, the program will exit with an error
  -v, --verbose...       Increase logging verbosity
  -q, --quiet...         Decrease logging verbosity
  -h, --help             Print help
  -V, --version          Print version
```


## Configuration file

Here's how to configure your config file:

```toml
[fumarole]
endpoints = ["https://fumarole.endpoint.rpcpool.com"]
x-token = "00000000-0000-0000-0000-000000000000"
```

You can test your configuration file with `test-config` subcommand:

```sh
fume --config path/to/config.toml test-config
```

By default, if you don't provide `--config`, fumarole CLI will use the value at `FUMAROLE_CONFIG` environment variable if set, 
otherwise fallback to `~/.fumarole/config.yaml`.


```sh
export FUMAROLE_CONFIG=path/to/config.toml
fume test-config
Successfully connected to Fumarole Service
```


## Create a Persistent Subscriber

```sh
fume create --name helloworld-1 \
```

## List all persistent subscribers

```sh
fume list
```

## Delete a persistent subscribers

```sh
fume delete --name helloworld
```

## Delete all persistent subscribers

```sh
fume delete-all
```

## Stream summary on terminal

To stream out from the CLI, you can use the `stream` command and its various features!

```sh
fume subscribe --name helloworld
```

### Filters

You can filter by event type : account, transaction, slot status and block meta:

```sh
fume subscribe --name <SUBSCRIBER_NAME> --include tx  # This will only stream out transaction
```

```sh
fume subscribe --name <SUBSCRIBER_NAME> --include account,slot  # This will only stream out account update and slot status
```

```sh
 fume subscribe --name <SUBSCRIBER_NAME> --include all  # This will stream everything : account update, transactions, slot status and block meta update.
```

You can also filter incoming Geyser events by account pubkeys, account owners and transaction account inclusion.

Here is an example to get all account updates owned by Token SPL program:

```sh
fume subscribe --name helloworld \
--owner TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

Here is how to chain multiple filters together:
 
```sh
fume -- subscribe --name test1 \
--include tx,account
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


### Enabling Prometheus metrics

When subscribing, you can enable prometheus metrics and bind to a port to view fumarole related metrics into HTML format.

```sh
fume subscribe --name test1 --prometheus 0
```

Using `--prometheus 0` this will bind to a random port on `127.0.0.1`.

You can specify the address like this:

```sh
fume subscribe --name test1 --prometheus 127.0.0.1:9999
```