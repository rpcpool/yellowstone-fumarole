# Fumarole Typescript client

This repo contains Fumarole Typescript client

The fumarole client is based on the reactive framework called [Rxjs](https://rxjs.dev/guide/overview).

## Usage

You can view examples in [here](https://github.com/rpcpool/yellowstone-fumarole/tree/main/examples/typescript).

### Create a Persistent Subscriber and consumes it.

```ts

const FUMAROLE_ENDPOINT = process.env.FUMAROLE_ENDPOINT!;
const FUMAROLE_X_TOKEN = process.env.FUMAROLE_X_TOKEN!;
const TOKEN_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

let subscriberName: string = "helloworld"
let client: FumaroleClient | undefined;

const config = {
    endpoint: FUMAROLE_ENDPOINT,
    xToken: FUMAROLE_X_TOKEN,
    maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
};

client = await FumaroleClient.connect(config);

const request: SubscribeRequest = {
    commitment: CommitmentLevel.PROCESSED,
    accounts: {
        token: {
        account: [],
        owner: [TOKEN_ADDRESS],
        filters: [],
        }
    },
    transactions: {
        token: {
        accountInclude: [TOKEN_ADDRESS],
        accountExclude: [],
        accountRequired: [],
        }
    },
    slots: {
        test: {
        filterByCommitment: true,
        }
    },
    transactionsStatus: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    ping: { id: Date.now() },
    accountsDataSlice: [],
    fromSlot: undefined,
};



await client.createPersistentSubscriber({
    consumerGroupName: subscriberName,
    initialOffsetPolicy: InitialOffsetPolicy.LATEST,
});


const {
    sink: _sink,
    source
} = await client.dragonsmouthSubscribe(
    groupName,
    request,
);


await source.forEach((update: SubscribeUpdate) => {
    // Do something with subscribe update
});

```


### Logging supports

You can set logging level via `FUMAROLE_LOG_LEVEL=<level>` environment variable.

Fumarole library supports [winston Logger](https://www.npmjs.com/package/winston) 
and need to be initialized at the root of your software:

```ts
import dotenv from "dotenv";
import {
    setDefaultFumaroleLogger
} from "@triton-one/yellowstone-fumarole";

// Load environment variable if dotenv supported.
dotenv();
// Will read FUMAROLE_LOG_LEVEL
// MUST BE DONE BEFORE USING FUMAROLE LIB
setDefaultFumaroleLogger();
```

By default, the Fumarole logger output JSON formatted output to the console.
One can change this behaviour, using `setCustomFumaroleLogger` which supports `winston.Logger`:

```ts
import dotenv from "dotenv";
import {
    setCustomFumaroleLogger
} from "@triton-one/yellowstone-fumarole";

// Load environment variable if dotenv supported.
dotenv();

// MUST BE DONE BEFORE USING FUMAROLE LIB
setCustomFumaroleLogger(winston.createLogger({
  level: process.env.FUMAROLE_CLIENT_LOG_LEVEL || "silent",
  format: winston.format.cli(),
  transports: [new winston.transports.Console()],
}));
```