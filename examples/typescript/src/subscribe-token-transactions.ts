import dotenv from "dotenv";

import {
  FumaroleClient,
  SubscribeRequest,
  CommitmentLevel,
  InitialOffsetPolicy,
  setDefaultFumaroleLogger,
  SubscribeUpdate,
} from "@triton-one/yellowstone-fumarole";
import { eachValueFrom } from "rxjs-for-await";

dotenv.config();

setDefaultFumaroleLogger();

// stringify bigint in json
function safeJsonStringify(obj: unknown): string {
  return JSON.stringify(
    obj,
    (_, v) => {
      if (typeof v === "bigint") return v.toString();
      if (v instanceof Error) return v.message;
      return v;
    },
    2
  );
}

const FUMAROLE_ENDPOINT = process.env.FUMAROLE_ENDPOINT!;
const FUMAROLE_X_TOKEN = process.env.FUMAROLE_X_TOKEN!;
const TOKEN_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
// const BGUM_ADDRESS = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";
// const KAMINO_ADDRESS = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

let isShuttingDown = false;

async function main() {
  let subscriberName: string | undefined;
  let client: FumaroleClient | undefined;

  const config = {
    endpoint: FUMAROLE_ENDPOINT,
    xToken: FUMAROLE_X_TOKEN,
    maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
  };

  client = await FumaroleClient.connect(config);

  const request: SubscribeRequest = {
    commitment: CommitmentLevel.PROCESSED,
    accounts: {},
    transactions: {
      token: {
        accountInclude: [TOKEN_ADDRESS],
        accountExclude: [],
        accountRequired: [],
      },
    },
    slots: {
      test: {
        filterByCommitment: true,
      },
    },
    transactionsStatus: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    ping: { id: Date.now() },
    accountsDataSlice: [],
    fromSlot: undefined,
  };

  // delete them all because they pile up and hit limit while developing
  await client.deleteAllPersistentSubscribers();

  subscriberName = `token-monitor-${Math.random().toString(36).substring(7)}`;
  console.log(`Creating persistent subscriber: ${subscriberName}`);

  console.log("Creating persistent subscriber with initialOffsetPolicy LATEST");
  try {
    await client.createPersistentSubscriber(
      subscriberName,
    );
  } catch (err) {
    console.error("Failed to create consumer group:", err);
    throw err;
  }

  const subscribeConfig = {
    concurrentDownloadLimit: 1,
    commitInterval: 5000,
    maxFailedSlotDownloadAttempt: 3,
    slotMemoryRetention: 1000,
    gcInterval: 1000,
  };
  
  const { sink: _sink, source } = await client.dragonsmouthSubscribeWithConfig(
    subscriberName,
    request,
    subscribeConfig
  );
  console.log("Subscription started. Listening for updates...");

  const block_map = {};
  await source.forEach((update) => {
    const slot: number = Number(getSlotFromUpdate(update));
    if (!(slot in block_map)) {
      block_map[slot] = {
        started: Date.now(),
        account: [],
        tx: [],
      };
    }

    if (update.account) {
      block_map[slot].account.push(update.account);
    } else if (update.slot) {
      const block = block_map[slot];
      delete block_map[slot];
      const e = Date.now() - block.started;
      console.log(
        `Slot ${slot} account count: ${block.account.length}, tx count: ${block.tx.length} in ${e} ms`
      );
    } else if (update.transaction) {
      const block = block_map[slot];
      block.tx.push(update.transaction);
    }
  });
}

function getSlotFromUpdate(update: SubscribeUpdate): bigint {
  if (update.account) {
    return update.account.slot;
  } else if (update.slot) {
    return update.slot.slot;
  } else if (update.transaction) {
    return update.transaction.slot;
  } else if (update.entry) {
    return update.entry.slot;
  } else if (update.blockMeta) {
    return update.blockMeta.slot;
  } else if (update.slot) {
    return update.slot.slot;
  } else {
    throw new Error("Unable to extract slot from update");
  }
}

async function handleShutdown(signal: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\nReceived ${signal}. Cleaning up...`);
  await new Promise((resolve) => setTimeout(resolve, 1000));
  process.exit(0);
}

process.on("SIGINT", () => handleShutdown("SIGINT"));
process.on("SIGTERM", () => handleShutdown("SIGTERM"));
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
});

main()
  .then(() => {
    console.log("Fumarole client initialized successfully");
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
