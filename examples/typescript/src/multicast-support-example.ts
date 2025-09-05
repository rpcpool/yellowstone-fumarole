import dotenv from "dotenv";

import {
  FumaroleClient,
  SubscribeRequest,
  CommitmentLevel,
  InitialOffsetPolicy,
  setDefaultFumaroleLogger,
} from "@triton-one/yellowstone-fumarole";
import { from, Observable } from "rxjs";
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
    transactions: {},
    slots: {
      example: {
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
    commitInterval: 1000,
    maxFailedSlotDownloadAttempt: 3,
    slotMemoryRetention: 1000,
    gcInterval: 1000,
  };

  console.log("Subscribe request:", safeJsonStringify(request));
  console.log("Subscribe config:", safeJsonStringify(subscribeConfig));
  console.log(`Starting subscription for group ${subscriberName}...`);

  const { sink: _sink, source } = await client.dragonsmouthSubscribeWithConfig(
    subscriberName,
    request,
    subscribeConfig
  );
  console.log("Subscription started. Listening for updates...");

  // Observable in rxjs can be subscribed multiple time.
  // In the case of Fumarole source, the `Observable` is "hot", meaning it can emit reference (not copy) to the same event to all subscribers.
  const sub1 = source.subscribe((next) => {
    if (next.slot) {
      console.log(`sub1 -- received slot update ${next.slot?.slot}`);
    }
  });
  const sub2 = source.subscribe((next) => {
    if (next.slot) {
      console.log(`sub2 -- received slot update ${next.slot?.slot}`);
    }
  });

  try {
    await new Promise((resolve) => {
      setTimeout(resolve, 5000);
    });
  } finally {
    console.log("10 seconds elapsed!");
    // Don't forget to unsubscribe!!!
    sub1.unsubscribe();
    sub2.unsubscribe();
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
