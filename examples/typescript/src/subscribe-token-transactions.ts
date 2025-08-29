import dotenv from "dotenv";

import {
  FumaroleClient,
  SubscribeRequest,
  DragonsmouthAdapterSession,
  CommitmentLevel,
  InitialOffsetPolicy,
  SubscribeUpdate,
} from "@triton-one/yellowstone-fumarole";
import { Observable } from "rxjs";

dotenv.config();

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
const TOKEN_ADDRESS = "Tokenkegqfezyinwajbnbgkpfxcwubvf9ss623vq5da";

let isShuttingDown = false;

async function main() {
  let groupName: string | undefined;
  let client: FumaroleClient | undefined;

  try {
    console.log(`Connecting to Fumarole server at ${FUMAROLE_ENDPOINT}...`);
    const config = {
      endpoint: FUMAROLE_ENDPOINT,
      xToken: FUMAROLE_X_TOKEN,
      maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
      xMetadata: {},
    };

    console.log(
      "Initializing Fumarole client with configuration:",
      safeJsonStringify(Object.assign({}, config, { xToken: "***" }))
    );

    client = await FumaroleClient.connect(config);

    const request: SubscribeRequest = {
      commitment: CommitmentLevel.CONFIRMED,
      accounts: {},
      transactions: {
        tokenFilter: {
          accountInclude: [TOKEN_ADDRESS],
          accountExclude: [],
          accountRequired: [TOKEN_ADDRESS],
        },
      },
      slots: {
        slotFilter: {
          filterByCommitment: true,
          interslotUpdates: true,
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
    await client.deleteAllConsumerGroups();

    groupName = `token-monitor-${Math.random().toString(36).substring(7)}`;
    console.log(`Creating consumer group: ${groupName}`);

    console.log("Creating consumer group with initialOffsetPolicy LATEST");
    try {
      await client.createConsumerGroup({
        consumerGroupName: groupName,
        initialOffsetPolicy: InitialOffsetPolicy.LATEST,
      });
    } catch (err) {
      console.error("Failed to create consumer group:", err);
      throw err;
    }

    const subscribeConfig = {
      concurrentDownloadLimit: 10,
      commitInterval: 1000,
      maxFailedSlotDownloadAttempt: 3,
      slotMemoryRetention: 1000,
      gcInterval: 1000,
    };

    console.log("Subscribe request:", safeJsonStringify(request));
    console.log("Subscribe config:", safeJsonStringify(subscribeConfig));

    // groupName = "helloworld-1"

    console.log(`Starting subscription for group ${groupName}...`);

    const observable: Observable<SubscribeUpdate> = await client.dragonsmouthSubscribeWithConfig(
      groupName,
      request,
      subscribeConfig,
    );

    observable.subscribe((next) => {
      console.log("Received update:", safeJsonStringify(next));
    });

    // console.log("Subscription started. Listening for updates...");
    // for await (const update of observable as any) {
    //   console.log("Received update:", safeJsonStringify(update));
    // }
  } catch (error) {
    console.error(error);
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

main().catch(console.error);
