import dotenv from "dotenv";

import {
  FumaroleClient,
  SubscribeRequest,
  CommitmentLevel,
  InitialOffsetPolicy,
  SubscribeUpdate,
  setDefaultLogger
} from "@triton-one/yellowstone-fumarole";
import { Observable } from "rxjs";

dotenv.config();

setDefaultLogger();

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
    const config = {
      endpoint: FUMAROLE_ENDPOINT,
      xToken: FUMAROLE_X_TOKEN,
      maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
      xMetadata: {},
    };

    client = await FumaroleClient.connect(config);

    const request: SubscribeRequest = {
      commitment: CommitmentLevel.CONFIRMED,
      accounts: { },
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
      concurrentDownloadLimit: 1,
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
    console.log("Subscription started. Listening for updates...");
    // Observable allow multiple subscription with shared event, no event copy.
    const sub1 = observable.subscribe((next) => {
      if (next.slot) {
        console.log(`sub1 -- Received slot: ${next.slot?.slot}`)
      }
    });

    const sub2 = observable.subscribe((next) => {
      if (next.slot) {
        console.log(`sub2 -- Received slot: ${next.slot?.slot}`)
      }
    });

    const deadline =  2000;
    console.log(`collecting data for ${deadline} millis...`)
    await new Promise((resolve) => setTimeout(resolve, deadline));
    sub1.unsubscribe();
    sub2.unsubscribe();
    console.log("finished!")
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

main()
  // .then(() => {
  //   console.log("Fumarole client initialized successfully");
  //   process.exit(0);
  // })
  .catch(err => {
    console.error(err);
    process.exit(1)
  });
