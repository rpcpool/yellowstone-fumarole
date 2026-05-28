import dotenv from "dotenv";
import {
  FumaroleClient,
  CommitmentLevel,
  SubscribeRequest,
  SubscribeUpdate,
} from "@triton-one/yellowstone-fumarole-client";

dotenv.config();

const FUMAROLE_ENDPOINT = process.env.FUMAROLE_ENDPOINT!;
const FUMAROLE_X_TOKEN = process.env.FUMAROLE_X_TOKEN!;
const TOKEN_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

let isShuttingDown = false;

async function main() {
  console.log("Connecting to Fumarole server...");
  const client = await FumaroleClient.connect({
    endpoint: FUMAROLE_ENDPOINT,
    xToken: FUMAROLE_X_TOKEN,
    maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
  });
  console.log("Connected\n");

  // Clean up stale subscribers from previous dev runs
  await client.deleteAllConsumerGroups();

  const subscriberName = `token-monitor-${Math.random().toString(36).substring(7)}`;
  console.log(`Creating persistent subscriber: ${subscriberName}`);
  await client.createConsumerGroup(subscriberName);

  const request: SubscribeRequest = {
    commitment: CommitmentLevel.PROCESSED,
    accounts: {
      all: {
        account: [],
        owner: [],
        filters: []
      }
    },
    transactions: {
      all: {
        accountInclude: [],
        accountExclude: [],
        accountRequired: [],
      },
    },
    slots: {
      all: { filterByCommitment: true },
    },
    transactionsStatus: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    accountsDataSlice: [],
  };

  const subscription = await client.subscribeWithConfig(subscriberName, request, {
    concurrentDownloadLimitPerTcp: 1,
    commitIntervalMs: 5_000,
    maxFailedSlotDownloadAttempt: 3,
    slotMemoryRetention: 1_000,
    gcInterval: 1_000,
  });

  console.log("Subscription started. Listening for token program updates…\n");

  // Per-slot accumulator: track accounts and transactions until the slot ends
  const blockMap = new Map<
    bigint,
    { started: number; accounts: number; transactions: number }
  >();

  for await (const event of subscription) {
    if (isShuttingDown) break;

    if (event.type === "slotEnded") {
      const block = blockMap.get(event.slot);
      if (block) {
        const elapsed = Date.now() - block.started;
        console.log(
          `Slot ${event.slot}: accounts=${block.accounts} txs=${block.transactions} (${elapsed} ms)`
        );
        blockMap.delete(event.slot);
      }
      continue;
    }

    // event.type === "data"
    const slot = event.slot;
    if (!blockMap.has(slot)) {
      blockMap.set(slot, { started: Date.now(), accounts: 0, transactions: 0 });
    }
    const block = blockMap.get(slot)!;
    const update: SubscribeUpdate = event.update;

    if (update.account) block.accounts++;
    if (update.transaction) block.transactions++;
  }
}

function handleShutdown(signal: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\nReceived ${signal}. Shutting down…`);
  setTimeout(() => process.exit(0), 500);
}

process.on("SIGINT", () => handleShutdown("SIGINT"));
process.on("SIGTERM", () => handleShutdown("SIGTERM"));
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled rejection:", reason);
});

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
