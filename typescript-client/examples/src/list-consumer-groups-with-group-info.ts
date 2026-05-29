import dotenv from "dotenv";
import { FumaroleClient } from "@triton-one/yellowstone-fumarole-client";

dotenv.config();

const FUMAROLE_ENDPOINT = process.env.FUMAROLE_ENDPOINT!;
const FUMAROLE_X_TOKEN = process.env.FUMAROLE_X_TOKEN!;

async function main() {
  console.log("Connecting to Fumarole server...");
  const client = await FumaroleClient.connect({
    endpoint: FUMAROLE_ENDPOINT,
    xToken: FUMAROLE_X_TOKEN,
    maxDecodingMessageSizeBytes: 100 * 1024 * 1024,
  });
  console.log("Connected successfully\n");

  // ── Version check ──────────────────────────────────────────────────────────
  const { version } = await client.version();
  console.log(`Service version: ${version}\n`);

  // ── List all consumer groups ───────────────────────────────────────────────
  console.log("Fetching consumer groups...");
  const { consumerGroups } = await client.listConsumerGroups();

  if (consumerGroups.length === 0) {
    console.log("No consumer groups found on server.");
    return;
  }

  console.log(`Found ${consumerGroups.length} consumer group(s).\n`);

  for (const group of consumerGroups) {
    console.log(`=== Consumer Group: ${group.consumerGroupName} ===`);
    console.log("Basic info:", JSON.stringify(group, bigintReplacer, 2));

    // ── Fetch detailed info per group ──────────────────────────────────────
    const info = await client.getConsumerGroupInfo(group.consumerGroupName);
    if (info) {
      console.log("Detailed info:", JSON.stringify(info, bigintReplacer, 2));
    } else {
      console.log("Group not found or inactive.");
    }
    console.log("=".repeat(45) + "\n");
  }
}

function bigintReplacer(_: string, v: unknown) {
  return typeof v === "bigint" ? v.toString() : v;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
