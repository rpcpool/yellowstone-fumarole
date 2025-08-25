import { FumaroleClient, FumaroleConfig } from "@triton-one/yellowstone-fumarole";
import dotenv from "dotenv";
dotenv.config()

const FUMAROLE_ENDPOINT = process.env.FUMAROLE_ENDPOINT!;
const FUMAROLE_X_TOKEN = process.env.FUMAROLE_X_TOKEN!;


async function main() {
  try {
    // Configure the client
    const config: FumaroleConfig = {
      endpoint: FUMAROLE_ENDPOINT,
      xToken: FUMAROLE_X_TOKEN,
      maxDecodingMessageSizeBytes: 100 * 1024 * 1024, // 100MB max message size
      xMetadata: {}, // Additional metadata if needed
    };

    // Connect to the Fumarole server
    console.log("Connecting to Fumarole server...");
    const client = await FumaroleClient.connect(config);
    console.log("Connected successfully");

    // List all consumer groups
    console.log("\nFetching consumer groups...");
    try {
      console.log("Sending listConsumerGroups request to server...");
      process.on("unhandledRejection", (reason, promise) => {
        console.error("Unhandled Rejection at:", promise, "reason:", reason);
      });

      const response = await client.listConsumerGroups().catch((error) => {
        console.error("Caught error during listConsumerGroups:", error);
        if (error.code) console.error("Error code:", error.code);
        if (error.details) console.error("Error details:", error.details);
        if (error.metadata) console.error("Error metadata:", error.metadata);
        if (error.stack) console.error("Error stack:", error.stack);
        throw error;
      });

      console.log("\n=== ListConsumerGroups Response ===");
      console.log(JSON.stringify(response, null, 2));
      console.log("=====================================\n");

      if (!response.consumerGroups || response.consumerGroups.length === 0) {
        console.log("No consumer groups found on server");
      } else {
        console.log(
          `Found ${response.consumerGroups.length} consumer groups. Fetching details...\n`
        );
        for (const group of response.consumerGroups) {
          console.log(`=== Consumer Group: ${group.consumerGroupName} ===`);
          console.log("Basic info:", JSON.stringify(group, null, 2));

          // Get detailed info for the group
          try {
            console.log(
              `\nFetching detailed info for group: ${group.consumerGroupName}`
            );
            const info = await client.getConsumerGroupInfo(
              group.consumerGroupName
            );
            if (info) {
              console.log("\nDetailed Group Info:");
              console.log("Status: Active");
              console.log("Server Response:", JSON.stringify(info, null, 2));
            } else {
              console.log("\nGroup Status: Not found or inactive");
            }
            console.log("===============================\n");
          } catch (err) {
            console.error(
              `\nError fetching group info from server: ${
                err instanceof Error ? err.message : String(err)
              }`
            );
          }
        }
      }
    } catch (error) {
      console.error(
        "Error:",
        error instanceof Error ? error.message : String(error)
      );
      process.exit(1);
    }
  } catch (error) {
    console.error(
      "Error:",
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  }
}

main().catch(console.error);
