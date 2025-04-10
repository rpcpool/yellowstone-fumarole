/**
 * Read the Fumarole documentation abd blog if you haven't already to understand what's happening in the following code
 * https://docs.triton.one/project-yellowstone/fumarole
 * https://blog.triton.one/introducing-yellowstone-fumarole
 *
 * Fumarole Example Client
 *
 * This example demonstrates how to use the Yellowstone Fumarole client to:
 * - Connect to a Fumarole endpoint
 * - Subscribe to account and transaction updates
 * - Manage consumer groups
 * - Handle streaming data
 * 
 * Comment/Uncomment blocks of code to perform the operations you want to try out
 */
import Client, {
  FumaroleSubscribeRequest,
} from "@triton-one/yellowstone-fumarole";
import {
  EventSubscriptionPolicy,
  InitialOffsetPolicy,
  FumaroleClient,
} from "@triton-one/yellowstone-fumarole/dist/types/grpc/fumarole";
import { CommitmentLevel } from "@triton-one/yellowstone-fumarole/dist/types/grpc/geyser";
import yargs from "yargs";
async function main() {
  const args = await parseCommandLineArguments();

  const client = new Client(args.endpoint, args.xToken, {
    "grpc.max_receive_message_length": 64 * 1024 * 1024, // 64MiB
  });

  const consumerGroupLabel = "hello3";

  // const consumerGroup = await client.createStaticConsumerGroup({
  //   commitmentLevel: CommitmentLevel.CONFIRMED,
  //   consumerGroupLabel: consumerGroupLabel,
  //   eventSubscriptionPolicy: EventSubscriptionPolicy.BOTH,
  //   initialOffsetPolicy: InitialOffsetPolicy.LATEST,
  // });
  // console.log(
  //   `Created Consumer Group with label ${consumerGroupLabel} and id ${consumerGroup.groupId}`
  // );

  // const consumerGroupInfo = await client.getConsumerGroupInfo({
  //   consumerGroupLabel: consumerGroupLabel,
  // });
  // console.log(`Consumer group ${consumerGroupLabel} info: `);
  // console.log(consumerGroupInfo);

  // const slotLagInfo = await client.getSlotLagInfo({
  //   consumerGroupLabel: consumerGroupLabel,
  // });
  // console.log(`Slot Lag Info: ${slotLagInfo}`);

  const subscribeRequest: FumaroleSubscribeRequest = {
    accounts: {
      tokenKeg: {
        account: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
        filters: [],
        owner: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
        nonemptyTxnSignature: false,
      },
    },
    consumerGroupLabel: consumerGroupLabel,
    transactions: {
      //   tokenKeg: {
      //     accountExclude: [],
      //     accountInclude: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
      //     accountRequired: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
      //   },
    },
  };

  await fumaroleSubscribe(client, subscribeRequest);

  const consumerGroupsList = await client.listConsumerGroups({});
  console.log(`Consumer groups`);
  console.log(consumerGroupsList);

  // const oldestSlot = await client.getOldestSlot({commitmentLevel: CommitmentLevel.CONFIRMED})
  // console.log(`Oldest slot ${oldestSlot}`);

  const commitmentLevels = await client.listAvailableCommitmentLevels({});
  console.log(`Commitment Levels`);
  console.log(commitmentLevels);

  const deleteConsumerGroupInfo = await client.deleteConsumerGroup({
    consumerGroupLabel: consumerGroupLabel,
  });
  console.log(`Delete consumer group result`);
  console.log(deleteConsumerGroupInfo);
}

async function fumaroleSubscribe(
  client: Client,
  subscribeRequest: FumaroleSubscribeRequest
) {
  // Subscribe for events
  const stream = await client.subscribe();
  // const stream = await client.subscribe({ compression: "gzip" });

  // Create `error` / `end` handler
  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("close", () => {
      resolve();
    });
  });

  // Handle updates
  stream.on("data", (data) => {
    console.log(data);
  });

  // Send subscribe request
  await new Promise<void>((resolve, reject) => {
    stream.write(subscribeRequest, (err) => {
      if (err === null || err === undefined) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    console.error(reason);
    throw reason;
  });

  await streamClosed;
}

function parseCommandLineArguments() {
  return yargs(process.argv.slice(3))
    .options({
      endpoint: {
        alias: "e",
        default: "http://localhost:10000",
        describe: "fumarole gRPC endpoint",
        type: "string",
      },
      "x-token": {
        describe: "token for auth, can be used only with ssl",
        type: "string",
      },
      commitment: {
        describe: "commitment level",
        choices: ["processed", "confirmed", "finalized"],
      },
    })
    .demandCommand(1)
    .help().argv;
}

main();
