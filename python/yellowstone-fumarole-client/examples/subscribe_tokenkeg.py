from typing import Optional
import uuid
import logging
import asyncio
from os import environ
from yellowstone_fumarole_client.config import FumaroleConfig
from yellowstone_fumarole_client import FumaroleClient, FumaroleSubscribeConfig
from yellowstone_fumarole_proto.fumarole_pb2 import CreateConsumerGroupRequest
from yellowstone_fumarole_client.error import FumaroleClientError
from yellowstone_fumarole_proto.geyser_pb2 import (
    CommitmentLevel,
    SubscribeRequest,
    SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions,
    SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterEntry,
    SubscribeRequestFilterSlots,
)
from yellowstone_fumarole_proto.geyser_pb2 import (
    SubscribeUpdate,
    SubscribeUpdateTransaction,
    SubscribeUpdateBlockMeta,
    SubscribeUpdateAccount,
    SubscribeUpdateEntry,
    SubscribeUpdateSlot,
)
import random
import string

def random_string(length=8):
    """Generate a random string of given length (default: 8)."""
    characters = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    return ''.join(random.choice(characters) for _ in range(length))


def fumarole_config() -> FumaroleConfig:

    path = environ["TEST_FUMAROLE_CONFIG"]

    with open(path, "r") as f:
        return FumaroleConfig.from_yaml(f)

TOKEN_ADDRESS = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
# BGUM_ADDRESS = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";

async def main():
    """
    test `DragonsmouthAdapterSession`
    """
    logging.debug("test_fumarole_delete_all")
    # Create a FumaroleClient instance
    config = fumarole_config()
    # x_subscription_id = str(uuid.uuid4())
    x_subscription_id = "d2ec45b8-4c2f-4678-a8dd-55cabcc1280a"
    config.x_metadata = {"x-subscription-id": x_subscription_id}

    client: FumaroleClient = await FumaroleClient.connect(config)
    await client.delete_all_consumer_groups()

    consumer_group_name = random_string(12)
    resp = await client.create_consumer_group(
        CreateConsumerGroupRequest(
            consumer_group_name=consumer_group_name,
        )
    )
    assert resp.consumer_group_id, "Failed to create consumer group"

    subscribe_config = FumaroleSubscribeConfig(
        concurrent_download_limit=4
    )
    logging.info(f"subscribe config : {subscribe_config}")
    session = await client.dragonsmouth_subscribe_with_config(
        consumer_group_name=consumer_group_name,
        request=SubscribeRequest(
            # accounts={"fumarole": SubscribeRequestFilterAccounts()},
            transactions={"fumarole": SubscribeRequestFilterTransactions(
                account_include=[TOKEN_ADDRESS]
            )},
            blocks_meta={"fumarole": SubscribeRequestFilterBlocksMeta()},
            entry={"fumarole": SubscribeRequestFilterEntry()},
            slots={"fumarole": SubscribeRequestFilterSlots(
                filter_by_commitment=True
            )},
            commitment=CommitmentLevel.PROCESSED
        ),
        config=subscribe_config,
    )
    logging.info("starting session")
    
    class BlockConstruction:
        def __init__(self):
            self.tx_vec: list[SubscribeUpdateTransaction] = []
            self.entry_vec: list[SubscribeUpdateEntry] = []
            self.account_vec: list[SubscribeUpdateAccount] = []
            self.meta: Optional[SubscribeUpdateBlockMeta] = None

        
        @classmethod
        def default(cls) -> "BlockConstruction":
            return cls()

    async with session:
        dragonsmouth_source = session.source
        block_map = dict()
        try:
            async for result in dragonsmouth_source:
                if result.HasField("block_meta"):
                    block_meta: SubscribeUpdateBlockMeta = result.block_meta
                    slot = block_meta.slot
                    block = block_map.setdefault(slot, BlockConstruction.default())
                    block.meta = block_meta
                elif result.HasField("transaction"):
                    tx: SubscribeUpdateTransaction = result.transaction
                    slot = tx.slot
                    block = block_map.setdefault(slot, BlockConstruction.default())
                    block.tx_vec.append(tx)
                elif result.HasField("account"):
                    account: SubscribeUpdateAccount = result.account
                    slot = account.slot
                    block = block_map.setdefault(slot, BlockConstruction.default())
                    block.account_vec.append(account)
                elif result.HasField("entry"):
                    entry: SubscribeUpdateEntry = result.entry
                    slot = entry.slot
                    block = block_map.setdefault(slot, BlockConstruction.default())
                    block.entry_vec.append(entry)
                elif result.HasField("slot"):
                    result: SubscribeUpdateSlot = result.slot
                    block = block_map.get(result.slot, None)
                    if block is not None:
                        logging.info(f"slot {result.slot} end, account {len(block.account_vec)}, tx {len(block.tx_vec)}")
                        continue
        except FumaroleClientError as e:
            logging.error(f"Fumarole client error occurred: {e}")
        stats = session.stats()
        assert stats.max_slot_seen >= max(block_map.keys())
        assert stats.log_committable_offset >= stats.log_committed_offset
        assert stats.log_committable_offset > 0
    logging.info(f"session ended, stats: {stats}")
    assert block_map, "No blocks received"


if __name__ == "__main__":
    logging.basicConfig(level=environ.get("LOG_LEVEL", "INFO"))
    asyncio.run(main())