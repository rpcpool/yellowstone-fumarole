from typing import Optional
import uuid
import pytest
import asyncio
import logging
from os import environ
from collections import defaultdict
from yellowstone_fumarole_client.config import FumaroleConfig
from yellowstone_fumarole_client import FumaroleClient
from yellowstone_fumarole_proto.fumarole_v2_pb2 import CreateConsumerGroupRequest
from yellowstone_fumarole_proto.geyser_pb2 import (
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


@pytest.fixture
def fumarole_config() -> FumaroleConfig:

    path = environ["TEST_FUMAROLE_CONFIG"]

    with open(path, "r") as f:
        return FumaroleConfig.from_yaml(f)


@pytest.mark.asyncio
async def test_fumarole_delete_all(fumarole_config):
    """
    Test the delete_all_cg function.
    """
    logging.debug("test_fumarole_delete_all")
    # Create a FumaroleClient instance

    fumarole_config.x_metadata = {"x-subscription-id": str(uuid.uuid4())}

    client: FumaroleClient = await FumaroleClient.connect(fumarole_config)
    # Call the delete_all_cg function
    await client.delete_all_consumer_groups()

    resp = await client.create_consumer_group(
        CreateConsumerGroupRequest(
            consumer_group_name="test",
        )
    )
    assert resp.consumer_group_id, "Failed to create consumer group"

    resp = await client.create_consumer_group(
        CreateConsumerGroupRequest(
            consumer_group_name="test2",
        )
    )

    logging.debug("create consumer group response: %s", resp)

    cg_list = await client.list_consumer_groups()

    assert len(cg_list.consumer_groups) == 2

    await client.delete_all_consumer_groups()

    cg_list = await client.list_consumer_groups()
    assert len(cg_list.consumer_groups) == 0

    cg_info = await client.get_consumer_group_info(consumer_group_name="test")
    assert cg_info is None, "Failed to get consumer group info"


@pytest.mark.asyncio
async def test_dragonsmouth_adapter(fumarole_config):
    """
    Test the delete_all_cg function.
    """
    logging.debug("test_fumarole_delete_all")
    # Create a FumaroleClient instance

    # x_subscription_id = str(uuid.uuid4())
    x_subscription_id = "d2ec45b8-4c2f-4678-a8dd-55cabcc1280a"
    fumarole_config.x_metadata = {"x-subscription-id": x_subscription_id}

    client: FumaroleClient = await FumaroleClient.connect(fumarole_config)
    await client.delete_all_consumer_groups()

    resp = await client.create_consumer_group(
        CreateConsumerGroupRequest(
            consumer_group_name="test",
        )
    )
    assert resp.consumer_group_id, "Failed to create consumer group"

    session = await client.dragonsmouth_subscribe(
        consumer_group_name="test",
        request=SubscribeRequest(
            # accounts={"fumarole": SubscribeRequestFilterAccounts()},
            transactions={"fumarole": SubscribeRequestFilterTransactions()},
            blocks_meta={"fumarole": SubscribeRequestFilterBlocksMeta()},
            entry={"fumarole": SubscribeRequestFilterEntry()},
            slots={"fumarole": SubscribeRequestFilterSlots()},
        ),
    )
    logging.warning("starting session")
    dragonsmouth_source = session.source
    handle = session.fumarole_handle

    class BlockConstruction:
        def __init__(self):
            self.tx_vec: list[SubscribeUpdateTransaction] = []
            self.entry_vec: list[SubscribeUpdateEntry] = []
            self.account_vec: list[SubscribeUpdateAccount] = []
            self.meta: Optional[SubscribeUpdateBlockMeta] = None

        def check_block_integrity(self) -> bool:
            assert self.meta is not None, "Block meta is not set"
            return (
                len(self.tx_vec) == self.meta.executed_transaction_count
                and len(self.entry_vec) == self.meta.entries_count
            )

    block_map = defaultdict(BlockConstruction)
    while True:
        tasks = [asyncio.create_task(dragonsmouth_source.get()), handle]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if tasks[0] == t:
                result: SubscribeUpdate = t.result()
                if result.HasField("block_meta"):
                    block_meta: SubscribeUpdateBlockMeta = result.block_meta
                    slot = block_meta.slot
                    block_map[slot].meta = block_meta
                elif result.HasField("transaction"):
                    tx: SubscribeUpdateTransaction = result.transaction
                    slot = tx.slot
                    block = block_map[slot]
                    block.tx_vec.append(tx)
                elif result.HasField("account"):
                    account: SubscribeUpdateAccount = result.account
                    slot = account.slot
                    block = block_map[slot]
                    block.account_vec.append(account)
                elif result.HasField("entry"):
                    entry: SubscribeUpdateEntry = result.entry
                    slot = entry.slot
                    block = block_map[slot]
                    block.entry_vec.append(entry)
                elif result.HasField("slot"):
                    result: SubscribeUpdateSlot = result.slot
                    block = block_map[result.slot]
                    assert block.check_block_integrity()
                    return
            else:
                result = t.result()
                raise RuntimeError("failed to get dragonsmouth source: %s" % result)
