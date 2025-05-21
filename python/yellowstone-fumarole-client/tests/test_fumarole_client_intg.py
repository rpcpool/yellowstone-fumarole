import uuid
import pytest
import asyncio
import logging
from os import environ

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

    fumarole_config.x_metadata = {"x-subscription-id": str(uuid.uuid4())}

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
            accounts={"fumarole": SubscribeRequestFilterAccounts()},
            transactions={"fumarole": SubscribeRequestFilterTransactions()},
            blocks_meta={"fumarole": SubscribeRequestFilterBlocksMeta()},
            entry={"fumarole": SubscribeRequestFilterEntry()},
            slots={"fumarole": SubscribeRequestFilterSlots()},
        ),
    )

    dragonsmouth_source = session.source
    fh = session.fumarole_handle
    while True:

        tasks = [asyncio.create_task(dragonsmouth_source.get()), fh]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for t in pending:
            t.cancel()

        for task in done:
            if task == tasks[0]:
                print(f"Consumed: {type(task.result())}")
            else:
                print(f"session ended with: {type(task.result())}")
                return
