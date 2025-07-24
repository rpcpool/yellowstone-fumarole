# Fumarole Python SDK

This module contains Fumarole SDK for `python` programming language.

## Configuration

```yaml
endpoint: <"https://fumarole.endpoint.rpcpool.com">
x-token: <YOUR X-TOKEN secret here>
```

## Manage consumer group

Refer to [fume CLI](https://crates.io/crates/yellowstone-fumarole-cli) to manage your consumer groups.

## Examples

```python

from typing import Optional
import uuid
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

async def dragonsmouth_like_session(fumarole_config):
    with open("~/.fumarole/config.yaml") as f:
        fumarole_config = FumaroleConfig.from_yaml(f)
    
    client: FumaroleClient = await FumaroleClient.connect(fumarole_config)
    await client.delete_all_consumer_groups()

    # --- This is optional ---
    resp = await client.create_consumer_group(
        CreateConsumerGroupRequest(
            consumer_group_name="test",
        )
    )
    assert resp.consumer_group_id, "Failed to create consumer group"
    # --- END OF OPTIONAL BLOCK ---

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
    dragonsmouth_source = session.source
    handle = session.fumarole_handle
    block_map = defaultdict(BlockConstruction)
    while True:
        tasks = [asyncio.create_task(dragonsmouth_source.get()), handle]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if tasks[0] == t:
                result: SubscribeUpdate = t.result()
                if result.HasField("block_meta"):
                    block_meta: SubscribeUpdateBlockMeta = result.block_meta
                elif result.HasField("transaction"):
                    tx: SubscribeUpdateTransaction = result.transaction
                elif result.HasField("account"):
                    account: SubscribeUpdateAccount = result.account
                elif result.HasField("entry"):
                    entry: SubscribeUpdateEntry = result.entry
                elif result.HasField("slot"):
                    result: SubscribeUpdateSlot = result.slot
            else:
                result = t.result()
                raise RuntimeError("failed to get dragonsmouth source: %s" % result)
```