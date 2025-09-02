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
            accounts={"fumarole": SubscribeRequestFilterAccounts()},
            transactions={"fumarole": SubscribeRequestFilterTransactions()},
            blocks_meta={"fumarole": SubscribeRequestFilterBlocksMeta()},
            entry={"fumarole": SubscribeRequestFilterEntry()},
            slots={"fumarole": SubscribeRequestFilterSlots()},
        ),
    )
    async with session:
        dragonsmouth_like_source = session.source
        # result: SubscribeUpdate
        async for result in dragonsmouth_like_source:
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

    # OUTSIDE THE SCOPE, YOU SHOULD NEVER USE `session` again.
```


At any point you can get a rough estimate if you are progression through the slot using `DragonsmouthAdapterSession.stats()` call:

```python

async with session:
    stats: FumaroleSubscribeStats = session.stats()
    print(f"{stats.log_committed_offset}, {stats.log_committable_offset}, {stats.max_slot_seen}")
```

`log_committed_offset` : what have been ACK so for to fumarole remote service.
`log_committable_offset` : what can be ACK to next commit call.
`max_slot_seen` : maximum slot seen in the inner fumarole client state -- not yet processed by your code.

