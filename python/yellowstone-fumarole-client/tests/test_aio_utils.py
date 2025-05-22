import logging
import pytest
import asyncio
from yellowstone_fumarole_client.utils.aio import JoinSet, never

# Helper coroutine for testing
@pytest.mark.asyncio
async def test_spawn_and_join_single_task():
    """Test spawning and joining a single task."""
    join_set = JoinSet()

    
    async def test():
        await asyncio.sleep(0.1)
        return 1
    

    handle = join_set.spawn(test())

    assert handle
    await asyncio.sleep(1)

    assert len(join_set) == 1

    result = join_set.join_next()
    assert result is not None

    result = await result
    assert result.result() == 1


# Helper coroutine for testing
@pytest.mark.asyncio
async def test_empty_joinset():
    join_set = JoinSet()

    maybe = join_set.join_next()
    assert maybe is None
    assert len(join_set) == 0


@pytest.mark.asyncio
async def test_it_should_handle_canceling_spawned_task():
    join_set = JoinSet()

    async def test():
        await asyncio.sleep(100)
        return 1

    handle = join_set.spawn(test())
    # 1st case : Test cancel before join_next
    handle.cancel()

    assert len(join_set) == 1
    result = await join_set.join_next()
    assert result.cancelled()
    assert len(join_set) == 0

    # 2nd case : Test cancel after join_next

    handle = join_set.spawn(test())
    fut = join_set.join_next()
    assert fut
    assert len(join_set) == 1

    handle.cancel()
    result = await fut
    assert result.cancelled()
    assert len(join_set) == 0


@pytest.mark.asyncio
async def test_it_should_be_cancel_safe():
    join_set = JoinSet()

    async def test():
        return 1

    handle = join_set.spawn(test())
    fut: asyncio.Future = join_set.join_next()
    assert fut
    assert len(join_set) == 1
    logging.debug("Cancelling the future")
    fut.cancel()

    assert fut.cancelled()
    assert len(join_set) == 1

    logging.debug("Waiting for the future to finish")
    fut = join_set.join_next()

    assert fut

    result = await fut
    assert result.result() == 1
    assert len(join_set) == 0



@pytest.mark.asyncio
async def test_spawn_task_identity():
    join_set = JoinSet()

    async def test():
        return 1

    handle = join_set.spawn(test())
    fut = join_set.join_next()
    result = await fut
    assert result.get_name() == handle.id()
    assert len(join_set) == 0