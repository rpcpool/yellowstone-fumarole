import logging
from typing import Coroutine
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

    barrier = asyncio.Event()
    async def test():
        await barrier.wait()
        return 1

    handle = join_set.spawn(test())
    fut: Coroutine = join_set.join_next()
    assert fut
    assert len(join_set) == 1
    logging.debug("Cancelling the future")
    task = asyncio.create_task(fut)
    assert task.cancel()
    assert len(join_set) == 1

    logging.debug("Waiting for the future to finish")
    fut: Coroutine = join_set.join_next()

    assert fut
    barrier.set()
    result = await fut
    assert result.result() == 1
    assert len(join_set) == 0


@pytest.mark.asyncio
async def test_it_should_be_cancel_safe_even_with_ready_future():
    join_set = JoinSet()
    async def test():
        return 1

    handle = join_set.spawn(test())
    fut: Coroutine = join_set.join_next()
    assert fut
    assert len(join_set) == 1
    logging.debug("Cancelling the future")
    task = asyncio.create_task(fut)
    await asyncio.sleep(1)
    # assert not task.cancel()
    # try:
    #     await task
    # except asyncio.CancelledError:
    #     pass
    # assert task.cancelled()
    assert len(join_set) == 1

    logging.debug("Waiting for the future to finish")
    fut: Coroutine = join_set.join_next()

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


@pytest.mark.asyncio
async def test_concurrent_spawn():
    join_set = JoinSet()

    barrier1 = asyncio.Event()
    barrier2 = asyncio.Event()
    async def test1():
        await barrier1.wait()
        return 1
    
    async def test2():
        await barrier2.wait()
        return 10

    ch1 = join_set.spawn(test1())
    ch2 = join_set.spawn(test2())


    assert len(join_set) == 2

    join_co = join_set.join_next()

    barrier1.set()
    barrier2.set()

    result1 = await join_co
    assert result1.result() in [1, 10]
    assert len(join_set) == 1
    result2 = await join_set.join_next()
    assert result2.result() in [1, 10]
    assert len(join_set) == 0
    assert result1.result() != result2.result()


@pytest.mark.asyncio
async def test_queue_is_cancel_safe():
    """
    Test that the queue is cancel safe.
    """
    queue = asyncio.Queue(maxsize=3)
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)

    assert not queue.empty()

    async def consumer(queue):
        print("Consumer waiting for item...")
        try:
            item = await queue.get()
            print(f"Got item: {item}")
        except asyncio.CancelledError:
            print("Consumer was cancelled!")


    assert queue.qsize() == 3
    task = asyncio.create_task(consumer(queue))

    await asyncio.sleep(1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert queue.qsize() == 3
    result = await queue.get()
    assert result == 1