




import asyncio
from collections import deque
import threading
import logging
import uuid

LOGGER = logging.getLogger(__name__)


async def never():
    """
    Create a forever pending future. This future is not set and will never be set.
    This is useful for testing purposes.
    """
    loop = asyncio.get_running_loop()
    return await loop.create_future()


class CancelHandle:

    def __init__(self, task: asyncio.Task):
        self._task = task

    def cancel(self) -> bool:
        return self._task.cancel()

    def id(self) -> int:
        return self._task.get_name()

class JoinSet:
    """
    A set of tasks that can be joined.
    """

    def __init__(self, *, loop: asyncio.AbstractEventLoop | None = None):
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            # fallback for when no loop is running yet
            self._loop = asyncio.get_event_loop()
        self.tasks = set()
        self.ready = set()
        self.waker = set()
        self.my_thread = threading.get_ident()

    def spawn(self, fut: asyncio.Future) -> CancelHandle:
        """
        Spawn an awaitable (coroutine, task, or future) and add it to the set as a task.
        """
        # Convert awaitable to task
        task = self._loop.create_task(fut)
        task.set_name(uuid.uuid4().int)

        def callback(task: asyncio.Task):
            self.tasks.discard(task)
            try:
                waker = self.waker.pop()
                if not waker.cancelled():
                    waker.set_result(task)
            except KeyError:
                # No waker available, add task to the ready queue
                pass
            self.ready.add(task)

        task.add_done_callback(callback)

        # Add task to the set
        self.tasks.add(task)
        return CancelHandle(task)

    def __len__(self) -> int:
        return len(self.tasks) + len(self.ready)
    
    def take(self) -> 'JoinSet':
        """Takes ownership of the JoinSet and returns a new JoinSet.
        """
        self.my_thread = threading.get_ident()
        return self

    def join_next(self) -> asyncio.Future | None:
        """
        Join the next task in the set if any, otherwise return None

        [Cacncel-Safety]
        This method is cancel-safe. The future returned by this method can be cancelled
        without affecting the JoinSet. The JoinSet will continue to track the tasks
        and will not be affected by the cancellation of the future.
        """

        # Check if the current thread is the same as the thread that created the JoinSet
        if self.my_thread != threading.get_ident():
            raise RuntimeError("JoinSet.join_next must be called from the same thread that created the JoinSet")

        if not self.tasks and not self.ready:
            return None
        
        fut  = self._loop.create_future()

        # assert not self.waker, "JoinSet.join_next requires exclusive access to join set"

        while True:
            try:
                task = self.ready.pop()
                task: asyncio.Task = task
                fut.set_result(task)
                return fut
            except KeyError:
                LOGGER.debug("No tasks ready")
                # No tasks are ready
                break
        
        # No tasks are ready
        # Add the future to the set

        def deregister_waker(task):
            try:
                if task.cancelled():
                    # If the task is cancelled, remove it from the set
                    self.waker.remove(fut)
                else:
                    actual_task = task.result()
                    self.ready.discard(actual_task)
            except KeyError:
                pass

        fut.add_done_callback(deregister_waker)

        self.waker.add(fut)

        # Check if there are any tasks that are already ready
        # in between the time we added the future and now
        try:
            task = self.ready.pop()
            LOGGER.debug("Task ready in between")
        except KeyError:
            if not self.tasks:
                # No tasks are ready, return the future
                LOGGER.debug("No tasks ready, returning None")
                return None
            LOGGER.debug("No tasks ready, but tasks exist, returning future")
            return fut

        # If there is a task ready, set the future to the result
        try:
            waker = self.waker.pop()
            waker.set_result(task)
            LOGGER.debug("Task ready, setting waker")
            return waker
        except KeyError:
            LOGGER.debug("No waker available, adding task to the ready queue")
            # No waker available, add task to the ready queue
            fut = self._loop.create_future()
            fut.set_result(task)
            return fut