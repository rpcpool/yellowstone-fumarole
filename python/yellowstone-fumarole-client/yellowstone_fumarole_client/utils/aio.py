import asyncio
import logging
from typing import Any, Coroutine

LOGGER = logging.getLogger(__name__)


async def never():
    """
    Create a forever pending future. This future is not set and will never be set.
    This is useful for testing purposes.
    """
    loop = asyncio.get_running_loop()
    return await loop.create_future()


class Interval:

    def __init__(self, interval: float):
        """
        Create an interval that will run the given factory every `interval` seconds.

        Args:
            interval: The interval in seconds.
            factory: A factory that returns a coroutine to be run at the interval.
        """
        self.interval = interval

    async def tick(self):
        await asyncio.sleep(self.interval)
