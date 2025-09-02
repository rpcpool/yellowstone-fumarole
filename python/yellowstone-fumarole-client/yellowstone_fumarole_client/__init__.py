import asyncio
import logging
from yellowstone_fumarole_client.grpc_connectivity import (
    FumaroleGrpcConnector,
)
from typing import AsyncGenerator, Optional
from dataclasses import dataclass
from yellowstone_fumarole_client.config import FumaroleConfig
from yellowstone_fumarole_client.runtime.aio import (
    AsyncioFumeDragonsmouthRuntime,
    DEFAULT_GC_INTERVAL,
    DEFAULT_SLOT_MEMORY_RETENTION,
    GrpcSlotDownloader,
)
from yellowstone_fumarole_client.runtime.state_machine import (
    FumaroleSM,
    FumeOffset,
)
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequest, SubscribeUpdate
from yellowstone_fumarole_proto.fumarole_pb2 import (
    ControlResponse,
    VersionRequest,
    VersionResponse,
    JoinControlPlane,
    ControlCommand,
    ListConsumerGroupsRequest,
    ListConsumerGroupsResponse,
    GetConsumerGroupInfoRequest,
    ConsumerGroupInfo,
    DeleteConsumerGroupRequest,
    DeleteConsumerGroupResponse,
    CreateConsumerGroupRequest,
    CreateConsumerGroupResponse,
)
from yellowstone_fumarole_proto.fumarole_pb2_grpc import FumaroleStub
import grpc

from yellowstone_fumarole_client import config

__all__ = [
    "FumaroleClient",
    "FumaroleConfig",
    "FumaroleSubscribeConfig",
    "DragonsmouthAdapterSession",
    "DEFAULT_DRAGONSMOUTH_CAPACITY",
    "DEFAULT_COMMIT_INTERVAL",
    "DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT",
    "DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP",
]

# Constants
DEFAULT_DRAGONSMOUTH_CAPACITY = 100000
DEFAULT_COMMIT_INTERVAL = 5.0  # seconds
DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3
DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 10

# Error classes


# FumaroleSubscribeConfig
@dataclass
class FumaroleSubscribeConfig:
    """Configuration for subscribing to a dragonsmouth stream."""

    # The maximum number of concurrent download tasks per TCP connection.
    concurrent_download_limit: int = DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP

    # The interval at which to commit the slot memory.
    commit_interval: float = DEFAULT_COMMIT_INTERVAL

    # The maximum number of failed slot download attempts before giving up.
    max_failed_slot_download_attempt: int = DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT

    # The maximum number of slots to download concurrently.
    data_channel_capacity: int = DEFAULT_DRAGONSMOUTH_CAPACITY

    # The interval at which to perform garbage collection on the slot memory.
    gc_interval: int = DEFAULT_GC_INTERVAL

    # How many processed slot numbers to retain in memory to avoid duplication.
    slot_memory_retention: int = DEFAULT_SLOT_MEMORY_RETENTION


@dataclass
class FumaroleSubscribeStats:
    """Commit/slot statistics for the Fumarole subscribe session."""

    # Last committed log offset in Fumarole -- this is a low-level, implementation detail.
    # NOTE: this should not be part as business logic, can change any time.
    log_committed_offset: FumeOffset
    # NOTE:: this is a low-level information, can change any time.
    log_committable_offset: FumeOffset
    # Max slot seen by the in the current session - does not mean it has been processed.
    max_slot_seen: int


# DragonsmouthAdapterSession
@dataclass
class DragonsmouthAdapterSession:
    """Session for interacting with the dragonsmouth-like stream."""

    # The queue for sending SubscribeRequest update to the dragonsmouth stream.
    sink: asyncio.Queue

    # The queue for receiving SubscribeUpdate from the dragonsmouth stream.
    source: AsyncGenerator[SubscribeUpdate, None]

    # The task handle for the fumarole runtime.
    _fumarole_handle: asyncio.Task

    _sm: FumaroleSM

    async def __aenter__(self):
        """Enter the session context."""
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.sink.shutdown()
        self._fumarole_handle.cancel()

    def stats(self) -> FumaroleSubscribeStats:
        """Get low-level statistics of the Fumarole state-machine."""
        commitable = self._sm.committable_offset
        committed = self._sm.last_committed_offset
        max_slot = self._sm.max_slot_detected
        return FumaroleSubscribeStats(
            log_committed_offset=committed,
            log_committable_offset=commitable,
            max_slot_seen=max_slot,
        )


# FumaroleClient
class FumaroleClient:
    """Fumarole client for interacting with the Fumarole server."""

    logger = logging.getLogger(__name__)

    def __init__(self, connector: FumaroleGrpcConnector, stub: FumaroleStub):
        self.connector = connector
        self.stub = stub

    @staticmethod
    async def connect(config: config.FumaroleConfig) -> "FumaroleClient":
        """Connect to the Fumarole server using the provided configuration.
        Args:
            config (FumaroleConfig): Configuration for the Fumarole client.
        """
        endpoint = config.endpoint
        connector = FumaroleGrpcConnector(config=config, endpoint=endpoint)
        FumaroleClient.logger.debug(f"Connecting to {endpoint}")
        client = await connector.connect()
        FumaroleClient.logger.debug(f"Connected to {endpoint}")
        return FumaroleClient(connector=connector, stub=client)

    async def version(self) -> VersionResponse:
        """Get the version of the Fumarole server."""
        request = VersionRequest()
        response = await self.stub.Version(request)
        return response

    async def dragonsmouth_subscribe(
        self, consumer_group_name: str, request: SubscribeRequest
    ) -> DragonsmouthAdapterSession:
        """Subscribe to a dragonsmouth stream with default configuration.

        Args:
            consumer_group_name (str): The name of the consumer group.
            request (SubscribeRequest): The request to subscribe to the dragonsmouth stream.
        """
        return await self.dragonsmouth_subscribe_with_config(
            consumer_group_name, request, FumaroleSubscribeConfig()
        )

    async def dragonsmouth_subscribe_with_config(
        self,
        consumer_group_name: str,
        request: SubscribeRequest,
        config: FumaroleSubscribeConfig,
    ) -> DragonsmouthAdapterSession:
        """Subscribe to a dragonsmouth stream with custom configuration.

        Args:
            consumer_group_name (str): The name of the consumer group.
            request (SubscribeRequest): The request to subscribe to the dragonsmouth stream.
            config (FumaroleSubscribeConfig): The configuration for the dragonsmouth subscription.
        """
        dragonsmouth_outlet = asyncio.Queue(maxsize=config.data_channel_capacity)
        fume_control_plane_q = asyncio.Queue(maxsize=100)

        initial_join = JoinControlPlane(consumer_group_name=consumer_group_name)
        initial_join_command = ControlCommand(initial_join=initial_join)
        await fume_control_plane_q.put(initial_join_command)

        FumaroleClient.logger.debug(
            f"Sent initial join command: {initial_join_command}"
        )

        async def control_plane_sink():
            while True:
                try:
                    update = await fume_control_plane_q.get()
                    yield update
                except (asyncio.CancelledError, asyncio.QueueShutDown):
                    break

        fume_control_plane_stream_rx: grpc.aio.StreamStreamCall = self.stub.Subscribe(
            control_plane_sink()
        )  # it's actually InterceptedStreamStreamCall, but grpc lib doesn't export it

        control_response: ControlResponse = await fume_control_plane_stream_rx.read()
        init = control_response.init
        if init is None:
            raise ValueError(f"Unexpected initial response: {control_response}")

        # Once we have the initial response, we can spin a task to read from the stream
        # and put the updates into the queue.
        # This is a bit of a hack, but we need a Queue not a StreamStreamMultiCallable
        # because Queue are cancel-safe, while Stream are not, or at least didn't find any docs about it.
        fume_control_plane_rx_q = asyncio.Queue(maxsize=100)

        async def control_plane_source():
            while True:
                try:
                    async for update in fume_control_plane_stream_rx:
                        await fume_control_plane_rx_q.put(update)
                except asyncio.QueueShutDown:
                    break
                except asyncio.CancelledError:
                    break
                finally:
                    fume_control_plane_rx_q.shutdown()

        control_plane_src_task = asyncio.create_task(control_plane_source())

        FumaroleClient.logger.debug(f"Control response: {control_response}")

        last_committed_offset = init.last_committed_offsets.get(0)
        if last_committed_offset is None:
            raise ValueError("No last committed offset")

        sm = FumaroleSM(last_committed_offset, config.slot_memory_retention)
        subscribe_request_queue = asyncio.Queue(maxsize=100)

        data_plane_client = await self.connector.connect()

        grpc_slot_downloader = GrpcSlotDownloader(
            client=data_plane_client,
        )

        rt = AsyncioFumeDragonsmouthRuntime(
            sm=sm,
            slot_downloader=grpc_slot_downloader,
            subscribe_request_update_q=subscribe_request_queue,
            subscribe_request=request,
            consumer_group_name=consumer_group_name,
            control_plane_tx_q=fume_control_plane_q,
            control_plane_rx_q=fume_control_plane_rx_q,
            dragonsmouth_outlet=dragonsmouth_outlet,
            commit_interval=config.commit_interval,
            gc_interval=config.gc_interval,
            max_concurrent_download=config.concurrent_download_limit,
        )

        async def rt_run(rt):
            async with rt as rt:
                await rt.run()

        rt_task = asyncio.create_task(rt_run(rt))

        async def fumarole_overseer():
            done, pending = await asyncio.wait(
                [rt_task, control_plane_src_task], return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()

        fumarole_handle = asyncio.create_task(fumarole_overseer())

        async def source_gen() -> AsyncGenerator[SubscribeUpdate, None]:
            try:
                while True:
                    update = await dragonsmouth_outlet.get()
                    yield update
            except (asyncio.CancelledError, asyncio.QueueShutDown):
                pass
            finally:
                dragonsmouth_outlet.shutdown()

        return DragonsmouthAdapterSession(
            sink=subscribe_request_queue,
            source=source_gen(),
            _fumarole_handle=fumarole_handle,
            _sm=sm,
        )

    async def list_consumer_groups(
        self,
    ) -> ListConsumerGroupsResponse:
        """Lists all consumer groups."""
        return await self.stub.ListConsumerGroups(ListConsumerGroupsRequest())

    async def get_consumer_group_info(
        self, consumer_group_name: str
    ) -> Optional[ConsumerGroupInfo]:
        """Gets information about a consumer group by name.
        Returns None if the consumer group does not exist.

        Args:
            consumer_group_name (str): The name of the consumer group to retrieve information for.
        """
        try:
            return await self.stub.GetConsumerGroupInfo(
                GetConsumerGroupInfoRequest(consumer_group_name=consumer_group_name)
            )
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            else:
                raise

    async def delete_consumer_group(
        self, consumer_group_name: str
    ) -> DeleteConsumerGroupResponse:
        """Delete a consumer group by name.

        NOTE: this operation is idempotent, meaning that if the consumer group does not exist, it will not raise an error.
        Args:
            consumer_group_name (str): The name of the consumer group to delete.
        """
        return await self.stub.DeleteConsumerGroup(
            DeleteConsumerGroupRequest(consumer_group_name=consumer_group_name)
        )

    async def delete_all_consumer_groups(
        self,
    ) -> DeleteConsumerGroupResponse:
        """Deletes all consumer groups."""
        consumer_group_list = await self.list_consumer_groups()

        tasks = []

        async with asyncio.TaskGroup() as tg:
            for group in consumer_group_list.consumer_groups:
                cg_name = group.consumer_group_name
                task = tg.create_task(self.delete_consumer_group(cg_name))
                tasks.append((cg_name, task))

        # Raise an error if any task fails
        for cg_name, task in tasks:
            result = task.result()
            if not result.success:
                raise RuntimeError(
                    f"Failed to delete consumer group {cg_name}: {result.error}"
                )

    async def create_consumer_group(
        self, request: CreateConsumerGroupRequest
    ) -> CreateConsumerGroupResponse:
        """Creates a new consumer group.
        Args:
            request (CreateConsumerGroupRequest): The request to create a consumer group.
        """
        return await self.stub.CreateConsumerGroup(request)
