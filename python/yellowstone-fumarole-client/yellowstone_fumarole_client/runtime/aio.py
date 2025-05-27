# DataPlaneConn
from abc import abstractmethod, ABC
import asyncio
import uuid
import grpc
from typing import Optional, List
from collections import abc, deque
from dataclasses import dataclass
import time
from yellowstone_fumarole_client.runtime.state_machine import (
    FumaroleSM,
    FumeDownloadRequest,
    FumeOffset,
    FumeShardIdx,
)
from yellowstone_fumarole_proto.geyser_pb2 import (
    SubscribeRequest,
    SubscribeUpdate,
    SubscribeUpdateSlot,
    CommitmentLevel as ProtoCommitmentLevel,
)
from yellowstone_fumarole_proto.fumarole_v2_pb2 import (
    ControlCommand,
    PollBlockchainHistory,
    CommitOffset,
    ControlResponse,
    DownloadBlockShard,
    BlockFilters,
)
from yellowstone_fumarole_proto.fumarole_v2_pb2_grpc import (
    Fumarole as GrpcFumaroleClient,
)
from yellowstone_fumarole_client.utils.aio import Interval
from yellowstone_fumarole_client.grpc_connectivity import FumaroleGrpcConnector
import logging


# Constants
DEFAULT_GC_INTERVAL = 5

DEFAULT_SLOT_MEMORY_RETENTION = 10000


# DownloadTaskResult
@dataclass
class CompletedDownloadBlockTask:
    """Represents a completed download block task."""

    slot: int
    block_uid: bytes
    shard_idx: FumeShardIdx
    total_event_downloaded: int


@dataclass
class DownloadBlockError:
    """Represents an error that occurred during the download of a block."""

    kind: str  # 'Disconnected', 'OutletDisconnected', 'BlockShardNotFound', 'FailedDownload', 'Fatal'
    message: str


@dataclass
class DownloadTaskResult:
    """Represents the result of a download task."""

    kind: str  # 'Ok' or 'Err'
    completed: Optional[CompletedDownloadBlockTask] = None
    slot: Optional[int] = None
    err: Optional[DownloadBlockError] = None


LOGGER = logging.getLogger(__name__)


class AsyncSlotDownloader(ABC):
    """Abstract base class for slot downloaders."""

    @abstractmethod
    async def run_download(
        self, subscribe_request: SubscribeRequest, spec: "DownloadTaskArgs"
    ) -> DownloadTaskResult:
        """Run the download task for a given slot."""
        pass


# TokioFumeDragonsmouthRuntime
class AsyncioFumeDragonsmouthRuntime:
    """Asynchronous runtime for Fumarole with Dragonsmouth-like stream support."""

    def __init__(
        self,
        sm: FumaroleSM,
        slot_downloader: AsyncSlotDownloader,
        subscribe_request_update_q: asyncio.Queue,
        subscribe_request: SubscribeRequest,
        consumer_group_name: str,
        control_plane_tx_q: asyncio.Queue,
        control_plane_rx_q: asyncio.Queue,
        dragonsmouth_outlet: asyncio.Queue,
        commit_interval: float,  # in seconds
        gc_interval: int,
        max_concurrent_download: int = 10,
    ):
        """Initialize the runtime with the given parameters.

        Args:
            sm (FumaroleSM): The state machine managing the Fumarole state.
            slot_downloader (AsyncSlotDownloader): The downloader for slots.
            subscribe_request_update_q (asyncio.Queue): The queue for subscribe request updates.
            subscribe_request (SubscribeRequest): The initial subscribe request.
            consumer_group_name (str): The name of the consumer group.
            control_plane_tx_q (asyncio.Queue): The queue for sending control commands.
            control_plane_rx_q (asyncio.Queue): The queue for receiving control responses.
            dragonsmouth_outlet (asyncio.Queue): The outlet for Dragonsmouth updates.
            commit_interval (float): The interval for committing offsets, in seconds.
            gc_interval (int): The interval for garbage collection, in seconds.
            max_concurrent_download (int): The maximum number of concurrent download tasks.
        """
        self.sm = sm
        self.slot_downloader: AsyncSlotDownloader = slot_downloader
        self.subscribe_request_update_q = subscribe_request_update_q
        self.subscribe_request = subscribe_request
        self.consumer_group_name = consumer_group_name
        self.control_plane_tx = control_plane_tx_q
        self.control_plane_rx = control_plane_rx_q
        self.dragonsmouth_outlet = dragonsmouth_outlet
        self.commit_interval = commit_interval
        self.gc_interval = gc_interval
        self.max_concurrent_download = max_concurrent_download
        self.download_tasks = dict()
        self.inner_runtime_channel: asyncio.Queue = asyncio.Queue()

    def _build_poll_history_cmd(
        self, from_offset: Optional[FumeOffset]
    ) -> ControlCommand:
        """Build a command to poll the blockchain history."""
        return ControlCommand(poll_hist=PollBlockchainHistory(shard_id=0, limit=None))

    def _build_commit_offset_cmd(self, offset: FumeOffset) -> ControlCommand:
        return ControlCommand(commit_offset=CommitOffset(offset=offset, shard_id=0))

    def _handle_control_response(self, control_response: ControlResponse):
        """Handle the control response received from the control plane."""
        response_field = control_response.WhichOneof("response")
        assert response_field is not None, "Control response is empty"

        match response_field:
            case "poll_hist":
                poll_hist = control_response.poll_hist
                LOGGER.debug(f"Received poll history {len(poll_hist.events)} events")
                self.sm.queue_blockchain_event(poll_hist.events)
            case "commit_offset":
                commit_offset = control_response.commit_offset
                LOGGER.debug(f"Received commit offset: {commit_offset}")
                self.sm.update_committed_offset(commit_offset.offset)
            case "pong":
                LOGGER.debug("Received pong")
            case _:
                raise ValueError("Unexpected control response")

    async def poll_history_if_needed(self):
        """Poll the history if the state machine needs new events."""
        if self.sm.need_new_blockchain_events():
            cmd = self._build_poll_history_cmd(self.sm.committable_offset)
            await self.control_plane_tx.put(cmd)

    def commitment_level(self):
        """Gets the commitment level from the subscribe request."""
        return self.subscribe_request.commitment

    def _schedule_download_task_if_any(self):
        """Schedules download tasks if there are any available slots."""
        while True:
            LOGGER.debug("Checking for download tasks to schedule")
            if len(self.download_tasks) >= self.max_concurrent_download:
                break

            # Pop a slot to download from the state machine
            LOGGER.debug("Popping slot to download")
            download_request = self.sm.pop_slot_to_download(self.commitment_level())
            if not download_request:
                LOGGER.debug("No download request available")
                break

            LOGGER.debug(f"Download request for slot {download_request.slot} popped")
            assert (
                download_request.blockchain_id
            ), "Download request must have a blockchain ID"
            download_task_args = DownloadTaskArgs(
                download_request=download_request,
                dragonsmouth_outlet=self.dragonsmouth_outlet,
            )

            coro = self.slot_downloader.run_download(
                self.subscribe_request, download_task_args
            )
            donwload_task = asyncio.create_task(coro)
            self.download_tasks[donwload_task] = download_request
            LOGGER.debug(f"Scheduling download task for slot {download_request.slot}")

    def _handle_download_result(self, download_result: DownloadTaskResult):
        """Handles the result of a download task."""
        if download_result.kind == "Ok":
            completed = download_result.completed
            LOGGER.debug(
                f"Download completed for slot {completed.slot}, shard {completed.shard_idx}, {completed.total_event_downloaded} total events"
            )
            self.sm.make_slot_download_progress(completed.slot, completed.shard_idx)
        else:
            slot = download_result.slot
            err = download_result.err
            raise RuntimeError(f"Failed to download slot {slot}: {err.message}")

    async def _force_commit_offset(self):
        LOGGER.debug(f"Force committing offset {self.sm.committable_offset}")
        await self.control_plane_tx.put(
            self._build_commit_offset_cmd(self.sm.committable_offset)
        )

    async def _commit_offset(self):
        if self.sm.last_committed_offset < self.sm.committable_offset:
            LOGGER.debug(f"Committing offset {self.sm.committable_offset}")
            await self._force_commit_offset()
        self.last_commit = time.time()

    async def _drain_slot_status(self):
        """Drains the slot status from the state machine and sends updates to the Dragonsmouth outlet."""
        commitment = self.subscribe_request.commitment
        slot_status_vec = deque()
        while slot_status := self.sm.pop_next_slot_status():
            slot_status_vec.append(slot_status)

        if not slot_status_vec:
            return

        LOGGER.debug(f"Draining {len(slot_status_vec)} slot status")
        for slot_status in slot_status_vec:
            matched_filters = []
            for filter_name, filter in self.subscribe_request.slots.items():
                if (
                    filter.filter_by_commitment
                    and slot_status.commitment_level == commitment
                ):
                    matched_filters.append(filter_name)
                elif not filter.filter_by_commitment:
                    matched_filters.append(filter_name)

            if matched_filters:
                update = SubscribeUpdate(
                    filters=matched_filters,
                    created_at=None,
                    slot=SubscribeUpdateSlot(
                        slot=slot_status.slot,
                        parent=slot_status.parent_slot,
                        status=slot_status.commitment_level,
                        dead_error=slot_status.dead_error,
                    ),
                )
                try:
                    await self.dragonsmouth_outlet.put(update)
                except asyncio.QueueFull:
                    return

            self.sm.mark_event_as_processed(slot_status.session_sequence)

    async def _handle_control_plane_resp(
        self, result: ControlResponse | Exception
    ) -> bool:
        """Handles the control plane response."""
        if isinstance(result, Exception):
            await self.dragonsmouth_outlet.put(result)
            return False
        self._handle_control_response(result)
        return True

    def handle_new_subscribe_request(self, subscribe_request: SubscribeRequest):
        self.subscribe_request = subscribe_request

    async def run(self):
        """Runs the Fumarole asyncio runtime."""
        LOGGER.debug(f"Fumarole runtime starting...")
        await self.control_plane_tx.put(self._build_poll_history_cmd(None))
        LOGGER.debug("Initial poll history command sent")
        await self._force_commit_offset()
        LOGGER.debug("Initial commit offset command sent")
        ticks = 0

        task_map = {
            asyncio.create_task(
                self.subscribe_request_update_q.get()
            ): "dragonsmouth_bidi",
            asyncio.create_task(self.control_plane_rx.get()): "control_plane_rx",
            asyncio.create_task(Interval(self.commit_interval).tick()): "commit_tick",
        }

        pending = set(task_map.keys())
        while pending:
            ticks += 1
            LOGGER.debug(f"Runtime loop tick")
            if ticks % self.gc_interval == 0:
                LOGGER.debug("Running garbage collection")
                self.sm.gc()
                ticks = 0
            LOGGER.debug(f"Polling history if needed")
            await self.poll_history_if_needed()
            LOGGER.debug("Scheduling download tasks if any")
            self._schedule_download_task_if_any()
            for t in self.download_tasks.keys():
                pending.add(t)
                task_map[t] = "download_task"

            download_task_inflight = len(self.download_tasks)
            LOGGER.debug(
                f"Current download tasks in flight: {download_task_inflight} / {self.max_concurrent_download}"
            )
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                result = t.result()
                name = task_map.pop(t)
                match name:
                    case "dragonsmouth_bidi":
                        LOGGER.debug("Dragonsmouth subscribe request received")
                        assert isinstance(
                            result, SubscribeRequest
                        ), "Expected SubscribeRequest"
                        self.handle_new_subscribe_request(result)
                        new_task = asyncio.create_task(
                            self.subscribe_request_update_q.get()
                        )
                        task_map[new_task] = "dragonsmouth_bidi"
                        pending.add(new_task)
                        pass
                    case "control_plane_rx":
                        LOGGER.debug("Control plane response received")
                        if not await self._handle_control_plane_resp(result):
                            LOGGER.debug("Control plane error")
                            return
                        new_task = asyncio.create_task(self.control_plane_rx.get())
                        task_map[new_task] = "control_plane_rx"
                        pending.add(new_task)
                    case "download_task":
                        LOGGER.debug("Download task result received")
                        assert self.download_tasks.pop(t)
                        self._handle_download_result(result)
                    case "commit_tick":
                        LOGGER.debug("Commit tick reached")
                        await self._commit_offset()
                        new_task = asyncio.create_task(
                            Interval(self.commit_interval).tick()
                        )
                        task_map[new_task] = "commit_tick"
                        pending.add(new_task)
                    case unknown:
                        raise RuntimeError(f"Unexpected task name: {unknown}")

            await self._drain_slot_status()

        LOGGER.debug("Fumarole runtime exiting")


# DownloadTaskRunnerChannels
@dataclass
class DownloadTaskRunnerChannels:
    download_task_queue_tx: asyncio.Queue
    cnc_tx: asyncio.Queue
    download_result_rx: asyncio.Queue


# DownloadTaskRunnerCommand
@dataclass
class DownloadTaskRunnerCommand:
    kind: str
    subscribe_request: Optional[SubscribeRequest] = None

    @classmethod
    def UpdateSubscribeRequest(cls, subscribe_request: SubscribeRequest):
        return cls(kind="UpdateSubscribeRequest", subscribe_request=subscribe_request)


# DownloadTaskArgs
@dataclass
class DownloadTaskArgs:
    download_request: FumeDownloadRequest
    dragonsmouth_outlet: asyncio.Queue


class GrpcSlotDownloader(AsyncSlotDownloader):

    def __init__(
        self,
        client: GrpcFumaroleClient,
    ):
        self.client = client

    async def run_download(
        self, subscribe_request: SubscribeRequest, spec: DownloadTaskArgs
    ) -> DownloadTaskResult:

        download_task = GrpcDownloadBlockTaskRun(
            download_request=spec.download_request,
            client=self.client,
            filters=BlockFilters(
                accounts=subscribe_request.accounts,
                transactions=subscribe_request.transactions,
                entries=subscribe_request.entry,
                blocks_meta=subscribe_request.blocks_meta,
            ),
            dragonsmouth_oulet=spec.dragonsmouth_outlet,
        )

        LOGGER.debug(f"Running download task for slot {spec.download_request.slot}")
        return await download_task.run()


# GrpcDownloadBlockTaskRun
class GrpcDownloadBlockTaskRun:
    def __init__(
        self,
        download_request: FumeDownloadRequest,
        client: GrpcFumaroleClient,
        filters: Optional[BlockFilters],
        dragonsmouth_oulet: asyncio.Queue,
    ):
        self.download_request = download_request
        self.client = client
        self.filters = filters
        self.dragonsmouth_oulet = dragonsmouth_oulet

    def map_tonic_error_code_to_download_block_error(
        self, e: grpc.aio.AioRpcError
    ) -> DownloadBlockError:
        code = e.code()
        if code == grpc.StatusCode.NOT_FOUND:
            return DownloadBlockError(
                kind="BlockShardNotFound", message="Block shard not found"
            )
        elif code == grpc.StatusCode.UNAVAILABLE:
            return DownloadBlockError(kind="Disconnected", message="Disconnected")
        elif code in (
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.DATA_LOSS,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        ):
            return DownloadBlockError(kind="FailedDownload", message="Failed download")
        elif code == grpc.StatusCode.INVALID_ARGUMENT:
            raise ValueError("Invalid argument")
        else:
            return DownloadBlockError(kind="Fatal", message=f"Unknown error: {code}")

    async def run(self) -> DownloadTaskResult:
        request = DownloadBlockShard(
            blockchain_id=self.download_request.blockchain_id,
            block_uid=self.download_request.block_uid,
            shard_idx=0,
            blockFilters=self.filters,
        )
        try:
            LOGGER.debug(
                f"Requesting download for block {self.download_request.block_uid.hex()} at slot {self.download_request.slot}"
            )
            resp = self.client.DownloadBlock(request)
        except grpc.aio.AioRpcError as e:
            LOGGER.error(f"Download block error: {e}")
            return DownloadTaskResult(
                kind="Err",
                slot=self.download_request.slot,
                err=self.map_tonic_error_code_to_download_block_error(e),
            )

        total_event_downloaded = 0
        try:
            async for data in resp:
                kind = data.WhichOneof("response")
                match kind:
                    case "update":
                        update = data.update
                        assert update is not None, "Update is None"
                        total_event_downloaded += 1
                        try:
                            await self.dragonsmouth_oulet.put(update)
                        except asyncio.QueueShutDown:
                            LOGGER.error("Dragonsmouth outlet is disconnected")
                            return DownloadTaskResult(
                                kind="Err",
                                slot=self.download_request.slot,
                                err=DownloadBlockError(
                                    kind="OutletDisconnected",
                                    message="Outlet disconnected",
                                ),
                            )
                    case "block_shard_download_finish":
                        LOGGER.debug(
                            f"Download finished for block {self.download_request.block_uid.hex()} at slot {self.download_request.slot}"
                        )
                        return DownloadTaskResult(
                            kind="Ok",
                            completed=CompletedDownloadBlockTask(
                                slot=self.download_request.slot,
                                block_uid=self.download_request.block_uid,
                                shard_idx=0,
                                total_event_downloaded=total_event_downloaded,
                            ),
                        )
                    case unknown:
                        raise RuntimeError("Unexpected response kind: {unknown}")
        except grpc.aio.AioRpcError as e:
            LOGGER.error(f"Download block error: {e}")
            return DownloadTaskResult(
                kind="Err",
                slot=self.download_request.slot,
                err=self.map_tonic_error_code_to_download_block_error(e),
            )

        return DownloadTaskResult(
            kind="Err",
            slot=self.download_request.slot,
            err=DownloadBlockError(kind="FailedDownload", message="Failed download"),
        )
