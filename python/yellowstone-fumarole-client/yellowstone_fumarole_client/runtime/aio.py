# DataPlaneConn
import asyncio
from typing import Optional, List
from collections import deque
from dataclasses import dataclass
import time
from yellowstone_fumarole_client.runtime.state_machine import (
    FumaroleSM,
    FumeDownloadRequest,
    FumeOffset,
    FumeShardIdx,
    CommitmentLevel,
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
from yellowstone_fumarole_client.grpc_connectivity import FumaroleGrpcConnector
import logging


# Constants
DEFAULT_GC_INTERVAL = 100

DEFAULT_SLOT_MEMORY_RETENTION = 10000


@dataclass
class DataPlaneConn:
    permits: int
    client: GrpcFumaroleClient
    rev: int

    def has_permit(self) -> bool:
        return self.permits > 0


# DownloadTaskResult
@dataclass
class CompletedDownloadBlockTask:
    slot: int
    block_uid: bytes
    shard_idx: FumeShardIdx
    total_event_downloaded: int


@dataclass
class DownloadBlockError:
    kind: str  # 'Disconnected', 'OutletDisconnected', 'BlockShardNotFound', 'FailedDownload', 'Fatal'
    message: str


@dataclass
class DownloadTaskResult:
    kind: str  # 'Ok' or 'Err'
    completed: Optional[CompletedDownloadBlockTask] = None
    slot: Optional[int] = None
    err: Optional[DownloadBlockError] = None


# DragonsmouthSubscribeRequestBidi
@dataclass
class DragonsmouthSubscribeRequestBidi:
    rx: asyncio.Queue


LOGGER = logging.getLogger(__name__)


# TokioFumeDragonsmouthRuntime
class AsyncioFumeDragonsmouthRuntime:

    def __init__(
        self,
        sm: FumaroleSM,
        download_task_runner_chans: "DownloadTaskRunnerChannels",
        dragonsmouth_bidi: DragonsmouthSubscribeRequestBidi,
        subscribe_request: SubscribeRequest,
        consumer_group_name: str,
        control_plane_q: asyncio.Queue,
        control_plane_stream_reader,
        dragonsmouth_outlet: asyncio.Queue,
        commit_interval: float,  # in seconds
        gc_interval: int,
    ):
        self.sm = sm
        self.download_task_runner_chans = download_task_runner_chans
        self.dragonsmouth_bidi = dragonsmouth_bidi
        self.subscribe_request = subscribe_request
        self.consumer_group_name = consumer_group_name
        self.control_plane_tx = control_plane_q
        self.control_plane_stream_rx = control_plane_stream_reader
        self.dragonsmouth_outlet = dragonsmouth_outlet
        self.commit_interval = commit_interval
        self.last_commit = time.time()
        self.gc_interval = gc_interval

    def build_poll_history_cmd(
        self, from_offset: Optional[FumeOffset]
    ) -> ControlCommand:
        return ControlCommand(poll_hist=PollBlockchainHistory(shard_id=0, limit=None))

    def build_commit_offset_cmd(self, offset: FumeOffset) -> ControlCommand:
        return ControlCommand(commit_offset=CommitOffset(offset=offset, shard_id=0))

    def handle_control_response(self, control_response: ControlResponse):

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
        if self.sm.need_new_blockchain_events():
            cmd = self.build_poll_history_cmd(self.sm.committable_offset)
            await self.control_plane_tx.put(cmd)

    def commitment_level(self):
        return self.subscribe_request.commitment

    def schedule_download_task_if_any(self):
        while True:
            if (
                not self.download_task_runner_chans.download_task_queue_tx.qsize() < 100
            ):  # Simulate try_reserve
                break
            download_request = self.sm.pop_slot_to_download(self.commitment_level())
            if not download_request:
                break
            download_task_args = DownloadTaskArgs(
                download_request=download_request,
                dragonsmouth_outlet=self.dragonsmouth_outlet,
            )
            LOGGER.debug(f"Scheduling download task for slot {download_request.slot}")
            asyncio.create_task(
                self.download_task_runner_chans.download_task_queue_tx.put(
                    download_task_args
                )
            )

    def handle_download_result(self, download_result: DownloadTaskResult):
        if download_result.kind == "Ok":
            completed = download_result.completed
            LOGGER.debug(
                f"Download completed for slot {completed.slot}, shard {completed.shard_idx}"
            )
            self.sm.make_slot_download_progress(completed.slot, completed.shard_idx)
        else:
            slot = download_result.slot
            err = download_result.err
            raise RuntimeError(f"Failed to download slot {slot}: {err.message}")

    async def force_commit_offset(self):
        LOGGER.debug(f"Force committing offset {self.sm.committable_offset}")
        await self.control_plane_tx.put(
            self.build_commit_offset_cmd(self.sm.committable_offset)
        )

    async def commit_offset(self):
        if self.sm.last_committed_offset < self.sm.committable_offset:
            LOGGER.debug(f"Committing offset {self.sm.committable_offset}")
            await self.force_commit_offset()
        self.last_commit = time.time()

    async def drain_slot_status(self):
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
                    and slot_status.commitment_level.value == commitment
                ):
                    matched_filters.append(filter_name)
                elif not filter.filter_by_commitment:
                    matched_filters.append(filter_name)

            if matched_filters:
                update = SubscribeUpdate(
                    filters=matched_filters,
                    created_at=None,
                    update_oneof=SubscribeUpdateSlot(
                        slot=slot_status.slot,
                        parent=slot_status.parent_slot,
                        status=slot_status.commitment_level.value,
                        dead_error=slot_status.dead_error,
                    ),
                )
                LOGGER.debug(f"Sending dragonsmouth update: {update}")
                try:
                    await self.dragonsmouth_outlet.put(update)
                except asyncio.QueueFull:
                    return

            self.sm.mark_event_as_processed(slot_status.session_sequence)

    async def handle_control_plane_resp(
        self, result: ControlResponse | Exception
    ) -> bool:
        if isinstance(result, Exception):
            await self.dragonsmouth_outlet.put(result)
            return False
        self.handle_control_response(result)
        return True

    async def handle_new_subscribe_request(self, subscribe_request: SubscribeRequest):
        self.subscribe_request = subscribe_request
        await self.download_task_runner_chans.cnc_tx.put(
            DownloadTaskRunnerCommand.UpdateSubscribeRequest(subscribe_request)
        )

    async def run(self):
        LOGGER.debug(f"Fumarole runtime starting...")
        await self.control_plane_tx.put(self.build_poll_history_cmd(None))
        LOGGER.debug("Initial poll history command sent")
        await self.force_commit_offset()
        LOGGER.debug("Initial commit offset command sent")
        ticks = 0
        while True:
            ticks += 1
            LOGGER.debug(f"Runtime loop tick")
            if ticks % self.gc_interval == 0:
                LOGGER.debug("Running garbage collection")
                self.sm.gc()
                ticks = 0

            if self.dragonsmouth_outlet.qsize() >= 100:  # Simulate is_closed
                LOGGER.debug("Detected dragonsmouth outlet closed")
                break

            commit_deadline = self.last_commit + self.commit_interval
            await self.poll_history_if_needed()
            self.schedule_download_task_if_any()

            tasks = [
                asyncio.create_task(self.dragonsmouth_bidi.rx.get()),
                asyncio.create_task(self.control_plane_stream_rx.read()),
                asyncio.create_task(
                    self.download_task_runner_chans.download_result_rx.get()
                ),
                asyncio.create_task(
                    asyncio.sleep(max(0, commit_deadline - time.time()))
                ),
            ]

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()

            for task in done:
                try:
                    result = task.result()
                    if task == tasks[0]:  # dragonsmouth_bidi.rx
                        LOGGER.debug("Dragonsmouth subscribe request received")
                        await self.handle_new_subscribe_request(result)
                    elif task == tasks[1]:  # control_plane_rx
                        if not await self.handle_control_plane_resp(result):
                            LOGGER.debug("Control plane error")
                            return
                    elif task == tasks[2]:  # download_result_rx
                        self.handle_download_result(result)
                    elif task == tasks[3]:  # sleep
                        LOGGER.debug("Commit deadline reached")
                        await self.commit_offset()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    LOGGER.error(f"Error: {e}")
                    raise e

            await self.drain_slot_status()

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


# DataPlaneTaskMeta
@dataclass
class DataPlaneTaskMeta:
    client_idx: int
    request: FumeDownloadRequest
    dragonsmouth_outlet: asyncio.Queue
    scheduled_at: float
    client_rev: int


# GrpcDownloadTaskRunner
class GrpcDownloadTaskRunner:
    def __init__(
        self,
        data_plane_channel_vec: List[DataPlaneConn],
        connector: FumaroleGrpcConnector,
        cnc_rx: asyncio.Queue,
        download_task_queue: asyncio.Queue,
        outlet: asyncio.Queue,
        max_download_attempt_by_slot: int,
        subscribe_request: SubscribeRequest,
    ):
        self.data_plane_channel_vec = data_plane_channel_vec
        self.connector = connector
        self.tasks = []
        self.task_meta = {}
        self.cnc_rx = cnc_rx
        self.download_task_queue = download_task_queue
        self.download_attempts = {}
        self.outlet = outlet
        self.max_download_attempt_per_slot = max_download_attempt_by_slot
        self.subscribe_request = subscribe_request
        self.task_counter = 0

    def find_least_use_client(self) -> Optional[int]:
        max_permits = -1
        best_idx = None
        for idx, conn in enumerate(self.data_plane_channel_vec):
            if conn.has_permit() and conn.permits > max_permits:
                max_permits = conn.permits
                best_idx = idx
        return best_idx

    async def handle_data_plane_task_result(
        self, task_id: int, result: DownloadTaskResult
    ):
        task_meta = self.task_meta.pop(task_id, None)
        if not task_meta:
            raise RuntimeError("Missing task meta")

        slot = task_meta.request.slot
        conn = self.data_plane_channel_vec[task_meta.client_idx]
        conn.permits += 1

        if result.kind == "Ok":
            completed = result.completed
            elapsed = time.time() - task_meta.scheduled_at
            LOGGER.debug(
                f"Downloaded slot {slot} in {elapsed}s, total events: {completed.total_event_downloaded}"
            )
            self.download_attempts.pop(slot, None)
            await self.outlet.put(result)
        else:
            err = result.err
            download_attempt = self.download_attempts.get(slot, 0)
            if err.kind in ("Disconnected", "FailedDownload"):
                if download_attempt >= self.max_download_attempt_per_slot:
                    LOGGER.error(
                        f"Download slot {slot} failed: {err.message}, max attempts reached"
                    )
                    await self.outlet.put(
                        DownloadTaskResult(kind="Err", slot=slot, err=err)
                    )
                    return
                remaining = self.max_download_attempt_per_slot - download_attempt
                LOGGER.debug(
                    f"Download slot {slot} failed: {err.message}, remaining attempts: {remaining}"
                )
                if task_meta.client_rev == conn.rev:
                    conn.client = await self.connector.connect()
                    conn.rev += 1
                LOGGER.debug(f"Download slot {slot} failed, rescheduling for retry...")
                task_spec = DownloadTaskArgs(
                    download_request=task_meta.request,
                    dragonsmouth_outlet=task_meta.dragonsmouth_outlet,
                )
                self.spawn_grpc_download_task(task_meta.client_idx, task_spec)
            elif err.kind == "OutletDisconnected":
                LOGGER.debug("Dragonsmouth outlet disconnected")
            elif err.kind == "BlockShardNotFound":
                LOGGER.error(f"Slot {slot} not found")
                await self.outlet.put(
                    DownloadTaskResult(kind="Err", slot=slot, err=err)
                )
            elif err.kind == "Fatal":
                raise RuntimeError(f"Fatal error: {err.message}")

    def spawn_grpc_download_task(self, client_idx: int, task_spec: DownloadTaskArgs):
        conn = self.data_plane_channel_vec[client_idx]
        client = conn.client  # Clone not needed in Python
        download_request = task_spec.download_request
        slot = download_request.slot
        task = GrpcDownloadBlockTaskRun(
            download_request=download_request,
            client=client,
            filters=BlockFilters(
                accounts=self.subscribe_request.accounts,
                transactions=self.subscribe_request.transactions,
                entries=self.subscribe_request.entry,
                blocks_meta=self.subscribe_request.blocks_meta,
            ),
            dragonsmouth_oulet=task_spec.dragonsmouth_outlet,
        )
        task_id = self.task_counter
        self.task_counter += 1
        self.tasks.append(asyncio.create_task(task.run(task_id)))
        self.download_attempts[slot] = self.download_attempts.get(slot, 0) + 1
        conn.permits -= 1
        self.task_meta[task_id] = DataPlaneTaskMeta(
            client_idx=client_idx,
            request=download_request,
            dragonsmouth_outlet=task_spec.dragonsmouth_outlet,
            scheduled_at=time.time(),
            client_rev=conn.rev,
        )

    def handle_control_command(self, cmd: DownloadTaskRunnerCommand):
        if cmd.kind == "UpdateSubscribeRequest":
            self.subscribe_request = cmd.subscribe_request

    async def run(self):
        while self.outlet.qsize() < 100:  # Simulate is_closed
            maybe_available_client_idx = self.find_least_use_client()
            tasks = [asyncio.create_task(self.cnc_rx.get())]
            if maybe_available_client_idx is not None:
                tasks.append(asyncio.create_task(self.download_task_queue.get()))
            for task in self.tasks[:]:
                if task.done():
                    self.tasks.remove(task)
                    task_id, result = task.result()
                    await self.handle_data_plane_task_result(task_id, result)
            if tasks:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        result = task.result()
                        if task == tasks[0]:  # cnc_rx
                            self.handle_control_command(result)
                        elif len(tasks) > 1 and task == tasks[1]:  # download_task_queue
                            self.spawn_grpc_download_task(
                                maybe_available_client_idx, result
                            )
                    except Exception as e:
                        LOGGER.debug(f"Error: {e}")
                        return
        LOGGER.debug("Closing GrpcDownloadTaskRunner loop")


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
        self, code: str
    ) -> DownloadBlockError:
        if code == "NotFound":
            return DownloadBlockError(
                kind="BlockShardNotFound", message="Block shard not found"
            )
        elif code == "Unavailable":
            return DownloadBlockError(kind="Disconnected", message="Disconnected")
        elif code in (
            "Internal",
            "Aborted",
            "DataLoss",
            "ResourceExhausted",
            "Unknown",
            "Cancelled",
            "DeadlineExceeded",
        ):
            return DownloadBlockError(kind="FailedDownload", message="Failed download")
        elif code == "InvalidArgument":
            raise ValueError("Invalid argument")
        else:
            return DownloadBlockError(kind="Fatal", message=f"Unknown error: {code}")

    async def run(self, task_id: int) -> tuple[int, DownloadTaskResult]:
        request = DownloadBlockShard(
            blockchain_id=self.download_request.blockchain_id,
            block_uid=self.download_request.block_uid,
            shard_idx=0,
            blockFilters=self.filters,
        )
        try:
            resp = self.client.DownloadBlock(request)
        except Exception as e:
            LOGGER.error(f"Download block error: {e}")
            return task_id, DownloadTaskResult(
                kind="Err",
                slot=self.download_request.slot,
                err=self.map_tonic_error_code_to_download_block_error(str(e)),
            )

        total_event_downloaded = 0
        async for data in resp:

            kind = data.WhichOneof("response")

            match kind:
                case "update":
                    update = data.update
                    assert update is not None, "Update is None"
                    total_event_downloaded += 1
                    try:
                        await self.dragonsmouth_oulet.put(update)
                    except asyncio.QueueFull:
                        return task_id, DownloadTaskResult(
                            kind="Err",
                            slot=self.download_request.slot,
                            err=DownloadBlockError(
                                kind="OutletDisconnected", message="Outlet disconnected"
                            ),
                        )
                case "block_shard_download_finish":
                    return task_id, DownloadTaskResult(
                        kind="Ok",
                        completed=CompletedDownloadBlockTask(
                            slot=self.download_request.slot,
                            block_uid=self.download_request.block_uid,
                            shard_idx=0,
                            total_event_downloaded=total_event_downloaded,
                        ),
                    )
                case _:
                    return task_id, DownloadTaskResult(
                        kind="Err",
                        slot=self.download_request.slot,
                        err=self.map_tonic_error_code_to_download_block_error(
                            "Unknown"
                        ),
                    )

        return task_id, DownloadTaskResult(
            kind="Err",
            slot=self.download_request.slot,
            err=DownloadBlockError(kind="FailedDownload", message="Failed download"),
        )
