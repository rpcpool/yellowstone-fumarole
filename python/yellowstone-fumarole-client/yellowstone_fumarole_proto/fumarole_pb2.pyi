import yellowstone_fumarole_proto.geyser_pb2 as _geyser_pb2
import yellowstone_fumarole_proto.solana_storage_pb2 as _solana_storage_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequest as SubscribeRequest
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterAccounts as SubscribeRequestFilterAccounts
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterAccountsFilter as SubscribeRequestFilterAccountsFilter
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterAccountsFilterMemcmp as SubscribeRequestFilterAccountsFilterMemcmp
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterAccountsFilterLamports as SubscribeRequestFilterAccountsFilterLamports
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterSlots as SubscribeRequestFilterSlots
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterTransactions as SubscribeRequestFilterTransactions
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterBlocks as SubscribeRequestFilterBlocks
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterBlocksMeta as SubscribeRequestFilterBlocksMeta
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestFilterEntry as SubscribeRequestFilterEntry
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestAccountsDataSlice as SubscribeRequestAccountsDataSlice
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeRequestPing as SubscribeRequestPing
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdate as SubscribeUpdate
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateAccount as SubscribeUpdateAccount
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateAccountInfo as SubscribeUpdateAccountInfo
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateSlot as SubscribeUpdateSlot
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateTransaction as SubscribeUpdateTransaction
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateTransactionInfo as SubscribeUpdateTransactionInfo
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateTransactionStatus as SubscribeUpdateTransactionStatus
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateBlock as SubscribeUpdateBlock
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateBlockMeta as SubscribeUpdateBlockMeta
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdateEntry as SubscribeUpdateEntry
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdatePing as SubscribeUpdatePing
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeUpdatePong as SubscribeUpdatePong
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeReplayInfoRequest as SubscribeReplayInfoRequest
from yellowstone_fumarole_proto.geyser_pb2 import SubscribeReplayInfoResponse as SubscribeReplayInfoResponse
from yellowstone_fumarole_proto.geyser_pb2 import PingRequest as PingRequest
from yellowstone_fumarole_proto.geyser_pb2 import PongResponse as PongResponse
from yellowstone_fumarole_proto.geyser_pb2 import GetLatestBlockhashRequest as GetLatestBlockhashRequest
from yellowstone_fumarole_proto.geyser_pb2 import GetLatestBlockhashResponse as GetLatestBlockhashResponse
from yellowstone_fumarole_proto.geyser_pb2 import GetBlockHeightRequest as GetBlockHeightRequest
from yellowstone_fumarole_proto.geyser_pb2 import GetBlockHeightResponse as GetBlockHeightResponse
from yellowstone_fumarole_proto.geyser_pb2 import GetSlotRequest as GetSlotRequest
from yellowstone_fumarole_proto.geyser_pb2 import GetSlotResponse as GetSlotResponse
from yellowstone_fumarole_proto.geyser_pb2 import GetVersionRequest as GetVersionRequest
from yellowstone_fumarole_proto.geyser_pb2 import GetVersionResponse as GetVersionResponse
from yellowstone_fumarole_proto.geyser_pb2 import IsBlockhashValidRequest as IsBlockhashValidRequest
from yellowstone_fumarole_proto.geyser_pb2 import IsBlockhashValidResponse as IsBlockhashValidResponse
from yellowstone_fumarole_proto.geyser_pb2 import CommitmentLevel as CommitmentLevel
from yellowstone_fumarole_proto.geyser_pb2 import SlotStatus as SlotStatus

DESCRIPTOR: _descriptor.FileDescriptor
PROCESSED: _geyser_pb2.CommitmentLevel
CONFIRMED: _geyser_pb2.CommitmentLevel
FINALIZED: _geyser_pb2.CommitmentLevel
SLOT_PROCESSED: _geyser_pb2.SlotStatus
SLOT_CONFIRMED: _geyser_pb2.SlotStatus
SLOT_FINALIZED: _geyser_pb2.SlotStatus
SLOT_FIRST_SHRED_RECEIVED: _geyser_pb2.SlotStatus
SLOT_COMPLETED: _geyser_pb2.SlotStatus
SLOT_CREATED_BANK: _geyser_pb2.SlotStatus
SLOT_DEAD: _geyser_pb2.SlotStatus

class InitialOffsetPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LATEST: _ClassVar[InitialOffsetPolicy]
    FROM_SLOT: _ClassVar[InitialOffsetPolicy]
LATEST: InitialOffsetPolicy
FROM_SLOT: InitialOffsetPolicy

class GetSlotRangeRequest(_message.Message):
    __slots__ = ("blockchain_id",)
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    def __init__(self, blockchain_id: _Optional[bytes] = ...) -> None: ...

class GetSlotRangeResponse(_message.Message):
    __slots__ = ("blockchain_id", "min_slot", "max_slot")
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    MIN_SLOT_FIELD_NUMBER: _ClassVar[int]
    MAX_SLOT_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    min_slot: int
    max_slot: int
    def __init__(self, blockchain_id: _Optional[bytes] = ..., min_slot: _Optional[int] = ..., max_slot: _Optional[int] = ...) -> None: ...

class GetChainTipRequest(_message.Message):
    __slots__ = ("blockchain_id",)
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    def __init__(self, blockchain_id: _Optional[bytes] = ...) -> None: ...

class GetChainTipResponse(_message.Message):
    __slots__ = ("blockchain_id", "shard_to_max_offset_map")
    class ShardToMaxOffsetMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    SHARD_TO_MAX_OFFSET_MAP_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    shard_to_max_offset_map: _containers.ScalarMap[int, int]
    def __init__(self, blockchain_id: _Optional[bytes] = ..., shard_to_max_offset_map: _Optional[_Mapping[int, int]] = ...) -> None: ...

class VersionRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class VersionResponse(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    def __init__(self, version: _Optional[str] = ...) -> None: ...

class GetConsumerGroupInfoRequest(_message.Message):
    __slots__ = ("consumer_group_name",)
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    consumer_group_name: str
    def __init__(self, consumer_group_name: _Optional[str] = ...) -> None: ...

class DeleteConsumerGroupRequest(_message.Message):
    __slots__ = ("consumer_group_name",)
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    consumer_group_name: str
    def __init__(self, consumer_group_name: _Optional[str] = ...) -> None: ...

class DeleteConsumerGroupResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class ListConsumerGroupsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListConsumerGroupsResponse(_message.Message):
    __slots__ = ("consumer_groups",)
    CONSUMER_GROUPS_FIELD_NUMBER: _ClassVar[int]
    consumer_groups: _containers.RepeatedCompositeFieldContainer[ConsumerGroupInfo]
    def __init__(self, consumer_groups: _Optional[_Iterable[_Union[ConsumerGroupInfo, _Mapping]]] = ...) -> None: ...

class ConsumerGroupInfo(_message.Message):
    __slots__ = ("id", "consumer_group_name", "is_stale", "blockchain_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_STALE_FIELD_NUMBER: _ClassVar[int]
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    consumer_group_name: str
    is_stale: bool
    blockchain_id: bytes
    def __init__(self, id: _Optional[str] = ..., consumer_group_name: _Optional[str] = ..., is_stale: bool = ..., blockchain_id: _Optional[bytes] = ...) -> None: ...

class GetSlotLagInfoRequest(_message.Message):
    __slots__ = ("consumer_group_name",)
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    consumer_group_name: str
    def __init__(self, consumer_group_name: _Optional[str] = ...) -> None: ...

class BlockFilters(_message.Message):
    __slots__ = ("accounts", "transactions", "entries", "blocks_meta")
    class AccountsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _geyser_pb2.SubscribeRequestFilterAccounts
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_geyser_pb2.SubscribeRequestFilterAccounts, _Mapping]] = ...) -> None: ...
    class TransactionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _geyser_pb2.SubscribeRequestFilterTransactions
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_geyser_pb2.SubscribeRequestFilterTransactions, _Mapping]] = ...) -> None: ...
    class EntriesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _geyser_pb2.SubscribeRequestFilterEntry
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_geyser_pb2.SubscribeRequestFilterEntry, _Mapping]] = ...) -> None: ...
    class BlocksMetaEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _geyser_pb2.SubscribeRequestFilterBlocksMeta
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_geyser_pb2.SubscribeRequestFilterBlocksMeta, _Mapping]] = ...) -> None: ...
    ACCOUNTS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    BLOCKS_META_FIELD_NUMBER: _ClassVar[int]
    accounts: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterAccounts]
    transactions: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterTransactions]
    entries: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterEntry]
    blocks_meta: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterBlocksMeta]
    def __init__(self, accounts: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterAccounts]] = ..., transactions: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterTransactions]] = ..., entries: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterEntry]] = ..., blocks_meta: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterBlocksMeta]] = ...) -> None: ...

class DownloadBlockShard(_message.Message):
    __slots__ = ("blockchain_id", "block_uid", "shard_idx", "blockFilters")
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_UID_FIELD_NUMBER: _ClassVar[int]
    SHARD_IDX_FIELD_NUMBER: _ClassVar[int]
    BLOCKFILTERS_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    block_uid: bytes
    shard_idx: int
    blockFilters: BlockFilters
    def __init__(self, blockchain_id: _Optional[bytes] = ..., block_uid: _Optional[bytes] = ..., shard_idx: _Optional[int] = ..., blockFilters: _Optional[_Union[BlockFilters, _Mapping]] = ...) -> None: ...

class Ping(_message.Message):
    __slots__ = ("ping_id",)
    PING_ID_FIELD_NUMBER: _ClassVar[int]
    ping_id: int
    def __init__(self, ping_id: _Optional[int] = ...) -> None: ...

class Pong(_message.Message):
    __slots__ = ("ping_id",)
    PING_ID_FIELD_NUMBER: _ClassVar[int]
    ping_id: int
    def __init__(self, ping_id: _Optional[int] = ...) -> None: ...

class DataCommand(_message.Message):
    __slots__ = ("download_block_shard", "filter_update")
    DOWNLOAD_BLOCK_SHARD_FIELD_NUMBER: _ClassVar[int]
    FILTER_UPDATE_FIELD_NUMBER: _ClassVar[int]
    download_block_shard: DownloadBlockShard
    filter_update: BlockFilters
    def __init__(self, download_block_shard: _Optional[_Union[DownloadBlockShard, _Mapping]] = ..., filter_update: _Optional[_Union[BlockFilters, _Mapping]] = ...) -> None: ...

class BlockShardDownloadFinish(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BlockNotFound(_message.Message):
    __slots__ = ("blockchain_id", "block_uid", "shard_idx")
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_UID_FIELD_NUMBER: _ClassVar[int]
    SHARD_IDX_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    block_uid: bytes
    shard_idx: int
    def __init__(self, blockchain_id: _Optional[bytes] = ..., block_uid: _Optional[bytes] = ..., shard_idx: _Optional[int] = ...) -> None: ...

class DataError(_message.Message):
    __slots__ = ("not_found",)
    NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    not_found: BlockNotFound
    def __init__(self, not_found: _Optional[_Union[BlockNotFound, _Mapping]] = ...) -> None: ...

class DataResponse(_message.Message):
    __slots__ = ("update", "block_shard_download_finish")
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    BLOCK_SHARD_DOWNLOAD_FINISH_FIELD_NUMBER: _ClassVar[int]
    update: _geyser_pb2.SubscribeUpdate
    block_shard_download_finish: BlockShardDownloadFinish
    def __init__(self, update: _Optional[_Union[_geyser_pb2.SubscribeUpdate, _Mapping]] = ..., block_shard_download_finish: _Optional[_Union[BlockShardDownloadFinish, _Mapping]] = ...) -> None: ...

class CommitOffset(_message.Message):
    __slots__ = ("offset", "shard_id")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    offset: int
    shard_id: int
    def __init__(self, offset: _Optional[int] = ..., shard_id: _Optional[int] = ...) -> None: ...

class PollBlockchainHistory(_message.Message):
    __slots__ = ("shard_id", "limit")
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    shard_id: int
    limit: int
    def __init__(self, shard_id: _Optional[int] = ..., limit: _Optional[int] = ..., **kwargs) -> None: ...

class BlockchainEvent(_message.Message):
    __slots__ = ("offset", "blockchain_id", "block_uid", "num_shards", "slot", "parent_slot", "commitment_level", "blockchain_shard_id", "dead_error")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_UID_FIELD_NUMBER: _ClassVar[int]
    NUM_SHARDS_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    PARENT_SLOT_FIELD_NUMBER: _ClassVar[int]
    COMMITMENT_LEVEL_FIELD_NUMBER: _ClassVar[int]
    BLOCKCHAIN_SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    DEAD_ERROR_FIELD_NUMBER: _ClassVar[int]
    offset: int
    blockchain_id: bytes
    block_uid: bytes
    num_shards: int
    slot: int
    parent_slot: int
    commitment_level: _geyser_pb2.CommitmentLevel
    blockchain_shard_id: int
    dead_error: str
    def __init__(self, offset: _Optional[int] = ..., blockchain_id: _Optional[bytes] = ..., block_uid: _Optional[bytes] = ..., num_shards: _Optional[int] = ..., slot: _Optional[int] = ..., parent_slot: _Optional[int] = ..., commitment_level: _Optional[_Union[_geyser_pb2.CommitmentLevel, str]] = ..., blockchain_shard_id: _Optional[int] = ..., dead_error: _Optional[str] = ...) -> None: ...

class BlockchainHistory(_message.Message):
    __slots__ = ("events",)
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[BlockchainEvent]
    def __init__(self, events: _Optional[_Iterable[_Union[BlockchainEvent, _Mapping]]] = ...) -> None: ...

class JoinControlPlane(_message.Message):
    __slots__ = ("consumer_group_name",)
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    consumer_group_name: str
    def __init__(self, consumer_group_name: _Optional[str] = ...) -> None: ...

class ControlCommand(_message.Message):
    __slots__ = ("initial_join", "commit_offset", "poll_hist", "ping")
    INITIAL_JOIN_FIELD_NUMBER: _ClassVar[int]
    COMMIT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    POLL_HIST_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    initial_join: JoinControlPlane
    commit_offset: CommitOffset
    poll_hist: PollBlockchainHistory
    ping: Ping
    def __init__(self, initial_join: _Optional[_Union[JoinControlPlane, _Mapping]] = ..., commit_offset: _Optional[_Union[CommitOffset, _Mapping]] = ..., poll_hist: _Optional[_Union[PollBlockchainHistory, _Mapping]] = ..., ping: _Optional[_Union[Ping, _Mapping]] = ...) -> None: ...

class ControlResponse(_message.Message):
    __slots__ = ("init", "commit_offset", "poll_hist", "pong")
    INIT_FIELD_NUMBER: _ClassVar[int]
    COMMIT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    POLL_HIST_FIELD_NUMBER: _ClassVar[int]
    PONG_FIELD_NUMBER: _ClassVar[int]
    init: InitialConsumerGroupState
    commit_offset: CommitOffsetResult
    poll_hist: BlockchainHistory
    pong: Pong
    def __init__(self, init: _Optional[_Union[InitialConsumerGroupState, _Mapping]] = ..., commit_offset: _Optional[_Union[CommitOffsetResult, _Mapping]] = ..., poll_hist: _Optional[_Union[BlockchainHistory, _Mapping]] = ..., pong: _Optional[_Union[Pong, _Mapping]] = ...) -> None: ...

class CommitOffsetResult(_message.Message):
    __slots__ = ("offset", "shard_id")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    offset: int
    shard_id: int
    def __init__(self, offset: _Optional[int] = ..., shard_id: _Optional[int] = ...) -> None: ...

class InitialConsumerGroupState(_message.Message):
    __slots__ = ("blockchain_id", "last_committed_offsets")
    class LastCommittedOffsetsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    BLOCKCHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_COMMITTED_OFFSETS_FIELD_NUMBER: _ClassVar[int]
    blockchain_id: bytes
    last_committed_offsets: _containers.ScalarMap[int, int]
    def __init__(self, blockchain_id: _Optional[bytes] = ..., last_committed_offsets: _Optional[_Mapping[int, int]] = ...) -> None: ...

class CreateConsumerGroupResponse(_message.Message):
    __slots__ = ("consumer_group_id",)
    CONSUMER_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    consumer_group_id: str
    def __init__(self, consumer_group_id: _Optional[str] = ...) -> None: ...

class CreateConsumerGroupRequest(_message.Message):
    __slots__ = ("consumer_group_name", "initial_offset_policy", "from_slot")
    CONSUMER_GROUP_NAME_FIELD_NUMBER: _ClassVar[int]
    INITIAL_OFFSET_POLICY_FIELD_NUMBER: _ClassVar[int]
    FROM_SLOT_FIELD_NUMBER: _ClassVar[int]
    consumer_group_name: str
    initial_offset_policy: InitialOffsetPolicy
    from_slot: int
    def __init__(self, consumer_group_name: _Optional[str] = ..., initial_offset_policy: _Optional[_Union[InitialOffsetPolicy, str]] = ..., from_slot: _Optional[int] = ...) -> None: ...
