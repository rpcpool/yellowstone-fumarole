import yellowstone_api.geyser_pb2 as _geyser_pb2
import yellowstone_api.solana_storage_pb2 as _solana_storage_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union
from yellowstone_api.geyser_pb2 import SubscribeRequest as SubscribeRequest
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterAccounts as SubscribeRequestFilterAccounts
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterAccountsFilter as SubscribeRequestFilterAccountsFilter
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterAccountsFilterMemcmp as SubscribeRequestFilterAccountsFilterMemcmp
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterAccountsFilterLamports as SubscribeRequestFilterAccountsFilterLamports
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterSlots as SubscribeRequestFilterSlots
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterTransactions as SubscribeRequestFilterTransactions
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterBlocks as SubscribeRequestFilterBlocks
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterBlocksMeta as SubscribeRequestFilterBlocksMeta
from yellowstone_api.geyser_pb2 import SubscribeRequestFilterEntry as SubscribeRequestFilterEntry
from yellowstone_api.geyser_pb2 import SubscribeRequestAccountsDataSlice as SubscribeRequestAccountsDataSlice
from yellowstone_api.geyser_pb2 import SubscribeRequestPing as SubscribeRequestPing
from yellowstone_api.geyser_pb2 import SubscribeUpdate as SubscribeUpdate
from yellowstone_api.geyser_pb2 import SubscribeUpdateAccount as SubscribeUpdateAccount
from yellowstone_api.geyser_pb2 import SubscribeUpdateAccountInfo as SubscribeUpdateAccountInfo
from yellowstone_api.geyser_pb2 import SubscribeUpdateSlot as SubscribeUpdateSlot
from yellowstone_api.geyser_pb2 import SubscribeUpdateTransaction as SubscribeUpdateTransaction
from yellowstone_api.geyser_pb2 import SubscribeUpdateTransactionInfo as SubscribeUpdateTransactionInfo
from yellowstone_api.geyser_pb2 import SubscribeUpdateTransactionStatus as SubscribeUpdateTransactionStatus
from yellowstone_api.geyser_pb2 import SubscribeUpdateBlock as SubscribeUpdateBlock
from yellowstone_api.geyser_pb2 import SubscribeUpdateBlockMeta as SubscribeUpdateBlockMeta
from yellowstone_api.geyser_pb2 import SubscribeUpdateEntry as SubscribeUpdateEntry
from yellowstone_api.geyser_pb2 import SubscribeUpdatePing as SubscribeUpdatePing
from yellowstone_api.geyser_pb2 import SubscribeUpdatePong as SubscribeUpdatePong
from yellowstone_api.geyser_pb2 import PingRequest as PingRequest
from yellowstone_api.geyser_pb2 import PongResponse as PongResponse
from yellowstone_api.geyser_pb2 import GetLatestBlockhashRequest as GetLatestBlockhashRequest
from yellowstone_api.geyser_pb2 import GetLatestBlockhashResponse as GetLatestBlockhashResponse
from yellowstone_api.geyser_pb2 import GetBlockHeightRequest as GetBlockHeightRequest
from yellowstone_api.geyser_pb2 import GetBlockHeightResponse as GetBlockHeightResponse
from yellowstone_api.geyser_pb2 import GetSlotRequest as GetSlotRequest
from yellowstone_api.geyser_pb2 import GetSlotResponse as GetSlotResponse
from yellowstone_api.geyser_pb2 import GetVersionRequest as GetVersionRequest
from yellowstone_api.geyser_pb2 import GetVersionResponse as GetVersionResponse
from yellowstone_api.geyser_pb2 import IsBlockhashValidRequest as IsBlockhashValidRequest
from yellowstone_api.geyser_pb2 import IsBlockhashValidResponse as IsBlockhashValidResponse
from yellowstone_api.geyser_pb2 import CommitmentLevel as CommitmentLevel
from yellowstone_api.geyser_pb2 import SlotStatus as SlotStatus

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

class ConsumerGroupType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATIC: _ClassVar[ConsumerGroupType]

class InitialOffsetPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EARLIEST: _ClassVar[InitialOffsetPolicy]
    LATEST: _ClassVar[InitialOffsetPolicy]
    SLOT: _ClassVar[InitialOffsetPolicy]

class EventSubscriptionPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCOUNT_UPDATE_ONLY: _ClassVar[EventSubscriptionPolicy]
    TRANSACTION_ONLY: _ClassVar[EventSubscriptionPolicy]
    BOTH: _ClassVar[EventSubscriptionPolicy]
STATIC: ConsumerGroupType
EARLIEST: InitialOffsetPolicy
LATEST: InitialOffsetPolicy
SLOT: InitialOffsetPolicy
ACCOUNT_UPDATE_ONLY: EventSubscriptionPolicy
TRANSACTION_ONLY: EventSubscriptionPolicy
BOTH: EventSubscriptionPolicy

class ListAvailableCommitmentLevelsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListAvailableCommitmentLevelsResponse(_message.Message):
    __slots__ = ("commitment_levels",)
    COMMITMENT_LEVELS_FIELD_NUMBER: _ClassVar[int]
    commitment_levels: _containers.RepeatedScalarFieldContainer[_geyser_pb2.CommitmentLevel]
    def __init__(self, commitment_levels: _Optional[_Iterable[_Union[_geyser_pb2.CommitmentLevel, str]]] = ...) -> None: ...

class GetConsumerGroupInfoRequest(_message.Message):
    __slots__ = ("consumer_group_label",)
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    consumer_group_label: str
    def __init__(self, consumer_group_label: _Optional[str] = ...) -> None: ...

class DeleteConsumerGroupRequest(_message.Message):
    __slots__ = ("consumer_group_label",)
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    consumer_group_label: str
    def __init__(self, consumer_group_label: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("id", "consumer_group_label", "consumer_group_type", "member_count", "commitment_level", "event_subscription_policy", "is_stale")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_GROUP_TYPE_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    COMMITMENT_LEVEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_SUBSCRIPTION_POLICY_FIELD_NUMBER: _ClassVar[int]
    IS_STALE_FIELD_NUMBER: _ClassVar[int]
    id: str
    consumer_group_label: str
    consumer_group_type: ConsumerGroupType
    member_count: int
    commitment_level: _geyser_pb2.CommitmentLevel
    event_subscription_policy: EventSubscriptionPolicy
    is_stale: bool
    def __init__(self, id: _Optional[str] = ..., consumer_group_label: _Optional[str] = ..., consumer_group_type: _Optional[_Union[ConsumerGroupType, str]] = ..., member_count: _Optional[int] = ..., commitment_level: _Optional[_Union[_geyser_pb2.CommitmentLevel, str]] = ..., event_subscription_policy: _Optional[_Union[EventSubscriptionPolicy, str]] = ..., is_stale: bool = ...) -> None: ...

class GetSlotLagInfoRequest(_message.Message):
    __slots__ = ("consumer_group_label",)
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    consumer_group_label: str
    def __init__(self, consumer_group_label: _Optional[str] = ...) -> None: ...

class GetSlotLagInfoResponse(_message.Message):
    __slots__ = ("max_slot_seen", "global_max_slot")
    MAX_SLOT_SEEN_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_MAX_SLOT_FIELD_NUMBER: _ClassVar[int]
    max_slot_seen: int
    global_max_slot: int
    def __init__(self, max_slot_seen: _Optional[int] = ..., global_max_slot: _Optional[int] = ...) -> None: ...

class SubscribeRequest(_message.Message):
    __slots__ = ("consumer_group_label", "consumer_id", "accounts", "transactions")
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
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    CONSUMER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNTS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTIONS_FIELD_NUMBER: _ClassVar[int]
    consumer_group_label: str
    consumer_id: int
    accounts: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterAccounts]
    transactions: _containers.MessageMap[str, _geyser_pb2.SubscribeRequestFilterTransactions]
    def __init__(self, consumer_group_label: _Optional[str] = ..., consumer_id: _Optional[int] = ..., accounts: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterAccounts]] = ..., transactions: _Optional[_Mapping[str, _geyser_pb2.SubscribeRequestFilterTransactions]] = ...) -> None: ...

class CreateStaticConsumerGroupResponse(_message.Message):
    __slots__ = ("group_id",)
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: str
    def __init__(self, group_id: _Optional[str] = ...) -> None: ...

class CreateStaticConsumerGroupRequest(_message.Message):
    __slots__ = ("consumer_group_label", "member_count", "initial_offset_policy", "commitment_level", "event_subscription_policy", "at_slot")
    CONSUMER_GROUP_LABEL_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    INITIAL_OFFSET_POLICY_FIELD_NUMBER: _ClassVar[int]
    COMMITMENT_LEVEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_SUBSCRIPTION_POLICY_FIELD_NUMBER: _ClassVar[int]
    AT_SLOT_FIELD_NUMBER: _ClassVar[int]
    consumer_group_label: str
    member_count: int
    initial_offset_policy: InitialOffsetPolicy
    commitment_level: _geyser_pb2.CommitmentLevel
    event_subscription_policy: EventSubscriptionPolicy
    at_slot: int
    def __init__(self, consumer_group_label: _Optional[str] = ..., member_count: _Optional[int] = ..., initial_offset_policy: _Optional[_Union[InitialOffsetPolicy, str]] = ..., commitment_level: _Optional[_Union[_geyser_pb2.CommitmentLevel, str]] = ..., event_subscription_policy: _Optional[_Union[EventSubscriptionPolicy, str]] = ..., at_slot: _Optional[int] = ...) -> None: ...
