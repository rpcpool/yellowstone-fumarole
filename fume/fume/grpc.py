from click import Tuple
import grpc
import sys
from typing import Callable, Literal, Optional, Union
from collections.abc import Generator
from yellowstone_api.fumarole_pb2_grpc import FumaroleStub
import yellowstone_api.fumarole_pb2 as fumarole_pb2
import yellowstone_api.geyser_pb2 as geyser_pb2
import base58


def _triton_sign_request(
    callback: grpc.AuthMetadataPluginCallback,
    x_token: Optional[str],
    error: Optional[Exception],
):
    # WARNING: metadata is a 1d-tuple (<value>,), the last comma is necessary
    metadata = (("x-token", x_token),)
    return callback(metadata, error)


class TritonAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, x_token: str):
        self.x_token = x_token

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ):
        return _triton_sign_request(callback, self.x_token, None)


def grpc_channel(endpoint: str, x_token=None, compression=None, *grpc_options):
    options = [("grpc.max_receive_message_length", 111111110), *grpc_options]
    if x_token is not None:
        auth = TritonAuthMetadataPlugin(x_token)
        # ssl_creds allow you to use our https endpoint
        # grpc.ssl_channel_credentials with no arguments will look through your CA trust store.
        ssl_creds = grpc.ssl_channel_credentials()

        # call credentials will be sent on each request if setup with composite_channel_credentials.
        call_creds: grpc.CallCredentials = grpc.metadata_call_credentials(auth)

        # Combined creds will store the channel creds aswell as the call credentials
        combined_creds = grpc.composite_channel_credentials(ssl_creds, call_creds)

        return grpc.secure_channel(
            endpoint,
            credentials=combined_creds,
            compression=compression,
            options=options,
        )
    else:
        return grpc.insecure_channel(endpoint, compression=compression, options=options)


InitialSeek = Union[Literal["earliest"], Literal["latest"], Literal["slot"]]

IncludeOption = Union[Literal["all"], Literal["account"], Literal["tx"]]

CommitmentOption = Union[
    Literal["processed"], Literal["confirmed"], Literal["finalized"]
]


def account_update_to_dict(account_update: geyser_pb2.SubscribeUpdateAccount) -> dict:
    pass


def tx_to_dict(tx_update: geyser_pb2.SubscribeUpdateTransaction) -> dict:
    pass


def subscribe_update_to_dict(obj, acc=None):
    if acc is None:
        acc = {}

    if hasattr(obj, "DESCRIPTOR"):
        for field in obj.DESCRIPTOR.fields:
            name = field.name
            value = getattr(obj, name)

            if isinstance(value, (int, float, bool, str, bytes, type(None))):
                if isinstance(value, bytes):
                    value = base58.b58encode(value).decode("utf-8")
                    acc[name] = value
                else:
                    acc[name] = value
            elif isinstance(value, list):
                acc[name] = []
                ref = acc[name]

            else:

                if hasattr(value, "DESCRIPTOR"):
                    acc[name] = {}
                    ref = acc[name]
                    subscribe_update_to_dict(value, ref)
                else:
                    try:
                        it = iter(value)
                        ref = []
                        acc[name] = ref
                        for v in value:
                            if isinstance(
                                v, (int, float, bool, str, bytes, type(None))
                            ):
                                if isinstance(v, bytes):
                                    v = base58.b58encode(v).decode("utf-8")
                                    ref.append(v)
                                else:
                                    ref.append(v)
                            else:
                                new_obj = {}
                                ref.append(new_obj)
                                subscribe_update_to_dict(v, new_obj)
                    except TypeError:
                        pass
    return acc


def summarize_account_update(account_update: geyser_pb2.SubscribeUpdateAccount) -> str:
    slot = account_update.slot
    pubkey = base58.b58encode(account_update.account.pubkey).decode("utf-8")
    owner = base58.b58encode(account_update.account.owner).decode("utf-8")
    account_info: geyser_pb2.SubscribeUpdateAccountInfo = account_update.account
    if account_info.txn_signature:
        txn_signature = base58.b58encode(account_info.txn_signature).decode("utf-8")
    else:
        txn_signature = None
    size = account_info.ByteSize()
    return (
        f"account,{slot},owner={owner},pubkey={pubkey},tx={txn_signature},size={size}"
    )


def summarize_tx_update(tx: geyser_pb2.SubscribeUpdateTransaction) -> str:
    slot = tx.slot
    tx_info = tx.transaction
    tx_id = base58.b58encode(tx_info.signature).decode("utf-8")
    return f"tx,{slot},{tx_id}"


def subscribe_update_to_summarize(subscribe_update: geyser_pb2.SubscribeUpdate) -> str:
    if subscribe_update.HasField("account"):
        account_update = getattr(subscribe_update, "account")
        return summarize_account_update(account_update)
    elif subscribe_update.HasField("transaction"):
        tx_update = getattr(subscribe_update, "transaction")
        return summarize_tx_update(tx_update)
    else:
        return None


class SubscribeFilterBuilder:
    def __init__(self):
        self.accounts = None
        self.owners = None
        self.tx_includes = None
        self.tx_excludes = None
        self.tx_requires = None
        self.tx_fail = None
        self.tx_vote = None

    def __getattr__(self, name):
        if not name.startswith("with_"):
            raise AttributeError(f"Attribute {name} not found")

        attr = name.split("with_")[1]

        def setter(value):
            setattr(self, attr, value)
            return self

        return setter

    def include_vote_tx(self):
        self.tx_vote = None
        return self

    def include_fail_tx(self):
        self.tx_fail = None
        return self

    def no_fail_tx(self):
        self.tx_fail = False
        return self

    def no_vote_tx(self, tx):
        self.tx_vote = False
        return self

    def build(self):
        return {
            "accounts": {
                "default": geyser_pb2.SubscribeRequestFilterAccounts(
                    account=self.accounts, owner=self.owners
                )
            },
            "transactions": {
                "default": geyser_pb2.SubscribeRequestFilterTransactions(
                    vote=self.tx_vote,
                    failed=self.tx_fail,
                    account_required=self.tx_requires,
                    account_include=self.tx_includes,
                    account_exclude=self.tx_excludes,
                )
            },
        }


class FumaroleClient:

    def __init__(self, channel, metadata: Optional[list[tuple[str, str]]] = None):
        self.stub = FumaroleStub(channel)
        self.metadata = metadata

    def list_available_commitments(self) -> list[str]:
        resp = self.stub.ListAvailableCommitmentLevels(
            fumarole_pb2.ListAvailableCommitmentLevelsRequest(), metadata=self.metadata
        )

    def delete_consumer_group(self, name: str) -> bool:
        resp = self.stub.DeleteConsumerGroup(
            fumarole_pb2.DeleteConsumerGroupRequest(consumer_group_label=name),
            metadata=self.metadata,
        )
        return resp.success

    def get_cg_info(self, name: str) -> Optional[fumarole_pb2.ConsumerGroupInfo]:
        try:
            resp = self.stub.GetConsumerGroupInfo(
                fumarole_pb2.GetConsumerGroupInfoRequest(consumer_group_label=name),
                metadata=self.metadata,
            )
            return resp
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            else:
                raise e

    def list_consumer_groups(
        self,
    ) -> list[fumarole_pb2.ConsumerGroupInfo]:
        resp = self.stub.ListConsumerGroups(
            fumarole_pb2.ListConsumerGroupsRequest(), metadata=self.metadata
        )

        return [cg for cg in resp.consumer_groups]

    def create_consumer_group(
        self,
        name: str,
        size: int = 1,
        include: IncludeOption = "all",
        initial_seek: InitialSeek = "latest",
        starting_slot: Optional[int] = None,
        commitment: CommitmentOption = "confirmed",
    ) -> str:
        if initial_seek == "earliest":
            initial_offset_policy = fumarole_pb2.InitialOffsetPolicy.EARLIEST
            starting_slot = None

        if initial_seek == "latest":
            initial_offset_policy = fumarole_pb2.InitialOffsetPolicy.LATEST
            starting_slot = None

        if initial_seek == "slot":
            initial_offset_policy = fumarole_pb2.InitialOffsetPolicy.SLOT

        if include == "account":
            event_subscription_policy = (
                fumarole_pb2.EventSubscriptionPolicy.ACCOUNT_UPDATE_ONLY
            )
        elif include == "tx":
            event_subscription_policy = (
                fumarole_pb2.EventSubscriptionPolicy.TRANSACTION_ONLY
            )
        else:
            event_subscription_policy = fumarole_pb2.EventSubscriptionPolicy.BOTH

        if commitment == "processed":
            commitment_level = geyser_pb2.CommitmentLevel.PROCESSED
        elif commitment == "confirmed":
            commitment_level = geyser_pb2.CommitmentLevel.CONFIRMED
        else:
            commitment_level = geyser_pb2.CommitmentLevel.FINALIZED

        resp = self.stub.CreateStaticConsumerGroup(
            fumarole_pb2.CreateStaticConsumerGroupRequest(
                consumer_group_label=name,
                member_count=size,
                initial_offset_policy=initial_offset_policy,
                at_slot=starting_slot,
                commitment_level=commitment_level,
                event_subscription_policy=event_subscription_policy,
            ),
            metadata=self.metadata,
        )

        return name

    def subscribe(
        self,
        consumer_group_label: str,
        member_idx: int = 0,
        mapper: Union[
            Literal["json"],
            Literal["summ"],
            Callable[[geyser_pb2.SubscribeUpdate], any],
        ] = "summ",
        **subscribe_request_kwargs,
    ) -> Generator[dict]:
        sub_req = fumarole_pb2.SubscribeRequest(
            consumer_group_label=consumer_group_label,
            consumer_id=member_idx,
            **subscribe_request_kwargs,
        )
        stream = self.stub.Subscribe(iter([sub_req]), metadata=self.metadata)
        if mapper == "json":
            map_fn = subscribe_update_to_dict
        elif mapper == "summ":
            map_fn = subscribe_update_to_summarize
        else:
            map_fn = mapper
        try:
            for subscribe_update in stream:
                result = map_fn(subscribe_update)
                if result is not None:
                    yield result
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                return
            else:
                raise e
