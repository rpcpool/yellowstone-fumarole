# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: fumarole.proto
# Protobuf Python Version: 5.28.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    28,
    1,
    '',
    'fumarole.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import yellowstone_api.geyser_pb2 as geyser__pb2
try:
  solana__storage__pb2 = geyser__pb2.solana__storage__pb2
except AttributeError:
  solana__storage__pb2 = geyser__pb2.solana_storage_pb2

from yellowstone_api.geyser_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0e\x66umarole.proto\x12\x08\x66umarole\x1a\x0cgeyser.proto\"&\n$ListAvailableCommitmentLevelsRequest\"[\n%ListAvailableCommitmentLevelsResponse\x12\x32\n\x11\x63ommitment_levels\x18\x01 \x03(\x0e\x32\x17.geyser.CommitmentLevel\";\n\x1bGetConsumerGroupInfoRequest\x12\x1c\n\x14\x63onsumer_group_label\x18\x01 \x01(\t\":\n\x1a\x44\x65leteConsumerGroupRequest\x12\x1c\n\x14\x63onsumer_group_label\x18\x01 \x01(\t\".\n\x1b\x44\x65leteConsumerGroupResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"\x1b\n\x19ListConsumerGroupsRequest\"R\n\x1aListConsumerGroupsResponse\x12\x34\n\x0f\x63onsumer_groups\x18\x01 \x03(\x0b\x32\x1b.fumarole.ConsumerGroupInfo\"\x98\x02\n\x11\x43onsumerGroupInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x1c\n\x14\x63onsumer_group_label\x18\x02 \x01(\t\x12\x38\n\x13\x63onsumer_group_type\x18\x03 \x01(\x0e\x32\x1b.fumarole.ConsumerGroupType\x12\x14\n\x0cmember_count\x18\x04 \x01(\r\x12\x31\n\x10\x63ommitment_level\x18\x05 \x01(\x0e\x32\x17.geyser.CommitmentLevel\x12\x44\n\x19\x65vent_subscription_policy\x18\x06 \x01(\x0e\x32!.fumarole.EventSubscriptionPolicy\x12\x10\n\x08is_stale\x18\x07 \x01(\x08\"5\n\x15GetSlotLagInfoRequest\x12\x1c\n\x14\x63onsumer_group_label\x18\x01 \x01(\t\"H\n\x16GetSlotLagInfoResponse\x12\x15\n\rmax_slot_seen\x18\x01 \x01(\x04\x12\x17\n\x0fglobal_max_slot\x18\x02 \x01(\x04\"\x94\x03\n\x10SubscribeRequest\x12\x1c\n\x14\x63onsumer_group_label\x18\x01 \x01(\t\x12\x18\n\x0b\x63onsumer_id\x18\x02 \x01(\rH\x00\x88\x01\x01\x12:\n\x08\x61\x63\x63ounts\x18\x03 \x03(\x0b\x32(.fumarole.SubscribeRequest.AccountsEntry\x12\x42\n\x0ctransactions\x18\x04 \x03(\x0b\x32,.fumarole.SubscribeRequest.TransactionsEntry\x1aW\n\rAccountsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x35\n\x05value\x18\x02 \x01(\x0b\x32&.geyser.SubscribeRequestFilterAccounts:\x02\x38\x01\x1a_\n\x11TransactionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x39\n\x05value\x18\x02 \x01(\x0b\x32*.geyser.SubscribeRequestFilterTransactions:\x02\x38\x01\x42\x0e\n\x0c_consumer_id\"5\n!CreateStaticConsumerGroupResponse\x12\x10\n\x08group_id\x18\x01 \x01(\t\"\xc5\x02\n CreateStaticConsumerGroupRequest\x12\x1c\n\x14\x63onsumer_group_label\x18\x01 \x01(\t\x12\x19\n\x0cmember_count\x18\x02 \x01(\rH\x00\x88\x01\x01\x12<\n\x15initial_offset_policy\x18\x03 \x01(\x0e\x32\x1d.fumarole.InitialOffsetPolicy\x12\x31\n\x10\x63ommitment_level\x18\x04 \x01(\x0e\x32\x17.geyser.CommitmentLevel\x12\x44\n\x19\x65vent_subscription_policy\x18\x05 \x01(\x0e\x32!.fumarole.EventSubscriptionPolicy\x12\x14\n\x07\x61t_slot\x18\x06 \x01(\x03H\x01\x88\x01\x01\x42\x0f\n\r_member_countB\n\n\x08_at_slot*\x1f\n\x11\x43onsumerGroupType\x12\n\n\x06STATIC\x10\x00*9\n\x13InitialOffsetPolicy\x12\x0c\n\x08\x45\x41RLIEST\x10\x00\x12\n\n\x06LATEST\x10\x01\x12\x08\n\x04SLOT\x10\x02*R\n\x17\x45ventSubscriptionPolicy\x12\x17\n\x13\x41\x43\x43OUNT_UPDATE_ONLY\x10\x00\x12\x14\n\x10TRANSACTION_ONLY\x10\x01\x12\x08\n\x04\x42OTH\x10\x02\x32\xcd\x05\n\x08\x46umarole\x12\x82\x01\n\x1dListAvailableCommitmentLevels\x12..fumarole.ListAvailableCommitmentLevelsRequest\x1a/.fumarole.ListAvailableCommitmentLevelsResponse\"\x00\x12\\\n\x14GetConsumerGroupInfo\x12%.fumarole.GetConsumerGroupInfoRequest\x1a\x1b.fumarole.ConsumerGroupInfo\"\x00\x12\x61\n\x12ListConsumerGroups\x12#.fumarole.ListConsumerGroupsRequest\x1a$.fumarole.ListConsumerGroupsResponse\"\x00\x12\x64\n\x13\x44\x65leteConsumerGroup\x12$.fumarole.DeleteConsumerGroupRequest\x1a%.fumarole.DeleteConsumerGroupResponse\"\x00\x12v\n\x19\x43reateStaticConsumerGroup\x12*.fumarole.CreateStaticConsumerGroupRequest\x1a+.fumarole.CreateStaticConsumerGroupResponse\"\x00\x12\x46\n\tSubscribe\x12\x1a.fumarole.SubscribeRequest\x1a\x17.geyser.SubscribeUpdate\"\x00(\x01\x30\x01\x12U\n\x0eGetSlotLagInfo\x12\x1f.fumarole.GetSlotLagInfoRequest\x1a .fumarole.GetSlotLagInfoResponse\"\x00P\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'fumarole_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_options = b'8\001'
  _globals['_CONSUMERGROUPTYPE']._serialized_start=1659
  _globals['_CONSUMERGROUPTYPE']._serialized_end=1690
  _globals['_INITIALOFFSETPOLICY']._serialized_start=1692
  _globals['_INITIALOFFSETPOLICY']._serialized_end=1749
  _globals['_EVENTSUBSCRIPTIONPOLICY']._serialized_start=1751
  _globals['_EVENTSUBSCRIPTIONPOLICY']._serialized_end=1833
  _globals['_LISTAVAILABLECOMMITMENTLEVELSREQUEST']._serialized_start=42
  _globals['_LISTAVAILABLECOMMITMENTLEVELSREQUEST']._serialized_end=80
  _globals['_LISTAVAILABLECOMMITMENTLEVELSRESPONSE']._serialized_start=82
  _globals['_LISTAVAILABLECOMMITMENTLEVELSRESPONSE']._serialized_end=173
  _globals['_GETCONSUMERGROUPINFOREQUEST']._serialized_start=175
  _globals['_GETCONSUMERGROUPINFOREQUEST']._serialized_end=234
  _globals['_DELETECONSUMERGROUPREQUEST']._serialized_start=236
  _globals['_DELETECONSUMERGROUPREQUEST']._serialized_end=294
  _globals['_DELETECONSUMERGROUPRESPONSE']._serialized_start=296
  _globals['_DELETECONSUMERGROUPRESPONSE']._serialized_end=342
  _globals['_LISTCONSUMERGROUPSREQUEST']._serialized_start=344
  _globals['_LISTCONSUMERGROUPSREQUEST']._serialized_end=371
  _globals['_LISTCONSUMERGROUPSRESPONSE']._serialized_start=373
  _globals['_LISTCONSUMERGROUPSRESPONSE']._serialized_end=455
  _globals['_CONSUMERGROUPINFO']._serialized_start=458
  _globals['_CONSUMERGROUPINFO']._serialized_end=738
  _globals['_GETSLOTLAGINFOREQUEST']._serialized_start=740
  _globals['_GETSLOTLAGINFOREQUEST']._serialized_end=793
  _globals['_GETSLOTLAGINFORESPONSE']._serialized_start=795
  _globals['_GETSLOTLAGINFORESPONSE']._serialized_end=867
  _globals['_SUBSCRIBEREQUEST']._serialized_start=870
  _globals['_SUBSCRIBEREQUEST']._serialized_end=1274
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_start=1074
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_end=1161
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_start=1163
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_end=1258
  _globals['_CREATESTATICCONSUMERGROUPRESPONSE']._serialized_start=1276
  _globals['_CREATESTATICCONSUMERGROUPRESPONSE']._serialized_end=1329
  _globals['_CREATESTATICCONSUMERGROUPREQUEST']._serialized_start=1332
  _globals['_CREATESTATICCONSUMERGROUPREQUEST']._serialized_end=1657
  _globals['_FUMAROLE']._serialized_start=1836
  _globals['_FUMAROLE']._serialized_end=2553
# @@protoc_insertion_point(module_scope)
