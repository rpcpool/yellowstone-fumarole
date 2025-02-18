# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: geyser.proto
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
    'geyser.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
import yellowstone_api.solana_storage_pb2 as solana__storage__pb2

from yellowstone_api.solana_storage_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cgeyser.proto\x12\x06geyser\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x14solana-storage.proto\"\x9c\n\n\x10SubscribeRequest\x12\x38\n\x08\x61\x63\x63ounts\x18\x01 \x03(\x0b\x32&.geyser.SubscribeRequest.AccountsEntry\x12\x32\n\x05slots\x18\x02 \x03(\x0b\x32#.geyser.SubscribeRequest.SlotsEntry\x12@\n\x0ctransactions\x18\x03 \x03(\x0b\x32*.geyser.SubscribeRequest.TransactionsEntry\x12M\n\x13transactions_status\x18\n \x03(\x0b\x32\x30.geyser.SubscribeRequest.TransactionsStatusEntry\x12\x34\n\x06\x62locks\x18\x04 \x03(\x0b\x32$.geyser.SubscribeRequest.BlocksEntry\x12=\n\x0b\x62locks_meta\x18\x05 \x03(\x0b\x32(.geyser.SubscribeRequest.BlocksMetaEntry\x12\x32\n\x05\x65ntry\x18\x08 \x03(\x0b\x32#.geyser.SubscribeRequest.EntryEntry\x12\x30\n\ncommitment\x18\x06 \x01(\x0e\x32\x17.geyser.CommitmentLevelH\x00\x88\x01\x01\x12\x46\n\x13\x61\x63\x63ounts_data_slice\x18\x07 \x03(\x0b\x32).geyser.SubscribeRequestAccountsDataSlice\x12/\n\x04ping\x18\t \x01(\x0b\x32\x1c.geyser.SubscribeRequestPingH\x01\x88\x01\x01\x12\x16\n\tfrom_slot\x18\x0b \x01(\x04H\x02\x88\x01\x01\x1aW\n\rAccountsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x35\n\x05value\x18\x02 \x01(\x0b\x32&.geyser.SubscribeRequestFilterAccounts:\x02\x38\x01\x1aQ\n\nSlotsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x32\n\x05value\x18\x02 \x01(\x0b\x32#.geyser.SubscribeRequestFilterSlots:\x02\x38\x01\x1a_\n\x11TransactionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x39\n\x05value\x18\x02 \x01(\x0b\x32*.geyser.SubscribeRequestFilterTransactions:\x02\x38\x01\x1a\x65\n\x17TransactionsStatusEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x39\n\x05value\x18\x02 \x01(\x0b\x32*.geyser.SubscribeRequestFilterTransactions:\x02\x38\x01\x1aS\n\x0b\x42locksEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x33\n\x05value\x18\x02 \x01(\x0b\x32$.geyser.SubscribeRequestFilterBlocks:\x02\x38\x01\x1a[\n\x0f\x42locksMetaEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x37\n\x05value\x18\x02 \x01(\x0b\x32(.geyser.SubscribeRequestFilterBlocksMeta:\x02\x38\x01\x1aQ\n\nEntryEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x32\n\x05value\x18\x02 \x01(\x0b\x32#.geyser.SubscribeRequestFilterEntry:\x02\x38\x01\x42\r\n\x0b_commitmentB\x07\n\x05_pingB\x0c\n\n_from_slot\"\xbf\x01\n\x1eSubscribeRequestFilterAccounts\x12\x0f\n\x07\x61\x63\x63ount\x18\x02 \x03(\t\x12\r\n\x05owner\x18\x03 \x03(\t\x12=\n\x07\x66ilters\x18\x04 \x03(\x0b\x32,.geyser.SubscribeRequestFilterAccountsFilter\x12#\n\x16nonempty_txn_signature\x18\x05 \x01(\x08H\x00\x88\x01\x01\x42\x19\n\x17_nonempty_txn_signature\"\xf3\x01\n$SubscribeRequestFilterAccountsFilter\x12\x44\n\x06memcmp\x18\x01 \x01(\x0b\x32\x32.geyser.SubscribeRequestFilterAccountsFilterMemcmpH\x00\x12\x12\n\x08\x64\x61tasize\x18\x02 \x01(\x04H\x00\x12\x1d\n\x13token_account_state\x18\x03 \x01(\x08H\x00\x12H\n\x08lamports\x18\x04 \x01(\x0b\x32\x34.geyser.SubscribeRequestFilterAccountsFilterLamportsH\x00\x42\x08\n\x06\x66ilter\"y\n*SubscribeRequestFilterAccountsFilterMemcmp\x12\x0e\n\x06offset\x18\x01 \x01(\x04\x12\x0f\n\x05\x62ytes\x18\x02 \x01(\x0cH\x00\x12\x10\n\x06\x62\x61se58\x18\x03 \x01(\tH\x00\x12\x10\n\x06\x62\x61se64\x18\x04 \x01(\tH\x00\x42\x06\n\x04\x64\x61ta\"m\n,SubscribeRequestFilterAccountsFilterLamports\x12\x0c\n\x02\x65q\x18\x01 \x01(\x04H\x00\x12\x0c\n\x02ne\x18\x02 \x01(\x04H\x00\x12\x0c\n\x02lt\x18\x03 \x01(\x04H\x00\x12\x0c\n\x02gt\x18\x04 \x01(\x04H\x00\x42\x05\n\x03\x63mp\"\x8f\x01\n\x1bSubscribeRequestFilterSlots\x12!\n\x14\x66ilter_by_commitment\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\x1e\n\x11interslot_updates\x18\x02 \x01(\x08H\x01\x88\x01\x01\x42\x17\n\x15_filter_by_commitmentB\x14\n\x12_interslot_updates\"\xd2\x01\n\"SubscribeRequestFilterTransactions\x12\x11\n\x04vote\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\x13\n\x06\x66\x61iled\x18\x02 \x01(\x08H\x01\x88\x01\x01\x12\x16\n\tsignature\x18\x05 \x01(\tH\x02\x88\x01\x01\x12\x17\n\x0f\x61\x63\x63ount_include\x18\x03 \x03(\t\x12\x17\n\x0f\x61\x63\x63ount_exclude\x18\x04 \x03(\t\x12\x18\n\x10\x61\x63\x63ount_required\x18\x06 \x03(\tB\x07\n\x05_voteB\t\n\x07_failedB\x0c\n\n_signature\"\xd9\x01\n\x1cSubscribeRequestFilterBlocks\x12\x17\n\x0f\x61\x63\x63ount_include\x18\x01 \x03(\t\x12!\n\x14include_transactions\x18\x02 \x01(\x08H\x00\x88\x01\x01\x12\x1d\n\x10include_accounts\x18\x03 \x01(\x08H\x01\x88\x01\x01\x12\x1c\n\x0finclude_entries\x18\x04 \x01(\x08H\x02\x88\x01\x01\x42\x17\n\x15_include_transactionsB\x13\n\x11_include_accountsB\x12\n\x10_include_entries\"\"\n SubscribeRequestFilterBlocksMeta\"\x1d\n\x1bSubscribeRequestFilterEntry\"C\n!SubscribeRequestAccountsDataSlice\x12\x0e\n\x06offset\x18\x01 \x01(\x04\x12\x0e\n\x06length\x18\x02 \x01(\x04\"\"\n\x14SubscribeRequestPing\x12\n\n\x02id\x18\x01 \x01(\x05\"\xb5\x04\n\x0fSubscribeUpdate\x12\x0f\n\x07\x66ilters\x18\x01 \x03(\t\x12\x31\n\x07\x61\x63\x63ount\x18\x02 \x01(\x0b\x32\x1e.geyser.SubscribeUpdateAccountH\x00\x12+\n\x04slot\x18\x03 \x01(\x0b\x32\x1b.geyser.SubscribeUpdateSlotH\x00\x12\x39\n\x0btransaction\x18\x04 \x01(\x0b\x32\".geyser.SubscribeUpdateTransactionH\x00\x12\x46\n\x12transaction_status\x18\n \x01(\x0b\x32(.geyser.SubscribeUpdateTransactionStatusH\x00\x12-\n\x05\x62lock\x18\x05 \x01(\x0b\x32\x1c.geyser.SubscribeUpdateBlockH\x00\x12+\n\x04ping\x18\x06 \x01(\x0b\x32\x1b.geyser.SubscribeUpdatePingH\x00\x12+\n\x04pong\x18\t \x01(\x0b\x32\x1b.geyser.SubscribeUpdatePongH\x00\x12\x36\n\nblock_meta\x18\x07 \x01(\x0b\x32 .geyser.SubscribeUpdateBlockMetaH\x00\x12-\n\x05\x65ntry\x18\x08 \x01(\x0b\x32\x1c.geyser.SubscribeUpdateEntryH\x00\x12.\n\ncreated_at\x18\x0b \x01(\x0b\x32\x1a.google.protobuf.TimestampB\x0e\n\x0cupdate_oneof\"o\n\x16SubscribeUpdateAccount\x12\x33\n\x07\x61\x63\x63ount\x18\x01 \x01(\x0b\x32\".geyser.SubscribeUpdateAccountInfo\x12\x0c\n\x04slot\x18\x02 \x01(\x04\x12\x12\n\nis_startup\x18\x03 \x01(\x08\"\xc8\x01\n\x1aSubscribeUpdateAccountInfo\x12\x0e\n\x06pubkey\x18\x01 \x01(\x0c\x12\x10\n\x08lamports\x18\x02 \x01(\x04\x12\r\n\x05owner\x18\x03 \x01(\x0c\x12\x12\n\nexecutable\x18\x04 \x01(\x08\x12\x12\n\nrent_epoch\x18\x05 \x01(\x04\x12\x0c\n\x04\x64\x61ta\x18\x06 \x01(\x0c\x12\x15\n\rwrite_version\x18\x07 \x01(\x04\x12\x1a\n\rtxn_signature\x18\x08 \x01(\x0cH\x00\x88\x01\x01\x42\x10\n\x0e_txn_signature\"\x8f\x01\n\x13SubscribeUpdateSlot\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\x13\n\x06parent\x18\x02 \x01(\x04H\x00\x88\x01\x01\x12\"\n\x06status\x18\x03 \x01(\x0e\x32\x12.geyser.SlotStatus\x12\x17\n\ndead_error\x18\x04 \x01(\tH\x01\x88\x01\x01\x42\t\n\x07_parentB\r\n\x0b_dead_error\"g\n\x1aSubscribeUpdateTransaction\x12;\n\x0btransaction\x18\x01 \x01(\x0b\x32&.geyser.SubscribeUpdateTransactionInfo\x12\x0c\n\x04slot\x18\x02 \x01(\x04\"\xd8\x01\n\x1eSubscribeUpdateTransactionInfo\x12\x11\n\tsignature\x18\x01 \x01(\x0c\x12\x0f\n\x07is_vote\x18\x02 \x01(\x08\x12?\n\x0btransaction\x18\x03 \x01(\x0b\x32*.solana.storage.ConfirmedBlock.Transaction\x12\x42\n\x04meta\x18\x04 \x01(\x0b\x32\x34.solana.storage.ConfirmedBlock.TransactionStatusMeta\x12\r\n\x05index\x18\x05 \x01(\x04\"\xa1\x01\n SubscribeUpdateTransactionStatus\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\x11\n\tsignature\x18\x02 \x01(\x0c\x12\x0f\n\x07is_vote\x18\x03 \x01(\x08\x12\r\n\x05index\x18\x04 \x01(\x04\x12<\n\x03\x65rr\x18\x05 \x01(\x0b\x32/.solana.storage.ConfirmedBlock.TransactionError\"\xa0\x04\n\x14SubscribeUpdateBlock\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\x11\n\tblockhash\x18\x02 \x01(\t\x12\x37\n\x07rewards\x18\x03 \x01(\x0b\x32&.solana.storage.ConfirmedBlock.Rewards\x12@\n\nblock_time\x18\x04 \x01(\x0b\x32,.solana.storage.ConfirmedBlock.UnixTimestamp\x12@\n\x0c\x62lock_height\x18\x05 \x01(\x0b\x32*.solana.storage.ConfirmedBlock.BlockHeight\x12\x13\n\x0bparent_slot\x18\x07 \x01(\x04\x12\x18\n\x10parent_blockhash\x18\x08 \x01(\t\x12\"\n\x1a\x65xecuted_transaction_count\x18\t \x01(\x04\x12<\n\x0ctransactions\x18\x06 \x03(\x0b\x32&.geyser.SubscribeUpdateTransactionInfo\x12\x1d\n\x15updated_account_count\x18\n \x01(\x04\x12\x34\n\x08\x61\x63\x63ounts\x18\x0b \x03(\x0b\x32\".geyser.SubscribeUpdateAccountInfo\x12\x15\n\rentries_count\x18\x0c \x01(\x04\x12-\n\x07\x65ntries\x18\r \x03(\x0b\x32\x1c.geyser.SubscribeUpdateEntry\"\xe2\x02\n\x18SubscribeUpdateBlockMeta\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\x11\n\tblockhash\x18\x02 \x01(\t\x12\x37\n\x07rewards\x18\x03 \x01(\x0b\x32&.solana.storage.ConfirmedBlock.Rewards\x12@\n\nblock_time\x18\x04 \x01(\x0b\x32,.solana.storage.ConfirmedBlock.UnixTimestamp\x12@\n\x0c\x62lock_height\x18\x05 \x01(\x0b\x32*.solana.storage.ConfirmedBlock.BlockHeight\x12\x13\n\x0bparent_slot\x18\x06 \x01(\x04\x12\x18\n\x10parent_blockhash\x18\x07 \x01(\t\x12\"\n\x1a\x65xecuted_transaction_count\x18\x08 \x01(\x04\x12\x15\n\rentries_count\x18\t \x01(\x04\"\x9d\x01\n\x14SubscribeUpdateEntry\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\r\n\x05index\x18\x02 \x01(\x04\x12\x12\n\nnum_hashes\x18\x03 \x01(\x04\x12\x0c\n\x04hash\x18\x04 \x01(\x0c\x12\"\n\x1a\x65xecuted_transaction_count\x18\x05 \x01(\x04\x12\"\n\x1astarting_transaction_index\x18\x06 \x01(\x04\"\x15\n\x13SubscribeUpdatePing\"!\n\x13SubscribeUpdatePong\x12\n\n\x02id\x18\x01 \x01(\x05\"\x1c\n\x0bPingRequest\x12\r\n\x05\x63ount\x18\x01 \x01(\x05\"\x1d\n\x0cPongResponse\x12\r\n\x05\x63ount\x18\x01 \x01(\x05\"\\\n\x19GetLatestBlockhashRequest\x12\x30\n\ncommitment\x18\x01 \x01(\x0e\x32\x17.geyser.CommitmentLevelH\x00\x88\x01\x01\x42\r\n\x0b_commitment\"^\n\x1aGetLatestBlockhashResponse\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\x11\n\tblockhash\x18\x02 \x01(\t\x12\x1f\n\x17last_valid_block_height\x18\x03 \x01(\x04\"X\n\x15GetBlockHeightRequest\x12\x30\n\ncommitment\x18\x01 \x01(\x0e\x32\x17.geyser.CommitmentLevelH\x00\x88\x01\x01\x42\r\n\x0b_commitment\".\n\x16GetBlockHeightResponse\x12\x14\n\x0c\x62lock_height\x18\x01 \x01(\x04\"Q\n\x0eGetSlotRequest\x12\x30\n\ncommitment\x18\x01 \x01(\x0e\x32\x17.geyser.CommitmentLevelH\x00\x88\x01\x01\x42\r\n\x0b_commitment\"\x1f\n\x0fGetSlotResponse\x12\x0c\n\x04slot\x18\x01 \x01(\x04\"\x13\n\x11GetVersionRequest\"%\n\x12GetVersionResponse\x12\x0f\n\x07version\x18\x01 \x01(\t\"m\n\x17IsBlockhashValidRequest\x12\x11\n\tblockhash\x18\x01 \x01(\t\x12\x30\n\ncommitment\x18\x02 \x01(\x0e\x32\x17.geyser.CommitmentLevelH\x00\x88\x01\x01\x42\r\n\x0b_commitment\"7\n\x18IsBlockhashValidResponse\x12\x0c\n\x04slot\x18\x01 \x01(\x04\x12\r\n\x05valid\x18\x02 \x01(\x08*>\n\x0f\x43ommitmentLevel\x12\r\n\tPROCESSED\x10\x00\x12\r\n\tCONFIRMED\x10\x01\x12\r\n\tFINALIZED\x10\x02*\xa1\x01\n\nSlotStatus\x12\x12\n\x0eSLOT_PROCESSED\x10\x00\x12\x12\n\x0eSLOT_CONFIRMED\x10\x01\x12\x12\n\x0eSLOT_FINALIZED\x10\x02\x12\x1d\n\x19SLOT_FIRST_SHRED_RECEIVED\x10\x03\x12\x12\n\x0eSLOT_COMPLETED\x10\x04\x12\x15\n\x11SLOT_CREATED_BANK\x10\x05\x12\r\n\tSLOT_DEAD\x10\x06\x32\x93\x04\n\x06Geyser\x12\x44\n\tSubscribe\x12\x18.geyser.SubscribeRequest\x1a\x17.geyser.SubscribeUpdate\"\x00(\x01\x30\x01\x12\x33\n\x04Ping\x12\x13.geyser.PingRequest\x1a\x14.geyser.PongResponse\"\x00\x12]\n\x12GetLatestBlockhash\x12!.geyser.GetLatestBlockhashRequest\x1a\".geyser.GetLatestBlockhashResponse\"\x00\x12Q\n\x0eGetBlockHeight\x12\x1d.geyser.GetBlockHeightRequest\x1a\x1e.geyser.GetBlockHeightResponse\"\x00\x12<\n\x07GetSlot\x12\x16.geyser.GetSlotRequest\x1a\x17.geyser.GetSlotResponse\"\x00\x12W\n\x10IsBlockhashValid\x12\x1f.geyser.IsBlockhashValidRequest\x1a .geyser.IsBlockhashValidResponse\"\x00\x12\x45\n\nGetVersion\x12\x19.geyser.GetVersionRequest\x1a\x1a.geyser.GetVersionResponse\"\x00\x42;Z9github.com/rpcpool/yellowstone-grpc/examples/golang/protoP\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'geyser_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'Z9github.com/rpcpool/yellowstone-grpc/examples/golang/proto'
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_SLOTSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_SLOTSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSSTATUSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSSTATUSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_BLOCKSENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_BLOCKSENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_BLOCKSMETAENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_BLOCKSMETAENTRY']._serialized_options = b'8\001'
  _globals['_SUBSCRIBEREQUEST_ENTRYENTRY']._loaded_options = None
  _globals['_SUBSCRIBEREQUEST_ENTRYENTRY']._serialized_options = b'8\001'
  _globals['_COMMITMENTLEVEL']._serialized_start=6188
  _globals['_COMMITMENTLEVEL']._serialized_end=6250
  _globals['_SLOTSTATUS']._serialized_start=6253
  _globals['_SLOTSTATUS']._serialized_end=6414
  _globals['_SUBSCRIBEREQUEST']._serialized_start=80
  _globals['_SUBSCRIBEREQUEST']._serialized_end=1388
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_start=719
  _globals['_SUBSCRIBEREQUEST_ACCOUNTSENTRY']._serialized_end=806
  _globals['_SUBSCRIBEREQUEST_SLOTSENTRY']._serialized_start=808
  _globals['_SUBSCRIBEREQUEST_SLOTSENTRY']._serialized_end=889
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_start=891
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSENTRY']._serialized_end=986
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSSTATUSENTRY']._serialized_start=988
  _globals['_SUBSCRIBEREQUEST_TRANSACTIONSSTATUSENTRY']._serialized_end=1089
  _globals['_SUBSCRIBEREQUEST_BLOCKSENTRY']._serialized_start=1091
  _globals['_SUBSCRIBEREQUEST_BLOCKSENTRY']._serialized_end=1174
  _globals['_SUBSCRIBEREQUEST_BLOCKSMETAENTRY']._serialized_start=1176
  _globals['_SUBSCRIBEREQUEST_BLOCKSMETAENTRY']._serialized_end=1267
  _globals['_SUBSCRIBEREQUEST_ENTRYENTRY']._serialized_start=1269
  _globals['_SUBSCRIBEREQUEST_ENTRYENTRY']._serialized_end=1350
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTS']._serialized_start=1391
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTS']._serialized_end=1582
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTER']._serialized_start=1585
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTER']._serialized_end=1828
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTERMEMCMP']._serialized_start=1830
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTERMEMCMP']._serialized_end=1951
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTERLAMPORTS']._serialized_start=1953
  _globals['_SUBSCRIBEREQUESTFILTERACCOUNTSFILTERLAMPORTS']._serialized_end=2062
  _globals['_SUBSCRIBEREQUESTFILTERSLOTS']._serialized_start=2065
  _globals['_SUBSCRIBEREQUESTFILTERSLOTS']._serialized_end=2208
  _globals['_SUBSCRIBEREQUESTFILTERTRANSACTIONS']._serialized_start=2211
  _globals['_SUBSCRIBEREQUESTFILTERTRANSACTIONS']._serialized_end=2421
  _globals['_SUBSCRIBEREQUESTFILTERBLOCKS']._serialized_start=2424
  _globals['_SUBSCRIBEREQUESTFILTERBLOCKS']._serialized_end=2641
  _globals['_SUBSCRIBEREQUESTFILTERBLOCKSMETA']._serialized_start=2643
  _globals['_SUBSCRIBEREQUESTFILTERBLOCKSMETA']._serialized_end=2677
  _globals['_SUBSCRIBEREQUESTFILTERENTRY']._serialized_start=2679
  _globals['_SUBSCRIBEREQUESTFILTERENTRY']._serialized_end=2708
  _globals['_SUBSCRIBEREQUESTACCOUNTSDATASLICE']._serialized_start=2710
  _globals['_SUBSCRIBEREQUESTACCOUNTSDATASLICE']._serialized_end=2777
  _globals['_SUBSCRIBEREQUESTPING']._serialized_start=2779
  _globals['_SUBSCRIBEREQUESTPING']._serialized_end=2813
  _globals['_SUBSCRIBEUPDATE']._serialized_start=2816
  _globals['_SUBSCRIBEUPDATE']._serialized_end=3381
  _globals['_SUBSCRIBEUPDATEACCOUNT']._serialized_start=3383
  _globals['_SUBSCRIBEUPDATEACCOUNT']._serialized_end=3494
  _globals['_SUBSCRIBEUPDATEACCOUNTINFO']._serialized_start=3497
  _globals['_SUBSCRIBEUPDATEACCOUNTINFO']._serialized_end=3697
  _globals['_SUBSCRIBEUPDATESLOT']._serialized_start=3700
  _globals['_SUBSCRIBEUPDATESLOT']._serialized_end=3843
  _globals['_SUBSCRIBEUPDATETRANSACTION']._serialized_start=3845
  _globals['_SUBSCRIBEUPDATETRANSACTION']._serialized_end=3948
  _globals['_SUBSCRIBEUPDATETRANSACTIONINFO']._serialized_start=3951
  _globals['_SUBSCRIBEUPDATETRANSACTIONINFO']._serialized_end=4167
  _globals['_SUBSCRIBEUPDATETRANSACTIONSTATUS']._serialized_start=4170
  _globals['_SUBSCRIBEUPDATETRANSACTIONSTATUS']._serialized_end=4331
  _globals['_SUBSCRIBEUPDATEBLOCK']._serialized_start=4334
  _globals['_SUBSCRIBEUPDATEBLOCK']._serialized_end=4878
  _globals['_SUBSCRIBEUPDATEBLOCKMETA']._serialized_start=4881
  _globals['_SUBSCRIBEUPDATEBLOCKMETA']._serialized_end=5235
  _globals['_SUBSCRIBEUPDATEENTRY']._serialized_start=5238
  _globals['_SUBSCRIBEUPDATEENTRY']._serialized_end=5395
  _globals['_SUBSCRIBEUPDATEPING']._serialized_start=5397
  _globals['_SUBSCRIBEUPDATEPING']._serialized_end=5418
  _globals['_SUBSCRIBEUPDATEPONG']._serialized_start=5420
  _globals['_SUBSCRIBEUPDATEPONG']._serialized_end=5453
  _globals['_PINGREQUEST']._serialized_start=5455
  _globals['_PINGREQUEST']._serialized_end=5483
  _globals['_PONGRESPONSE']._serialized_start=5485
  _globals['_PONGRESPONSE']._serialized_end=5514
  _globals['_GETLATESTBLOCKHASHREQUEST']._serialized_start=5516
  _globals['_GETLATESTBLOCKHASHREQUEST']._serialized_end=5608
  _globals['_GETLATESTBLOCKHASHRESPONSE']._serialized_start=5610
  _globals['_GETLATESTBLOCKHASHRESPONSE']._serialized_end=5704
  _globals['_GETBLOCKHEIGHTREQUEST']._serialized_start=5706
  _globals['_GETBLOCKHEIGHTREQUEST']._serialized_end=5794
  _globals['_GETBLOCKHEIGHTRESPONSE']._serialized_start=5796
  _globals['_GETBLOCKHEIGHTRESPONSE']._serialized_end=5842
  _globals['_GETSLOTREQUEST']._serialized_start=5844
  _globals['_GETSLOTREQUEST']._serialized_end=5925
  _globals['_GETSLOTRESPONSE']._serialized_start=5927
  _globals['_GETSLOTRESPONSE']._serialized_end=5958
  _globals['_GETVERSIONREQUEST']._serialized_start=5960
  _globals['_GETVERSIONREQUEST']._serialized_end=5979
  _globals['_GETVERSIONRESPONSE']._serialized_start=5981
  _globals['_GETVERSIONRESPONSE']._serialized_end=6018
  _globals['_ISBLOCKHASHVALIDREQUEST']._serialized_start=6020
  _globals['_ISBLOCKHASHVALIDREQUEST']._serialized_end=6129
  _globals['_ISBLOCKHASHVALIDRESPONSE']._serialized_start=6131
  _globals['_ISBLOCKHASHVALIDRESPONSE']._serialized_end=6186
  _globals['_GEYSER']._serialized_start=6417
  _globals['_GEYSER']._serialized_end=6948
# @@protoc_insertion_point(module_scope)
