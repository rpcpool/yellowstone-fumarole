export { FumaroleClient } from './client.js'
export { FumaroleSubscription } from './subscription.js'
export type { FumaroleEvent } from './subscription.js'
export type { FumaroleConfigOptions, FumaroleSubscribeConfigOptions } from './config.js'

// Re-export the most commonly needed proto types so callers can use one import.
export {
  CommitmentLevel,
  SubscribeRequest,
  SubscribeUpdate,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterTransactions,
  SubscribeRequestFilterBlocks,
  SubscribeRequestFilterBlocksMeta,
  SubscribeRequestFilterEntry,
  SubscribeRequestFilterSlots,
} from './grpc/geyser.js'

export {
  ConsumerGroupInfo,
  CreateConsumerGroupRequest,
  CreateConsumerGroupResponse,
  DeleteConsumerGroupRequest,
  DeleteConsumerGroupResponse,
  GetChainTipRequest,
  GetChainTipResponse,
  GetSlotRangeResponse,
  InitialOffsetPolicy,
  ListConsumerGroupsResponse,
  VersionResponse,
} from './grpc/fumarole.js'
