export interface FumaroleConfigOptions {
  /** gRPC endpoint URL, e.g. `"https://fumarole.triton.one"` */
  endpoint: string
  /** Authentication token */
  xToken?: string
  /** Maximum protobuf message size in bytes. Default: 512 MB */
  maxDecodingMessageSizeBytes?: number
}

export interface FumaroleSubscribeConfigOptions {
  /** Number of parallel data-plane TCP connections (default: 1) */
  numDataPlaneTcpConnections?: number
  /** Offset commit interval in milliseconds (default: 10 000) */
  commitIntervalMs?: number
  /** Max consecutive failed slot downloads before session fails (default: 3) */
  maxFailedSlotDownloadAttempt?: number
  /** Garbage-collection tick interval (default: 100) */
  gcInterval?: number
  /** Slots kept in the dedup window (default: 1 000) */
  slotMemoryRetention?: number
  /** Disable offset commits entirely */
  noCommit?: boolean
  /** Automatically commit progress after each event */
  autoCommit?: boolean
}
