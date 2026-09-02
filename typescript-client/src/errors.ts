/** A gRPC status parsed out of a native-binding error. */
export interface GrpcErrorInfo {
  /** Numeric gRPC status code, e.g. `5` for `NOT_FOUND`. */
  code: number
  /** PascalCase gRPC status name, e.g. `"NotFound"`. */
  codeName: string
  /** Human-readable status message from the server. */
  message: string
}

/** Canonical gRPC status codes surfaced by the Fumarole service. */
export const GrpcStatus = {
  NOT_FOUND: 5,
  UNAVAILABLE: 14,
} as const

// The native binding has no way to attach a real gRPC status code to a thrown
// JS error (napi-rs errors only carry napi's own fixed set of status kinds,
// which collapses every RPC failure to `Error { code: 'GenericFailure' }').
// It embeds the code in this documented, stable message prefix instead.
const GRPC_ERROR_PATTERN = /^\[grpc (\d+) (\w+)\] ([\s\S]*)$/

/**
 * Extracts the gRPC status code from an error thrown by the native binding, if present.
 *
 * Prefer this over matching on `err.message` directly — the `[grpc <code> <name>]` prefix
 * is a stable, documented format, unlike the human-readable text that follows it.
 *
 * @param err  The value caught from a rejected client call.
 * @returns    The parsed status, or `null` if `err` isn't a native-binding gRPC error.
 */
export function parseGrpcError(err: unknown): GrpcErrorInfo | null {
  if (!(err instanceof Error)) return null
  const match = GRPC_ERROR_PATTERN.exec(err.message)
  if (!match) return null
  return { code: Number(match[1]), codeName: match[2], message: match[3] }
}

/**
 * Returns the gRPC status code of `err`, or `undefined` if it isn't a native-binding gRPC error.
 *
 * @param err  The value caught from a rejected client call.
 */
export function getGrpcStatusCode(err: unknown): number | undefined {
  return parseGrpcError(err)?.code
}

/**
 * Returns `true` if `err` is a native-binding gRPC error with the given status `code`.
 *
 * @param err   The value caught from a rejected client call.
 * @param code  A status from {@link GrpcStatus} (or any raw gRPC status number).
 */
export function isGrpcStatus(err: unknown, code: number): boolean {
  return getGrpcStatusCode(err) === code
}
