/**
 * Asynchronous utilities for TypeScript
 */

/**
 * Create a forever pending promise. This promise is not resolved and will never be resolved.
 * This is useful for testing purposes.
 * @returns A promise that never resolves
 */
export async function never(): Promise<never> {
  return new Promise<never>(() => {
    // This promise intentionally never resolves
  });
}

/**
 * A class that represents an interval that can be used to run async operations periodically
 */
export class Interval {
  private readonly interval: number;

  /**
   * Create an interval that will run every `interval` seconds.
   * @param interval The interval in seconds
   */
  constructor(interval: number) {
    this.interval = interval;
  }

  /**
   * Wait for the interval duration
   * @returns A promise that resolves after the interval duration
   */
  async tick(): Promise<void> {
    // Convert seconds to milliseconds for setTimeout
    return new Promise((resolve) => setTimeout(resolve, this.interval * 1000));
  }
}

/**
 * Type for any function that returns a Promise
 */
export type AsyncFunction<T> = () => Promise<T>;

/**
 * Helper functions and utilities for logging
 */
export const logger = {
  debug: (...args: any[]) => console.debug("[DEBUG]", ...args),
  info: (...args: any[]) => console.info("[INFO]", ...args),
  warn: (...args: any[]) => console.warn("[WARN]", ...args),
  error: (...args: any[]) => console.error("[ERROR]", ...args),
};
