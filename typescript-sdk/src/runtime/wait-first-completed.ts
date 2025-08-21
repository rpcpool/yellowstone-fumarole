//// Custom TypeScript implementation for Python's `asyncio.wait`

type WaitResult<T> = {
  done: Set<Promise<T>>;
  pending: Set<Promise<T>>;
};

export async function waitFirstCompleted<T>(
  promises: Set<Promise<T>>
): Promise<WaitResult<T>> {
  if (promises.size === 0) {
    return { done: new Set(), pending: new Set() };
  }

  // Map original promises to tracking wrappers
  const wrapped = new Map<
    Promise<T>,
    Promise<{ promise: Promise<T>; status: "fulfilled" | "rejected"; value?: T; reason?: unknown }>
  >();

  for (const p of promises) {
    wrapped.set(
      p,
      p.then(
        value => ({ promise: p, status: "fulfilled", value }),
        reason => ({ promise: p, status: "rejected", reason })
      )
    );
  }

  // Wait for the first one to settle
  let first;
  try {
    first = await Promise.race(wrapped.values());
  } catch {
    // This branch should not happen since we handle rejection inside wrapper
    throw new Error("Unexpected race rejection");
  }

  // Collect all results, but do not cancel still-pending promises
  const results = await Promise.allSettled(wrapped.values());

  const done = new Set<Promise<T>>();
  const stillPending = new Set(promises);

  for (const r of results) {
    if (r.status === "fulfilled") {
      const { promise } = r.value;
      if (promise === first.promise) {
        done.add(promise);
        stillPending.delete(promise);
      }
    }
  }

  return { done, pending: stillPending };
}
