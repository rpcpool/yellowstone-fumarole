type WaitResult<T> = {
  done: Promise<T>[];
  pending: Promise<T>[];
};

export async function waitFirstCompleted<T>(promises: Promise<T>[]): Promise<WaitResult<T>> {
  if (promises.length === 0) {
    return { done: [], pending: [] };
  }

  return new Promise<WaitResult<T>>((resolve) => {
    let settled = false;

    promises.forEach((p) => {
      p.then(
        () => {
          if (!settled) {
            settled = true;
            const done = [p];
            const pending = promises.filter((q) => q !== p);
            resolve({ done, pending });
          }
        },
        () => {
          if (!settled) {
            settled = true;
            const done = [p];
            const pending = promises.filter((q) => q !== p);
            resolve({ done, pending });
          }
        }
      );
    });
  });
}
