/**
 * Simple concurrency limiter — no external deps (p-limit v7 is ESM-only, incompatible with CommonJS NestJS).
 * Runs async mapper over items with a max concurrency limit.
 */
export async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
  onProgress?: (done: number, total: number) => void,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let cursor = 0;
  let done = 0;
  const total = items.length;
  const limit = Math.max(1, concurrency);

  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= total) return;
      results[idx] = await fn(items[idx], idx);
      done++;
      onProgress?.(done, total);
    }
  }

  const workers: Promise<void>[] = [];
  const workerCount = Math.min(limit, total);
  for (let i = 0; i < workerCount; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

/**
 * Run async tasks with concurrency limit, settling all (never throws — returns {ok, err}).
 */
export async function mapAllSettled<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<{ ok: boolean; value?: R; error?: Error; index: number }[]> {
  const results: { ok: boolean; value?: R; error?: Error; index: number }[] =
    new Array(items.length);
  let cursor = 0;
  const total = items.length;
  const limit = Math.max(1, concurrency);

  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= total) return;
      try {
        results[idx] = {
          ok: true,
          value: await fn(items[idx], idx),
          index: idx,
        };
      } catch (error) {
        results[idx] = {
          ok: false,
          error: error instanceof Error ? error : new Error(String(error)),
          index: idx,
        };
      }
    }
  }

  const workers: Promise<void>[] = [];
  const workerCount = Math.min(limit, total);
  for (let i = 0; i < workerCount; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return results;
}

/** Sleep helper. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
