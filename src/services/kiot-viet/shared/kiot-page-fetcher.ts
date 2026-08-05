import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { KiotVietAuthService } from '../auth.service';
import { KiotRateLimiter } from './kiot-rate-limiter';
import { mapWithConcurrency, sleep } from './concurrency.util';

export interface FetchPageParams {
  currentItem: number;
  pageSize: number;
  [key: string]: any;
}

export interface KiotListResponse<T> {
  data: T[];
  total?: number;
  timestamp?: string;
  removedIds?: any[];
}

export interface PageFetcherOptions {
  /** Endpoint path, e.g. '/users' or '/invoices'. */
  endpoint: string;
  /** Base query params (without currentItem/pageSize). */
  baseParams?: Record<string, any>;
  /** Page size (default 100, max 100 per KiotViet API). */
  pageSize?: number;
  /** Max concurrent page fetches (default SYNC_FETCH_CONCURRENCY or 5). */
  concurrency?: number;
  /** Per-request timeout ms (default 45000). */
  timeoutMs?: number;
  /** HTTP-level retries per page (default 5). */
  maxRetries?: number;
  /** Max consecutive empty pages before stopping (default 5). */
  maxEmptyPages?: number;
  /** Called with each page's data as it arrives (for streaming saves). */
  onPage?: (pageData: any[], pageIndex: number, total: number) => Promise<void>;
  /** Logger label. */
  label?: string;
}

/**
 * Fetches all pages of a KiotViet list endpoint with parallelism.
 * Strategy: fetch page 1 to learn `total`, then fetch remaining pages concurrently
 * (rate-limited). If onPage is provided, streams each page to the caller as it arrives
 * (recommended — avoids holding all data in memory).
 */
@Injectable()
export class KiotPageFetcher {
  private readonly logger = new Logger(KiotPageFetcher.name);
  private readonly baseUrl: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
    private readonly rateLimiter: KiotRateLimiter,
  ) {
    const url = process.env.KIOT_BASE_URL;
    if (!url)
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    this.baseUrl = url.replace(/\/$/, '');
  }

  private getConcurrency(opt?: number): number {
    if (opt && opt > 0) return opt;
    const env = Number(process.env.SYNC_FETCH_CONCURRENCY);
    return Number.isFinite(env) && env > 0 ? env : 5;
  }

  /**
   * Fetch a single page with retry + rate limiting.
   */
  async fetchPage<T = any>(
    endpoint: string,
    params: FetchPageParams,
    opts: { timeoutMs?: number; maxRetries?: number; label?: string } = {},
  ): Promise<KiotListResponse<T>> {
    const { timeoutMs = 45000, maxRetries = 5, label = endpoint } = opts;
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      await this.rateLimiter.acquire();
      try {
        const headers = await this.authService.getRequestHeaders();
        const query = new URLSearchParams();
        for (const [k, v] of Object.entries(params)) {
          if (v === undefined || v === null) continue;
          query.append(k, String(v));
        }
        const url = `${this.baseUrl}${endpoint}?${query}`;
        const { data } = await firstValueFrom(
          this.httpService.get<KiotListResponse<T>>(url, {
            headers,
            timeout: timeoutMs,
          }),
        );
        return data;
      } catch (error) {
        lastError = error as Error;
        const status = error?.response?.status;
        // 4xx (except 429) are not retryable.
        if (status && status >= 400 && status < 500 && status !== 429) {
          throw error;
        }
        if (attempt < maxRetries) {
          // 429 (rate limit): use longer backoff, respect Retry-After header if present.
          let delay: number;
          if (status === 429) {
            const retryAfter = error?.response?.headers?.['retry-after'];
            delay = retryAfter
              ? Number(retryAfter) * 1000
              : Math.min(5000 * attempt, 30000);
          } else {
            delay = Math.min(2000 * attempt, 16000);
          }
          this.logger.warn(
            `${label} fetch attempt ${attempt}/${maxRetries} failed (status=${status}): ${lastError.message}; retry in ${delay}ms`,
          );
          await sleep(delay);
        }
      }
    }
    throw lastError;
  }

  /**
   * Fetch all pages. If `onPage` is provided, pages are streamed (preferred).
   * Otherwise returns the full aggregated data array.
   */
  async fetchAll<T = any>(
    options: PageFetcherOptions,
  ): Promise<{
    data: T[];
    total: number;
    serverTimestamp?: string;
    /** Ids KiotViet reports as deleted. Only populated when includeRemoveIds=true. */
    removedIds?: number[];
  }> {
    const {
      endpoint,
      baseParams = {},
      pageSize = 100,
      timeoutMs = 45000,
      maxRetries = 5,
      maxEmptyPages = 5,
      onPage,
      label = endpoint,
    } = options;
    const concurrency = this.getConcurrency(options.concurrency);

    // Page 1
    const first = await this.fetchPage<T>(
      endpoint,
      { ...baseParams, currentItem: 0, pageSize },
      { timeoutMs, maxRetries, label },
    );
    const firstData = first.data || [];
    const total = first.total ?? firstData.length;
    this.logger.log(`${label}: total=${total}, page1=${firstData.length}`);

    if (onPage) {
      await onPage(firstData, 0, total);
    }

    // KiotViet returns its own server clock on list responses. It is the only safe
    // cursor for the next incremental run: using our local clock risks skipping
    // records whenever the two clocks drift.
    const serverTimestamp = first.timestamp;

    // Deletions arrive only on the FIRST page's envelope, so capture them here.
    // Without webhooks this is the only signal that a record was removed upstream.
    const removedIds = Array.isArray(first.removedIds)
      ? first.removedIds.map((v: any) => Number(v)).filter((n: number) => Number.isFinite(n) && n > 0)
      : undefined;

    if (firstData.length === 0 || firstData.length >= total) {
      return { data: onPage ? [] : firstData, total, serverTimestamp, removedIds };
    }

    // Remaining pages: build list of offsets, fetch concurrently.
    const offsets: number[] = [];
    for (let off = pageSize; off < total; off += pageSize) {
      offsets.push(off);
    }

    const allData: T[] = onPage ? [] : [...firstData];
    let consecutiveEmpty = 0;
    let processed = firstData.length;

    await mapWithConcurrency(offsets, concurrency, async (offset, idx) => {
      const page = await this.fetchPage<T>(
        endpoint,
        { ...baseParams, currentItem: offset, pageSize },
        { timeoutMs, maxRetries, label },
      );
      const pageData = page.data || [];
      if (pageData.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= maxEmptyPages) {
          this.logger.warn(
            `${label}: stopping after ${maxEmptyPages} empty pages at offset ${offset}`,
          );
        }
        return;
      }
      consecutiveEmpty = 0;
      processed += pageData.length;
      if (onPage) {
        await onPage(pageData, idx + 1, total);
      } else {
        allData.push(...pageData);
      }
      if (processed % 1000 < pageSize) {
        this.logger.log(
          `${label}: progress ${processed}/${total} (${((processed / total) * 100).toFixed(1)}%)`,
        );
      }
    });

    this.logger.log(`${label}: fetched ${processed}/${total} records`);
    return { data: allData, total, serverTimestamp, removedIds };
  }
}
