import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { sleep } from './concurrency.util';

/**
 * Token-bucket rate limiter shared across all KiotViet fetches.
 * KiotViet GET limit: 5000 requests/hour (per API doc).
 * We default to 4500/hour with a small burst capacity for safety margin.
 *
 * Refills continuously (not in discrete ticks) so bursts are smoothed.
 */
@Injectable()
export class KiotRateLimiter {
  private readonly logger = new Logger(KiotRateLimiter.name);
  private tokens: number;
  private readonly capacity: number;
  private readonly refillPerMs: number;
  private lastRefillMs: number;
  private waitCount = 0;

  constructor(configService: ConfigService) {
    const perHour = this.parseInt(
      configService.get<string>('SYNC_RATE_LIMIT_PER_HOUR'),
      4500,
    );
    const burst = this.parseInt(
      configService.get<string>('SYNC_RATE_LIMIT_BURST'),
      1,
    );
    this.capacity = Math.max(1, burst);
    this.refillPerMs = perHour / 3600000; // tokens per ms
    this.tokens = this.capacity;
    this.lastRefillMs = Date.now();
    this.logger.log(
      `Rate limiter initialized: ${perHour}/hour, burst ${this.capacity}`,
    );
  }

  private parseInt(v: string | undefined, def: number): number {
    if (v === undefined || v === null || v === '') return def;
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? n : def;
  }

  private refill(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefillMs;
    if (elapsed > 0) {
      this.tokens = Math.min(
        this.capacity,
        this.tokens + elapsed * this.refillPerMs,
      );
      this.lastRefillMs = now;
    }
  }

  /**
   * Acquire 1 token, waiting if necessary. Resolves when a token has been consumed.
   */
  async acquire(): Promise<void> {
    while (true) {
      this.refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      // Compute wait time until 1 token is available.
      const waitMs = Math.ceil((1 - this.tokens) / this.refillPerMs);
      this.waitCount++;
      if (this.waitCount % 100 === 1) {
        this.logger.debug(
          `Rate limit reached, waiting ${waitMs}ms for token (tokens=${this.tokens.toFixed(2)})`,
        );
      }
      await sleep(Math.max(waitMs, 10));
    }
  }

  /** Current available tokens (for diagnostics). */
  availableTokens(): number {
    this.refill();
    return this.tokens;
  }
}
