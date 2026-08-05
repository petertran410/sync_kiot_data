import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

/**
 * Resolves the KiotViet `retailerId` (shop id).
 *
 * It used to be hardcoded as the literal `310831` in fourteen places across five
 * files. That silently mislabels every row if the credentials ever point at a
 * different shop, and it makes the value impossible to change without a code edit.
 *
 * Resolution order:
 *   1. the value on the KiotViet payload itself (authoritative)
 *   2. `KIOT_RETAILER_ID` from configuration
 *   3. the legacy literal, so existing deployments keep working unchanged
 *
 * `learn()` lets sync code record the id observed on real API responses, which is
 * then used for payloads that omit it (several webhook envelopes do).
 */
@Injectable()
export class RetailerContext {
  private readonly logger = new Logger(RetailerContext.name);

  /** Legacy hardcoded value, retained as the final fallback. */
  static readonly LEGACY_DEFAULT = 310831;

  private readonly configured: number | null;
  private observed: number | null = null;
  private warnedMismatch = false;

  constructor(config: ConfigService) {
    const raw = config.get<string>('KIOT_RETAILER_ID');
    const parsed = Number(raw);
    this.configured =
      raw !== undefined &&
      raw !== null &&
      raw !== '' &&
      Number.isFinite(parsed) &&
      parsed > 0
        ? Math.trunc(parsed)
        : null;

    if (this.configured) {
      this.logger.log(`retailerId from config: ${this.configured}`);
    } else {
      this.logger.warn(
        `KIOT_RETAILER_ID not set — falling back to ${RetailerContext.LEGACY_DEFAULT}. ` +
          `Set it explicitly to avoid mislabelling rows if the credentials change shop.`,
      );
    }
  }

  /**
   * Record a retailerId seen on a real KiotViet response, so later payloads that
   * omit the field can still be attributed correctly.
   */
  learn(value: unknown): void {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return;
    const id = Math.trunc(n);

    if (this.observed === id) return;
    this.observed = id;

    if (this.configured && this.configured !== id && !this.warnedMismatch) {
      this.warnedMismatch = true;
      this.logger.error(
        `retailerId mismatch: KIOT_RETAILER_ID=${this.configured} but the API returned ${id}. ` +
          `The API value wins. Check that KIOT_RETAILER_ID matches the credentials in use.`,
      );
    }
  }

  /** The best-known retailerId, ignoring any per-payload value. */
  get(): number {
    return this.observed ?? this.configured ?? RetailerContext.LEGACY_DEFAULT;
  }

  /**
   * Preferred accessor. Uses the payload's own value when present, since that is
   * always authoritative, and remembers it for later.
   */
  resolve(payloadValue?: unknown): number {
    const n = Number(payloadValue);
    if (Number.isFinite(n) && n > 0) {
      const id = Math.trunc(n);
      this.learn(id);
      return id;
    }
    return this.get();
  }
}
