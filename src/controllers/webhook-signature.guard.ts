import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash, createHmac, timingSafeEqual } from 'crypto';
import { Request } from 'express';

/**
 * Verifies the KiotViet webhook signature (`X-Hub-Signature`) over the raw
 * request body, keyed by KIOT_WEBHOOK_SECRET. The documented signature is
 * HMAC-SHA256, while live deliveries may explicitly use `sha1=`.
 *
 * Per doc section 2.11.1: the secret is the Base64 string supplied when registering
 * the webhook; KiotViet computes an HMAC and sends its digest in the header.
 * Live retailers differ on whether the Base64 text or its decoded bytes are
 * used as the HMAC key, so both representations are verified.
 *
 * FAIL-CLOSED. If no secret is configured, every request is rejected. The previous
 * implementation returned `true` in that case, which left all webhook endpoints
 * writing to the database with no authentication whatsoever.
 *
 * To run without verification (local development only) set
 * `KIOT_WEBHOOK_ALLOW_UNSIGNED=true` — an explicit, auditable opt-out.
 *
 * IMPORTANT operational note: per doc 2.11.1 step 5 we must answer 401 on a
 * signature mismatch, but the same doc warns that ANY 4xx response causes KiotViet
 * to STOP delivering to that endpoint. A wrong secret therefore silently disables
 * the subscription. That is why mismatches are logged at `error` level, and why the
 * registry reconcile in the webhook registry service re-checks `isActive`.
 */
@Injectable()
export class WebhookSignatureGuard implements CanActivate {
  private readonly logger = new Logger(WebhookSignatureGuard.name);
  private readonly secret: string | undefined;
  private readonly secretKeys: Array<{ mode: string; key: string | Buffer }>;
  private readonly allowUnsigned: boolean;

  constructor(configService: ConfigService) {
    this.secret = configService.get<string>('KIOT_WEBHOOK_SECRET') || undefined;
    this.secretKeys = this.buildSecretKeys(this.secret);
    this.allowUnsigned =
      String(
        configService.get<string>('KIOT_WEBHOOK_ALLOW_UNSIGNED') ?? '',
      ).toLowerCase() === 'true';

    if (this.secret) {
      const fingerprint = createHash('sha256')
        .update(this.secret)
        .digest('hex')
        .slice(0, 12);
      this.logger.log(
        `Webhook signature verification enabled ` +
          `(secret fingerprint: ${fingerprint}, key modes: ${this.secretKeys.map((item) => item.mode).join(', ')})`,
      );
    } else if (this.allowUnsigned) {
      this.logger.warn(
        'KIOT_WEBHOOK_SECRET is not set and KIOT_WEBHOOK_ALLOW_UNSIGNED=true — ' +
          'webhook endpoints are UNAUTHENTICATED. Never use this in production.',
      );
    } else {
      this.logger.error(
        'KIOT_WEBHOOK_SECRET is not set — all webhook requests will be rejected with 401. ' +
          'Set the secret, or set KIOT_WEBHOOK_ALLOW_UNSIGNED=true for local development.',
      );
    }
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    if (!this.secret) {
      if (this.allowUnsigned) return true;
      throw new UnauthorizedException(
        'Webhook signature verification is not configured',
      );
    }

    const req = context
      .switchToHttp()
      .getRequest<Request & { rawBody?: Buffer }>();
    const signature = req.header('x-hub-signature');
    if (!signature) {
      throw new UnauthorizedException('Missing X-Hub-Signature header');
    }

    const raw = req.rawBody;
    if (!raw) {
      // Indicates rawBody:true was lost in bootstrap, or a proxy rewrote the body.
      this.logger.error('Raw body unavailable — cannot verify signature');
      throw new UnauthorizedException(
        'Raw body unavailable for signature verification',
      );
    }

    // The v4.7.1 document says HMAC-SHA256, but live KiotViet deliveries use
    // `sha1=<hex>` on some retailers. Select the HMAC algorithm from the
    // explicit prefix instead of stripping only `sha256=` and rejecting valid
    // deliveries. Unprefixed signatures retain the documented SHA-256 default.
    const parsed = signature.trim().match(/^(sha1|sha256)=(.+)$/i);
    const declaredAlgorithm = parsed?.[1]?.toLowerCase();
    const received = (parsed?.[2] ?? signature).trim();

    const algorithms = this.signatureAlgorithms(declaredAlgorithm, received);

    // The doc shows `body.CreateHmacSignature(Secret)` but never states the digest
    // encoding. .NET helpers return Base64 about as often as hex, and guessing wrong
    // means every delivery 401s — which per doc 2.11.1 makes KiotViet permanently
    // disable the endpoint. So accept either encoding.
    let matchedMode: string | null = null;
    for (const algorithm of algorithms) {
      for (const secretKey of this.secretKeys) {
        for (const payload of this.payloadCandidates(raw)) {
          const mac = createHmac(algorithm, secretKey.key)
            .update(payload.value)
            .digest();
          const candidates = [mac.toString('hex'), mac.toString('base64')];
          if (
            candidates.some((expected) => this.safeEqual(expected, received))
          ) {
            matchedMode = `${algorithm}/${secretKey.mode}/${payload.mode}`;
            break;
          }
        }
        if (matchedMode) break;
      }
      if (matchedMode) break;
    }
    const ok = matchedMode !== null;
    if (!ok) {
      this.logger.error(
        `Webhook signature mismatch — responding 401. NOTE: KiotViet stops delivering ` +
          `to an endpoint after a 4xx, so verify KIOT_WEBHOOK_SECRET matches the value ` +
          `used at registration, then re-register. ` +
          `(declared algorithm: ${declaredAlgorithm ?? 'none'}, digest length: ${received.length}, ` +
          `raw bytes: ${raw.length}, received prefix: ${received.slice(0, 12)})`,
      );
      throw new UnauthorizedException('Invalid webhook signature');
    }
    this.logger.debug(
      `Webhook signature verified (${matchedMode})`,
    );
    return true;
  }

  /**
   * KiotViet asks callers to Base64-encode their random secret before
   * registration. Retailer deployments differ on whether that encoded text or
   * its decoded bytes become the HMAC key, so verify both representations.
   */
  private buildSecretKeys(
    secret: string | undefined,
  ): Array<{ mode: string; key: string | Buffer }> {
    if (!secret) return [];
    const keys: Array<{ mode: string; key: string | Buffer }> = [
      { mode: 'base64-text', key: secret },
    ];

    if (/^[A-Za-z0-9+/]+={0,2}$/.test(secret) && secret.length % 4 === 0) {
      const decoded = Buffer.from(secret, 'base64');
      if (decoded.length > 0) {
        keys.push({ mode: 'base64-decoded', key: decoded });
      }
    }
    return keys;
  }

  private signatureAlgorithms(
    declared: string | undefined,
    received: string,
  ): Array<'sha1' | 'sha256'> {
    const inferred =
      received.length === 40 || received.length === 28 ? 'sha1' : 'sha256';
    return Array.from(
      new Set([declared, inferred, 'sha1', 'sha256'].filter(Boolean)),
    ) as Array<'sha1' | 'sha256'>;
  }

  private payloadCandidates(
    raw: Buffer,
  ): Array<{ mode: string; value: Buffer | string }> {
    const candidates: Array<{ mode: string; value: Buffer | string }> = [
      { mode: 'raw', value: raw },
    ];
    const text = raw.toString('utf8');
    const withoutBom = text.replace(/^\uFEFF/, '');
    if (withoutBom !== text) {
      candidates.push({ mode: 'without-bom', value: withoutBom });
    }
    const trimmed = withoutBom.trim();
    if (trimmed !== withoutBom) {
      candidates.push({ mode: 'trimmed', value: trimmed });
    }
    try {
      const compact = JSON.stringify(JSON.parse(withoutBom));
      if (compact !== withoutBom && compact !== trimmed) {
        candidates.push({ mode: 'json-compact', value: compact });
      }
    } catch {
      // Invalid JSON is handled by the controller after signature verification.
    }
    return candidates;
  }

  /** Length-checked, constant-time comparison. Hex compare is case-insensitive. */
  private safeEqual(expected: string, received: string): boolean {
    const exp = /^[0-9a-f]+$/i.test(expected)
      ? expected.toLowerCase()
      : expected;
    const rec = /^[0-9a-f]+$/i.test(expected)
      ? received.toLowerCase()
      : received;
    const a = Buffer.from(exp, 'utf8');
    const b = Buffer.from(rec, 'utf8');
    if (a.length !== b.length) return false;
    return timingSafeEqual(a, b);
  }
}
