import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHmac, timingSafeEqual } from 'crypto';
import { Request } from 'express';

/**
 * Verifies the KiotViet webhook signature (`X-Hub-Signature`) using HMAC-SHA-256
 * over the raw request body, keyed by KIOT_WEBHOOK_SECRET.
 *
 * Per doc section 2.11.1: the secret is the Base64 string supplied when registering
 * the webhook; KiotViet computes `HMACSHA256(secret, body)` and sends the hex digest.
 * The secret is used as the key *verbatim* (the Base64 text itself, not its decoded
 * bytes) because that is the string handed to KiotViet at registration time.
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
  private readonly allowUnsigned: boolean;

  constructor(configService: ConfigService) {
    this.secret = configService.get<string>('KIOT_WEBHOOK_SECRET') || undefined;
    this.allowUnsigned =
      String(
        configService.get<string>('KIOT_WEBHOOK_ALLOW_UNSIGNED') ?? '',
      ).toLowerCase() === 'true';

    if (this.secret) {
      this.logger.log('Webhook signature verification enabled');
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

    // KiotViet may prefix with "sha256=".
    const received = (
      signature.startsWith('sha256=') ? signature.slice(7) : signature
    ).trim();

    // The doc shows `body.CreateHmacSignature(Secret)` but never states the digest
    // encoding. .NET helpers return Base64 about as often as hex, and guessing wrong
    // means every delivery 401s — which per doc 2.11.1 makes KiotViet permanently
    // disable the endpoint. So accept either encoding.
    const mac = createHmac('sha256', this.secret).update(raw).digest();
    const candidates = [mac.toString('hex'), mac.toString('base64')];

    const ok = candidates.some((expected) =>
      this.safeEqual(expected, received),
    );
    if (!ok) {
      this.logger.error(
        `Webhook signature mismatch — responding 401. NOTE: KiotViet stops delivering ` +
          `to an endpoint after a 4xx, so verify KIOT_WEBHOOK_SECRET matches the value ` +
          `used at registration, then re-register. (received prefix: ${received.slice(0, 12)})`,
      );
      throw new UnauthorizedException('Invalid webhook signature');
    }
    return true;
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
