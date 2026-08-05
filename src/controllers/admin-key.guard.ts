import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash, timingSafeEqual } from 'crypto';
import { Request } from 'express';

/**
 * Protects the webhook administration endpoints.
 *
 * These endpoints can register and unregister KiotViet subscriptions and requeue
 * events, so they must not be reachable anonymously. They also cannot sit behind
 * `WebhookSignatureGuard`, because that verifies a KiotViet HMAC over the request
 * body — a signature an operator calling these endpoints could not produce.
 *
 * FAIL-CLOSED: with no `ADMIN_API_KEY` configured, every request is rejected.
 * Send the key as `X-Admin-Key` or `Authorization: Bearer <key>`.
 */
@Injectable()
export class AdminKeyGuard implements CanActivate {
  private readonly logger = new Logger(AdminKeyGuard.name);
  private readonly key: string | undefined;

  constructor(config: ConfigService) {
    this.key = config.get<string>('ADMIN_API_KEY') || undefined;
    if (!this.key) {
      this.logger.error(
        'ADMIN_API_KEY is not set — webhook admin endpoints will reject every request. ' +
          'Set it to enable webhook registration and queue management.',
      );
    }
  }

  canActivate(context: ExecutionContext): boolean {
    if (!this.key) {
      throw new UnauthorizedException('ADMIN_API_KEY is not configured');
    }

    const req = context.switchToHttp().getRequest<Request>();
    const header = req.header('x-admin-key');
    const bearer = req.header('authorization');
    const provided =
      header ?? (bearer?.startsWith('Bearer ') ? bearer.slice(7) : undefined);

    if (!provided) {
      throw new UnauthorizedException('Missing X-Admin-Key header');
    }

    // Hash both sides to a fixed length so timingSafeEqual cannot throw on
    // mismatched lengths, and so length itself is not observable.
    const a = createHash('sha256').update(provided).digest();
    const b = createHash('sha256').update(this.key).digest();
    if (!timingSafeEqual(a, b)) {
      this.logger.warn('Rejected webhook admin request: bad admin key');
      throw new UnauthorizedException('Invalid admin key');
    }
    return true;
  }
}
