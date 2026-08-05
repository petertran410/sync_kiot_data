import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHmac, timingSafeEqual } from 'crypto';
import { Request } from 'express';

@Injectable()
export class SePaySignatureGuard implements CanActivate {
  private readonly secret: string;

  constructor(config: ConfigService) {
    this.secret = config.get<string>('SEPAY_WEBHOOK_SECRET') ?? '';
  }

  canActivate(context: ExecutionContext): boolean {
    if (!this.secret) {
      throw new UnauthorizedException('SePay webhook secret is not configured');
    }

    const request = context
      .switchToHttp()
      .getRequest<Request & { rawBody?: Buffer }>();
    const signature = request.header('x-sepay-signature');
    const timestampText = request.header('x-sepay-timestamp');
    const timestamp = Number(timestampText);

    if (!signature || !timestampText || !Number.isFinite(timestamp)) {
      throw new UnauthorizedException('Missing SePay signature headers');
    }
    if (Math.abs(Math.floor(Date.now() / 1000) - timestamp) > 300) {
      throw new UnauthorizedException('SePay webhook request expired');
    }
    if (!request.rawBody) {
      throw new UnauthorizedException('Raw body unavailable');
    }

    const expected =
      'sha256=' +
      createHmac('sha256', this.secret)
        .update(`${timestamp}.`)
        .update(request.rawBody)
        .digest('hex');
    if (!this.safeEqual(expected, signature.trim().toLowerCase())) {
      throw new UnauthorizedException('Invalid SePay webhook signature');
    }
    return true;
  }

  private safeEqual(expected: string, received: string): boolean {
    const left = Buffer.from(expected);
    const right = Buffer.from(received);
    return left.length === right.length && timingSafeEqual(left, right);
  }
}
