import { createHmac, timingSafeEqual } from 'crypto';
import { ConfigService } from '@nestjs/config';
import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Request } from 'express';

export const DELIVERY_REPORT_COOKIE = 'delivery_report_access';
export const DELIVERY_REPORT_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60;

type AccessTokenPayload = {
  exp: number;
  iat: number;
};

function base64Url(value: string): string {
  return Buffer.from(value).toString('base64url');
}

function sign(value: string, secret: string): string {
  return createHmac('sha256', secret).update(value).digest('base64url');
}

export function signDeliveryReportImage(
  imageId: number,
  expiresAt: number,
  secret: string,
): string {
  return sign(`delivery-report-image:${imageId}:${expiresAt}`, secret);
}

export function signSharedDeliveryReportImage(
  imageId: number,
  storedFileName: string,
  secret: string,
): string {
  return sign(
    `delivery-report-shared-image:${imageId}:${storedFileName}`,
    secret,
  );
}

export function verifySharedDeliveryReportImage(
  imageId: number,
  storedFileName: string,
  signature: string,
  secret: string,
): boolean {
  if (
    !secret ||
    !Number.isSafeInteger(imageId) ||
    imageId <= 0 ||
    typeof storedFileName !== 'string' ||
    !storedFileName ||
    typeof signature !== 'string'
  ) {
    return false;
  }

  return safeEqual(
    signature,
    signSharedDeliveryReportImage(imageId, storedFileName, secret),
  );
}

export function verifyDeliveryReportImage(
  imageId: number,
  expires: string,
  signature: string,
  secret: string,
): boolean {
  if (
    !secret ||
    !Number.isSafeInteger(imageId) ||
    imageId <= 0 ||
    typeof expires !== 'string' ||
    !/^\d+$/.test(expires) ||
    typeof signature !== 'string'
  ) {
    return false;
  }

  const expiresAt = Number(expires);
  if (
    !Number.isSafeInteger(expiresAt) ||
    expiresAt <= Math.floor(Date.now() / 1000)
  ) {
    return false;
  }

  return safeEqual(
    signature,
    signDeliveryReportImage(imageId, expiresAt, secret),
  );
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return (
    leftBuffer.length === rightBuffer.length &&
    timingSafeEqual(leftBuffer, rightBuffer)
  );
}

export function getCookie(request: Request, name: string): string | null {
  const cookieHeader = request.headers.cookie;
  if (!cookieHeader) return null;

  for (const part of cookieHeader.split(';')) {
    const separator = part.indexOf('=');
    if (separator < 0) continue;
    const key = part.slice(0, separator).trim();
    if (key !== name) continue;
    return decodeURIComponent(part.slice(separator + 1).trim());
  }
  return null;
}

export function createDeliveryReportToken(secret: string): string {
  const now = Math.floor(Date.now() / 1000);
  const payload: AccessTokenPayload = {
    iat: now,
    exp: now + DELIVERY_REPORT_TOKEN_TTL_SECONDS,
  };
  const encodedPayload = base64Url(JSON.stringify(payload));
  return `${encodedPayload}.${sign(encodedPayload, secret)}`;
}

export function verifyDeliveryReportToken(
  token: string,
  secret: string,
): boolean {
  const separator = token.lastIndexOf('.');
  if (separator <= 0) return false;

  const encodedPayload = token.slice(0, separator);
  const signature = token.slice(separator + 1);
  if (!safeEqual(signature, sign(encodedPayload, secret))) return false;

  try {
    const payload = JSON.parse(
      Buffer.from(encodedPayload, 'base64url').toString('utf8'),
    ) as AccessTokenPayload;
    return (
      Number.isFinite(payload.exp) &&
      Number.isFinite(payload.iat) &&
      payload.exp > Math.floor(Date.now() / 1000)
    );
  } catch {
    return false;
  }
}

export function assertDeliveryReportPassword(
  password: string,
  configuredPassword: string,
): void {
  if (!configuredPassword) {
    throw new UnauthorizedException(
      'Delivery report password is not configured',
    );
  }
  if (!safeEqual(password, configuredPassword)) {
    throw new UnauthorizedException('Invalid password');
  }
}

@Injectable()
export class DeliveryReportAuthGuard implements CanActivate {
  constructor(private readonly config: ConfigService) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<Request>();
    const secret = this.config.get<string>('PACKING_FORM_TOKEN_SECRET') ?? '';
    const token = getCookie(request, DELIVERY_REPORT_COOKIE);

    if (!token || !secret || !verifyDeliveryReportToken(token, secret)) {
      throw new UnauthorizedException(
        'Delivery report session is missing or expired',
      );
    }
    return true;
  }
}
