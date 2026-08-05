import { ConfigService } from '@nestjs/config';
import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { createHmac } from 'crypto';
import { SePaySignatureGuard } from './sepay-signature.guard';

describe('SePaySignatureGuard', () => {
  const secret = 'test-secret';
  const rawBody = Buffer.from('{"id":92704,"transferAmount":500}');

  function context(headers: Record<string, string>): ExecutionContext {
    const request = {
      rawBody,
      header: (name: string) => headers[name.toLowerCase()],
    };
    return {
      switchToHttp: () => ({ getRequest: () => request }),
    } as ExecutionContext;
  }

  it('accepts a valid timestamped raw-body signature', () => {
    const timestamp = String(Math.floor(Date.now() / 1000));
    const signature =
      'sha256=' +
      createHmac('sha256', secret)
        .update(`${timestamp}.`)
        .update(rawBody)
        .digest('hex');
    const guard = new SePaySignatureGuard({
      get: () => secret,
    } as unknown as ConfigService);

    expect(
      guard.canActivate(
        context({
          'x-sepay-signature': signature,
          'x-sepay-timestamp': timestamp,
        }),
      ),
    ).toBe(true);
  });

  it('rejects an expired request', () => {
    const timestamp = String(Math.floor(Date.now() / 1000) - 301);
    const guard = new SePaySignatureGuard({
      get: () => secret,
    } as unknown as ConfigService);

    expect(() =>
      guard.canActivate(
        context({
          'x-sepay-signature': 'sha256=invalid',
          'x-sepay-timestamp': timestamp,
        }),
      ),
    ).toThrow(UnauthorizedException);
  });

  it('fails closed before the secret is configured', () => {
    const guard = new SePaySignatureGuard({
      get: () => '',
    } as unknown as ConfigService);

    expect(() => guard.canActivate(context({}))).toThrow(
      'SePay webhook secret is not configured',
    );
  });
});
