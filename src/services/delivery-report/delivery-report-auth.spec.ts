import {
  createDeliveryReportToken,
  DELIVERY_REPORT_TOKEN_TTL_SECONDS,
  signDeliveryReportImage,
  signSharedDeliveryReportImage,
  verifyDeliveryReportImage,
  verifyDeliveryReportToken,
  verifySharedDeliveryReportImage,
} from './delivery-report-auth';

describe('delivery report auth', () => {
  it('creates a token that is valid for seven days', () => {
    const token = createDeliveryReportToken('test-secret');

    expect(verifyDeliveryReportToken(token, 'test-secret')).toBe(true);
    expect(DELIVERY_REPORT_TOKEN_TTL_SECONDS).toBe(7 * 24 * 60 * 60);
    expect(verifyDeliveryReportToken(token, 'wrong-secret')).toBe(false);
  });

  it('rejects malformed and expired tokens', () => {
    expect(verifyDeliveryReportToken('not-a-token', 'test-secret')).toBe(false);

    const now = Date.now;
    const baseTime = now();
    Date.now = () => baseTime;
    const token = createDeliveryReportToken('test-secret');
    Date.now = () => baseTime + 8 * 24 * 60 * 60 * 1000;
    try {
      expect(verifyDeliveryReportToken(token, 'test-secret')).toBe(false);
    } finally {
      Date.now = now;
    }
  });

  it('accepts only a signed, unexpired URL for the matching image', () => {
    const expiresAt = Math.floor(Date.now() / 1000) + 60;
    const signature = signDeliveryReportImage(42, expiresAt, 'test-secret');

    expect(
      verifyDeliveryReportImage(
        42,
        String(expiresAt),
        signature,
        'test-secret',
      ),
    ).toBe(true);
    expect(
      verifyDeliveryReportImage(
        43,
        String(expiresAt),
        signature,
        'test-secret',
      ),
    ).toBe(false);
    expect(
      verifyDeliveryReportImage(
        42,
        String(expiresAt),
        signature,
        'other-secret',
      ),
    ).toBe(false);
    expect(
      verifyDeliveryReportImage(42, String(expiresAt), signature, ''),
    ).toBe(false);
    expect(
      verifyDeliveryReportImage(42, 'not-a-date', signature, 'test-secret'),
    ).toBe(false);
    expect(
      verifyDeliveryReportImage(
        42,
        String(expiresAt),
        'tampered',
        'test-secret',
      ),
    ).toBe(false);

    const past = Math.floor(Date.now() / 1000) - 1;
    expect(
      verifyDeliveryReportImage(
        42,
        String(past),
        signDeliveryReportImage(42, past, 'test-secret'),
        'test-secret',
      ),
    ).toBe(false);
  });

  it('keeps a shared image link valid independently of the session lifetime', () => {
    const signature = signSharedDeliveryReportImage(
      42,
      'stored-image.webp',
      'test-secret',
    );
    const verify = () =>
      verifySharedDeliveryReportImage(
        42,
        'stored-image.webp',
        signature,
        'test-secret',
      );
    expect(verify()).toBe(true);
    expect(
      verifySharedDeliveryReportImage(
        43,
        'stored-image.webp',
        signature,
        'test-secret',
      ),
    ).toBe(false);
    expect(
      verifySharedDeliveryReportImage(
        42,
        'other.webp',
        signature,
        'test-secret',
      ),
    ).toBe(false);
    expect(
      verifySharedDeliveryReportImage(
        42,
        'stored-image.webp',
        signature,
        'other-secret',
      ),
    ).toBe(false);

    const now = Date.now;
    Date.now = () => now() + 365 * 24 * 60 * 60 * 1000;
    try {
      expect(verify()).toBe(true);
    } finally {
      Date.now = now;
    }
  });
});
