import {
  createDeliveryReportToken,
  DELIVERY_REPORT_TOKEN_TTL_SECONDS,
  verifyDeliveryReportToken,
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
});
