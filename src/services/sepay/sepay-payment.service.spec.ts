import { ConfigService } from '@nestjs/config';
import { SePayPaymentService } from './sepay-payment.service';

describe('SePayPaymentService account resolution', () => {
  function service(findFirst: jest.Mock) {
    const config = {
      get: (key: string) => {
        if (key === 'KIOT_BASE_URL') return 'https://public.kiotapi.com';
        if (key === 'SEPAY_PAYMENT_MODE') return 'dry-run';
        if (key === 'SEPAY_ACCOUNT_MAP') {
          return '{"QRPSEP1ZZZZ54400023":"1068602436"}';
        }
        return undefined;
      },
    } as unknown as ConfigService;

    return new SePayPaymentService(
      { bankAccount: { findFirst } } as any,
      {} as any,
      {} as any,
      {} as any,
      config,
    );
  }

  it('prefers a direct KiotViet accountNumber match', async () => {
    const findFirst = jest.fn().mockResolvedValue({ kiotVietId: 821595 });
    const paymentService = service(findFirst);

    await expect(
      (paymentService as any).resolveAccountId(
        '1068602436',
        'QRPSEP1ZZZZ54400023',
      ),
    ).resolves.toBe(821595);
    expect(findFirst).toHaveBeenCalledTimes(1);
    expect(findFirst).toHaveBeenCalledWith({
      where: { accountNumber: '1068602436', deletedAt: null },
      select: { kiotVietId: true },
    });
  });

  it('uses subAccount mapping only when accountNumber does not match', async () => {
    const findFirst = jest
      .fn()
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ kiotVietId: 821595 });
    const paymentService = service(findFirst);

    await expect(
      (paymentService as any).resolveAccountId(
        'UNKNOWN_ACCOUNT',
        'QRPSEP1ZZZZ54400023',
      ),
    ).resolves.toBe(821595);
    expect(findFirst).toHaveBeenNthCalledWith(1, {
      where: { accountNumber: 'UNKNOWN_ACCOUNT', deletedAt: null },
      select: { kiotVietId: true },
    });
    expect(findFirst).toHaveBeenNthCalledWith(2, {
      where: { accountNumber: '1068602436', deletedAt: null },
      select: { kiotVietId: true },
    });
  });
});
