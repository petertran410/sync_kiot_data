import { ConfigService } from '@nestjs/config';
import { SePaySyncService } from './sepay-sync.service';

describe('SePaySyncService', () => {
  function service(createMany = jest.fn()) {
    const config = {
      get: (key: string) => {
        if (key === 'SEPAY_API_BASE_URL') return 'https://my.sepay.vn/userapi';
        if (key === 'SEPAY_API_TOKEN') return 'test-token';
        if (key === 'SEPAY_SYNC_FROM_DATE') return '2020-01-01 00:00:00';
        return undefined;
      },
    } as unknown as ConfigService;
    return new SePaySyncService(
      { sePayTransaction: { createMany } } as any,
      {} as any,
      config,
    );
  }

  it('normalizes an incoming v1 transaction as historical IMPORTED data', () => {
    const syncService = service();
    const row = (syncService as any).toRow({
      id: '49682',
      bank_brand_name: 'Vietcombank',
      account_number: '1068602436',
      sub_account: 'QRPSEP1ZZZZ54400023',
      transaction_date: '2026-08-06 08:00:00',
      amount_in: '500.00',
      amount_out: '0.00',
      accumulated: '1000.00',
      code: null,
      transaction_content: 'Thanh toan HD000095',
      reference_number: 'FT123',
      bank_account_id: '19',
    });

    expect(row).toEqual(
      expect.objectContaining({
        sepayTransactionId: '49682',
        accountNumber: '1068602436',
        subAccount: 'QRPSEP1ZZZZ54400023',
        transferType: 'in',
        content: 'Thanh toan HD000095',
        status: 'IMPORTED',
      }),
    );
    expect(row.transferAmount.toString()).toBe('500');
  });

  it('normalizes an outgoing v1 transaction using amount_out', () => {
    const syncService = service();
    const row = (syncService as any).toRow({
      id: '49683',
      account_number: '1068602436',
      transaction_date: '2026-08-06 09:00:00',
      amount_in: '0.00',
      amount_out: '250.00',
      transaction_content: 'Phi ngan hang',
    });

    expect(row.transferType).toBe('out');
    expect(row.transferAmount.toString()).toBe('250');
  });

  it('bulk inserts with database-level duplicate protection', async () => {
    const createMany = jest.fn().mockResolvedValue({ count: 1 });
    const syncService = service(createMany);
    const transaction = {
      id: '49682',
      account_number: '1068602436',
      transaction_date: '2026-08-06 08:00:00',
      amount_in: '500.00',
      amount_out: '0.00',
      transaction_content: 'Thanh toan HD000095',
    };

    await expect((syncService as any).insert([transaction])).resolves.toBe(1);
    expect(createMany).toHaveBeenCalledWith({
      data: [expect.objectContaining({ sepayTransactionId: '49682' })],
      skipDuplicates: true,
    });
  });
});
