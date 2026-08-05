import { SePayIngestService } from './sepay-ingest.service';

describe('SePayIngestService', () => {
  const payload = {
    id: 92704,
    gateway: 'Vietcombank',
    transactionDate: '2026-08-05 10:00:00',
    accountNumber: '1068602436',
    subAccount: 'QRPSEP1ZZZZ54400023',
    content: 'Thanh toan HD000095',
    transferType: 'in',
    transferAmount: 500,
    accumulated: 100000,
    referenceCode: 'FT123',
  };

  it('stores the raw transaction with its stable SePay id', async () => {
    const create = jest.fn().mockResolvedValue({ id: 1 });
    const prisma = {
      ensureConnected: jest.fn(),
      sePayTransaction: { create },
    } as any;
    const service = new SePayIngestService(prisma);

    await expect(service.ingest(payload)).resolves.toEqual({
      duplicate: false,
    });
    expect(create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        sepayTransactionId: '92704',
        accountNumber: '1068602436',
        subAccount: 'QRPSEP1ZZZZ54400023',
        content: 'Thanh toan HD000095',
        rawPayload: payload,
        status: 'RECEIVED',
      }),
    });
  });

  it('treats a database unique collision as a successful replay', async () => {
    const updateMany = jest.fn().mockResolvedValue({ count: 0 });
    const prisma = {
      ensureConnected: jest.fn(),
      sePayTransaction: {
        create: jest.fn().mockRejectedValue({ code: 'P2002' }),
        updateMany,
      },
    } as any;
    const service = new SePayIngestService(prisma);

    await expect(service.ingest(payload)).resolves.toEqual({ duplicate: true });
    expect(updateMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { sepayTransactionId: '92704', status: 'IMPORTED' },
        data: expect.objectContaining({
          status: 'RECEIVED',
          processedAt: null,
        }),
      }),
    );
  });
});
