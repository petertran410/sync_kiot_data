import { SePayWorkerService } from './sepay-worker.service';

describe('SePayWorkerService document code resolution', () => {
  function service(rows: Array<{ code: string }>) {
    return new SePayWorkerService(
      { $queryRaw: jest.fn().mockResolvedValue(rows) } as any,
      {} as any,
      { get: () => false } as any,
    );
  }

  it('maps a bank-normalized invoice code to the dotted KiotViet code', async () => {
    const worker = service([{ code: 'HD000095.01' }]);

    await expect(
      (worker as any).resolveDocumentCode('INVOICE', 'HD00009501'),
    ).resolves.toBe('HD000095.01');
  });

  it('keeps the parsed code when no local normalized match exists', async () => {
    const worker = service([]);

    await expect(
      (worker as any).resolveDocumentCode('ORDER', 'DH000087'),
    ).resolves.toBe('DH000087');
  });

  it('rejects ambiguous normalized matches', async () => {
    const worker = service([{ code: 'HD000095.01' }, { code: 'HD000095-01' }]);

    await expect(
      (worker as any).resolveDocumentCode('INVOICE', 'HD00009501'),
    ).rejects.toThrow(
      'Ambiguous invoice code HD00009501: HD000095.01, HD000095-01',
    );
  });
});
