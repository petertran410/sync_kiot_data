import { extractDocumentCode } from './sepay.types';

describe('extractDocumentCode', () => {
  it('extracts and normalizes an order code', () => {
    expect(extractDocumentCode('Thanh toán dh000087')).toEqual({
      type: 'ORDER',
      code: 'DH000087',
    });
  });

  it('extracts an invoice code', () => {
    expect(extractDocumentCode('Thanh toan HD000095')).toEqual({
      type: 'INVOICE',
      code: 'HD000095',
    });
  });

  it('returns null for unrelated transfer content', () => {
    expect(extractDocumentCode('Chuyen tien ca phe')).toBeNull();
  });

  it('rejects ambiguous content', () => {
    expect(() =>
      extractDocumentCode('Thanh toan DH000087 va HD000095'),
    ).toThrow('Multiple document codes found');
  });
});
