export const SEPAY_STATUS = {
  Received: 'RECEIVED',
  Processing: 'PROCESSING',
  Processed: 'PROCESSED',
  DryRun: 'DRY_RUN',
  Ignored: 'IGNORED',
  Failed: 'FAILED',
} as const;

export interface SePayWebhookPayload {
  id: string | number;
  gateway?: string;
  transactionDate: string;
  accountNumber: string;
  subAccount?: string;
  code?: string | null;
  content: string;
  transferType: string;
  description?: string;
  transferAmount: number;
  accumulated?: number;
  referenceCode?: string;
}

export interface ParsedDocumentCode {
  type: 'ORDER' | 'INVOICE';
  code: string;
}

export function extractDocumentCode(
  content: string,
): ParsedDocumentCode | null {
  const matches = Array.from(content.matchAll(/\b(?:DH|HD)\d+\b/gi)).map(
    (match) => match[0].toUpperCase(),
  );
  const unique = Array.from(new Set(matches));
  if (unique.length === 0) return null;
  if (unique.length > 1) {
    throw new Error(`Multiple document codes found: ${unique.join(', ')}`);
  }
  return {
    type: unique[0].startsWith('DH') ? 'ORDER' : 'INVOICE',
    code: unique[0],
  };
}
