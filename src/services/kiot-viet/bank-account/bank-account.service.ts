import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'bankaccount_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'bankName', type: 'text' },
  { name: 'accountNumber', type: 'text' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
];

const UPDATE_COLUMNS = ['bankName', 'accountNumber', 'retailerId'];

@Injectable()
export class KiotVietBankAccountService {
  private readonly logger = new Logger(KiotVietBankAccountService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly removedIdsHandler: RemovedIdsHandler,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  async syncIncremental() {
    return this.runSync('incremental');
  }

  async syncBankAccounts(): Promise<void> {
    await this.syncFull();
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`BankAccount sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['bankaccount']);
    try {
      // Doc 2.9 documents `/BankAccounts`; `/accounts` returns 404.
      const resp = await this.pageFetcher.fetchPage<any>('/BankAccounts', {
        currentItem: 0,
        pageSize: 100,
        includeRemoveIds: true,
      });
      const data = resp.data || [];
      const rows = data.map((b: any) => ({
        kiotVietId: b.id,
        bankName: b.bankName,
        accountNumber: b.accountNumber || null,
        retailerId: b.retailerId ?? null,
        createdDate: b.createdDate ? new Date(b.createdDate) : new Date(),
      }));
      const affected = await this.bulkUpsert.bulkUpsert({
        table: '"BankAccount"',
        columns: COLUMNS,
        rows,
        conflictTarget: '"kiotVietId"',
        updateColumns: UPDATE_COLUMNS,
      });
      // KiotViet reports upstream deletions here; nothing read this before.
      const removed = this.removedIdsHandler.extract(resp);
      if (removed.length) {
        await this.removedIdsHandler.apply('bankAccount', removed);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: rows.length, expectedTotal: rows.length, affected },
        resp.timestamp,
      );
      this.logger.log(
        `bankaccount-${mode} completed: ${rows.length} (affected ${affected})`,
      );
      return { total: rows.length, processed: rows.length, affected };
    } catch (error) {
      this.logger.error(`bankaccount-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message);
      throw error;
    }
  }
}
