import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'trademark_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = ['name', 'retailerId', 'modifiedDate', 'lastSyncedAt'];

@Injectable()
export class KiotVietTradeMarkService {
  private readonly logger = new Logger(KiotVietTradeMarkService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  async syncFull() {
    return this.runSync('full', {});
  }

  async syncIncremental() {
    const last = await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const lastModifiedFrom = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      lastModifiedFrom: lastModifiedFrom.toISOString(),
    });
  }

  async syncHistoricalTradeMarks(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Trademark sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['trademark']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/trademark',
          baseParams: extra,
          label: `trademark-${mode}`,
          onPage: async (pageData) => {
            const now = new Date();
            const rows = pageData.map((tm: any) => ({
              kiotVietId: tm.tradeMarkId ?? tm.id,
              name: tm.tradeMarkName ?? tm.name,
              retailerId: tm.retailerId ?? null,
              createdDate: tm.createdDate ? new Date(tm.createdDate) : now,
              modifiedDate: tm.modifiedDate ? new Date(tm.modifiedDate) : now,
              lastSyncedAt: now,
            }));
            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"TradeMark"',
              columns: COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `trademark-${mode}: saved ${rows.length} (affected ${affected}), total ${processed}`,
            );
          },
        });
      total = t;
      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`trademark-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }
}
