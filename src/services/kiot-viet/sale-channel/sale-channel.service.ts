import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'salechannel_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'isActive', type: 'boolean' },
  { name: 'position', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
];

const UPDATE_COLUMNS = ['name', 'isActive', 'position', 'retailerId'];

@Injectable()
export class KiotVietSaleChannelService {
  private readonly logger = new Logger(KiotVietSaleChannelService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  async syncIncremental() {
    return this.runSync('incremental');
  }

  /** Backward-compat alias. */
  async syncSaleChannels(): Promise<void> {
    await this.syncFull();
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`SaleChannel sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['salechannel']);
    try {
      // Singular, per doc 2.18 and confirmed against the live API — `/salechannels` is a 404.
      const resp = await this.pageFetcher.fetchPage<any>('/salechannel', {
        currentItem: 0,
        pageSize: 100,
      });
      const data = resp.data || [];
      const rows = data.map((s: any) => ({
        kiotVietId: s.id,
        name: s.name,
        isActive: s.isActivate !== undefined ? s.isActivate : true,
        position: s.position || 0,
        retailerId: s.retailerId ?? null,
        createdDate: s.createdDate ? new Date(s.createdDate) : new Date(),
      }));
      const affected = await this.bulkUpsert.bulkUpsert({
        table: '"SaleChannel"',
        columns: COLUMNS,
        rows,
        conflictTarget: '"kiotVietId"',
        updateColumns: UPDATE_COLUMNS,
      });
      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: rows.length, expectedTotal: rows.length, affected },
        resp.timestamp,
      );
      this.logger.log(
        `salechannel-${mode} completed: ${rows.length} (affected ${affected})`,
      );
      return { total: rows.length, processed: rows.length, affected };
    } catch (error) {
      this.logger.error(`salechannel-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message);
      throw error;
    }
  }
}
