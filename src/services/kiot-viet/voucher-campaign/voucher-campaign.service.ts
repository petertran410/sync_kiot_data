import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'voucher_campaign_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'code', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'isActive', type: 'boolean' },
  { name: 'startDate', type: 'timestamp' },
  { name: 'endDate', type: 'timestamp' },
  { name: 'prereqPrice', type: 'numeric' },
  { name: 'quantity', type: 'int' },
  { name: 'price', type: 'numeric' },
  { name: 'isGlobal', type: 'boolean' },
  { name: 'forAllCusGroup', type: 'boolean' },
  { name: 'forAllUser', type: 'boolean' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'name',
  'isActive',
  'startDate',
  'endDate',
  'prereqPrice',
  'quantity',
  'price',
  'isGlobal',
  'forAllCusGroup',
  'forAllUser',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietVoucherCampaign {
  private readonly logger = new Logger(KiotVietVoucherCampaign.name);

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

  async syncAllVoucherCampaigns(): Promise<void> {
    await this.syncFull();
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`VoucherCampaign sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['voucher_campaign']);
    try {
      const resp = await this.pageFetcher.fetchPage<any>('/vouchercampaign', {
        currentItem: 0,
        pageSize: 100,
      });
      const data = resp.data || [];
      const now = new Date();
      const rows = data.map((c: any) => ({
        kiotVietId: c.id,
        code: c.code,
        name: c.name,
        isActive: c.isActive ?? true,
        startDate: c.startDate ? new Date(c.startDate) : now,
        endDate: c.endDate ? new Date(c.endDate) : now,
        prereqPrice: c.prereqPrice ?? null,
        quantity: c.quantity ?? 0,
        price: c.price ?? 0,
        isGlobal: c.isGlobal ?? false,
        forAllCusGroup: c.forAllCusGroup ?? false,
        forAllUser: c.forAllUser ?? false,
        lastSyncedAt: now,
      }));
      const affected = await this.bulkUpsert.bulkUpsert({
        table: '"VoucherCampaign"',
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
        `voucher-campaign-${mode} completed: ${rows.length} (affected ${affected})`,
      );
      return { total: rows.length, processed: rows.length, affected };
    } catch (error) {
      this.logger.error(`voucher-campaign-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message);
      throw error;
    }
  }
}
