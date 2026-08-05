import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'surcharge_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'code', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'valueRatio', type: 'numeric' },
  { name: 'value', type: 'numeric' },
  { name: 'retailerId', type: 'int' },
  { name: 'isActive', type: 'boolean' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'name',
  'valueRatio',
  'value',
  'retailerId',
  'isActive',
  'modifiedDate',
];

@Injectable()
export class KiotVietSurchargeService {
  private readonly logger = new Logger(KiotVietSurchargeService.name);

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

  async syncSurcharges(): Promise<void> {
    await this.syncFull();
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Surcharge sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['surcharge']);
    try {
      // `/surchages` — the misspelling is KiotViet's own and is the real path
      // (doc 2.10, "Cập nhật lại URL: https://public.kiotapi.com/surchages").
      // `/surcharges` returns 404.
      const resp = await this.pageFetcher.fetchPage<any>('/surchages', {
        currentItem: 0,
        pageSize: 100,
      });
      const data = resp.data || [];
      const rows = data.map((s: any) => ({
        kiotVietId: s.id,
        code: s.code || null,
        name: s.name,
        valueRatio: s.valueRatio ?? null,
        value: s.value != null ? parseFloat(s.value) : null,
        retailerId: s.retailerId ?? null,
        isActive: s.isActive !== undefined ? s.isActive : true,
        createdDate: s.createdDate ? new Date(s.createdDate) : new Date(),
        modifiedDate: s.modifiedDate ? new Date(s.modifiedDate) : new Date(),
      }));
      const affected = await this.bulkUpsert.bulkUpsert({
        table: '"Surcharge"',
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
        `surcharge-${mode} completed: ${rows.length} (affected ${affected})`,
      );
      return { total: rows.length, processed: rows.length, affected };
    } catch (error) {
      // KiotViet answers HTTP 420 with KvValidateSurchargeException ("Chưa bật thu
      // khác trong thiết lập cửa hàng") when the surcharge feature is switched off
      // for the shop. That is a store configuration state, not a failure: there is
      // simply nothing to sync, and retrying can never succeed. Treat it as an
      // empty, skipped run so it does not mark the whole sync job as failed.
      if (this.isFeatureDisabled(error)) {
        this.logger.warn(
          `surcharge-${mode} skipped: the surcharge feature ("thu khác") is disabled ` +
            `for this shop. Enable it in KiotViet store settings to sync surcharges.`,
        );
        await this.syncControl.markCompleted(SYNC_NAME, {
          processedCount: 0,
          expectedTotal: 0,
          skipped: 'feature_disabled',
        });
        return { total: 0, processed: 0, skipped: true };
      }

      this.logger.error(`surcharge-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message);
      throw error;
    }
  }

  /** True when the shop has the surcharge feature turned off. */
  private isFeatureDisabled(error: any): boolean {
    const status = error?.response?.status;
    const code = error?.response?.data?.responseStatus?.errorCode;
    return status === 420 || code === 'KvValidateSurchargeException';
  }
}
