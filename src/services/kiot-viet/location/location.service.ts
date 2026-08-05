import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'location_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'normalName', type: 'text' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = ['name', 'normalName', 'lastSyncedAt'];

/**
 * Sync `GET /locations` (doc 2.21) into the `Location` table.
 *
 * The `Location` model existed with no service to populate it, so `Customer.locationName`
 * and `OrderDelivery.locationName` were free-text with nothing to join against.
 *
 * Note: doc 2.21 states "Request: Không có tham số" — the endpoint accepts no
 * `lastModifiedFrom`, so an incremental run is necessarily identical to a full one.
 * That is acceptable: this is a small, near-static reference list.
 */
@Injectable()
export class KiotVietLocationService {
  private readonly logger = new Logger(KiotVietLocationService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  /** Identical to a full run — the endpoint exposes no date filter. */
  async syncIncremental() {
    return this.runSync('incremental');
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn('Location sync already running, skipping');
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['location']);
    let processed = 0;
    let total = 0;

    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/locations',
          baseParams: {},
          label: `location-${mode}`,
          onPage: async (pageData) => {
            const now = new Date();
            const rows = pageData
              .map((loc: any) => ({
                kiotVietId: loc.id ?? loc.locationId,
                name: loc.name ?? null,
                normalName: loc.normalName ?? null,
                lastSyncedAt: now,
              }))
              .filter((r) => r.kiotVietId != null && r.name != null);

            if (rows.length === 0) return;

            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"Location"',
              columns: COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `location-${mode}: saved ${rows.length} (affected ${affected}), total ${processed}`,
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
      this.logger.error(`location-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }
}
