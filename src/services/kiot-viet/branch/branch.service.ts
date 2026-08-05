import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'branch_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'code', type: 'text' },
  { name: 'contactNumber', type: 'text' },
  { name: 'subContactNumber', type: 'text' },
  { name: 'email', type: 'text' },
  { name: 'address', type: 'text' },
  { name: 'location', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'isActive', type: 'boolean' },
  { name: 'isLock', type: 'boolean' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'name',
  'code',
  'contactNumber',
  'subContactNumber',
  'email',
  'address',
  'location',
  'wardName',
  'isActive',
  'isLock',
  'retailerId',
  'modifiedDate',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietBranchService {
  private readonly logger = new Logger(KiotVietBranchService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly removedIdsHandler: RemovedIdsHandler,
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

  async syncHistoricalBranches(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Branch sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['branch']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/branches',
          baseParams: { includeRemoveIds: true, ...extra },
          label: `branch-${mode}`,
          onPage: async (pageData) => {
            const now = new Date();
            const rows = pageData.map((b: any) => ({
              kiotVietId: b.id,
              name: b.branchName,
              code: b.branchCode || null,
              contactNumber: b.contactNumber || null,
              subContactNumber: b.subContactNumber || null,
              email: b.email || null,
              address: b.address || null,
              location: b.location || null,
              wardName: b.wardName || null,
              isActive: b.isActive ?? true,
              isLock: b.isLock ?? false,
              retailerId: b.retailerId ?? null,
              createdDate: b.createdDate ? new Date(b.createdDate) : now,
              modifiedDate: b.modifiedDate ? new Date(b.modifiedDate) : now,
              lastSyncedAt: now,
            }));
            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"Branch"',
              columns: COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `branch-${mode}: saved ${rows.length} (affected ${affected}), total ${processed}`,
            );
          },
        });
      total = t;
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('branch', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`branch-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }
}
