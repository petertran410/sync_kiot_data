import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'supplier_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'contactNumber', type: 'text' },
  { name: 'email', type: 'text' },
  { name: 'address', type: 'text' },
  { name: 'locationName', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'organization', type: 'text' },
  { name: 'taxCode', type: 'text' },
  { name: 'comments', type: 'text' },
  { name: 'groups', type: 'text' },
  { name: 'isActive', type: 'boolean' },
  { name: 'debt', type: 'numeric' },
  { name: 'totalInvoiced', type: 'numeric' },
  { name: 'totalInvoicedWithoutReturn', type: 'numeric' },
  { name: 'retailerId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'name',
  'contactNumber',
  'email',
  'address',
  'locationName',
  'wardName',
  'organization',
  'taxCode',
  'comments',
  'groups',
  'isActive',
  'debt',
  'totalInvoiced',
  'totalInvoicedWithoutReturn',
  'retailerId',
  'branchId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietSupplierService {
  private readonly logger = new Logger(KiotVietSupplierService.name);

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

  async syncHistoricalSuppliers(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Supplier sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['supplier']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/suppliers',
          baseParams: { includeRemoveIds: true,
            includeTotal: true,
            includeSupplierGroup: true,
            ...extra,
          },
          label: `supplier-${mode}`,
          onPage: async (pageData) => {
            const now = new Date();
            const rows = pageData.map((s: any) => ({
              kiotVietId: s.id,
              code: (s.code || '').trim(),
              name: (s.name || '').trim(),
              contactNumber: s.contactNumber || null,
              email: s.email || null,
              address: s.address || null,
              locationName: s.locationName || null,
              wardName: s.wardName || null,
              organization: s.organization || null,
              taxCode: s.taxCode || null,
              comments: s.comments || null,
              groups: s.groups || null,
              isActive: s.isActive ?? true,
              debt: s.debt ?? null,
              totalInvoiced: s.totalInvoiced ?? null,
              totalInvoicedWithoutReturn: s.totalInvoicedWithoutReturn ?? null,
              retailerId: s.retailerId ?? null,
              branchId: s.branchId ?? null,
              createdDate: s.createdDate ? new Date(s.createdDate) : now,
              modifiedDate: s.modifiedDate ? new Date(s.modifiedDate) : now,
              lastSyncedAt: now,
            }));
            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"Supplier"',
              columns: COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `supplier-${mode}: saved ${rows.length} (affected ${affected}), total ${processed}`,
            );
          },
        });
      total = t;
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('supplier', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`supplier-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }
}
