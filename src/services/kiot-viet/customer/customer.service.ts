import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';
import { PrismaService } from '../../../prisma/prisma.service';

const SYNC_NAME = 'customer_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'type', type: 'int' },
  { name: 'gender', type: 'boolean' },
  { name: 'birthDate', type: 'timestamp' },
  { name: 'contactNumber', type: 'text' },
  { name: 'subNumber', type: 'text' },
  { name: 'identificationNumber', type: 'text' },
  { name: 'address', type: 'text' },
  { name: 'locationName', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'email', type: 'text' },
  { name: 'organization', type: 'text' },
  { name: 'comments', type: 'text' },
  { name: 'taxCode', type: 'text' },
  { name: 'groups', type: 'text' },
  { name: 'debt', type: 'numeric' },
  { name: 'totalInvoiced', type: 'numeric' },
  { name: 'totalPoint', type: 'real' },
  { name: 'totalRevenue', type: 'numeric' },
  { name: 'rewardPoint', type: 'bigint' },
  { name: 'psidFacebook', type: 'bigint' },
  { name: 'retailerId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'name',
  'type',
  'gender',
  'birthDate',
  'contactNumber',
  'subNumber',
  'identificationNumber',
  'address',
  'locationName',
  'wardName',
  'email',
  'organization',
  'comments',
  'taxCode',
  'groups',
  'debt',
  'totalInvoiced',
  'totalPoint',
  'totalRevenue',
  'rewardPoint',
  'psidFacebook',
  'retailerId',
  'branchId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
     private readonly syncControl: SyncControlHelper,
     private readonly removedIdsHandler: RemovedIdsHandler,
     private readonly prisma: PrismaService,
  ) {}

  async syncFull() {
    return this.runSync('full', {});
  }

  async syncIncremental() {
    const last = await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const lastModifiedFrom =
      last ?? new Date(Date.now() - 31 * 24 * 3600 * 1000);
    return this.runSync('incremental', {
      lastModifiedFrom: lastModifiedFrom.toISOString(),
    });
  }

  async syncHistoricalCustomers(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Customer sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['customer']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/customers',
          baseParams: { includeRemoveIds: true,
            orderBy: 'id',
            orderDirection: 'DESC',
            includeTotal: true,
            includeCustomerGroup: true,
            includeCustomerSocial: true,
            ...extra,
          },
          label: `customer-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`customer-${mode}: processed ${processed} so far`);
          },
        });
      total = t;
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('customer', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`customer-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(customers: any[]): Promise<number> {
    if (!customers.length) return 0;
    const now = new Date();
    const branchIds = this.uniqueNum(customers.map((c) => c.branchId));
    const branchMap = await this.relationMap.buildIdMap('branch', branchIds);

    const rows = customers.map((c) => ({
      kiotVietId: c.id,
      code: c.code,
      name: c.name,
      type: c.type ?? null,
      gender: c.gender ?? null,
      birthDate: c.birthDate ? new Date(c.birthDate) : null,
      contactNumber: c.contactNumber ?? '',
      subNumber: c.subNumber ?? '',
      identificationNumber: c.identificationNumber ?? '',
      address: c.address ?? '',
      locationName: c.locationName ?? '',
      wardName: c.wardName ?? '',
      email: c.email ?? '',
      organization: c.organization ?? '',
      comments: c.comments ?? '',
      taxCode: c.taxCode ?? '',
      groups: c.groups || null,
      debt: c.debt ?? 0,
      totalInvoiced: c.totalInvoiced ?? 0,
      totalPoint: c.totalPoint ?? null,
      totalRevenue: c.totalRevenue ?? 0,
      rewardPoint: c.rewardPoint ? BigInt(c.rewardPoint) : 0,
      psidFacebook: c.psidFacebook ? BigInt(c.psidFacebook) : null,
      retailerId: c.retailerId ?? null,
      branchId: c.branchId ? (branchMap.get(Number(c.branchId)) ?? null) : null,
      createdDate: c.createdDate ? new Date(c.createdDate) : now,
      modifiedDate: c.modifiedDate ? new Date(c.modifiedDate) : now,
       lastSyncedAt: now,
     }));

     await this.bulkUpsert.bulkUpsert({
       table: '"Customer"',
       columns: COLUMNS,
       rows,
       conflictTarget: '"kiotVietId"',
       updateColumns: UPDATE_COLUMNS,
       compareColumns: UPDATE_COLUMNS.filter(
         (column) => column !== 'lastSyncedAt',
       ),
     });

     // Only Customer records with a usable phone number are eligible for Lark.
     // Existing Lark records are still handled separately if KiotViet deletes them.
     const larkEligibleIds = customers
       .filter((customer) => this.hasContactNumber(customer.contactNumber))
       .map((customer) => BigInt(customer.id));
     if (larkEligibleIds.length > 0) {
       await this.prisma.customer.updateMany({
         where: { kiotVietId: { in: larkEligibleIds } },
         data: { larkSyncStatus: 'PENDING' },
       });
     }

     return customers.length;
  }

  private hasContactNumber(value: unknown): boolean {
    return typeof value === 'string' && value.trim().length > 0;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
