import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RetailerContext } from '../shared/retailer-context';

const SYNC_NAME = 'cashflow_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'userId', type: 'int' },
  { name: 'address', type: 'text' },
  { name: 'locationName', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'contactNumber', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'createdBy', type: 'bigint' },
  { name: 'usedForFinancialReporting', type: 'int' },
  { name: 'branchName', type: 'text' },
  { name: 'partnerName', type: 'text' },
  { name: 'userName', type: 'text' },
  { name: 'accountId', type: 'int' },
  { name: 'origin', type: 'text' },
  { name: 'cashFlowGroupId', type: 'int' },
  { name: 'cashGroup', type: 'text' },
  { name: 'statusValue', type: 'text' },
  { name: 'method', type: 'text' },
  { name: 'partnerType', type: 'text' },
  { name: 'partnerId', type: 'bigint' },
  { name: 'branchId', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'transDate', type: 'timestamp' },
  { name: 'amount', type: 'numeric' },
  { name: 'description', type: 'text' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'userId',
  'address',
  'locationName',
  'wardName',
  'contactNumber',
  'status',
  'createdBy',
  'usedForFinancialReporting',
  'branchName',
  'partnerName',
  'userName',
  'accountId',
  'origin',
  'cashFlowGroupId',
  'cashGroup',
  'statusValue',
  'method',
  'partnerType',
  'partnerId',
  'branchId',
  'retailerId',
  'transDate',
  'amount',
  'description',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietCashflowService {
  private readonly logger = new Logger(KiotVietCashflowService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly retailer: RetailerContext,
  ) {}

  async syncFull() {
    return this.runSync('full', {});
  }

  async syncIncremental() {
    const last = await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const startDate = last ?? new Date(Date.now() - 2 * 24 * 3600 * 1000);
    return this.runSync('incremental', {
      startDate: startDate.toISOString().slice(0, 10),
    });
  }

  async syncHistoricalCashflows(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Cashflow sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['cashflow']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/cashflow',
          baseParams: {
            includeAccount: true,
            includeBranch: true,
            includeUser: true,
            ...extra,
          },
          label: `cashflow-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`cashflow-${mode}: processed ${processed} so far`);
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
      this.logger.error(`cashflow-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(cashflows: any[]): Promise<number> {
    if (!cashflows.length) return 0;
    const now = new Date();
    const branchIds = this.uniqueNum(cashflows.map((c) => c.branchId));
    const branchMap = await this.relationMap.buildIdMap('branch', branchIds);

    const rows = cashflows.map((c) => {
      const branch = c.branchId ? branchMap.get(Number(c.branchId)) : undefined;
      return {
        kiotVietId: c.id,
        code: (c.code || '').trim(),
        userId: c.userId ?? null,
        address: c.address ?? '',
        locationName: c.locationName ?? '',
        wardName: c.wardName ?? '',
        contactNumber: c.contactNumber ?? '',
        status: c.status ?? null,
        createdBy: c.createdBy ?? null,
        usedForFinancialReporting: c.usedForFinancialReporting ?? null,
        branchName: branch ? null : null, // branch.name not fetched in map; set below
        partnerName: c.partnerName ?? '',
        userName: c.user ?? '',
        accountId: c.accountId ?? null,
        origin: c.origin ?? '',
        cashFlowGroupId: c.cashFlowGroupId ?? null,
        cashGroup: c.cashGroup ?? '',
        statusValue: c.statusValue ?? '',
        method: c.method ?? '',
        partnerType: c.partnerType ?? '',
        partnerId: c.partnerId ?? null,
        branchId: branch ?? null,
        retailerId: this.retailer.resolve(c.retailerId),
        transDate: c.transDate ? new Date(c.transDate) : now,
        amount: c.amount ?? 0,
        description: c.description ?? '',
        lastSyncedAt: now,
      };
    });

    await this.bulkUpsert.bulkUpsert({
      table: '"Cashflow"',
      columns: COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: UPDATE_COLUMNS,
    });

    return cashflows.length;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
