import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';
import { RetailerContext } from '../shared/retailer-context';
import { mapWithConcurrency } from '../shared/concurrency.util';

const SYNC_NAME = 'pricebook_historical';

const PB_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'isActive', type: 'boolean' },
  { name: 'isGlobal', type: 'boolean' },
  { name: 'startDate', type: 'timestamp' },
  { name: 'endDate', type: 'timestamp' },
  { name: 'forAllCusGroup', type: 'boolean' },
  { name: 'forAllUser', type: 'boolean' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const PB_UPDATE = [
  'name',
  'isActive',
  'isGlobal',
  'startDate',
  'endDate',
  'forAllCusGroup',
  'forAllUser',
  'retailerId',
  'modifiedDate',
  'lastSyncedAt',
];

const PBB_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'priceBookId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'branchName', type: 'text' },
  { name: 'retailerId', type: 'int' },
  { name: 'lineNumber', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];
const PBB_UPDATE = [
  'kiotVietId',
  'branchId',
  'branchName',
  'retailerId',
  'lastSyncedAt',
];

const PBCG_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'priceBookId', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'lineNumber', type: 'int' },
  { name: 'customerGroupId', type: 'int' },
  { name: 'customerGroupName', type: 'text' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];
const PBCG_UPDATE = [
  'kiotVietId',
  'customerGroupId',
  'customerGroupName',
  'retailerId',
  'lastSyncedAt',
];

const PBU_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'priceBookId', type: 'int' },
  { name: 'userId', type: 'bigint' },
  { name: 'userName', type: 'text' },
  { name: 'lineNumber', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];
const PBU_UPDATE = ['kiotVietId', 'userId', 'userName', 'lastSyncedAt'];

@Injectable()
export class KiotVietPriceBookService {
  private readonly logger = new Logger(KiotVietPriceBookService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly removedIdsHandler: RemovedIdsHandler,
    private readonly retailer: RetailerContext,
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

  async syncHistoricalPriceBooks(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`PriceBook sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['pricebook']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/pricebooks',
          baseParams: { includeRemoveIds: true,
            includePriceBookBranch: true,
            includePriceBookCustomerGroups: true,
            includePriceBookUsers: true,
            ...extra,
          },
          label: `pricebook-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`pricebook-${mode}: processed ${processed} so far`);
          },
        });
      total = t;
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('priceBook', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`pricebook-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(pricebooks: any[]): Promise<number> {
    if (!pricebooks.length) return 0;
    const now = new Date();

    // Enrich each pricebook with full detail (branches/groups/users) in parallel.
    // The detail endpoint may return the pricebook directly OR wrapped in {data: {...}}.
    // Always fall back to the list item (which always has id/name) if extraction fails.
    const enriched = await mapWithConcurrency(
      pricebooks,
      5,
      async (pb: any) => {
        try {
          const resp: any = await this.pageFetcher.fetchPage<any>(
            `/pricebooks/${pb.id}`,
            {
              currentItem: 0,
              pageSize: 100,
              includePriceBookBranch: true,
              includePriceBookCustomerGroups: true,
              includePriceBookUsers: true,
            } as any,
            { label: `pricebook-detail-${pb.id}` },
          );
          const detail: any =
            resp &&
            typeof resp === 'object' &&
            'data' in resp &&
            typeof resp.data === 'object' &&
            resp.data !== null &&
            !Array.isArray(resp.data)
              ? resp.data
              : resp;
          return detail && detail.id != null ? { ...pb, ...detail } : pb;
        } catch (e) {
          this.logger.warn(`Failed to enrich pricebook ${pb.id}: ${e.message}`);
          return pb;
        }
      },
    );

    const branchIds = this.uniqueNum(
      enriched.flatMap((p: any) =>
        (p.priceBookBranches ?? []).map((b: any) => b.branchId),
      ),
    );
    const groupIds = this.uniqueNum(
      enriched.flatMap((p: any) =>
        (p.priceBookCustomerGroups ?? []).map((g: any) => g.customerGroupId),
      ),
    );
    const [branchMap, groupMap] = await Promise.all([
      this.relationMap.buildIdMap('branch', branchIds),
      this.relationMap.buildIdMap('customerGroup', groupIds),
    ]);

    const rows = enriched.map((p: any) => ({
      kiotVietId: p.id,
      name: p.name,
      isActive: p.isActive ?? true,
      isGlobal: p.isGlobal ?? false,
      startDate: p.startDate ? new Date(p.startDate) : null,
      endDate: p.endDate ? new Date(p.endDate) : null,
      forAllCusGroup: p.forAllCusGroup ?? false,
      forAllUser: p.forAllUser ?? false,
      retailerId: this.retailer.resolve(p.retailerId),
      createdDate: p.createdDate ? new Date(p.createdDate) : now,
      modifiedDate: p.modifiedDate ? new Date(p.modifiedDate) : now,
      lastSyncedAt: now,
    }));

    await this.bulkUpsert.bulkUpsert({
      table: '"PriceBook"',
      columns: PB_COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: PB_UPDATE,
    });

    const pbIdMap = await this.relationMap.buildIdMap(
      'priceBook',
      this.uniqueNum(enriched.map((p: any) => p.id)),
    );

    const pbbRows: any[] = [];
    const pbcgRows: any[] = [];
    const pbuRows: any[] = [];

    for (const p of enriched) {
      const pbDbId = pbIdMap.get(Number(p.id));
      if (!pbDbId) continue;

      (p.priceBookBranches ?? []).forEach((b: any, idx: number) => {
        pbbRows.push({
          kiotVietId: b.id ? BigInt(b.id) : null,
          priceBookId: pbDbId,
          branchId: b.branchId
            ? (branchMap.get(Number(b.branchId)) ?? null)
            : null,
          branchName: b.branchName || null,
          retailerId: b.retailerId ?? null,
          lineNumber: idx + 1,
          lastSyncedAt: now,
        });
      });

      (p.priceBookCustomerGroups ?? []).forEach((g: any, idx: number) => {
        pbcgRows.push({
          kiotVietId: g.id ? BigInt(g.id) : null,
          priceBookId: pbDbId,
          retailerId: g.retailerId ?? null,
          lineNumber: idx + 1,
          customerGroupId: g.customerGroupId
            ? (groupMap.get(Number(g.customerGroupId)) ?? null)
            : null,
          customerGroupName: g.customerGroupName || null,
          lastSyncedAt: now,
        });
      });

      (p.priceBookUsers ?? []).forEach((u: any, idx: number) => {
        pbuRows.push({
          kiotVietId: u.id ? BigInt(u.id) : null,
          priceBookId: pbDbId,
          userId: u.userId ? BigInt(u.userId) : null,
          userName: u.userName || null,
          lineNumber: idx + 1,
          lastSyncedAt: now,
        });
      });
    }

    // Archive mode: existing child rows are retained even when KiotViet omits them.
    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"PriceBookBranch"',
        columns: PBB_COLUMNS,
        rows: pbbRows,
        conflictTarget: '("priceBookId", "lineNumber")',
        updateColumns: PBB_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"PriceBookCustomerGroup"',
        columns: PBCG_COLUMNS,
        rows: pbcgRows,
        conflictTarget: '("priceBookId", "lineNumber")',
        updateColumns: PBCG_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"PriceBookUser"',
        columns: PBU_COLUMNS,
        rows: pbuRows,
        conflictTarget: '("priceBookId", "lineNumber")',
        updateColumns: PBU_UPDATE,
        skipUnchanged: false,
      }),
    ]);

    return enriched.length;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
