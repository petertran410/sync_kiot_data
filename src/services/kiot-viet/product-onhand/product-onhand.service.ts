import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'product_onhand_historical';

/**
 * Inventory sync via KiotViet `GET /productOnHands` — returns per-branch stock
 * for all products. Much lighter than full product sync.
 *
 * ProductInventory is keyed by (productId, lineNumber). Because the existing schema has no
 * unique constraint on (productId, branchId), `lineNumber` stores the local Branch.id
 * so it remains readable and syncs can still upsert one row per product/branch.
 *
 * On conflict we only refresh stock fields (onHand, reserved, modifiedDate) so we
 * don't clobber richer data (cost, minQuantity, ...) written by the product sync.
 */
const INVENTORY_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'onHand', type: 'int' },
  { name: 'reserved', type: 'int' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'lineNumber', type: 'int' },
  { name: 'branchName', type: 'text' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'actualReserved', type: 'int' },
  { name: 'isActive', type: 'boolean' },
];

// /productOnHands only returns stock quantities. Enrich display fields from local Product/Branch,
// but preserve richer inventory fields (cost/minQuantity/maxQuantity/onOrder) written by product sync.
const INVENTORY_UPDATE = [
  'branchId',
  'onHand',
  'reserved',
  'modifiedDate',
  'lastSyncedAt',
  'branchName',
  'productCode',
  'productName',
  'actualReserved',
  'isActive',
];

@Injectable()
export class KiotVietProductOnHandService {
  private readonly logger = new Logger(KiotVietProductOnHandService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  /** Full inventory sync across all branches. */
  async syncFull(
    branchIds?: number[],
  ): Promise<{ total: number; processed: number }> {
    return this.runSync('full', {}, branchIds);
  }

  /** Incremental: only stock changed since last successful sync. */
  async syncIncremental(
    branchIds?: number[],
  ): Promise<{ total: number; processed: number }> {
    const last = await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const lastModifiedFrom = last ?? new Date(Date.now() - 24 * 3600 * 1000);
    return this.runSync(
      'incremental',
      { lastModifiedFrom: lastModifiedFrom.toISOString() },
      branchIds,
    );
  }

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
    branchIds?: number[],
  ): Promise<{ total: number; processed: number }> {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`ProductOnHand sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['product_onhand']);
    let processed = 0;
    let total = 0;
    try {
      const baseParams: Record<string, any> = { ...extra };
      if (branchIds && branchIds.length) baseParams.branchIds = branchIds;

      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/productOnHands',
          baseParams,
          label: `product-onhand-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(
              `product-onhand-${mode}: processed ${processed} products so far`,
            );
          },
        });
      total = t;
      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      this.logger.log(
        `product-onhand-${mode} completed: ${processed}/${total} products`,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`product-onhand-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(products: any[]): Promise<number> {
    if (!products.length) return 0;
    const now = new Date();

    const productKvIds = this.uniqueNum(products.map((p) => p.id));
    const branchKvIds = this.uniqueNum(
      products.flatMap((p) => (p.inventories ?? []).map((inv) => inv.branchId)),
    );

    const [productMap, branchMap] = await Promise.all([
      this.relationMap.buildIdMap('product', productKvIds),
      this.relationMap.buildIdMap('branch', branchKvIds),
    ]);

    const productDbIds = Array.from(new Set(Array.from(productMap.values())));
    const branchDbIds = Array.from(new Set(Array.from(branchMap.values())));

    const [productInfos, branchInfos] = await Promise.all([
      productDbIds.length
        ? this.prisma.product.findMany({
            where: { id: { in: productDbIds } },
            select: { id: true, code: true, name: true, isActive: true },
          })
        : Promise.resolve([] as any[]),
      branchDbIds.length
        ? this.prisma.branch.findMany({
            where: { id: { in: branchDbIds } },
            select: { id: true, name: true },
          })
        : Promise.resolve([] as any[]),
    ]);

    const productInfoById = new Map<number, any>(
      productInfos.map((p: any) => [p.id, p] as const),
    );
    const branchInfoById = new Map<number, any>(
      branchInfos.map((b: any) => [b.id, b] as const),
    );

    const rows: any[] = [];
    for (const p of products) {
      const productDbId = productMap.get(Number(p.id));
      if (!productDbId) continue; // skip unknown products (FK constraint)
      const productInfo = productInfoById.get(productDbId);
      for (const inv of p.inventories ?? []) {
        const branchKvId = inv.branchId ? Number(inv.branchId) : null;
        if (!branchKvId) continue;
        const branchDbId = branchMap.get(branchKvId) ?? null;
        const branchInfo = branchDbId ? branchInfoById.get(branchDbId) : null;
        // lineNumber stores the local Branch.id so it stays readable and matches `branchId`.
        // Fallback to KiotViet branchId only when the local Branch row is missing.
        const lineNumber = branchDbId ?? branchKvId;
        rows.push({
          productId: productDbId,
          branchId: branchDbId ?? null,
          onHand: inv.onhand ?? inv.onHand ?? 0,
          reserved: inv.reserved ?? 0,
          modifiedDate: inv.modifiedDate ? new Date(inv.modifiedDate) : now,
          lastSyncedAt: now,
          lineNumber,
          branchName: inv.branchName ?? branchInfo?.name ?? null,
          productCode: p.code || productInfo?.code || null,
          productName: p.name || productInfo?.name || null,
          actualReserved: inv.actualReserved ?? null,
          isActive: inv.isActive ?? productInfo?.isActive ?? null,
        });
      }
    }

    // Rows with no resolvable branch are dropped: the conflict target below is
    // ("productId","branchId"), and Postgres treats NULL as distinct in a unique
    // index, so a null branch would insert a fresh duplicate on every run.
    const usableRows = rows.filter((r) => r.branchId != null);
    const skipped = rows.length - usableRows.length;
    if (skipped > 0) {
      this.logger.warn(
        `${skipped} on-hand row(s) skipped: branch not resolvable locally. Run the branch sync first.`,
      );
    }

    await this.bulkUpsert.bulkUpsert({
      table: '"ProductInventory"',
      columns: INVENTORY_COLUMNS,
      rows: usableRows,
      // Must match the ("productId","branchId") unique index. This previously used
      // ("productId","lineNumber"), an index that no longer exists, so every batch
      // failed with "no unique or exclusion constraint matching the ON CONFLICT
      // specification" and all on-hand rows were silently discarded.
      conflictTarget: '("productId", "branchId")',
      updateColumns: INVENTORY_UPDATE,
      skipUnchanged: false,
    });

    return products.length;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
