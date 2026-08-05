import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'product_historical';

const PRODUCT_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'barCode', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'fullName', type: 'text' },
  { name: 'categoryId', type: 'int' },
  { name: 'categoryName', type: 'text' },
  { name: 'parent_name', type: 'varchar' },
  { name: 'child_name', type: 'varchar' },
  { name: 'branch_name', type: 'varchar' },
  { name: 'tradeMarkId', type: 'int' },
  { name: 'tradeMarkName', type: 'text' },
  { name: 'type', type: 'int' },
  { name: 'description', type: 'text' },
  { name: 'allowsSale', type: 'boolean' },
  { name: 'hasVariants', type: 'boolean' },
  { name: 'basePrice', type: 'numeric' },
  { name: 'unit', type: 'text' },
  { name: 'masterProductId', type: 'bigint' },
  { name: 'masterUnitId', type: 'bigint' },
  { name: 'conversionValue', type: 'real' },
  { name: 'weight', type: 'real' },
  { name: 'isLotSerialControl', type: 'boolean' },
  { name: 'isBatchExpireControl', type: 'boolean' },
  { name: 'isManufactured', type: 'boolean' },
  { name: 'orderTemplate', type: 'text' },
  { name: 'minQuantity', type: 'int' },
  { name: 'maxQuantity', type: 'int' },
  { name: 'isRewardPoint', type: 'boolean' },
  { name: 'isActive', type: 'boolean' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const PRODUCT_UPDATE = [
  'code',
  'barCode',
  'name',
  'fullName',
  'categoryId',
  'categoryName',
  'parent_name',
  'child_name',
  'branch_name',
  'tradeMarkId',
  'tradeMarkName',
  'type',
  'description',
  'allowsSale',
  'hasVariants',
  'basePrice',
  'unit',
  'masterProductId',
  'masterUnitId',
  'conversionValue',
  'weight',
  'isLotSerialControl',
  'isBatchExpireControl',
  'isManufactured',
  'orderTemplate',
  'minQuantity',
  'maxQuantity',
  'isRewardPoint',
  'isActive',
  'retailerId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

const INVENTORY_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'onHand', type: 'int' },
  { name: 'reserved', type: 'int' },
  { name: 'onOrder', type: 'int' },
  { name: 'cost', type: 'numeric' },
  { name: 'minQuantity', type: 'int' },
  { name: 'maxQuantity', type: 'int' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'branchName', type: 'text' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'actualReserved', type: 'int' },
  { name: 'isActive', type: 'boolean' },
  { name: 'lineNumber', type: 'int' },
];

const INVENTORY_UPDATE = [
  'branchId',
  'onHand',
  'reserved',
  'onOrder',
  'cost',
  'minQuantity',
  'maxQuantity',
  'modifiedDate',
  'lastSyncedAt',
  'branchName',
  'productCode',
  'productName',
  'actualReserved',
  'isActive',
];

const PRICEBOOK_DETAIL_COLUMNS: ColumnSpec[] = [
  { name: 'priceBookId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'productKiotId', type: 'bigint' },
  { name: 'price', type: 'numeric' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'lineNumber', type: 'int' },
  { name: 'priceBookName', type: 'text' },
  { name: 'productName', type: 'text' },
];

const PRICEBOOK_DETAIL_UPDATE = [
  'priceBookId',
  'productKiotId',
  'price',
  'lastSyncedAt',
  'priceBookName',
  'productName',
];

const IMAGE_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'lineNumber', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'imageUrl', type: 'jsonb' },
];

const IMAGE_UPDATE = ['imageUrl', 'lastSyncedAt'];

const FORMULA_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'productKiotVietId', type: 'bigint' },
  { name: 'materialId', type: 'bigint' },
  { name: 'materialCode', type: 'text' },
  { name: 'materialName', type: 'text' },
  { name: 'materialFullName', type: 'text' },
  { name: 'quantity', type: 'real' },
  { name: 'basePrice', type: 'numeric' },
  { name: 'lineNumber', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const FORMULA_UPDATE = [
  'productKiotVietId',
  'materialId',
  'materialCode',
  'materialName',
  'materialFullName',
  'quantity',
  'basePrice',
  'lastSyncedAt',
];

// The four blocks below persist data the API was already being asked for
// (includeSerials / includeBatchExpires / includeWarranties, plus the always-present
// `units` array) but which previously had nowhere to go and was silently discarded.

const UNIT_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'unitKiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'name', type: 'text' },
  { name: 'fullName', type: 'text' },
  { name: 'unit', type: 'text' },
  { name: 'conversionValue', type: 'real' },
  { name: 'basePrice', type: 'numeric' },
  { name: 'isCurrent', type: 'boolean' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UNIT_UPDATE = [
  'code',
  'name',
  'fullName',
  'unit',
  'conversionValue',
  'basePrice',
  'isCurrent',
  'lastSyncedAt',
];

const SERIAL_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'serialNumber', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'quantity', type: 'real' },
  { name: 'isCurrent', type: 'boolean' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const SERIAL_UPDATE = [
  'status',
  'quantity',
  'isCurrent',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

const BATCH_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'branchId', type: 'int' },
  { name: 'batchName', type: 'text' },
  { name: 'onHand', type: 'real' },
  { name: 'expireDate', type: 'timestamp' },
  { name: 'fullNameVirgule', type: 'text' },
  { name: 'isCurrent', type: 'boolean' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const BATCH_UPDATE = [
  'onHand',
  'expireDate',
  'fullNameVirgule',
  'isCurrent',
  'lastSyncedAt',
];

const WARRANTY_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'productId', type: 'int' },
  { name: 'description', type: 'text' },
  { name: 'numberTime', type: 'int' },
  { name: 'timeType', type: 'int' },
  { name: 'warrantyType', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdBy', type: 'bigint' },
  { name: 'isCurrent', type: 'boolean' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const WARRANTY_UPDATE = [
  'productId',
  'description',
  'numberTime',
  'timeType',
  'warrantyType',
  'retailerId',
  'createdBy',
  'isCurrent',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

@Injectable()
export class KiotVietProductService {
  private readonly logger = new Logger(KiotVietProductService.name);
  private materialCodes = new Set<string>();

  constructor(
    private readonly prismaService: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
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

  async syncHistoricalProducts(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Product sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    this.materialCodes = new Set<string>();
    await this.syncControl.markRunning(SYNC_NAME, mode, ['product']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/products',
          baseParams: { includeRemoveIds: true,
            orderBy: 'id',
            orderDirection: 'DESC',
            includeInventory: true,
            includePricebook: true,
            includeSerials: true,
            includeBatchExpires: true,
            includeWarranties: true,
            includeQuantity: true,
            includeMaterial: true,
            ...extra,
          },
          label: `product-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`product-${mode}: processed ${processed} so far`);
          },
        });
      total = t;
      await this.recomputeMaterialFlags();
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('product', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`product-${mode} failed: ${error.message}`);
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

    const categoryIds = this.uniqueNum(products.map((p) => p.categoryId));
    const tradeMarkIds = this.uniqueNum(products.map((p) => p.tradeMarkId));
    const branchIds = this.uniqueNum(
      products.flatMap((p) => (p.inventories ?? []).map((i) => i.branchId)),
    );
    const priceBookIds = this.uniqueNum(
      products.flatMap((p) => (p.priceBooks ?? []).map((pb) => pb.priceBookId)),
    );

    const [categoryMap, tradeMarkMap, branchMap, priceBookMap] =
      await Promise.all([
        this.relationMap.buildIdMap('category', categoryIds),
        this.relationMap.buildIdMap('tradeMark', tradeMarkIds),
        this.relationMap.buildIdMap('branch', branchIds),
        this.relationMap.buildIdMap('priceBook', priceBookIds),
      ]);

    const productRows = products.map((p) => ({
      kiotVietId: p.id,
      code: (p.code || '').trim(),
      barCode: p.barCode || null,
      name: p.name,
      fullName: p.fullName ?? p.name,
      categoryId: p.categoryId
        ? (categoryMap.get(Number(p.categoryId)) ?? null)
        : null,
      categoryName: p.categoryName || null,
      parent_name: null,
      child_name: null,
      branch_name: null,
      tradeMarkId: p.tradeMarkId
        ? (tradeMarkMap.get(Number(p.tradeMarkId)) ?? null)
        : null,
      tradeMarkName: p.tradeMarkName || null,
      type: p.type ?? 1,
      description: p.description ?? '',
      allowsSale: p.allowsSale ?? true,
      hasVariants: p.hasVariants ?? false,
      basePrice: p.basePrice ?? 0,
      unit: p.unit ?? '',
      masterProductId: p.masterProductId ? BigInt(p.masterProductId) : null,
      masterUnitId: p.masterUnitId ? BigInt(p.masterUnitId) : null,
      conversionValue: p.conversionValue ?? 1,
      weight: p.weight ?? 0,
      isLotSerialControl: p.isLotSerialControl ?? false,
      isBatchExpireControl: p.isBatchExpireControl ?? false,
      isManufactured: (p.productFormulas?.length ?? 0) > 0,
      orderTemplate: p.orderTemplate?.trim() || null,
      minQuantity: p.minQuantity ?? null,
      maxQuantity: p.maxQuantity ?? null,
      isRewardPoint: p.isRewardPoint ?? true,
      isActive: p.isActive ?? true,
      retailerId: p.retailerId ?? null,
      createdDate: p.createdDate ? new Date(p.createdDate) : now,
      modifiedDate: p.modifiedDate ? new Date(p.modifiedDate) : now,
      lastSyncedAt: now,
    }));

    await this.bulkUpsert.bulkUpsert({
      table: '"Product"',
      columns: PRODUCT_COLUMNS,
      rows: productRows,
      conflictTarget: '"kiotVietId"',
      updateColumns: PRODUCT_UPDATE,
    });

    await this.backfillUnresolvedProductReferences();

    const productIdMap = await this.relationMap.buildIdMap(
      'product',
      this.uniqueNum(products.map((p) => p.id)),
    );

    const inventoryRows: any[] = [];
    const priceBookDetailRows: any[] = [];
    const imageRows: any[] = [];
    const formulaRows: any[] = [];
    const unitRows: any[] = [];
    const serialRows: any[] = [];
    const batchRows: any[] = [];
    const warrantyRows: any[] = [];
    // Serial/batch rows are keyed on (productId, branchId, ...). Postgres treats NULL
    // as distinct in a unique index, so a null branchId would defeat the upsert and
    // insert a duplicate on every run. Those rows are skipped and counted instead.
    let skippedNoBranch = 0;

    for (const p of products) {
      const productDbId = productIdMap.get(Number(p.id));
      if (!productDbId) continue;
      const productKvId = BigInt(p.id);

      (p.inventories ?? []).forEach((inv: any, idx: number) => {
        const branchKvId = inv.branchId ? Number(inv.branchId) : null;
        const branchDbId = branchKvId
          ? (branchMap.get(branchKvId) ?? null)
          : null;
        // The conflict target is ("productId","branchId"). Postgres treats NULL as
        // distinct in a unique index, so a null branch would insert a fresh duplicate
        // on every sync instead of updating. Skip and report those instead.
        if (branchDbId === null) {
          skippedNoBranch++;
          return;
        }
        inventoryRows.push({
          productId: productDbId,
          branchId: branchDbId,
          onHand: inv.onHand ?? inv.onhand ?? 0,
          reserved: inv.reserved ?? 0,
          onOrder: inv.onOrder ?? null,
          cost: inv.cost ?? null,
          minQuantity: inv.minQuantity ?? inv.minQuality ?? null,
          maxQuantity: inv.maxQuantity ?? inv.maxQuality ?? null,
          modifiedDate: now,
          lastSyncedAt: now,
          branchName: inv.branchName || null,
          productCode: p.code,
          productName: p.name,
          actualReserved: inv.actualReserved ?? null,
          isActive: inv.isActive ?? null,
          // lineNumber stores the local Branch.id so it stays readable and matches `branchId`.
          // Fallback to KiotViet branchId only when the local Branch row is missing.
          lineNumber: branchDbId ?? branchKvId ?? idx + 1,
        });
      });

      (p.priceBooks ?? []).forEach((pb: any, idx: number) => {
        priceBookDetailRows.push({
          priceBookId: pb.priceBookId
            ? (priceBookMap.get(Number(pb.priceBookId)) ?? null)
            : null,
          productId: productDbId,
          productKiotId: productKvId,
          price: pb.price ?? 0,
          lastSyncedAt: now,
          lineNumber: idx + 1,
          priceBookName: pb.priceBookName || null,
          productName: p.name,
        });
      });

      (p.images ?? []).forEach((img: any, idx: number) => {
        imageRows.push({
          productId: productDbId,
          lineNumber: idx + 1,
          lastSyncedAt: now,
          imageUrl: img.image ? { url: img.image } : null,
        });
      });

      (p.productFormulas ?? []).forEach((f: any, idx: number) => {
        if (f.materialCode) this.materialCodes.add(f.materialCode.trim());
        formulaRows.push({
          productId: productDbId,
          productKiotVietId: productKvId,
          materialId: BigInt(f.materialId),
          materialCode: f.materialCode,
          materialName: f.materialName ?? null,
          materialFullName: f.materialFullName ?? null,
          quantity: f.quantity ?? null,
          basePrice: f.basePrice ?? null,
          lineNumber: idx + 1,
          lastSyncedAt: now,
        });
      });

      // --- units -------------------------------------------------------------
      // Alternate packagings of the same item. Previously requested from the API
      // and discarded because no table existed.
      const seenUnits = new Set<string>();
      for (const u of p.units ?? []) {
        if (u?.id === null || u?.id === undefined) continue;
        const key = String(u.id);
        if (seenUnits.has(key)) continue;
        seenUnits.add(key);
        unitRows.push({
          productId: productDbId,
          unitKiotVietId: BigInt(u.id),
          code: u.code ?? null,
          name: u.name ?? null,
          fullName: u.fullName ?? null,
          unit: u.unit ?? null,
          conversionValue: u.conversionValue ?? null,
          basePrice: u.basePrice ?? null,
          isCurrent: true,
          lastSyncedAt: now,
        });
      }

      // --- serials (IMEI) ----------------------------------------------------
      // The doc's indentation is ambiguous about whether these sit on the product
      // or inside each inventory entry, so both shapes are accepted.
      const serialSources = [
        ...(p.productSerials ?? []),
        ...(p.inventories ?? []).flatMap(
          (inv: any) => inv?.productSerials ?? [],
        ),
      ];
      const seenSerials = new Set<string>();
      for (const s of serialSources) {
        const serialNumber = s?.serialNumber ?? s?.SerialNumber;
        if (!serialNumber) continue;
        const branchKvId = s.branchId ?? s.BranchId;
        const branchDbId = branchKvId
          ? (branchMap.get(Number(branchKvId)) ?? null)
          : null;
        if (branchDbId === null) {
          skippedNoBranch++;
          continue;
        }
        const key = `${branchDbId}|${serialNumber}`;
        if (seenSerials.has(key)) continue;
        seenSerials.add(key);
        serialRows.push({
          productId: productDbId,
          branchId: branchDbId,
          serialNumber: String(serialNumber),
          status: s.status ?? null,
          quantity: s.quantity ?? null,
          isCurrent: true,
          createdDate: s.createdDate ? new Date(s.createdDate) : null,
          modifiedDate: s.modifiedDate ? new Date(s.modifiedDate) : null,
          lastSyncedAt: now,
        });
      }

      // --- batch / expiry ----------------------------------------------------
      const batchSources = [
        ...(p.productBatchExpires ?? []),
        ...(p.inventories ?? []).flatMap(
          (inv: any) => inv?.productBatchExpires ?? [],
        ),
      ];
      const seenBatches = new Set<string>();
      for (const b of batchSources) {
        const batchName = b?.batchName ?? b?.BatchName;
        if (!batchName) continue;
        const branchKvId = b.branchId ?? b.BranchId;
        const branchDbId = branchKvId
          ? (branchMap.get(Number(branchKvId)) ?? null)
          : null;
        if (branchDbId === null) {
          skippedNoBranch++;
          continue;
        }
        const key = `${branchDbId}|${batchName}`;
        if (seenBatches.has(key)) continue;
        seenBatches.add(key);
        batchRows.push({
          productId: productDbId,
          branchId: branchDbId,
          batchName: String(batchName),
          onHand: b.onHand ?? null,
          expireDate: b.expireDate ? new Date(b.expireDate) : null,
          fullNameVirgule: b.fullNameVirgule ?? null,
          isCurrent: true,
          lastSyncedAt: now,
        });
      }

      // --- warranties --------------------------------------------------------
      // The doc spells this id `Id` (capital) while its siblings are lower-camel,
      // so both spellings are read.
      const seenWarranties = new Set<string>();
      for (const w of p.productWarranties ?? []) {
        const kvId = w?.id ?? w?.Id;
        if (kvId === null || kvId === undefined) continue;
        const key = String(kvId);
        if (seenWarranties.has(key)) continue;
        seenWarranties.add(key);
        warrantyRows.push({
          kiotVietId: BigInt(kvId),
          productId: productDbId,
          description: w.description ?? null,
          numberTime: w.numberTime ?? null,
          timeType: w.timeType ?? null,
          warrantyType: w.warrantyType ?? null,
          retailerId: w.retailerId ?? null,
          createdBy: w.createdBy ? BigInt(w.createdBy) : null,
          isCurrent: true,
          createdDate: w.createdDate ? new Date(w.createdDate) : null,
          modifiedDate: w.modifiedDate ? new Date(w.modifiedDate) : null,
          lastSyncedAt: now,
        });
      }
    }

    if (skippedNoBranch > 0) {
      this.logger.warn(
        `${skippedNoBranch} serial/batch row(s) skipped: branch could not be resolved. ` +
          `Run the branch sync first so these can be keyed correctly.`,
      );
    }

    // Archive mode: existing child rows are retained even when KiotViet omits them.
    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"ProductInventory"',
        columns: INVENTORY_COLUMNS,
        rows: inventoryRows,
        // Natural key, per the Phase 1 schema change. The previous
        // ("productId","lineNumber") target no longer has a matching unique index.
        conflictTarget: '("productId", "branchId")',
        updateColumns: INVENTORY_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"PriceBookDetail"',
        columns: PRICEBOOK_DETAIL_COLUMNS,
        rows: priceBookDetailRows,
        conflictTarget: '("priceBookId", "productId")',
        updateColumns: PRICEBOOK_DETAIL_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"ProductImage"',
        columns: IMAGE_COLUMNS,
        rows: imageRows,
        conflictTarget: '("productId", "lineNumber")',
        updateColumns: IMAGE_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"ProductFormula"',
        columns: FORMULA_COLUMNS,
        rows: formulaRows,
        conflictTarget: '("productId", "lineNumber")',
        updateColumns: FORMULA_UPDATE,
        skipUnchanged: false,
      }),
      // Four tables that previously had no writer: the API data was requested via
      // includeSerials / includeBatchExpires / includeWarranties, parsed, then dropped.
      this.bulkUpsert.bulkUpsert({
        table: '"ProductUnit"',
        columns: UNIT_COLUMNS,
        rows: unitRows,
        conflictTarget: '("productId", "unitKiotVietId")',
        updateColumns: UNIT_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"ProductSerial"',
        columns: SERIAL_COLUMNS,
        rows: serialRows,
        conflictTarget: '("productId", "branchId", "serialNumber")',
        updateColumns: SERIAL_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"ProductBatchExpire"',
        columns: BATCH_COLUMNS,
        rows: batchRows,
        conflictTarget: '("productId", "branchId", "batchName")',
        updateColumns: BATCH_UPDATE,
        skipUnchanged: false,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"ProductWarranty"',
        columns: WARRANTY_COLUMNS,
        rows: warrantyRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: WARRANTY_UPDATE,
        skipUnchanged: false,
      }),
    ]);

    return products.length;
  }

  private async recomputeMaterialFlags() {
    const codes = Array.from(this.materialCodes);
    if (codes.length === 0) return;
    this.logger.log(
      `Recomputing isMaterial flags for ${codes.length} material codes`,
    );
    await this.prismaService.product.updateMany({
      where: { isMaterial: true },
      data: { isMaterial: false },
    });
    // Update in chunks to avoid huge IN clauses
    const CHUNK = 500;
    for (let i = 0; i < codes.length; i += CHUNK) {
      await this.prismaService.product.updateMany({
        where: { code: { in: codes.slice(i, i + CHUNK) } },
        data: { isMaterial: true },
      });
    }
  }

  private async backfillUnresolvedProductReferences(): Promise<void> {
    const [orderDetails, invoiceDetails, returnDetails] = await this.prismaService.$transaction([
      this.prismaService.$executeRaw`
        UPDATE "OrderDetail" AS detail
        SET "productId" = product.id
        FROM "Product" AS product
        WHERE detail."productId" IS NULL
          AND detail."productKiotVietId" = product."kiotVietId"
      `,
      this.prismaService.$executeRaw`
        UPDATE "InvoiceDetail" AS detail
        SET "productId" = product.id
        FROM "Product" AS product
        WHERE detail."productId" IS NULL
          AND detail."productKiotVietId" = product."kiotVietId"
      `,
      this.prismaService.$executeRaw`
        UPDATE "ReturnDetail" AS detail
        SET "productId" = product.id
        FROM "Product" AS product
        WHERE detail."productId" IS NULL
          AND detail."productKiotVietId" = product."kiotVietId"
      `,
    ]);

    const total = orderDetails + invoiceDetails + returnDetails;
    if (total > 0) {
      this.logger.log(
        `Back-filled ${total} transaction detail product reference(s) ` +
          `(orders=${orderDetails}, invoices=${invoiceDetails}, returns=${returnDetails})`,
      );
    }
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
