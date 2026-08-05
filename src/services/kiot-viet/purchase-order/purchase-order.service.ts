import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'purchase_order_historical';

const PO_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'branchId', type: 'int' },
  { name: 'purchaseDate', type: 'timestamp' },
  { name: 'discountRatio', type: 'real' },
  { name: 'discount', type: 'numeric' },
  { name: 'total', type: 'numeric' },
  { name: 'supplierId', type: 'int' },
  { name: 'purchaseById', type: 'bigint' },
  { name: 'paidAmount', type: 'numeric' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'branchName', type: 'text' },
  { name: 'description', type: 'text' },
  { name: 'purchaseName', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'supplierCode', type: 'text' },
  { name: 'supplierName', type: 'text' },
  { name: 'totalPayment', type: 'numeric' },
  { name: 'exReturnSuppliers', type: 'numeric' },
  { name: 'exReturnThirdParty', type: 'numeric' },
];

const PO_UPDATE = [
  'code',
  'branchId',
  'purchaseDate',
  'discountRatio',
  'discount',
  'total',
  'supplierId',
  'purchaseById',
  'paidAmount',
  'retailerId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
  'branchName',
  'description',
  'purchaseName',
  'status',
  'supplierCode',
  'supplierName',
  'totalPayment',
  'exReturnSuppliers',
  'exReturnThirdParty',
];

const POD_COLUMNS: ColumnSpec[] = [
  { name: 'purchaseOrderId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'quantity', type: 'real' },
  { name: 'price', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'serialNumbers', type: 'text' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'purchaseOrderCode', type: 'text' },
  { name: 'lineNumber', type: 'int' },
  { name: 'uniqueKey', type: 'text' },
];

const POD_UPDATE = [
  'productId',
  'quantity',
  'price',
  'discount',
  'serialNumbers',
  'productCode',
  'productName',
  'purchaseOrderCode',
  'uniqueKey',
];

const PAYMENT_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'amount', type: 'numeric' },
  { name: 'method', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'transDate', type: 'timestamp' },
  { name: 'purchaseOrderId', type: 'int' },
  { name: 'statusValue', type: 'text' },
  { name: 'description', type: 'text' },
];

const PAYMENT_UPDATE = [
  'code',
  'amount',
  'method',
  'status',
  'transDate',
  'purchaseOrderId',
  'statusValue',
  'description',
];

@Injectable()
export class KiotVietPurchaseOrderService {
  private readonly logger = new Logger(KiotVietPurchaseOrderService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  async syncFull() {
    return this.runSync('full', {});
  }

  async syncIncremental() {
    const last = await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const fromPurchaseDate = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      fromPurchaseDate: fromPurchaseDate.toISOString().slice(0, 10),
    });
  }

  async syncHistoricalPurchaseOrder(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`PurchaseOrder sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['purchase_order']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/purchaseorders',
          baseParams: {
            includePayment: true,
            includeOrderDelivery: true,
            ...extra,
          },
          label: `purchase-order-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(
              `purchase-order-${mode}: processed ${processed} so far`,
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
      this.logger.error(`purchase-order-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(items: any[]): Promise<number> {
    if (!items.length) return 0;
    const now = new Date();

    const branchIds = this.uniqueNum(items.map((o) => o.branchId));
    const supplierIds = this.uniqueNum(items.map((o) => o.supplierId));
    const userIds = this.uniqueNum(items.map((o) => o.purchaseById));
    const productIds = this.uniqueNum(
      items.flatMap((o) =>
        (o.purchaseOrderDetails ?? []).map((d) => d.productId),
      ),
    );

    const [branchMap, supplierMap, userMap, productMap] = await Promise.all([
      this.relationMap.buildIdMap('branch', branchIds),
      this.relationMap.buildIdMap('supplier', supplierIds),
      this.relationMap.buildIdMap('user', userIds),
      this.relationMap.buildIdMap('product', productIds),
    ]);

    const rows = items.map((o) => {
      const supplierDbId = o.supplierId
        ? supplierMap.get(Number(o.supplierId))
        : undefined;
      const userDbId = o.purchaseById
        ? userMap.get(Number(o.purchaseById))
        : undefined;
      const branchDbId = o.branchId
        ? branchMap.get(Number(o.branchId))
        : undefined;
      return {
        kiotVietId: o.id,
        code: (o.code || '').trim(),
        branchId: branchDbId ?? null,
        purchaseDate: o.purchaseDate ? new Date(o.purchaseDate) : now,
        discountRatio: o.discountRatio ?? null,
        discount: o.discount ?? null,
        total: o.total || 0,
        supplierId: supplierDbId ?? null,
        purchaseById: userDbId ?? null,
        paidAmount: o.totalPayment ?? 0,
        retailerId: o.retailerId ?? null,
        createdDate: o.createdDate ? new Date(o.createdDate) : now,
        modifiedDate: now,
        lastSyncedAt: now,
        branchName: branchDbId ? null : null,
        description: o.description || '',
        purchaseName: null,
        status: o.status ?? null,
        supplierCode: o.supplierCode || null,
        supplierName: o.supplierName || null,
        totalPayment: o.totalPayment ?? null,
        exReturnSuppliers: o.exReturnSuppliers ?? null,
        exReturnThirdParty: o.exReturnThirdParty ?? null,
      };
    });

    await this.bulkUpsert.bulkUpsert({
      table: '"PurchaseOrder"',
      columns: PO_COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: PO_UPDATE,
    });

    const poIdMap = await this.relationMap.buildIdMap(
      'purchaseOrder',
      this.uniqueNum(items.map((o) => o.id)),
    );

    const detailRows: any[] = [];
    const paymentRows: any[] = [];
    for (const o of items) {
      const poDbId = poIdMap.get(Number(o.id));
      if (!poDbId) continue;
      (o.purchaseOrderDetails ?? []).forEach((d: any, idx: number) => {
        const product = productMap.get(Number(d.productId));
        if (!product) return;
        const ln = d.lineNumber ?? idx + 1;
        detailRows.push({
          purchaseOrderId: poDbId,
          productId: product,
          quantity: d.quantity ?? 0,
          price: d.price ?? 0,
          discount: d.discount ?? d.disount ?? null,
          serialNumbers: d.serialNumbers || null,
          productCode: d.productCode || null,
          productName: d.productName || null,
          purchaseOrderCode: o.code,
          lineNumber: ln,
          uniqueKey: `${o.id}.${ln}`,
        });
      });
      for (const p of o.payments ?? []) {
        paymentRows.push({
          kiotVietId: p.id ? BigInt(p.id) : null,
          code: p.code || null,
          amount: p.amount ?? 0,
          method: p.method || 'Cash',
          status: p.status ?? null,
          transDate: p.transDate ? new Date(p.transDate) : now,
          purchaseOrderId: poDbId,
          statusValue: p.statusValue || null,
          description: p.description || null,
        });
      }
    }

    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"PurchaseOrderDetail"',
        columns: POD_COLUMNS,
        rows: detailRows,
        conflictTarget: '("purchaseOrderId", "lineNumber")',
        updateColumns: POD_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"Payment"',
        columns: PAYMENT_COLUMNS,
        rows: paymentRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: PAYMENT_UPDATE,
      }),
    ]);

    return items.length;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
