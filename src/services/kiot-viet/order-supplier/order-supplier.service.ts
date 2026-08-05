import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'order_supplier_historical';

const OS_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'orderDate', type: 'timestamp' },
  { name: 'branchId', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'userId', type: 'bigint' },
  { name: 'description', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'statusValue', type: 'text' },
  { name: 'discountRatio', type: 'real' },
  { name: 'productQty', type: 'real' },
  { name: 'discount', type: 'numeric' },
  { name: 'total', type: 'numeric' },
  { name: 'exReturnSuppliers', type: 'numeric' },
  { name: 'exReturnThirdParty', type: 'numeric' },
  { name: 'totalAmt', type: 'numeric' },
  { name: 'totalQty', type: 'real' },
  { name: 'totalQuantity', type: 'real' },
  { name: 'totalProductType', type: 'int' },
  { name: 'subTotal', type: 'numeric' },
  { name: 'paidAmount', type: 'numeric' },
  { name: 'toComplete', type: 'boolean' },
  { name: 'viewPrice', type: 'boolean' },
  { name: 'supplierDebt', type: 'numeric' },
  { name: 'supplierOldDebt', type: 'numeric' },
  { name: 'purchaseOrderCodes', type: 'text' },
  { name: 'supplierId', type: 'bigint' },
  { name: 'supplierCode', type: 'text' },
  { name: 'supplierName', type: 'text' },
  { name: 'createdBy', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const OS_UPDATE = [
  'code',
  'orderDate',
  'branchId',
  'retailerId',
  'userId',
  'description',
  'status',
  'statusValue',
  'discountRatio',
  'productQty',
  'discount',
  'total',
  'exReturnSuppliers',
  'exReturnThirdParty',
  'totalAmt',
  'totalQty',
  'totalQuantity',
  'totalProductType',
  'subTotal',
  'paidAmount',
  'toComplete',
  'viewPrice',
  'supplierDebt',
  'supplierOldDebt',
  'purchaseOrderCodes',
  'supplierId',
  'supplierCode',
  'supplierName',
  'createdBy',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
];

const OSD_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'orderSupplierId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'quantity', type: 'real' },
  { name: 'price', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'allocation', type: 'numeric' },
  { name: 'description', type: 'text' },
  { name: 'orderByNumber', type: 'int' },
  { name: 'allocationSuppliers', type: 'numeric' },
  { name: 'allocationThirdParty', type: 'numeric' },
  { name: 'orderQuantity', type: 'real' },
  { name: 'subTotal', type: 'numeric' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'orderSupplierCode', type: 'text' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
];

const OSD_UPDATE = [
  'orderSupplierId',
  'productId',
  'quantity',
  'price',
  'discount',
  'allocation',
  'description',
  'orderByNumber',
  'allocationSuppliers',
  'allocationThirdParty',
  'orderQuantity',
  'subTotal',
  'createdDate',
  'orderSupplierCode',
  'productCode',
  'productName',
];

@Injectable()
export class KiotVietOrderSupplierService {
  private readonly logger = new Logger(KiotVietOrderSupplierService.name);

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
    const lastModifiedFrom = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      lastModifiedFrom: lastModifiedFrom.toISOString(),
    });
  }

  async syncHistoricalOrderSuppliers(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`OrderSupplier sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['order_supplier']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/ordersuppliers',
          baseParams: extra,
          label: `order-supplier-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(
              `order-supplier-${mode}: processed ${processed} so far`,
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
      // KiotViet answers HTTP 420 with KvValidateOrderSupplierException ("Thiết lập
      // 'Đặt hàng nhập' đang không được bật") when the purchase-order feature is off
      // for this shop. That is a store configuration state, not a failure: there is
      // nothing to sync and retrying can never succeed. Record it as an empty,
      // skipped run so it does not fail the whole sync job.
      if (this.isFeatureDisabled(error)) {
        this.logger.warn(
          `order-supplier-${mode} skipped: the "Đặt hàng nhập" feature is disabled for ` +
            `this shop. Enable it in KiotViet store settings to sync supplier orders.`,
        );
        await this.syncControl.markCompleted(SYNC_NAME, {
          processedCount: 0,
          expectedTotal: 0,
          skipped: 'feature_disabled',
        });
        return { total: 0, processed: 0, skipped: true };
      }

      this.logger.error(`order-supplier-${mode} failed: ${error.message}`);
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
    const userIds = this.uniqueNum(items.map((o) => o.userId));
    const productIds = this.uniqueNum(
      items.flatMap((o) =>
        (o.orderSupplierDetails ?? []).map((d) => d.productId),
      ),
    );

    const [branchMap, userMap, productMap] = await Promise.all([
      this.relationMap.buildIdMap('branch', branchIds),
      this.relationMap.buildIdMap('user', userIds),
      this.relationMap.buildIdMap('product', productIds),
    ]);

    const rows = items.map((o) => {
      const userDbId = o.userId ? userMap.get(Number(o.userId)) : undefined;
      return {
        kiotVietId: o.id,
        code: o.code,
        orderDate: o.orderDate ? new Date(o.orderDate) : now,
        branchId: o.branchId
          ? (branchMap.get(Number(o.branchId)) ?? null)
          : null,
        retailerId: o.retailerId ?? null,
        userId: userDbId ?? null,
        description: o.description || '',
        status: o.status ?? null,
        statusValue: o.statusValue || '',
        discountRatio: o.discountRatio || 0,
        productQty: o.productQty || 0,
        discount: o.discount || 0,
        total: o.total || 0,
        exReturnSuppliers: o.exReturnSuppliers || 0,
        exReturnThirdParty: o.exReturnThirdParty || 0,
        totalAmt: o.totalAmt || 0,
        totalQty: o.totalQty || 0,
        totalQuantity: o.totalQuantity || 0,
        totalProductType: o.totalProductType || 0,
        subTotal: o.subTotal || 0,
        paidAmount: o.paidAmount || 0,
        toComplete: o.toComplete || false,
        viewPrice: o.viewPrice || false,
        supplierDebt: o.supplierDebt || 0,
        supplierOldDebt: o.supplierOldDebt || 0,
        purchaseOrderCodes: o.purchaseOrderCodes || '',
        supplierId: o.supplierId ? BigInt(o.supplierId) : null,
        supplierCode: o.supplierCode || null,
        supplierName: o.supplierName || null,
        createdBy: userDbId ?? null,
        createdDate: o.createdDate ? new Date(o.createdDate) : now,
        modifiedDate: now,
        lastSyncedAt: now,
      };
    });

    await this.bulkUpsert.bulkUpsert({
      table: '"OrderSupplier"',
      columns: OS_COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: OS_UPDATE,
    });

    const osIdMap = await this.relationMap.buildIdMap(
      'orderSupplier',
      this.uniqueNum(items.map((o) => o.id)),
    );

    const detailRows: any[] = [];
    for (const o of items) {
      const osDbId = osIdMap.get(Number(o.id));
      if (!osDbId) continue;
      for (const d of o.orderSupplierDetails ?? []) {
        const product = productMap.get(Number(d.productId));
        if (!product) continue;
        detailRows.push({
          kiotVietId: BigInt(d.id),
          orderSupplierId: osDbId,
          productId: product,
          quantity: d.quantity ?? 0,
          price: d.price || 0,
          discount: d.discount || 0,
          allocation: d.allocation || 0,
          description: d.description || '',
          orderByNumber: d.orderByNumber || 0,
          allocationSuppliers: d.allocationSuppliers ?? null,
          allocationThirdParty: d.allocationThirdParty ?? null,
          orderQuantity: d.orderQuantity ?? 0,
          subTotal: d.subTotal || 0,
          createdDate: d.createdDate ? new Date(d.createdDate) : now,
          orderSupplierCode: o.code,
          productCode: d.productCode || null,
          productName: d.productName || null,
        });
      }
    }

    await this.bulkUpsert.bulkUpsert({
      table: '"OrderSupplierDetail"',
      columns: OSD_COLUMNS,
      rows: detailRows,
      conflictTarget: '"kiotVietId"',
      updateColumns: OSD_UPDATE,
    });

    return items.length;
  }

  /** True when the shop has the "Đặt hàng nhập" feature turned off. */
  private isFeatureDisabled(error: any): boolean {
    const status = error?.response?.status;
    const code = error?.response?.data?.responseStatus?.errorCode;
    return status === 420 || code === 'KvValidateOrderSupplierException';
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
