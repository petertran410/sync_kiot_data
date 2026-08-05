import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RetailerContext } from '../shared/retailer-context';

const SYNC_NAME = 'order_historical';

const ORDER_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'purchaseDate', type: 'timestamp' },
  { name: 'branchId', type: 'int' },
  { name: 'soldById', type: 'bigint' },
  { name: 'soldByKiotVietId', type: 'bigint' },
  { name: 'soldByName', type: 'text' },
  { name: 'cashierId', type: 'bigint' },
  { name: 'customerId', type: 'int' },
  { name: 'total', type: 'numeric' },
  { name: 'totalPayment', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'discountRatio', type: 'real' },
  { name: 'status', type: 'int' },
  { name: 'description', type: 'text' },
  { name: 'usingCod', type: 'boolean' },
  { name: 'saleChannelId', type: 'int' },
  { name: 'expectedDelivery', type: 'timestamp' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'statusValue', type: 'text' },
  { name: 'customerCode', type: 'text' },
  { name: 'customerName', type: 'text' },
  { name: 'saleChannelName', type: 'text' },
];

const ORDER_UPDATE = [
  'code',
  'purchaseDate',
  'branchId',
  'soldById',
  'soldByKiotVietId',
  'soldByName',
  'cashierId',
  'customerId',
  'total',
  'totalPayment',
  'discount',
  'discountRatio',
  'status',
  'description',
  'usingCod',
  'saleChannelId',
  'expectedDelivery',
  'retailerId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
  'statusValue',
  'customerCode',
  'customerName',
  'saleChannelName',
];

const ORDER_DETAIL_COLUMNS: ColumnSpec[] = [
  { name: 'orderId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'productKiotVietId', type: 'bigint' },
  { name: 'quantity', type: 'real' },
  { name: 'price', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'discountRatio', type: 'real' },
  { name: 'note', type: 'text' },
  { name: 'isMaster', type: 'boolean' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'lineNumber', type: 'int' },
];

const ORDER_DETAIL_UPDATE = [
  'productId',
  'productKiotVietId',
  'quantity',
  'price',
  'discount',
  'discountRatio',
  'note',
  'isMaster',
  'productCode',
  'productName',
];

const ORDER_DELIVERY_COLUMNS: ColumnSpec[] = [
  { name: 'orderId', type: 'int' },
  { name: 'deliveryCode', type: 'text' },
  { name: 'type', type: 'int' },
  { name: 'price', type: 'numeric' },
  { name: 'receiver', type: 'text' },
  { name: 'contactNumber', type: 'text' },
  { name: 'address', type: 'text' },
  { name: 'locationId', type: 'int' },
  { name: 'locationName', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'weight', type: 'real' },
  { name: 'length', type: 'real' },
  { name: 'width', type: 'real' },
  { name: 'height', type: 'real' },
];

const ORDER_DELIVERY_UPDATE = [
  'deliveryCode',
  'type',
  'price',
  'receiver',
  'contactNumber',
  'address',
  'locationId',
  'locationName',
  'wardName',
  'weight',
  'length',
  'width',
  'height',
];

const PAYMENT_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'amount', type: 'numeric' },
  { name: 'method', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'transDate', type: 'timestamp' },
  { name: 'accountId', type: 'int' },
  { name: 'orderId', type: 'int' },
  { name: 'description', type: 'text' },
];

const PAYMENT_UPDATE = [
  'code',
  'amount',
  'method',
  'status',
  'transDate',
  'accountId',
  'orderId',
  'description',
];

const ORDER_SURCHARGE_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'orderId', type: 'int' },
  { name: 'surchargeId', type: 'int' },
  { name: 'surchargeName', type: 'text' },
  { name: 'surValue', type: 'numeric' },
  { name: 'price', type: 'numeric' },
];

const ORDER_SURCHARGE_UPDATE = [
  'surchargeId',
  'surchargeName',
  'surValue',
  'price',
];

@Injectable()
export class KiotVietOrderService {
  private readonly logger = new Logger(KiotVietOrderService.name);
  /** Rows whose soldById could not satisfy the FK, tallied per sync run. */
  private unknownSoldBy = 0;
  /** Rows examined for soldById, so the summary ratio has a correct denominator. */
  private soldBySeen = 0;
  /** staff "id|name" -> row count, so the summary names who is missing. */
  private unknownSoldByNames = new Map<string, number>();

  constructor(
    private readonly prismaService: PrismaService,
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
    const lastModifiedFrom = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      lastModifiedFrom: lastModifiedFrom.toISOString(),
    });
  }

  async syncHistoricalOrders(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Order sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['order']);
    let processed = 0;
    let total = 0;
    // Accumulated across pages and reported once at the end. Logging per page
    // produced a warning every 100 rows, which reads like a recurring failure.
    this.unknownSoldBy = 0;
    this.soldBySeen = 0;
    this.unknownSoldByNames = new Map<string, number>();
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/orders',
          baseParams: {
            orderBy: 'id',
            orderDirection: 'DESC',
            includeOrderDelivery: true,
            includePayment: true,
            ...extra,
          },
          label: `order-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`order-${mode}: processed ${processed} so far`);
          },
        });
      total = t;
      this.reportUnknownSoldBy(processed);
      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      return { total, processed };
    } catch (error) {
      this.logger.error(`order-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(orders: any[]): Promise<number> {
    if (!orders.length) return 0;
    const now = new Date();

    const branchIds = this.uniqueNum(orders.map((o) => o.branchId));
    const customerIds = this.uniqueNum(orders.map((o) => o.customerId));
    const saleChannelIds = this.uniqueNum(
      orders.map((o) => o.SaleChannelId ?? o.saleChannelId),
    );
    const productIds = this.uniqueNum(
      orders.flatMap((o) => (o.orderDetails ?? []).map((d) => d.productId)),
    );
    const bankAccountIds = this.uniqueNum(
      orders.flatMap((o) =>
        (o.payments ?? []).filter((p) => p.accountId).map((p) => p.accountId),
      ),
    );
    const surchargeIds = this.uniqueNum(
      orders.flatMap((o) =>
        (o.invoiceOrderSurcharges ?? [])
          .filter((s) => s.surchargeId)
          .map((s) => s.surchargeId),
      ),
    );

    const soldByIds = this.uniqueNum(
      orders.flatMap((o) => [o.soldById, o.cashierId]).filter((v) => v != null),
    );

    const [
      branchMap,
      customerMap,
      saleChannelMap,
      productMap,
      bankAccountMap,
      surchargeMap,
      userMap,
    ] = await Promise.all([
      this.relationMap.buildIdMap('branch', branchIds),
      this.relationMap.buildIdMap('customer', customerIds),
      this.relationMap.buildIdMap('saleChannel', saleChannelIds),
      this.relationMap.buildIdMap('product', productIds),
      this.relationMap.buildIdMap('bankAccount', bankAccountIds),
      this.relationMap.buildIdMap('surcharge', surchargeIds),
      // Order.soldById / cashierId are FKs onto User.kiotVietId. GET /users only
      // returns ACTIVE users, but orders keep referencing staff who have since been
      // removed — one such id appeared on ~98% of orders here. Writing it straight
      // through violated Order_soldById_fkey, and because BulkUpsertHelper logs and
      // continues, those orders were dropped while the sync still reported success.
      // So resolve against the users we actually hold and null out the unknown ones.
      this.relationMap.buildIdMap('user', soldByIds),
    ]);

    const orderRows = orders.map((o) => {
      const saleChannelId = o.SaleChannelId ?? o.saleChannelId;
      const saleChannelDbId = saleChannelId
        ? saleChannelMap.get(Number(saleChannelId))
        : undefined;
      // Only keep the id when the User row exists, otherwise the FK rejects the order.
      const rawSoldBy = o.soldById ?? null;
      const soldByKnown = rawSoldBy != null && userMap.has(Number(rawSoldBy));
      this.soldBySeen++;
      if (rawSoldBy != null && !soldByKnown) {
        this.unknownSoldBy++;
        const key = `${rawSoldBy}|${o.soldByName || 'unknown'}`;
        this.unknownSoldByNames.set(
          key,
          (this.unknownSoldByNames.get(key) ?? 0) + 1,
        );
      }
      const soldById = soldByKnown ? BigInt(rawSoldBy) : null;

      const rawCashier = o.cashierId ?? rawSoldBy;
      const cashierKnown =
        rawCashier != null && userMap.has(Number(rawCashier));
      const cashierId = cashierKnown ? BigInt(rawCashier) : null;

      return {
        kiotVietId: o.id,
        code: o.code,
        purchaseDate: o.purchaseDate ? new Date(o.purchaseDate) : now,
        branchId: o.branchId
          ? (branchMap.get(Number(o.branchId)) ?? null)
          : null,
        soldById,
        // Preserved regardless of whether the FK could be satisfied, so a deleted
        // staff member's identity is never lost. `soldById` is back-filled later
        // if that user reappears in GET /users.
        soldByKiotVietId: rawSoldBy != null ? BigInt(rawSoldBy) : null,
        soldByName: o.soldByName || null,
        cashierId,
        customerId: o.customerId
          ? (customerMap.get(Number(o.customerId)) ?? null)
          : null,
        total: o.total ?? 0,
        totalPayment: o.totalPayment ?? 0,
        discount: o.discount ?? 0,
        discountRatio: o.discountRatio ?? 0,
        status: o.status ?? null,
        description: o.description ?? '',
        usingCod: o.usingCod ?? false,
        saleChannelId: saleChannelDbId ?? 1,
        expectedDelivery: o.expectedDelivery
          ? new Date(o.expectedDelivery)
          : null,
        retailerId: this.retailer.resolve(o.retailerId),
        createdDate: o.createdDate ? new Date(o.createdDate) : now,
        modifiedDate: o.modifiedDate ? new Date(o.modifiedDate) : now,
        lastSyncedAt: now,
        statusValue: o.statusValue || null,
        customerCode: o.customerCode || null,
        customerName: o.customerName || null,
        saleChannelName: saleChannelDbId ? null : null, // resolved below if needed
      };
    });

    await this.bulkUpsert.bulkUpsert({
      table: '"Order"',
      columns: ORDER_COLUMNS,
      rows: orderRows,
      conflictTarget: '"kiotVietId"',
      updateColumns: ORDER_UPDATE,
    });

    const orderIdMap = await this.relationMap.buildIdMap(
      'order',
      this.uniqueNum(orders.map((o) => o.id)),
    );

    const detailRows: any[] = [];
    const deliveryRows: any[] = [];
    const paymentRows: any[] = [];
    const surchargeRows: any[] = [];
    /** Detail lines kept with a null productId because the product is absent locally. */
    let unresolvedProducts = 0;

    for (const o of orders) {
      const orderDbId = orderIdMap.get(Number(o.id));
      if (!orderDbId) continue;

      if (o.orderDetails) {
        o.orderDetails.forEach((d: any, idx: number) => {
          // `productId` is nullable now, so a line whose product is missing locally
          // (deleted upstream, or not yet synced) is KEPT rather than silently
          // dropped. productCode/productName/productKiotVietId preserve what was
          // sold, and the FK is back-filled when the product later syncs in.
          const product = productMap.get(Number(d.productId)) ?? null;
          if (!product) unresolvedProducts++;
          const ln = d.lineNumber ?? idx + 1;
          detailRows.push({
            orderId: orderDbId,
            productId: product,
            productKiotVietId: d.productId ? BigInt(d.productId) : null,
            quantity: d.quantity ?? 0,
            price: d.price ?? 0,
            discount: d.discount ?? 0,
            discountRatio: d.discountRatio ?? 0,
            note: d.note ?? '',
            isMaster: d.isMaster ?? true,
          productCode: d.productCode || '',
          productName: d.productName || '',
            lineNumber: ln,
          });
        });
      }

      if (o.orderDelivery) {
        const dl = o.orderDelivery;
        deliveryRows.push({
          orderId: orderDbId,
          deliveryCode: dl.deliveryCode || null,
          type: dl.type ?? null,
          price: dl.price ?? 0,
          receiver: dl.receiver ?? '',
          contactNumber: dl.contactNumber ?? '',
          address: dl.address ?? '',
          locationId: dl.locationId ?? null,
          locationName: dl.locationName ?? '',
          wardName: dl.wardName ?? '',
          weight: dl.weight ?? 0,
          length: dl.length ?? 0,
          width: dl.width ?? 0,
          height: dl.height ?? 0,
        });
      }

      if (o.payments) {
        for (const p of o.payments) {
          paymentRows.push({
            kiotVietId: p.id ? BigInt(p.id) : null,
            code: p.code || null,
            amount: p.amount ?? 0,
            method: p.method || 'Cash',
            status: p.status ?? null,
            transDate: p.transDate ? new Date(p.transDate) : now,
            accountId: p.accountId
              ? (bankAccountMap.get(Number(p.accountId)) ?? null)
              : null,
            orderId: orderDbId,
            description: p.description || null,
          });
        }
      }

      if (o.invoiceOrderSurcharges) {
        for (const s of o.invoiceOrderSurcharges) {
          surchargeRows.push({
            kiotVietId: s.id ? BigInt(s.id) : null,
            orderId: orderDbId,
            surchargeId: s.surchargeId
              ? (surchargeMap.get(Number(s.surchargeId)) ?? null)
              : null,
            surchargeName: s.surchargeName || null,
            surValue: s.surValue ?? null,
            price: s.price ?? null,
          });
        }
      }
    }

    if (unresolvedProducts > 0) {
      this.logger.warn(
        `${unresolvedProducts}/${detailRows.length} order line(s) reference a product ` +
          `missing from the Product table. Kept with productId=null (was previously ` +
          `dropped, losing the line). Re-run after a product sync to back-fill the link.`,
      );
    }

    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"OrderDetail"',
        columns: ORDER_DETAIL_COLUMNS,
        rows: detailRows,
        conflictTarget: '("orderId", "lineNumber")',
        updateColumns: ORDER_DETAIL_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"OrderDelivery"',
        columns: ORDER_DELIVERY_COLUMNS,
        rows: deliveryRows,
        conflictTarget: '"orderId"',
        updateColumns: ORDER_DELIVERY_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"Payment"',
        columns: PAYMENT_COLUMNS,
        rows: paymentRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: PAYMENT_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"OrderSurcharge"',
        columns: ORDER_SURCHARGE_COLUMNS,
        rows: surchargeRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: ORDER_SURCHARGE_UPDATE,
      }),
    ]);

    return orders.length;
  }

  /**
   * One summary line per sync run instead of a warning every page.
   *
   * These rows are not errors or data loss: `GET /users` only lists active staff,
   * so transactions made by removed staff can never satisfy the FK. The identity
   * is preserved in `soldByKiotVietId` / `soldByName`, and the user sync back-fills
   * `soldById` if that staff member ever reappears.
   */
  private reportUnknownSoldBy(processed: number): void {
    void processed;
    if (this.unknownSoldBy === 0) return;
    const staff = Array.from(this.unknownSoldByNames.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([key, count]) => {
        const [id, name] = key.split('|');
        return `${name} (id=${id}, ${count} row(s))`;
      })
      .join('; ');
    this.logger.log(
      `${this.unknownSoldBy}/${this.soldBySeen} order(s) were sold by staff no longer ` +
        `returned by GET /users, so soldById is null. Identity preserved in ` +
        `soldByKiotVietId/soldByName. Staff: ${staff}`,
    );
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
