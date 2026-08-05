import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RetailerContext } from '../shared/retailer-context';
import { lineSubTotal } from '../shared/line-math';

const SYNC_NAME = 'invoice_historical';

const INVOICE_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'orderCode', type: 'text' },
  { name: 'orderId', type: 'int' },
  { name: 'purchaseDate', type: 'timestamp' },
  { name: 'branchId', type: 'int' },
  { name: 'soldById', type: 'bigint' },
  { name: 'soldByKiotVietId', type: 'bigint' },
  { name: 'soldByName', type: 'text' },
  { name: 'customerId', type: 'int' },
  { name: 'total', type: 'numeric' },
  { name: 'totalPayment', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'discountRatio', type: 'real' },
  { name: 'status', type: 'int' },
  { name: 'description', type: 'text' },
  { name: 'usingCod', type: 'boolean' },
  { name: 'saleChannelId', type: 'int' },
  { name: 'isApplyVoucher', type: 'boolean' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'customerCode', type: 'text' },
  { name: 'customerName', type: 'text' },
  { name: 'statusValue', type: 'text' },
];

const INVOICE_UPDATE = [
  'code',
  'orderCode',
  'orderId',
  'purchaseDate',
  'branchId',
  'soldById',
  'soldByKiotVietId',
  'soldByName',
  'customerId',
  'total',
  'totalPayment',
  'discount',
  'discountRatio',
  'status',
  'description',
  'usingCod',
  'saleChannelId',
  'isApplyVoucher',
  'retailerId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
  'customerCode',
  'customerName',
  'statusValue',
];

const INVOICE_DETAIL_COLUMNS: ColumnSpec[] = [
  { name: 'invoiceId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'invoiceKiotVietId', type: 'bigint' },
  { name: 'productKiotVietId', type: 'bigint' },
  { name: 'quantity', type: 'real' },
  { name: 'price', type: 'numeric' },
  { name: 'discount', type: 'numeric' },
  { name: 'discountRatio', type: 'real' },
  { name: 'note', type: 'text' },
  { name: 'serialNumbers', type: 'text' },
  { name: 'subTotal', type: 'numeric' },
  { name: 'lineNumber', type: 'int' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'uniqueKey', type: 'text' },
];

const INVOICE_DETAIL_UPDATE = [
  'productId',
  'invoiceKiotVietId',
  'productKiotVietId',
  'quantity',
  'price',
  'discount',
  'discountRatio',
  'note',
  'serialNumbers',
  'subTotal',
  'productCode',
  'productName',
  'uniqueKey',
];

const INVOICE_DELIVERY_COLUMNS: ColumnSpec[] = [
  { name: 'invoiceId', type: 'int' },
  { name: 'deliveryCode', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'type', type: 'int' },
  { name: 'price', type: 'numeric' },
  { name: 'receiver', type: 'text' },
  { name: 'contactNumber', type: 'text' },
  { name: 'address', type: 'text' },
  { name: 'locationId', type: 'int' },
  { name: 'locationName', type: 'text' },
  { name: 'wardName', type: 'text' },
  { name: 'usingPriceCod', type: 'boolean' },
  { name: 'priceCodPayment', type: 'numeric' },
  { name: 'weight', type: 'real' },
  { name: 'length', type: 'real' },
  { name: 'width', type: 'real' },
  { name: 'height', type: 'real' },
];

const INVOICE_DELIVERY_UPDATE = [
  'deliveryCode',
  'status',
  'type',
  'price',
  'receiver',
  'contactNumber',
  'address',
  'locationId',
  'locationName',
  'wardName',
  'usingPriceCod',
  'priceCodPayment',
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
  { name: 'invoiceId', type: 'int' },
  { name: 'description', type: 'text' },
];

const PAYMENT_UPDATE = [
  'code',
  'amount',
  'method',
  'status',
  'transDate',
  'accountId',
  'invoiceId',
  'description',
];

const INVOICE_SURCHARGE_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'invoiceId', type: 'int' },
  { name: 'surchargeId', type: 'int' },
  { name: 'surchargeName', type: 'text' },
  { name: 'surValue', type: 'numeric' },
  { name: 'price', type: 'numeric' },
];

const INVOICE_SURCHARGE_UPDATE = [
  'surchargeId',
  'surchargeName',
  'surValue',
  'price',
];

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
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
    const fromPurchaseDate = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      fromPurchaseDate: fromPurchaseDate.toISOString().slice(0, 10),
    });
  }

  async syncHistoricalInvoices(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Invoice sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['invoice']);
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
          endpoint: '/invoices',
          baseParams: {
            orderBy: 'id',
            orderDirection: 'DESC',
            includeInvoiceDelivery: true,
            includePayment: true,
            includeTotal: true,
            ...extra,
          },
          label: `invoice-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`invoice-${mode}: processed ${processed} so far`);
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
      this.logger.error(`invoice-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  private async processPage(invoices: any[]): Promise<number> {
    if (!invoices.length) return 0;
    const now = new Date();

    // Collect relation ids
    const branchIds = this.uniqueNum(invoices.map((i) => i.branchId));
    const customerIds = this.uniqueNum(invoices.map((i) => i.customerId));
    const saleChannelIds = this.uniqueNum(invoices.map((i) => i.saleChannelId));
    const orderIds = this.uniqueNum(invoices.map((i) => i.orderId));
    const productIds = this.uniqueNum(
      invoices.flatMap((i) => (i.invoiceDetails ?? []).map((d) => d.productId)),
    );
    const bankAccountIds = this.uniqueNum(
      invoices.flatMap((i) =>
        (i.payments ?? []).filter((p) => p.accountId).map((p) => p.accountId),
      ),
    );
    const surchargeIds = this.uniqueNum(
      invoices.flatMap((i) =>
        (i.invoiceOrderSurcharges ?? [])
          .filter((s) => s.surchargeId)
          .map((s) => s.surchargeId),
      ),
    );

    const soldByIds = this.uniqueNum(
      invoices.map((i) => i.soldById).filter((v) => v != null),
    );

    const [
      branchMap,
      customerMap,
      saleChannelMap,
      orderMap,
      productMap,
      bankAccountMap,
      surchargeMap,
      userMap,
    ] = await Promise.all([
      this.relationMap.buildIdMap('branch', branchIds),
      this.relationMap.buildIdMap('customer', customerIds),
      this.relationMap.buildIdMap('saleChannel', saleChannelIds),
      this.relationMap.buildIdMap('order', orderIds),
      this.relationMap.buildIdMap('product', productIds),
      this.relationMap.buildIdMap('bankAccount', bankAccountIds),
      this.relationMap.buildIdMap('surcharge', surchargeIds),
      // Invoice.soldById is a FK onto User.kiotVietId, but GET /users returns only
      // active staff while invoices keep referencing removed staff. Unresolvable ids
      // violated Invoice_soldById_fkey and the invoice was dropped silently.
      this.relationMap.buildIdMap('user', soldByIds),
    ]);

    // Build + upsert invoices
    const invoiceRows = invoices.map((inv) => {
      const rawSoldBy = inv.soldById ?? null;
      const soldByKnown = rawSoldBy != null && userMap.has(Number(rawSoldBy));
      this.soldBySeen++;
      if (rawSoldBy != null && !soldByKnown) {
        this.unknownSoldBy++;
        const key = `${rawSoldBy}|${inv.soldByName || 'unknown'}`;
        this.unknownSoldByNames.set(
          key,
          (this.unknownSoldByNames.get(key) ?? 0) + 1,
        );
      }

      return {
        kiotVietId: inv.id,
        code: inv.code,
        orderCode: inv.orderCode || null,
        orderId: inv.orderId
          ? (orderMap.get(Number(inv.orderId)) ?? null)
          : null,
        purchaseDate: inv.purchaseDate ? new Date(inv.purchaseDate) : now,
        branchId: inv.branchId
          ? (branchMap.get(Number(inv.branchId)) ?? null)
          : null,
        soldById: soldByKnown ? BigInt(rawSoldBy) : null,
        // Retained even when the FK cannot be satisfied, so a deleted staff
        // member's identity survives. Back-filled if the user syncs in later.
        soldByKiotVietId: rawSoldBy != null ? BigInt(rawSoldBy) : null,
        soldByName: inv.soldByName || null,
        customerId: inv.customerId
          ? (customerMap.get(Number(inv.customerId)) ?? null)
          : null,
        total: inv.total ?? 0,
        totalPayment: inv.totalPayment ?? 0,
        discount: inv.discount ?? 0,
        discountRatio: inv.discountRatio || 0,
        status: inv.status ?? 0,
        description: inv.description || null,
        usingCod: inv.usingCod || false,
        saleChannelId: inv.saleChannelId
          ? (saleChannelMap.get(Number(inv.saleChannelId)) ?? 1)
          : 1,
        isApplyVoucher: inv.isApplyVoucher || false,
        retailerId: this.retailer.resolve(inv.retailerId),
        createdDate: inv.createdDate ? new Date(inv.createdDate) : now,
        modifiedDate: inv.modifiedDate ? new Date(inv.modifiedDate) : now,
        lastSyncedAt: now,
        customerCode: inv.customerCode || null,
        customerName: inv.customerName || null,
        statusValue: inv.statusValue || null,
      };
    });

    await this.bulkUpsert.bulkUpsert({
      table: '"Invoice"',
      columns: INVOICE_COLUMNS,
      rows: invoiceRows,
      conflictTarget: '"kiotVietId"',
      updateColumns: INVOICE_UPDATE,
    });

    // Map invoice kiotVietId -> db id
    const invoiceIdMap = await this.relationMap.buildIdMap(
      'invoice',
      this.uniqueNum(invoices.map((i) => i.id)),
    );

    // Build children
    const detailRows: any[] = [];
    /** Detail lines kept with a null productId because the product is absent locally. */
    let unresolvedProducts = 0;
    const deliveryRows: any[] = [];
    const paymentRows: any[] = [];
    const surchargeRows: any[] = [];

    for (const inv of invoices) {
      const invoiceDbId = invoiceIdMap.get(Number(inv.id));
      if (!invoiceDbId) continue;
      const invoiceKvId = BigInt(inv.id);

      // Details
      if (inv.invoiceDetails) {
        inv.invoiceDetails.forEach((d: any, idx: number) => {
          // `productId` is nullable now: a line whose product is absent locally
          // (deleted upstream, or not yet synced) is KEPT instead of being dropped
          // by the FK. productCode/productName/productKiotVietId preserve what was
          // sold, so invoice totals still reconcile against KiotViet.
          const product = productMap.get(Number(d.productId)) ?? null;
          if (!product) unresolvedProducts++;
          const ln = d.lineNumber ?? idx + 1;
          detailRows.push({
            invoiceId: invoiceDbId,
            productId: product,
            invoiceKiotVietId: invoiceKvId,
            productKiotVietId: d.productId ? BigInt(d.productId) : null,
            quantity: d.quantity ?? 0,
            price: d.price ?? 0,
            discount: d.discount ?? null,
            discountRatio: d.discountRatio ?? null,
            note: d.note || null,
            serialNumbers: d.serialNumbers || null,
            subTotal: lineSubTotal({ subTotal: d.subTotal, price: d.price, discount: d.discount, quantity: d.quantity }),
            lineNumber: ln,
            productCode: d.productCode || null,
            productName: d.productName || null,
            uniqueKey: `${inv.id}.${ln}`,
          });
        });
      }

      // Delivery
      if (inv.invoiceDelivery) {
        const dl = inv.invoiceDelivery;
        deliveryRows.push({
          invoiceId: invoiceDbId,
          deliveryCode: dl.deliveryCode || null,
          status: dl.status ?? 0,
          type: dl.type ?? null,
          price: dl.price ?? null,
          receiver: dl.receiver || null,
          contactNumber: dl.contactNumber || null,
          address: dl.address || null,
          locationId: dl.locationId ?? null,
          locationName: dl.locationName || null,
          wardName: dl.wardName || null,
          usingPriceCod: dl.usingPriceCod || false,
          priceCodPayment: dl.priceCodPayment ?? null,
          weight: dl.weight ?? null,
          length: dl.length ?? null,
          width: dl.width ?? null,
          height: dl.height ?? null,
        });
      }

      // Payments
      if (inv.payments) {
        for (const p of inv.payments) {
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
            invoiceId: invoiceDbId,
            description: p.description || null,
          });
        }
      }

      // Surcharges
      if (inv.invoiceOrderSurcharges) {
        for (const s of inv.invoiceOrderSurcharges) {
          surchargeRows.push({
            kiotVietId: s.id ? BigInt(s.id) : null,
            invoiceId: invoiceDbId,
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

    // Bulk upsert children in parallel
    if (unresolvedProducts > 0) {
      this.logger.warn(
        `${unresolvedProducts}/${detailRows.length} invoice line(s) reference a product ` +
          `missing from the Product table. Kept with productId=null (was previously ` +
          `dropped, losing revenue lines). Re-run after a product sync to back-fill.`,
      );
    }

    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"InvoiceDetail"',
        columns: INVOICE_DETAIL_COLUMNS,
        rows: detailRows,
        conflictTarget: '("invoiceId", "lineNumber")',
        updateColumns: INVOICE_DETAIL_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"InvoiceDelivery"',
        columns: INVOICE_DELIVERY_COLUMNS,
        rows: deliveryRows,
        conflictTarget: '"invoiceId"',
        updateColumns: INVOICE_DELIVERY_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"Payment"',
        columns: PAYMENT_COLUMNS,
        rows: paymentRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: PAYMENT_UPDATE,
      }),
      this.bulkUpsert.bulkUpsert({
        table: '"InvoiceSurcharge"',
        columns: INVOICE_SURCHARGE_COLUMNS,
        rows: surchargeRows,
        conflictTarget: '"kiotVietId"',
        updateColumns: INVOICE_SURCHARGE_UPDATE,
      }),
    ]);

    return invoices.length;
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
      `${this.unknownSoldBy}/${this.soldBySeen} invoice(s) were sold by staff no longer ` +
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
