import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'return_historical';

const RETURN_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'invoiceId', type: 'int' },
  { name: 'returnDate', type: 'timestamp' },
  { name: 'branchId', type: 'int' },
  { name: 'receivedById', type: 'bigint' },
  { name: 'customerId', type: 'int' },
  { name: 'returnTotal', type: 'numeric' },
  { name: 'totalPayment', type: 'numeric' },
  { name: 'status', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'branchName', type: 'text' },
  { name: 'customerCode', type: 'text' },
  { name: 'customerName', type: 'text' },
  { name: 'soldByName', type: 'text' },
];

const RETURN_UPDATE = [
  'code',
  'invoiceId',
  'returnDate',
  'branchId',
  'receivedById',
  'customerId',
  'returnTotal',
  'totalPayment',
  'status',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
  'branchName',
  'customerCode',
  'customerName',
  'soldByName',
];

const RD_COLUMNS: ColumnSpec[] = [
  { name: 'returnId', type: 'int' },
  { name: 'productId', type: 'int' },
  { name: 'productKiotVietId', type: 'bigint' },
  { name: 'quantity', type: 'real' },
  { name: 'price', type: 'numeric' },
  { name: 'note', type: 'text' },
  { name: 'usePoint', type: 'boolean' },
  { name: 'subTotal', type: 'numeric' },
  { name: 'lineNumber', type: 'int' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
];

const RD_UPDATE = [
  'productId',
  'productKiotVietId',
  'quantity',
  'price',
  'note',
  'usePoint',
  'subTotal',
  'productCode',
  'productName',
];

const PAYMENT_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'amount', type: 'numeric' },
  { name: 'method', type: 'text' },
  { name: 'status', type: 'int' },
  { name: 'transDate', type: 'timestamp' },
  { name: 'accountId', type: 'int' },
  { name: 'returnId', type: 'int' },
  { name: 'description', type: 'text' },
];

const PAYMENT_UPDATE = [
  'code',
  'amount',
  'method',
  'status',
  'transDate',
  'accountId',
  'returnId',
  'description',
];

@Injectable()
export class KiotVietReturnService {
  private readonly logger = new Logger(KiotVietReturnService.name);

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
    const fromReturnDate = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      fromReturnDate: fromReturnDate.toISOString().slice(0, 10),
    });
  }

  async syncHistoricalReturns(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Return sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['return']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/returns',
          baseParams: {
            pageSize: 100,
            includePayment: true,
            orderBy: 'id',
            orderDirection: 'DESC',
            ...extra,
          },
          label: `return-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`return-${mode}: processed ${processed} so far`);
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
      this.logger.error(`return-${mode} failed: ${error.message}`);
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

    const invoiceIds = this.uniqueNum(items.map((r) => r.invoiceId));
    const branchIds = this.uniqueNum(items.map((r) => r.branchId));
    const customerIds = this.uniqueNum(items.map((r) => r.customerId));
    const productIds = this.uniqueNum(
      items.flatMap((r) => (r.returnDetails ?? []).map((d) => d.productId)),
    );
    const bankAccountIds = this.uniqueNum(
      items.flatMap((r) =>
        (r.payments ?? []).filter((p) => p.accountId).map((p) => p.accountId),
      ),
    );

    const [invoiceMap, branchMap, customerMap, productMap, bankAccountMap] =
      await Promise.all([
        this.relationMap.buildIdMap('invoice', invoiceIds),
        this.relationMap.buildIdMap('branch', branchIds),
        this.relationMap.buildIdMap('customer', customerIds),
        this.relationMap.buildIdMap('product', productIds),
        this.relationMap.buildIdMap('bankAccount', bankAccountIds),
      ]);

    const rows = items.map((r) => ({
      kiotVietId: r.id,
      code: r.code,
      invoiceId: r.invoiceId
        ? (invoiceMap.get(Number(r.invoiceId)) ?? null)
        : null,
      returnDate: r.returnDate ? new Date(r.returnDate) : now,
      branchId: r.branchId ? (branchMap.get(Number(r.branchId)) ?? null) : null,
      receivedById: r.receivedById ? BigInt(r.receivedById) : null,
      customerId: r.customerId
        ? (customerMap.get(Number(r.customerId)) ?? null)
        : null,
      returnTotal: r.returnTotal ?? 0,
      totalPayment: r.totalPayment ?? 0,
      status: r.status ?? 0,
      createdDate: r.createdDate ? new Date(r.createdDate) : now,
      modifiedDate: r.modifiedDate ? new Date(r.modifiedDate) : now,
      lastSyncedAt: now,
      branchName: r.branchName || null,
      customerCode: r.customerCode || null,
      customerName: r.customerName || null,
      soldByName: r.soldByName || '',
    }));

    await this.bulkUpsert.bulkUpsert({
      table: '"Return"',
      columns: RETURN_COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: RETURN_UPDATE,
    });

    const returnIdMap = await this.relationMap.buildIdMap(
      'return',
      this.uniqueNum(items.map((r) => r.id)),
    );

    const detailRows: any[] = [];
    const paymentRows: any[] = [];
    /** Detail lines kept with a null productId because the product is absent locally. */
    let unresolvedProducts = 0;
    for (const r of items) {
      const returnDbId = returnIdMap.get(Number(r.id));
      if (!returnDbId) continue;
      (r.returnDetails ?? []).forEach((d: any, idx: number) => {
        // `productId` is nullable now: keep the returned line even when the product
        // row is missing locally, instead of dropping it via the FK.
        const product = productMap.get(Number(d.productId)) ?? null;
        if (!product) unresolvedProducts++;
        const ln = d.lineNumber ?? idx + 1;
        detailRows.push({
          returnId: returnDbId,
          productId: product,
          productKiotVietId: d.productId ? BigInt(d.productId) : null,
          quantity: d.quantity ?? 0,
          price: d.price ?? 0,
          note: d.note || null,
          usePoint: d.usePoint || false,
          subTotal: d.subTotal ?? 0,
          lineNumber: ln,
          productCode: d.productCode || '',
          productName: d.productName || '',
        });
      });
      for (const p of r.payments ?? []) {
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
          returnId: returnDbId,
          description: p.description || null,
        });
      }
    }

    if (unresolvedProducts > 0) {
      this.logger.warn(
        `${unresolvedProducts}/${detailRows.length} return line(s) reference a product ` +
          `missing from the Product table. Kept with productId=null (was previously ` +
          `dropped). Re-run after a product sync to back-fill the link.`,
      );
    }

    await Promise.all([
      this.bulkUpsert.bulkUpsert({
        table: '"ReturnDetail"',
        columns: RD_COLUMNS,
        rows: detailRows,
        conflictTarget: '("returnId", "lineNumber")',
        updateColumns: RD_UPDATE,
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
