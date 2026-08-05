import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'transfer_historical';

const TRANSFER_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'code', type: 'text' },
  { name: 'description', type: 'text' },
  { name: 'dispatchedDate', type: 'timestamp' },
  { name: 'fromBranchId', type: 'int' },
  { name: 'receivedDate', type: 'timestamp' },
  { name: 'retailerId', type: 'int' },
  { name: 'status', type: 'int' },
  { name: 'toBranchId', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const TRANSFER_UPDATE = [
  'code',
  'description',
  'dispatchedDate',
  'fromBranchId',
  'receivedDate',
  'retailerId',
  'status',
  'toBranchId',
  'lastSyncedAt',
];

const TD_COLUMNS: ColumnSpec[] = [
  { name: 'productId', type: 'int' },
  { name: 'productCode', type: 'text' },
  { name: 'productName', type: 'text' },
  { name: 'sendQuantity', type: 'int' },
  { name: 'receivedQuantity', type: 'int' },
  { name: 'sendPrice', type: 'int' },
  { name: 'receivePrice', type: 'int' },
  { name: 'price', type: 'int' },
  { name: 'transferId', type: 'int' },
  { name: 'lineNumber', type: 'int' },
  { name: 'uniqueKey', type: 'text' },
];

const TD_UPDATE = [
  'productId',
  'productCode',
  'productName',
  'sendQuantity',
  'receivedQuantity',
  'sendPrice',
  'receivePrice',
  'price',
  'uniqueKey',
];

@Injectable()
export class KiotVietTransferService {
  private readonly logger = new Logger(KiotVietTransferService.name);

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
    const fromReceivedDate = last ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      fromReceivedDate: fromReceivedDate.toISOString().slice(0, 10),
    });
  }

  async syncHistoricalTransfers(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Transfer sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['transfer']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/transfers',
          baseParams: extra,
          label: `transfer-${mode}`,
          onPage: async (pageData) => {
            processed += await this.processPage(pageData);
            this.logger.log(`transfer-${mode}: processed ${processed} so far`);
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
      this.logger.error(`transfer-${mode} failed: ${error.message}`);
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

    const productIds = this.uniqueNum(
      items.flatMap((t) => (t.transferDetails ?? []).map((d) => d.productId)),
    );
    const productMap = await this.relationMap.buildIdMap('product', productIds);

    const rows = items.map((t) => ({
      kiotVietId: t.id,
      code: (t.code || '').trim(),
      description: t.description ?? '',
      dispatchedDate: t.dispatchedDate ? new Date(t.dispatchedDate) : now,
      fromBranchId: t.fromBranchId ?? null,
      receivedDate: t.receivedDate ? new Date(t.receivedDate) : now,
      retailerId: t.retailerId ?? null,
      status: t.status ?? null,
      toBranchId: t.toBranchId ?? null,
      lastSyncedAt: now,
    }));

    await this.bulkUpsert.bulkUpsert({
      table: '"Transfer"',
      columns: TRANSFER_COLUMNS,
      rows,
      conflictTarget: '"kiotVietId"',
      updateColumns: TRANSFER_UPDATE,
    });

    const transferIdMap = await this.relationMap.buildIdMap(
      'transfer',
      this.uniqueNum(items.map((t) => t.id)),
    );

    const detailRows: any[] = [];
    for (const t of items) {
      const transferDbId = transferIdMap.get(Number(t.id));
      if (!transferDbId) continue;
      (t.transferDetails ?? []).forEach((d: any, idx: number) => {
        const product = productMap.get(Number(d.productId));
        if (!product) return;
        const ln = d.lineNumber ?? idx + 1;
        detailRows.push({
          productId: product,
          productCode: d.productCode || null,
          productName: d.productName || null,
          sendQuantity: d.sendQuantity ?? 0,
          receivedQuantity: d.receiveQuantity ?? 0,
          sendPrice: d.sendPrice ?? 0,
          receivePrice: d.receivePrice ?? 0,
          price: d.price ?? 0,
          transferId: transferDbId,
          lineNumber: ln,
          uniqueKey: `${t.id}.${ln}`,
        });
      });
    }

    await this.bulkUpsert.bulkUpsert({
      table: '"TransferDetail"',
      columns: TD_COLUMNS,
      rows: detailRows,
      conflictTarget: '("transferId", "lineNumber")',
      updateColumns: TD_UPDATE,
    });

    return items.length;
  }

  private uniqueNum(arr: any[]): number[] {
    return Array.from(
      new Set(arr.filter((v) => v !== null && v !== undefined).map(Number)),
    );
  }
}
