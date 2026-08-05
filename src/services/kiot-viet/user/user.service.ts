import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'user_historical';

const USER_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'userName', type: 'text' },
  { name: 'givenName', type: 'text' },
  { name: 'mobilePhone', type: 'text' },
  { name: 'email', type: 'text' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'userName',
  'givenName',
  'mobilePhone',
  'email',
  'retailerId',
  'createdDate',
];

@Injectable()
export class KiotVietUserService {
  private readonly logger = new Logger(KiotVietUserService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly removedIdsHandler: RemovedIdsHandler,
  ) {}

  /** Full sync — no date filter. */
  async syncFull(): Promise<{ total: number; processed: number }> {
    return this.runSync('full', {});
  }

  /** Incremental sync — only records modified since last successful sync. */
  async syncIncremental(): Promise<{ total: number; processed: number }> {
    const lastCompletedAt =
      await this.syncControl.getLastCompletedAt(SYNC_NAME);
    const lastModifiedFrom = lastCompletedAt ?? new Date('2024-12-01');
    return this.runSync('incremental', {
      lastModifiedFrom: lastModifiedFrom.toISOString(),
    });
  }

  /** Backward-compat alias. */
  async syncHistoricalUsers(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extraParams: Record<string, any>,
  ): Promise<{ total: number; processed: number }> {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`User sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['user']);
    let processed = 0;
    let total = 0;
    try {
      const { total: t, serverTimestamp, removedIds } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/users',
          baseParams: { includeRemoveIds: true, ...extraParams },
          label: `user-${mode}`,
          onPage: async (pageData) => {
            const rows = pageData.map((u: any) => ({
              kiotVietId: u.id,
              userName: u.userName,
              givenName: u.givenName,
              mobilePhone: u.mobilePhone || '',
              email: u.email || '',
              retailerId: u.retailerId ?? null,
              createdDate: u.createdDate ? new Date(u.createdDate) : new Date(),
            }));
            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"User"',
              columns: USER_COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `user-${mode}: saved ${rows.length} (affected ${affected}), total processed ${processed}`,
            );
          },
        });
      total = t;
      // Staff that were previously unresolvable (deleted upstream, then restored,
      // or simply synced after the transactions were) can now satisfy the FK.
      await this.backfillSoldByReferences();
      // Stamp rows KiotViet reports as deleted. Without webhooks this is the
      // only deletion signal, and it was previously never read.
      if (removedIds?.length) {
        await this.removedIdsHandler.apply('user', removedIds);
      }

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: processed, expectedTotal: total },
        serverTimestamp,
      );
      this.logger.log(`user-${mode} completed: ${processed}/${total}`);
      return { total, processed };
    } catch (error) {
      this.logger.error(`user-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  /**
   * Link `Order.soldById` / `Invoice.soldById` for rows that were stored with a
   * null FK because the staff member was missing from the User table at the time.
   * `soldByKiotVietId` retains the original id, so the link can be restored once
   * that user exists locally. Rows whose staff member is still absent are left
   * untouched — the identity is still readable via soldByKiotVietId/soldByName.
   */
  private async backfillSoldByReferences(): Promise<void> {
    const [orders, invoices] = await this.prismaService.$transaction([
      this.prismaService.$executeRaw`
        UPDATE "Order" AS o
        SET "soldById" = u."kiotVietId"
        FROM "User" AS u
        WHERE o."soldById" IS NULL
          AND o."soldByKiotVietId" = u."kiotVietId"
      `,
      this.prismaService.$executeRaw`
        UPDATE "Invoice" AS i
        SET "soldById" = u."kiotVietId"
        FROM "User" AS u
        WHERE i."soldById" IS NULL
          AND i."soldByKiotVietId" = u."kiotVietId"
      `,
    ]);

    if (orders + invoices > 0) {
      this.logger.log(
        `Back-filled soldById on ${orders} order(s) and ${invoices} invoice(s)`,
      );
    }
  }
}
