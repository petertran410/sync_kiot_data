import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Customer, LarkSyncStatus, Prisma } from '@prisma/client';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkBaseClient } from '../lark-base.client';

const MAX_RETRIES = 8;
const LARK_BATCH_SIZE = 500;
const MAX_DRAIN_BATCHES = 1000;

const FIELD = {
  currentPoints: 'Điểm Hiện Tại',
  totalRevenue: 'Tổng Doanh Thu',
  createdDate: 'Thời Gian Tạo',
  phone: 'Số Điện Thoại',
  location: 'Khu Vực',
  groups: 'Nhóm Khách Hàng',
  name: 'Tên Khách Hàng',
  retailerId: 'Id Cửa Hàng',
  kiotVietId: 'kiotVietId',
  totalPoints: 'Tổng Điểm',
  birthDate: 'Ngày Sinh',
  email: 'Email của Khách Hàng',
  comments: 'Ghi Chú',
  identification: 'CCCD Của Khách Hàng',
  subPhone: 'Số Điện Thoại Phụ',
  taxCode: 'Mã Số Thuế',
  gender: 'Giới Tính',
  address: 'Địa Chỉ Khách Hàng',
  modifiedDate: 'Thời Gian Cập Nhật',
  ward: 'Phường xã',
  debt: 'Nợ Hiện Tại',
  type: 'Loại Khách Hàng',
  company: 'Công Ty',
  totalInvoiced: 'Tổng Bán',
  code: 'Mã Khách Hàng',
} as const;

interface SyncResult {
  processed: number;
  synced: number;
  failed: number;
  deleted: number;
  skipped: number;
}

interface CustomerLarkIndexes {
  byKiotVietId: Map<string, string>;
  byCode: Map<string, string>;
}

@Injectable()
export class LarkCustomerSyncService {
  private readonly logger = new Logger(LarkCustomerSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly kiotVietIdFieldName = FIELD.kiotVietId;
  private readonly batchSize: number;
  private running = false;

  constructor(
    config: ConfigService,
    private readonly prisma: PrismaService,
    private readonly lark: LarkBaseClient,
  ) {
    this.baseToken = this.required(config, 'LARK_CUSTOMER_BASE_ID');
    this.tableId = this.required(config, 'LARK_CUSTOMER_TABLE_ID');
    this.batchSize = Math.min(
      Math.max(Number(config.get('LARK_CUSTOMER_SYNC_BATCH_SIZE') ?? 50), 1),
      100,
    );
  }

  /** Runs one bounded queue drain. Safe from a cron, webhook, or manual trigger. */
  async syncPending(limit = this.batchSize): Promise<SyncResult> {
    if (this.running) {
      this.logger.warn('Customer Lark sync skipped: a previous batch is still running');
      return { processed: 0, synced: 0, failed: 0, deleted: 0, skipped: 0 };
    }
    this.running = true;
    try {
      return await this.syncPendingInternal(Math.min(Math.max(limit, 1), 100));
    } finally {
      this.running = false;
    }
  }

  /**
   * Sync exactly one Customer after a KiotViet webhook has committed it to DB.
   * Throws on Lark failure so the durable webhook event is retried by its worker.
   */
  async syncCustomerById(
    customerId: number,
  ): Promise<'synced' | 'deleted' | 'skipped'> {
    const customer = await this.prisma.customer.findUnique({
      where: { id: customerId },
    });
    if (!customer) throw new Error(`Customer ${customerId} not found after upsert`);

    if (customer.deletedAt) {
      await this.deleteCustomer(customer);
      this.logger.log(
        `Webhook Customer ${customer.id} (${customer.code}) deleted from Lark`,
      );
      return 'deleted';
    }

    if (!customer.contactNumber?.trim()) {
      await this.prisma.customer.update({
        where: { id: customer.id },
        data: { larkSyncStatus: LarkSyncStatus.SKIP },
      });
      this.logger.log(
        `Webhook Customer ${customer.id} (${customer.code}) skipped for Lark: contactNumber is empty`,
      );
      return 'skipped';
    }

    this.logger.log(
      `Webhook Customer ${customer.id} (${customer.code}) syncing to Lark`,
    );
    const indexes = await this.loadLarkIndexes();
    const existingRecordId = this.resolveLarkRecordId(customer, indexes);
    await this.upsertCustomer(customer, existingRecordId);
    this.logger.log(
      `Webhook Customer ${customer.id} (${customer.code}) synced to Lark`,
    );
    return 'synced';
  }

  /**
   * Drains the entire eligible Customer queue in bounded API batches. Use this
   * for a first backfill or a manual catch-up; the minute cron uses syncPending.
   */
  async drainPending(): Promise<SyncResult> {
    if (this.running) {
      this.logger.warn('Customer Lark drain skipped: a previous batch is still running');
      return { processed: 0, synced: 0, failed: 0, deleted: 0, skipped: 0 };
    }

    this.running = true;
    const total = { processed: 0, synced: 0, failed: 0, deleted: 0, skipped: 0 };
    try {
      const queued = await this.eligibleCount();
      this.logger.log(
        `Customer Lark drain started: ${queued} eligible Customer record(s), ` +
          `batch size ${this.batchSize}`,
      );

      for (let batchNumber = 1; batchNumber <= MAX_DRAIN_BATCHES; batchNumber++) {
        const result = await this.syncPendingInternal(this.batchSize);
        total.processed += result.processed;
        total.synced += result.synced;
        total.failed += result.failed;
        total.deleted += result.deleted;
        total.skipped += result.skipped;

        const remaining = await this.eligibleCount();
        this.logger.log(
          `Customer Lark drain progress: batch ${batchNumber}, ${total.processed}/${queued} processed, ` +
            `${total.synced} synced, ${total.deleted} deleted, ${total.failed} failed, ${remaining} remaining`,
        );
        if (result.processed === 0 || remaining === 0) break;
      }

      this.logger.log(
        `Customer Lark drain finished: ${total.processed} processed, ${total.synced} synced, ` +
          `${total.deleted} deleted, ${total.failed} failed`,
      );
      return total;
    } finally {
      this.running = false;
    }
  }

  private async syncPendingInternal(limit: number): Promise<SyncResult> {
    const result: SyncResult = { processed: 0, synced: 0, failed: 0, deleted: 0, skipped: 0 };
    const customers = await this.pendingCustomers(limit);
    const indexes = await this.loadLarkIndexes();
    this.logger.log(
      `Customer Lark sync started: ${customers.length} record(s) selected; ` +
        `${indexes.byKiotVietId.size} Lark ID(s), ${indexes.byCode.size} customer code(s) indexed`,
    );

    const liveCustomers = customers.filter((customer) => !customer.deletedAt);
    const deletedCustomers = customers.filter((customer) => customer.deletedAt);
    // The fresh Lark index is authoritative. larkRecordId is only a cache and
    // may point to a record that was manually removed from Base.
    const updateCustomers = liveCustomers.filter((customer) =>
      Boolean(this.resolveLarkRecordId(customer, indexes)),
    );
    const createCustomers = liveCustomers.filter(
      (customer) => !this.resolveLarkRecordId(customer, indexes),
    );

    const codeFallbackCount = updateCustomers.filter(
      (customer) =>
        !indexes.byKiotVietId.has(String(customer.kiotVietId)) &&
        indexes.byCode.has(customer.code.trim()),
    ).length;
    if (codeFallbackCount > 0) {
      this.logger.log(
        `${codeFallbackCount} Customer record(s) matched existing Lark data by Mã Khách Hàng`,
      );
    }

    const staleCacheCount = createCustomers.filter(
      (customer) => customer.larkRecordId,
    ).length;
    if (staleCacheCount > 0) {
      this.logger.warn(
        `${staleCacheCount} Customer record(s) have stale larkRecordId caches; recreating them`,
      );
    }

    await this.syncUpdates(updateCustomers, indexes, result);
    await this.syncCreates(createCustomers, result);
    await this.syncDeletes(deletedCustomers, indexes, result);

    this.logger.log(
      `Customer Lark sync finished: ${result.synced} synced, ${result.deleted} deleted, ${result.failed} failed`,
    );
    return result;
  }
  async getStats() {
    const [total, synced, pending, failed, deletedPending] = await Promise.all([
      this.prisma.customer.count(),
      this.prisma.customer.count({
        where: { larkSyncStatus: LarkSyncStatus.SYNCED },
      }),
      this.prisma.customer.count({
        where: { larkSyncStatus: LarkSyncStatus.PENDING },
      }),
      this.prisma.customer.count({
        where: { larkSyncStatus: LarkSyncStatus.FAILED },
      }),
      this.prisma.customer.count({
        where: { deletedAt: { not: null }, larkRecordId: { not: null } },
      }),
    ]);
    return { total, synced, pending, failed, deletedPending };
  }

  private async eligibleCount(): Promise<number> {
    const retryCutoff = new Date(Date.now() - 30_000);
    return this.prisma.customer.count({
      where: this.pendingWhere(retryCutoff),
    });
  }

  private pendingWhere(retryCutoff: Date) {
    return {
      OR: [
        { deletedAt: { not: null }, larkRecordId: { not: null } },
        {
          deletedAt: null,
          contactNumber: { not: '' },
          OR: [
            { larkSyncStatus: LarkSyncStatus.PENDING },
            {
              larkSyncStatus: LarkSyncStatus.FAILED,
              larkSyncRetries: { lt: MAX_RETRIES },
              OR: [
                { larkSyncedAt: null },
                { larkSyncedAt: { lt: retryCutoff } },
              ],
            },
            { larkRecordId: null },
          ],
        },
      ],
    };
  }

  private async pendingCustomers(take: number): Promise<Customer[]> {
    const retryCutoff = new Date(Date.now() - 30_000);
    return this.prisma.customer.findMany({
      where: this.pendingWhere(retryCutoff),
      orderBy: [{ deletedAt: 'desc' }, { lastSyncedAt: 'asc' }],
      take,
    });
  }

  private async syncUpdates(
    customers: Customer[],
    indexes: CustomerLarkIndexes,
    result: SyncResult,
  ): Promise<void> {
    for (const batch of this.chunks(customers, LARK_BATCH_SIZE)) {
      const records = batch.map((customer) => ({
        record_id: this.resolveLarkRecordId(customer, indexes)!,
        fields: this.fields(customer),
      }));
      try {
        await this.lark.batchUpdate(this.baseToken, this.tableId, records);
        await this.markSynced(
          batch.map((customer) => ({
            id: customer.id,
            recordId: this.resolveLarkRecordId(customer, indexes)!,
          })),
        );
        result.processed += batch.length;
        result.synced += batch.length;
        this.logger.log(
          `Lark batch update: ${batch.length} Customer record(s) synced (${result.processed} processed)`,
        );
      } catch (error: any) {
        if (this.lark.isNotFound(error)) {
          await this.recoverMissingUpdateRecords(batch, result);
        } else {
          await this.failBatch(batch, error, result, 'update');
        }
      }
    }
  }

  private async syncCreates(customers: Customer[], result: SyncResult): Promise<void> {
    for (const batch of this.chunks(customers, LARK_BATCH_SIZE)) {
      try {
        const recordIds = await this.lark.batchCreate(
          this.baseToken,
          this.tableId,
          batch.map((customer) => ({ fields: this.fields(customer) })),
        );
        if (recordIds.length !== batch.length) {
          throw new Error(
            `Lark batch create returned ${recordIds.length}/${batch.length} record IDs`,
          );
        }
        await this.markSynced(
          batch.map((customer, index) => ({ id: customer.id, recordId: recordIds[index] })),
        );
        result.processed += batch.length;
        result.synced += batch.length;
        this.logger.log(
          `Lark batch create: ${batch.length} Customer record(s) synced (${result.processed} processed)`,
        );
      } catch (error: any) {
        await this.failBatch(batch, error, result, 'create');
      }
    }
  }

  private async syncDeletes(
    customers: Customer[],
    indexes: CustomerLarkIndexes,
    result: SyncResult,
  ): Promise<void> {
    for (const batch of this.chunks(customers, LARK_BATCH_SIZE)) {
      try {
        const recordIds = batch
          .map((customer) => this.resolveLarkRecordId(customer, indexes))
          .filter((recordId): recordId is string => Boolean(recordId));
        await this.lark.batchRemove(
          this.baseToken,
          this.tableId,
          recordIds,
        );
        await this.prisma.customer.updateMany({
          where: { id: { in: batch.map((customer) => customer.id) } },
          data: {
            larkRecordId: null,
            larkSyncStatus: LarkSyncStatus.SYNCED,
            larkSyncRetries: 0,
            larkSyncedAt: new Date(),
          },
        });
        result.processed += batch.length;
        result.deleted += batch.length;
        this.logger.log(
          `Lark batch delete: ${batch.length} Customer record(s) deleted (${result.processed} processed)`,
        );
      } catch (error: any) {
        await this.failBatch(batch, error, result, 'delete');
      }
    }
  }

  /**
   * A record can disappear after the index was loaded. Refresh once, update
   * records that still exist, and recreate the missing subset.
   */
  private async recoverMissingUpdateRecords(
    customers: Customer[],
    result: SyncResult,
  ): Promise<void> {
    this.logger.warn(
      `Lark record disappeared during batch update; refreshing index for ${customers.length} Customer record(s)`,
    );
    const refreshedIndexes = await this.loadLarkIndexes();
    for (const customer of customers) {
      try {
        let recordId = this.resolveLarkRecordId(customer, refreshedIndexes);
        if (recordId) {
          try {
            await this.lark.update(
              this.baseToken,
              this.tableId,
              recordId,
              this.fields(customer),
            );
          } catch (error) {
            if (!this.lark.isNotFound(error)) throw error;
            recordId = null;
          }
        }

        if (!recordId) {
          recordId = await this.lark.create(
            this.baseToken,
            this.tableId,
            this.fields(customer),
          );
        }
        if (!recordId) {
          throw new Error(
            `Lark did not return record id for Customer ${customer.id}`,
          );
        }

        await this.markSynced([{ id: customer.id, recordId }]);
        result.processed++;
        result.synced++;
      } catch (error: any) {
        result.processed++;
        result.failed++;
        await this.markFailed(customer.id, error);
      }
    }
  }

  private async markSynced(records: Array<{ id: number; recordId: string }>): Promise<void> {
    await this.prisma.$transaction(
      records.map((record) =>
        this.prisma.customer.update({
          where: { id: record.id },
          data: {
            larkRecordId: record.recordId,
            larkSyncStatus: LarkSyncStatus.SYNCED,
            larkSyncRetries: 0,
            larkSyncedAt: new Date(),
          },
        }),
      ),
    );
  }

  private async failBatch(
    customers: Customer[],
    error: Error,
    result: SyncResult,
    operation: string,
  ): Promise<void> {
    this.logger.error(
      `Lark batch ${operation} failed for ${customers.length} Customer record(s): ${error.message}`,
    );
    for (const customer of customers) {
      result.processed++;
      result.failed++;
      await this.markFailed(customer.id, error);
    }
  }

  private chunks<T>(items: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let offset = 0; offset < items.length; offset += size) {
      chunks.push(items.slice(offset, offset + size));
    }
    return chunks;
  }

  private async loadLarkIndexes(): Promise<CustomerLarkIndexes> {
    const indexes = await this.lark.listRecordIdsByFields(
      this.baseToken,
      this.tableId,
      [FIELD.kiotVietId, FIELD.code],
    );
    return {
      byKiotVietId:
        indexes.get(FIELD.kiotVietId) ?? new Map<string, string>(),
      byCode: indexes.get(FIELD.code) ?? new Map<string, string>(),
    };
  }

  private resolveLarkRecordId(
    customer: Customer,
    indexes: CustomerLarkIndexes,
  ): string | null {
    return (
      indexes.byKiotVietId.get(String(customer.kiotVietId)) ??
      indexes.byCode.get(customer.code.trim()) ??
      null
    );
  }

  private async upsertCustomer(
    customer: Customer,
    indexedRecordId?: string | null,
  ): Promise<void> {
    const fields = this.fields(customer);
    const indexWasSupplied = indexedRecordId !== undefined;
    // A freshly indexed match by kiotVietId or customer code is authoritative.
    // Only use the DB cache when no index was supplied (legacy/manual caller).
    let recordId =
      indexedRecordId === undefined ? customer.larkRecordId : indexedRecordId;

    if (!recordId && !indexWasSupplied) {
      recordId = await this.lark.searchByKiotVietId(
        this.baseToken,
        this.tableId,
        this.kiotVietIdFieldName,
        String(customer.kiotVietId),
      );
    }

    if (recordId) {
      try {
        await this.lark.update(this.baseToken, this.tableId, recordId, fields);
      } catch (error) {
        if (!this.lark.isNotFound(error)) throw error;
        this.logger.warn(
          `Lark record ${recordId} for Customer ${customer.id} disappeared; resolving by ID/code before create`,
        );
        const refreshedIndexes = await this.loadLarkIndexes();
        recordId = this.resolveLarkRecordId(customer, refreshedIndexes);
        if (recordId) {
          await this.lark.update(
            this.baseToken,
            this.tableId,
            recordId,
            fields,
          );
        }
      }
    }

    if (recordId && recordId !== customer.larkRecordId) {
      this.logger.log(
        `Customer ${customer.id} (${customer.code}) linked to existing Lark record ${recordId}`,
      );
    }

    if (!recordId) {
      // Clear the stale cache before create so a retry cannot reuse it.
      if (customer.larkRecordId) {
        await this.prisma.customer.update({
          where: { id: customer.id },
          data: { larkRecordId: null },
        });
      }
      recordId = await this.lark.create(this.baseToken, this.tableId, fields);
    }
    if (!recordId) {
      throw new Error(`Lark did not return record id for customer ${customer.id}`);
    }

    await this.prisma.customer.update({
      where: { id: customer.id },
      data: {
        larkRecordId: recordId,
        larkSyncStatus: LarkSyncStatus.SYNCED,
        larkSyncRetries: 0,
        larkSyncedAt: new Date(),
      },
    });
  }

  private async deleteCustomer(customer: Customer): Promise<void> {
    if (customer.larkRecordId) {
      try {
        await this.lark.remove(this.baseToken, this.tableId, customer.larkRecordId);
      } catch (error) {
        if (!this.lark.isNotFound(error)) throw error;
      }
    }

    await this.prisma.customer.update({
      where: { id: customer.id },
      data: {
        larkRecordId: null,
        larkSyncStatus: LarkSyncStatus.SYNCED,
        larkSyncRetries: 0,
        larkSyncedAt: new Date(),
      },
    });
  }

  private fields(customer: Customer): Record<string, unknown> {
    const decimal = (value: Prisma.Decimal | null) =>
      value === null ? null : Number(value.toString());
    const date = (value: Date | null) => (value ? value.getTime() : null);

    return Object.fromEntries(
      Object.entries({
        [FIELD.kiotVietId]: this.safeNumber(customer.kiotVietId),
        [FIELD.name]: customer.name,
        [FIELD.code]: customer.code,
        [FIELD.phone]: customer.contactNumber,
        [FIELD.subPhone]: customer.subNumber,
        [FIELD.identification]: customer.identificationNumber,
        [FIELD.address]: customer.address,
        [FIELD.location]: customer.locationName,
        [FIELD.ward]: customer.wardName,
        [FIELD.email]: customer.email,
        [FIELD.company]: customer.organization,
        [FIELD.taxCode]: customer.taxCode,
        [FIELD.comments]: customer.comments,
        [FIELD.groups]: customer.groups,
        [FIELD.retailerId]:
          customer.retailerId === null ? null : String(customer.retailerId),
        [FIELD.debt]: this.safeNumber(decimal(customer.debt)),
        [FIELD.totalInvoiced]: this.safeNumber(decimal(customer.totalInvoiced)),
        [FIELD.totalPoints]: this.safeNumber(customer.totalPoint),
        [FIELD.totalRevenue]: this.safeNumber(decimal(customer.totalRevenue)),
        [FIELD.currentPoints]:
          customer.rewardPoint === null
            ? null
            : this.safeNumber(customer.rewardPoint),
        [FIELD.gender]:
          customer.gender === null ? null : customer.gender ? 'Nam' : 'Nữ',
        [FIELD.type]:
          customer.type === null
            ? null
            : customer.type === 1
              ? 'Công Ty'
              : 'Cá Nhân',
        [FIELD.birthDate]: date(customer.birthDate),
        [FIELD.createdDate]: date(customer.createdDate),
        [FIELD.modifiedDate]: date(customer.modifiedDate),
      }).filter(([, value]) => value !== null && value !== undefined),
    );
  }

  private safeNumber(value: unknown): number | null {
    if (value === null || value === undefined || value === '') return null;
    const number = Number(value.toString());
    return Number.isFinite(number) ? number : null;
  }

  private async markFailed(id: number, error: Error): Promise<void> {
    this.logger.error(`Customer ${id} Lark sync failed: ${error.message}`);
    await this.prisma.customer.update({
      where: { id },
      data: {
        larkSyncStatus: LarkSyncStatus.FAILED,
        larkSyncRetries: { increment: 1 },
        // Re-use larkSyncedAt as the retry-attempt timestamp so failed rows
        // respect the 30-second cooldown before the next drain pass.
        larkSyncedAt: new Date(),
      },
    });
  }

  private required(config: ConfigService, key: string): string {
    const value = config.get<string>(key);
    if (!value) throw new Error(`${key} must be configured`);
    return value;
  }
}
