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
    const existingIds = await this.lark.listRecordIdsByField(
      this.baseToken,
      this.tableId,
      this.kiotVietIdFieldName,
    );
    this.logger.log(
      `Customer Lark sync started: ${customers.length} record(s) selected; ` +
        `${existingIds.size} Lark record(s) indexed`,
    );

    const liveCustomers = customers.filter((customer) => !customer.deletedAt);
    const deletedCustomers = customers.filter((customer) => customer.deletedAt);
    const updateCustomers = liveCustomers.filter((customer) =>
      Boolean(customer.larkRecordId ?? existingIds.get(String(customer.kiotVietId))),
    );
    const createCustomers = liveCustomers.filter(
      (customer) =>
        !customer.larkRecordId && !existingIds.has(String(customer.kiotVietId)),
    );

    await this.syncUpdates(updateCustomers, existingIds, result);
    await this.syncCreates(createCustomers, result);
    await this.syncDeletes(deletedCustomers, result);

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
    existingIds: Map<string, string>,
    result: SyncResult,
  ): Promise<void> {
    for (const batch of this.chunks(customers, LARK_BATCH_SIZE)) {
      const records = batch.map((customer) => ({
        record_id:
          customer.larkRecordId ?? existingIds.get(String(customer.kiotVietId))!,
        fields: this.fields(customer),
      }));
      try {
        await this.lark.batchUpdate(this.baseToken, this.tableId, records);
        await this.markSynced(
          batch.map((customer) => ({
            id: customer.id,
            recordId:
              customer.larkRecordId ?? existingIds.get(String(customer.kiotVietId))!,
          })),
        );
        result.processed += batch.length;
        result.synced += batch.length;
        this.logger.log(
          `Lark batch update: ${batch.length} Customer record(s) synced (${result.processed} processed)`,
        );
      } catch (error: any) {
        await this.failBatch(batch, error, result, 'update');
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

  private async syncDeletes(customers: Customer[], result: SyncResult): Promise<void> {
    for (const batch of this.chunks(customers, LARK_BATCH_SIZE)) {
      try {
        await this.lark.batchRemove(
          this.baseToken,
          this.tableId,
          batch.map((customer) => customer.larkRecordId!).filter(Boolean),
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

  private async upsertCustomer(customer: Customer): Promise<void> {
    const fields = this.fields(customer);
    let recordId = customer.larkRecordId;

    if (!recordId) {
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
        recordId = null;
      }
    }

    if (!recordId) {
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
