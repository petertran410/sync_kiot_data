import { LarkAuthService } from './../auth/lark-auth.service';
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { firstValueFrom } from 'rxjs';

const LARK_CASHFLOW_FIELDS = {
  PRIMARY_CODE: 'Mã Sổ Quỹ',
  KIOTVIET_ID: 'kiotVietId',
  WARD: 'Phường',
  PAYMENT_METHOD: 'Phương Thức Thanh Toán',
  RECEIVER_TYPE: 'Loại Người Nộp',
  SORT_BUSINESS: 'Lọc Kết Quả Kinh Doanh',
  CUSTOMER_NAME: 'Tên Khách Hàng',
  STATUS: 'Tình Trạng',
  TRANS_DATE: 'Ngày Chuyển Tiền',
  AMOUNT: 'Số Tiền',
  CREATOR: 'Người Tạo',
  DESCRIPTION: 'Mô Tả',
  BRANCH: 'Chi Nhánh',
  CASH_GROUP: 'Nhóm Tiền',
  ADDRESS: 'Địa Chỉ',
  CASH_ORIGIN: 'Loại Tiền Gốc',
  CUSTOMER_ID: 'Id Khách Hàng',
} as const;

const PARTNER_TYPE = {
  KHACH_HANG: 'Khách Hàng',
  NHA_CUNG_CAP: 'Nhà Cung Cấp',
  DOI_TAC_GIAO_HANG: 'Đối Tác Giao Hàng',
};

const USE_REPORT = {
  KHONG_HOACH_TOAN: 'Không Hoạch Toán',
  HOACH_TOAN: 'Đưa Vào Hoạch Toán',
};

interface LarkBaseRecord {
  record_id?: string;
  fields: Record<string, any>;
}

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    items?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

interface BatchResult {
  successRecords: any[];
  failedRecords: any[];
}

@Injectable()
export class LarkCashflowSyncService {
  private readonly logger = new Logger(LarkCashflowSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();

  private existingRecordsCache = new Map<number, string>();
  private cashflowCodeCache = new Map<string, string>();
  private cacheLoaded = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 600;
  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_CASHFLOW_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>('LARK_CASHFLOW_TABLE_ID');

    if (!baseToken || !tableId) {
      throw new Error('LarkBase casflow configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncCashflowToLarkBase(cashflows: any[]): Promise<void> {
    const lockKey = `lark_cashflow_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${cashflows.length} cashflows...`,
      );

      const cashflowsToSync = cashflows.filter(
        (o) => o.larkSyncStatus === 'PENDING' || o.larkSyncStatus === 'FAILED',
      );

      if (cashflowsToSync.length === 0) {
        this.logger.log('No cashflows need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (cashflowsToSync.length < 5) {
        this.logger.log(
          `Small sync (${cashflowsToSync.length} cashflows) - using lightweight mode`,
        );
        await this.syncWithoutCache(cashflowsToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = cashflows.filter(
        (o) => o.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = cashflows.filter(
        (o) => o.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingCount} PENDING + ${failedCount} FAILED cashflows`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(cashflowsToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const { newCashflows, updateCashflows } =
        await this.categorizeCashflows(cashflowsToSync);

      this.logger.log(
        `Categorization: ${newCashflows.length} new, ${updateCashflows.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newCashflows.length > 0) {
        for (let i = 0; i < newCashflows.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newCashflows.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new cashflows batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newCashflows.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewCashflows(batch);
        }
      }

      if (updateCashflows.length > 0) {
        for (let i = 0; i < updateCashflows.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateCashflows.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update cashflows batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateCashflows.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateCashflows(batch);
        }
      }

      this.logger.log('LarkBase cashflow sync completed!');
    } catch (error) {
      this.logger.error(`LarkBase cashflow sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (this.isCacheValid() && this.existingRecordsCache.size > 5000) {
          this.logger.log(
            `Large cache available (${this.existingRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastCacheLoadTime) {
          const cacheAgeMinutes =
            (Date.now() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
          if (cacheAgeMinutes < 45 && this.existingRecordsCache.size > 500) {
            this.logger.log(
              `Recent cache (${cacheAgeMinutes.toFixed(1)}min old, ${this.existingRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearCache();
        await this.loadExistingRecords();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = attempt * 2000;
          this.logger.log(`Waiting ${delay / 1000}s before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }
    return false;
  }

  private isCacheValid(): boolean {
    if (!this.cacheLoaded || !this.lastCacheLoadTime) {
      return false;
    }

    const cacheAge = Date.now() - this.lastCacheLoadTime.getTime();
    const maxAge = this.CACHE_VALIDITY_MINUTES * 60 * 1000;

    return cacheAge < maxAge && this.existingRecordsCache.size > 0;
  }

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getCashflowHeaders();
      let pageToken = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      const pageSize = 1000;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params: any = {
          page_size: pageSize,
          ...(pageToken && { page_token: pageToken }),
        };

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(url, {
            headers,
            params,
            timeout: 15000,
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          for (const record of records) {
            const kiotVietIdField =
              record.fields[LARK_CASHFLOW_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const cashflowCodeField =
              record.fields[LARK_CASHFLOW_FIELDS.PRIMARY_CODE];
            if (cashflowCodeField) {
              this.cashflowCodeCache.set(
                String(cashflowCodeField).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token || '';

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheBuilt}/${totalLoaded} records (${loadTime}ms/page)`,
            );
          }
        } else {
          throw new Error(
            `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
          );
        }
      } while (pageToken);

      this.cacheLoaded = true;
      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `Cashflow cache loaded: ${this.existingRecordsCache.size} by ID, ${this.cashflowCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cashflow cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeCashflows(cashflows: any[]): Promise<any> {
    const newCashflows: any[] = [];
    const updateCashflows: any[] = [];

    const duplicateDetected = cashflows.filter((cashflow) => {
      const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);
      return this.existingRecordsCache.has(kiotVietId);
    });

    if (duplicateDetected.length > 0) {
      this.logger.warn(
        `Detected ${duplicateDetected.length} cashflows already in cache: ${duplicateDetected
          .map((o) => o.kiotVietId)
          .slice(0, 5)
          .join(', ')}`,
      );
    }

    for (const cashflow of cashflows) {
      const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Cashflow ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && cashflow.code) {
        existingRecordId = this.cashflowCodeCache.get(
          String(cashflow.code).trim(),
        );
      }

      if (existingRecordId) {
        updateCashflows.push({ ...cashflow, larkRecordId: existingRecordId });
      } else {
        this.pendingCreation.add(kiotVietId);
        newCashflows.push(cashflow);
      }
    }

    return { newCashflows, updateCashflows };
  }

  private async syncWithoutCache(cashflows: any[]): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingCashflows = await this.prismaService.cashflow.findMany({
      where: {
        kiotVietId: { in: cashflows.map((o) => o.kiotVietId) },
      },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingCashflows.forEach((o) => {
      if (o.larkRecordId) {
        quickCache.set(Number(o.kiotVietId), o.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newCashflows, updateCashflows } =
        await this.categorizeCashflows(cashflows);

      if (newCashflows.length > 0) {
        await this.processNewCashflows(newCashflows);
      }

      if (updateCashflows.length > 0) {
        await this.processUpdateCashflows(updateCashflows);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewCashflows(cashflows: any[]): Promise<void> {
    if (cashflows.length === 0) return;

    this.logger.log(`Creating ${cashflows.length} new cashflows...`);

    const batches = this.chunkArray(cashflows, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const cashflow of batch) {
        const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(cashflow);
        } else {
          this.logger.warn(
            `Skipping duplicate cashflow ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all cashflows already exist`);
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} cashflows)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateCashflows(verifiedBatch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processUpdateCashflows(cashflows: any[]): Promise<void> {
    if (cashflows.length === 0) return;

    this.logger.log(`Updating ${cashflows.length} existing cashflows...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 100;

    for (let i = 0; i < cashflows.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = cashflows.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (cashflow) => {
          try {
            const updated = await this.updateSingleCashflow(cashflow);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([cashflow], 'SYNCED');
            } else {
              createFallbacks.push(cashflow);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${cashflow.code}: ${error.message}`,
            );
            createFallbacks.push(cashflow);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < cashflows.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `Processing ${createFallbacks.length} update fallbacks as new cashflows...`,
      );
      await this.processNewCashflows(createFallbacks);
    }

    this.logger.log(
      `Update complete: ${successCount} updated, ${failedCount} failed, ${createFallbacks.length} fallback to create`,
    );
  }

  private async batchCreateCashflows(cashflows: any[]): Promise<BatchResult> {
    const records = cashflows.map((cashflow) => ({
      fields: this.mapCashflowToLarkBase(cashflow),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCashflowHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          const successCount = createdRecords.length;
          const successRecords = cashflows.slice(0, successCount);
          const failedRecords = cashflows.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const cashflow = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            successRecords.forEach((cashflow) => {
              const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            failedRecords.forEach((cashflow) => {
              const kiotVietId = this.safeBigIntToNumber(cashflow.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            if (cashflow.code) {
              this.cashflowCodeCache.set(
                String(cashflow.code).trim(),
                createdRecord.record_id,
              );
            }
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshCashflowToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: cashflows };
      } catch (error) {
        this.logger.error('Batch create error details:', {
          status: error.response?.status,
          statusText: error.response?.statusText,
          data: error.response?.data,
          config: {
            url: error.config?.url,
            method: error.config?.method,
            data: JSON.parse(error.config?.data || '{}'),
          },
        });

        if (records && records.length > 0) {
          this.logger.error(
            'Sample record being sent:',
            JSON.stringify(records[0], null, 2),
          );
        }

        return { successRecords: [], failedRecords: cashflows };
      }
    }

    return { successRecords: [], failedRecords: cashflows };
  }

  private async updateSingleCashflow(cashflow: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCashflowHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${cashflow.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapCashflowToLarkBase(cashflow) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${cashflow.larkRecordId} for cashflow ${cashflow.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshCashflowToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshCashflowToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${cashflow.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private mapCashflowToLarkBase(cashflow: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (cashflow.code) {
      fields[LARK_CASHFLOW_FIELDS.PRIMARY_CODE] = cashflow.code;
    }

    if (cashflow.kiotVietId !== null && cashflow.kiotVietId !== undefined) {
      fields[LARK_CASHFLOW_FIELDS.KIOTVIET_ID] = Number(cashflow.kiotVietId);
    }

    if (cashflow.wardName) {
      fields[LARK_CASHFLOW_FIELDS.WARD] = cashflow.wardName || '';
    }

    if (cashflow.method) {
      if (cashflow.method === 'Transfer') {
        fields[LARK_CASHFLOW_FIELDS.PAYMENT_METHOD] = 'Chuyển Khoản';
      }
      if (cashflow.method === 'Cash') {
        fields[LARK_CASHFLOW_FIELDS.PAYMENT_METHOD] = 'Tiền Mặt';
      }
    }

    if (cashflow.partnerType) {
      if (cashflow.partnerType === 'C') {
        fields[LARK_CASHFLOW_FIELDS.RECEIVER_TYPE] = PARTNER_TYPE.KHACH_HANG;
      }

      if (cashflow.partnerType === 'S') {
        fields[LARK_CASHFLOW_FIELDS.RECEIVER_TYPE] = PARTNER_TYPE.NHA_CUNG_CAP;
      }

      if (cashflow.partnerType === 'D') {
        fields[LARK_CASHFLOW_FIELDS.RECEIVER_TYPE] =
          PARTNER_TYPE.DOI_TAC_GIAO_HANG;
      }
    }

    if (cashflow.usedForFinancialReporting) {
      if (cashflow.usedForFinancialReporting === 0) {
        fields[LARK_CASHFLOW_FIELDS.SORT_BUSINESS] =
          USE_REPORT.KHONG_HOACH_TOAN;
      }

      if (cashflow.usedForFinancialReporting === 1) {
        fields[LARK_CASHFLOW_FIELDS.SORT_BUSINESS] = USE_REPORT.HOACH_TOAN;
      }
    }

    if (cashflow.partnerName) {
      fields[LARK_CASHFLOW_FIELDS.CUSTOMER_NAME] = cashflow.partnerName || '';
    }

    if (cashflow.statusValue) {
      fields[LARK_CASHFLOW_FIELDS.STATUS] = cashflow.statusValue;
    }

    if (cashflow.transDate) {
      fields[LARK_CASHFLOW_FIELDS.TRANS_DATE] = new Date(
        cashflow.transDate,
      ).getTime();
    }

    if (cashflow.amount) {
      fields[LARK_CASHFLOW_FIELDS.AMOUNT] = Number(cashflow.amount) || 0;
    }

    if (cashflow.userName) {
      fields[LARK_CASHFLOW_FIELDS.CREATOR] = cashflow.userName;
    }

    if (cashflow.description) {
      fields[LARK_CASHFLOW_FIELDS.DESCRIPTION] = cashflow.description;
    }

    if (cashflow.cashGroup) {
      fields[LARK_CASHFLOW_FIELDS.CASH_GROUP] = cashflow.cashGroup;
    }

    if (cashflow.locationName) {
      fields[LARK_CASHFLOW_FIELDS.ADDRESS] = cashflow.locationName;
    }

    if (cashflow.origin) {
      fields[LARK_CASHFLOW_FIELDS.CASH_ORIGIN] = cashflow.origin;
    }

    if (cashflow.partnerId) {
      fields[LARK_CASHFLOW_FIELDS.CUSTOMER_ID] = Number(cashflow.partnerId);
    }

    return fields;
  }

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.cashflow.count();
    const synced = await this.prismaService.cashflow.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.cashflow.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.cashflow.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    const canRetryFailed = failed > 0;

    return {
      total,
      synced,
      pending,
      failed,
      progress,
      canRetryFailed,
      summary: `${synced}/${total} synced (${progress}%)`,
    };
  }

  private async updateDatabaseStatus(
    cashflows: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (cashflows.length === 0) return;

    const cashflowIds = cashflows.map((i) => i.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.cashflow.updateMany({
      where: { id: { in: cashflowIds } },
      data: updateData,
    });
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.cashflowCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getCashflowHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data.code === 0) {
          const totalRecords = response.data.data?.total || 0;
          this.logger.log(`✅ LarkBase connection successful`);
          this.logger.log(
            `📊 LarkBase table has ${totalRecords} existing records`,
          );
          return;
        }

        throw new Error(`Connection test failed: ${response.data.msg}`);
      } catch (error) {
        if (retryCount < maxRetries) {
          const delay = (retryCount + 1) * 2000;
          this.logger.warn(
            `⚠️ Connection attempt ${retryCount + 1} failed: ${error.message}`,
          );
          this.logger.log(`🔄 Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          this.logger.error(
            '❌ LarkBase connection test failed after all retries',
          );
          throw new Error(`Cannot connect to LarkBase: ${error.message}`);
        }
      }
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    const syncName = 'cashflow_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: syncName,
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();

      if (lockAge < 10 * 60 * 1000) {
        const isProcessActive = await this.isLockProcessActive(existingLock);

        if (isProcessActive) {
          throw new Error('Another sync is already running');
        } else {
          this.logger.warn(
            `🔓 Clearing inactive lock (age: ${Math.round(lockAge / 1000)}s)`,
          );
          await this.forceReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['cashflow'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
    });

    this.logger.debug(
      `🔒 Acquired sync lock: ${lockKey} (PID: ${process.pid})`,
    );
  }

  private async isLockProcessActive(lockRecord: any): Promise<boolean> {
    try {
      if (!lockRecord.progress?.processId) {
        return false;
      }

      const currentHostname = require('os').hostname();
      if (lockRecord.progress.hostname !== currentHostname) {
        return false;
      }

      const lockAge = Date.now() - lockRecord.startedAt.getTime();
      if (lockAge > 5 * 60 * 1000) {
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Could not verify lock process: ${error.message}`);
      return false;
    }
  }

  private async waitForLockAvailability(
    syncName: string,
    maxWaitMs: number = 30000,
  ): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const existingLock = await this.prismaService.syncControl.findFirst({
        where: { name: syncName, isRunning: true },
      });

      if (!existingLock) {
        return;
      }

      this.logger.debug(
        `⏳ Waiting for lock release... (${Math.round((Date.now() - startTime) / 1000)}s)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    throw new Error(`Lock wait timeout after ${maxWaitMs / 1000}s`);
  }

  private async forceReleaseLock(syncName: string): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: syncName },
      data: {
        isRunning: false,
        status: 'force_released',
        error: 'Lock force released due to inactivity',
        completedAt: new Date(),
        progress: {},
      },
    });
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'cashflow_lark_sync',
        isRunning: true,
      },
    });

    if (
      lockRecord &&
      lockRecord.progress &&
      typeof lockRecord.progress === 'object' &&
      'lockKey' in lockRecord.progress &&
      lockRecord.progress.lockKey === lockKey
    ) {
      await this.prismaService.syncControl.update({
        where: {
          id: lockRecord.id,
        },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: {},
        },
      });

      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    }
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
      array.slice(i * size, i * size + size),
    );
  }

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) return 0;

    if (typeof value === 'bigint') {
      return Number(value);
    }

    if (typeof value === 'number') {
      return Math.floor(value);
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return 0;
      const parsed = parseInt(trimmed, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    try {
      const asString = String(value).trim();
      const parsed = parseInt(asString, 10);
      return isNaN(parsed) ? 0 : parsed;
    } catch {
      return 0;
    }
  }
}
