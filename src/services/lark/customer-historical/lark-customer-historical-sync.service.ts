import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_CUSTOMER_FIELDS = {
  PRIMARY_NAME: 'Tên Khách Hàng',
  CUSTOMER_CODE: 'Mã Khách Hàng',
  PHONE_NUMBER: 'Số Điện Thoại',
  STORE_ID: 'Id Cửa Hàng',
  BRANCH: 'Branch',
  COMPANY: 'Công Ty',
  EMAIL: 'Email của Khách Hàng',
  ADDRESS: 'Địa Chỉ Khách Hàng',
  CURRENT_DEBT: 'Nợ Hiện Tại',
  TAX_CODE: 'Mã Số Thuế',
  TOTAL_POINTS: 'Tổng Điểm',
  TOTAL_REVENUE: 'Tổng Doanh Thu',
  GENDER: 'Giới Tính',
  WARD_NAME: 'Phường xã',
  CURRENT_POINTS: 'Điểm Hiện Tại',
  KIOTVIET_ID: 'kiotVietId',
  TOTAL_INVOICED: 'Tổng Bán',
  COMMENTS: 'Ghi Chú',
  MODIFIED_DATE: 'Thời Gian Cập Nhật',
  CREATED_DATE: 'Thời Gian Tạo',
  FACEBOOK_ID: 'Facebook Khách Hàng',
  LOCATION_NAME: 'Khu Vực',
  CUSTOMER_GROUPS: 'Nhóm Khách Hàng',
  DATE_OF_BIRTH: 'Ngày Sinh',
  TYPE: 'Loại Khách Hàng',
  SUB_PHONE: 'Số Điện Thoại Phụ',
  IDENTIFICATION_NUMBER: 'CCCD Của Khách Hàng',
} as const;

const GENDER_OPTIONS = {
  MALE: 'Nam',
  FEMALE: 'Nữ',
} as const;

const BRANCH_OPTIONS = {
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
};

const TYPE_CUSTOMER = {
  CONG_TY: 'Công Ty',
  CA_NHAN: 'Cá Nhân',
};

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
export class LarkCustomerHistoricalSyncService {
  private readonly logger = new Logger(LarkCustomerHistoricalSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<number, string> = new Map();
  private customerCodeCache: Map<string, string> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_CUSTOMER_HISTORICAL_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_CUSTOMER_HISTORICAL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase customer configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    const lockKey = `lark_customer_historical_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${customers.length} customers...`,
      );

      const customersToSync = customers.filter(
        (i) => i.larkSyncStatus === 'PENDING' || i.larkSyncStatus === 'FAILED',
      );

      if (customersToSync.length === 0) {
        this.logger.log('No customers need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (customersToSync.length < 5) {
        this.logger.log(
          `Small sync (${customersToSync.length} customers) - using lightweight mode`,
        );

        await this.syncWithoutCache(customersToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = customers.filter(
        (i) => i.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = customers.filter(
        (i) => i.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingCount} PENDING + ${failedCount} FAILED customers`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(customersToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const { newCustomers, updateCustomers } =
        await this.categorizeCustomers(customersToSync);

      this.logger.log(
        `Categorization: ${newCustomers.length} new, ${updateCustomers.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newCustomers.length > 0) {
        for (let i = 0; i < newCustomers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newCustomers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new customers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newCustomers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewCustomers(batch);
        }
      }

      if (updateCustomers.length > 0) {
        for (let i = 0; i < updateCustomers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateCustomers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update Customers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateCustomers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateCustomers(batch);
        }
      }

      this.logger.log('LarkBase customers sync completed successfully');
    } catch (error) {
      this.logger.error(`LarkBase customer sync failed: ${error.message}`);
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
        await this.loadExistingRecordsCache();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.error(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = attempt * 1500;
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
    const maxAge = this.CACHE_VALIDITY_MINUTES * 100 * 1000;

    return cacheAge < maxAge && this.existingRecordsCache.size > 0;
  }

  private async loadExistingRecordsCache(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getCustomerHistoricalHeaders();
      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheBuilt = 0;
      const pageSize = 500;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params = new URLSearchParams({
          page_size: String(pageSize),
        });

        if (pageToken) {
          params.append('page_token', pageToken);
        }

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 90000,
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          for (const record of records) {
            const kiotVietIdField =
              record.fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const customerCodeField =
              record.fields[LARK_CUSTOMER_FIELDS.CUSTOMER_CODE];
            if (customerCodeField) {
              this.customerCodeCache.set(
                String(customerCodeField).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token;

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
        `Customer cache loaded: ${this.existingRecordsCache.size} by ID, ${this.customerCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Customer cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeCustomers(customers: any[]): Promise<any> {
    const newCustomers: any[] = [];
    const updateCustomers: any[] = [];

    for (const customer of customers) {
      const kiotVietId = customer.kiotVietId
        ? typeof customer.kiotVietId === 'bigint'
          ? Number(customer.kiotVietId)
          : Number(customer.kiotVietId)
        : 0;

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Customers ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && customer.code) {
        existingRecordId = this.customerCodeCache.get(
          String(customer.code).trim(),
        );
      }

      if (existingRecordId) {
        updateCustomers.push({ ...customer, larkRecordId: existingRecordId });
      } else {
        newCustomers.push(customer);
      }
    }

    return { newCustomers, updateCustomers };
  }

  private async syncWithoutCache(customers: any[]): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingCustomers = await this.prismaService.customer.findMany({
      where: {
        kiotVietId: { in: customers.map((i) => i.kiotVietId) },
      },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingCustomers.forEach((i) => {
      if (i.larkRecordId) {
        quickCache.set(Number(i.kiotVietId), i.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newCustomers, updateCustomers } =
        await this.categorizeCustomers(customers);

      if (newCustomers.length > 0) {
        await this.processNewCustomers(newCustomers);
      }

      if (updateCustomers.length > 0) {
        await this.processUpdateCustomers(updateCustomers);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewCustomers(customers: any[]): Promise<void> {
    if (customers.length === 0) return;

    this.logger.log(`Creating ${customers.length} new customers...`);

    const batches = this.chunkArray(customers, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const customer of batch) {
        const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(customer);
        } else {
          this.logger.warn(
            `Skipping duplicate customer ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all customers already exist`);
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} customers)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateCustomers(verifiedBatch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');

        successRecords.forEach((record) => {
          const kiotVietId = this.safeBigIntToNumber(record.kiotVietId);
          this.pendingCreation.delete(kiotVietId);
        });
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );
    }

    this.logger.log(
      `Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processUpdateCustomers(customers: any[]): Promise<void> {
    if (customers.length === 0) return;

    this.logger.log(`Updating ${customers.length} existing customers...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 20;

    for (let i = 0; i < customers.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = customers.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (customer) => {
          try {
            const updated = await this.updateSingleCustomer(customer);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([customer], 'SYNCED');
            } else {
              createFallbacks.push(customer);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${customer.code}: ${error.message}`,
            );
            createFallbacks.push(customer);
          }
        }),
      );
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `Creating ${createFallbacks.length} customers that failed update...`,
      );
      await this.processNewCustomers(createFallbacks);
    }

    this.logger.log(
      `Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  private async batchCreateCustomers(customers: any[]): Promise<BatchResult> {
    const records = customers.map((customer) => ({
      fields: this.mapCustomerToLarkBase(customer),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getCustomerHistoricalHeaders();
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
          const successRecords = customers.slice(0, successCount);
          const failedRecords = customers.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const customer = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (customer.code) {
              this.customerCodeCache.set(
                String(customer.code).trim(),
                createdRecord.record_id,
              );
            }

            if (createdRecord.record_id && customer.id) {
              await this.prismaService.customer.update({
                where: { id: customer.id },
                data: {
                  larkRecordId: createdRecord.record_id,
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                  larkSyncRetries: 0,
                },
              });
            }
          }

          successRecords.forEach((customer) => {
            const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
            this.pendingCreation.delete(kiotVietId);
          });

          failedRecords.forEach((customer) => {
            const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
            this.pendingCreation.delete(kiotVietId);
          });

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshCustomerHistoricalToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: customers };
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

        return { successRecords: [], failedRecords: customers };
      }
    }

    return { successRecords: [], failedRecords: customers };
  }

  private async updateSingleCustomer(customer: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getCustomerHistoricalHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${customer.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapCustomerToLarkBase(customer) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${customer.larkRecordId} for customer ${customer.code}`,
          );
          await this.prismaService.customer.update({
            where: { id: customer.id },
            data: {
              larkRecordId: customer.larkRecordId,
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshCustomerHistoricalToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshCustomerHistoricalToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${customer.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers =
          await this.larkAuthService.getCustomerHistoricalHeaders();
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
          const delay = (retryCount + 1) * 500;
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
    const syncName = 'customer_historical_lark_sync';

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
        entities: ['customer'],
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
      await new Promise((resolve) => setTimeout(resolve, 500));
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
        name: 'customer_historical_lark_sync',
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

  private async updateDatabaseStatus(
    customers: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (customers.length === 0) return;

    const customerIds = customers.map((i) => i.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.customer.updateMany({
      where: { id: { in: customerIds } },
      data: updateData,
    });
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.customerCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
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

  private mapCustomerToLarkBase(customer: any): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = Number(customer.kiotVietId || 0);

    if (customer.name) {
      fields[LARK_CUSTOMER_FIELDS.PRIMARY_NAME] = customer.name;
    }

    if (customer.code) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_CODE] = customer.code;
    }

    if (customer.contactNumber) {
      fields[LARK_CUSTOMER_FIELDS.PHONE_NUMBER] = customer.contactNumber || '';
    }

    if (customer.retailerId) {
      fields[LARK_CUSTOMER_FIELDS.STORE_ID] = '310831';
    }

    if (customer.organization) {
      fields[LARK_CUSTOMER_FIELDS.COMPANY] = customer.organization || '';
    }

    if (customer.email) {
      fields[LARK_CUSTOMER_FIELDS.EMAIL] = customer.email || '';
    }

    if (customer.address) {
      fields[LARK_CUSTOMER_FIELDS.ADDRESS] = customer.address || '';
    }

    if (customer.debt !== null && customer.debt !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_DEBT] = Number(customer.debt || 0);
    }

    if (customer.taxCode) {
      fields[LARK_CUSTOMER_FIELDS.TAX_CODE] = customer.taxCode || '';
    }

    if (customer.totalPoint !== null && customer.totalPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_POINTS] =
        Number(customer.totalPoint) || 0;
    }

    if (customer.totalRevenue !== null && customer.totalRevenue !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_REVENUE] =
        Number(customer.totalRevenue) || 0;
    }

    if (customer.gender !== null && customer.gender !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
        ? GENDER_OPTIONS.MALE
        : GENDER_OPTIONS.FEMALE;
    }

    if (customer.branchId !== null && customer.branchId !== undefined) {
      if (customer.branchId === 635934) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.CUA_HANG_DIEP_TRA;
      } else if (customer.branchId === 154833) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_HA_NOI;
      } else if (customer.branchId === 402819) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_SAI_GON;
      } else if (customer.branchId === 631164) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.VAN_PHONG_HA_NOI;
      }
    }

    if (customer.groups !== null && customer.groups !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_GROUPS] = customer.groups || '';
    }

    if (customer.wardName) {
      fields[LARK_CUSTOMER_FIELDS.WARD_NAME] = customer.wardName || '';
    }

    if (customer.rewardPoint !== null && customer.rewardPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_POINTS] =
        Number(customer.rewardPoint) || 0;
    }

    if (customer.type !== null && customer.type !== undefined) {
      const typeMapping = {
        0: TYPE_CUSTOMER.CA_NHAN,
        1: TYPE_CUSTOMER.CONG_TY,
      };

      fields[LARK_CUSTOMER_FIELDS.TYPE] = typeMapping[customer.type];
    }

    if (
      customer.totalInvoiced !== null &&
      customer.totalInvoiced !== undefined
    ) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_INVOICED] =
        Number(customer.totalInvoiced) || 0;
    }

    if (customer.comments) {
      fields[LARK_CUSTOMER_FIELDS.COMMENTS] = customer.comments || '';
    }

    if (customer.birthDate) {
      fields[LARK_CUSTOMER_FIELDS.DATE_OF_BIRTH] = new Date(
        customer.birthDate,
      ).getTime();
    }

    if (customer.modifiedDate) {
      fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = new Date(
        customer.modifiedDate,
      ).getTime();
    }

    if (customer.createdDate) {
      fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = new Date(
        customer.createdDate,
      ).getTime();
    }

    if (customer.locationName) {
      fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName || '';
    }

    if (customer.psidFacebook) {
      fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] = String(
        customer.psidFacebook || '',
      );
    }

    if (customer.subNumber) {
      fields[LARK_CUSTOMER_FIELDS.SUB_PHONE] = customer.subNumber || '';
    }

    if (customer.identificationNumber) {
      fields[LARK_CUSTOMER_FIELDS.IDENTIFICATION_NUMBER] =
        customer.identificationNumber || '';
    }

    return fields;
  }
}
