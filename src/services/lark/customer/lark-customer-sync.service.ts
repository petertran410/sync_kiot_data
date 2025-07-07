// src/services/lark/customer/lark-customer-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// ✅ EXACT field names from Khách Hàng.rtf
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
export class LarkCustomerSyncService {
  private readonly logger = new Logger(LarkCustomerSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // Cache management
  private existingRecordsCache: Map<number, string> = new Map();
  private customerCodeCache: Map<string, string> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 30;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase customer configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ============================================================================
  // MAIN SYNC METHOD WITH IMPROVED ERROR HANDLING
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    const lockKey = `lark_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers (IMPROVED MODE)...`,
      );

      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (customersToSync.length === 0) {
        this.logger.log('📋 No customers need LarkBase sync');
        return;
      }

      const pendingCount = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = customers.filter(
        (c) => c.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED customers`,
      );

      // Test LarkBase connection
      await this.testLarkBaseConnection();

      // Load cache with retry mechanism
      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn(
          '⚠️ Cache loading failed - will use alternative duplicate detection',
        );
        // Continue with careful duplicate handling instead of stopping
      }

      // Categorize customers
      const { newCustomers, updateCustomers } =
        this.categorizeCustomers(customersToSync);

      this.logger.log(
        `📋 Categorization: ${newCustomers.length} new, ${updateCustomers.length} updates`,
      );

      // Process in smaller batches to avoid timeouts
      const BATCH_SIZE_FOR_SYNC = 50;

      // Process new customers in batches
      if (newCustomers.length > 0) {
        for (let i = 0; i < newCustomers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newCustomers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new customers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newCustomers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewCustomers(batch);
        }
      }

      // Process updates in batches
      if (updateCustomers.length > 0) {
        for (let i = 0; i < updateCustomers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateCustomers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateCustomers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateCustomers(batch);
        }
      }

      this.logger.log(`🎉 LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync failed: ${error.message}`);

      // Mark all as failed to prevent data corruption
      await this.updateDatabaseStatus(customers, 'FAILED');

      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  // ============================================================================
  // IMPROVED CACHE LOADING WITH STRING HANDLING
  // ============================================================================

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(
          `📥 Loading cache (attempt ${attempt}/${maxRetries})...`,
        );

        // Check if cache is still valid
        if (this.isCacheValid()) {
          this.logger.log('✅ Using existing valid cache');
          return true;
        }

        // Clear old cache
        this.clearCache();

        // Load new cache
        await this.loadExistingRecordsCache();

        // Validate cache
        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `✅ Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`⚠️ Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.error(
          `❌ Cache loading attempt ${attempt} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = attempt * 3000;
          this.logger.log(`⏳ Waiting ${delay / 1000}s before retry...`);
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

  private async loadExistingRecordsCache(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();
      let page_token = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      let stringConversions = 0;
      const pageSize = 100; // Reduced for better stability

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({
          page_size: pageSize.toString(),
          ...(page_token && { page_token }),
        });

        const startTime = Date.now();

        try {
          const response = await firstValueFrom(
            this.httpService.get<LarkBatchResponse>(`${url}?${params}`, {
              headers,
              timeout: 60000, // Increased to 60s
            }),
          );

          const loadTime = Date.now() - startTime;

          if (response.data.code === 0) {
            const records = response.data.data?.items || [];

            for (const record of records) {
              // CRITICAL FIX: Handle string kiotVietId from LarkBase
              const kiotVietIdRaw =
                record.fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID];

              // Enhanced conversion with better string handling
              let kiotVietId = 0;

              if (kiotVietIdRaw !== null && kiotVietIdRaw !== undefined) {
                if (typeof kiotVietIdRaw === 'string') {
                  const trimmed = kiotVietIdRaw.trim();
                  if (trimmed !== '') {
                    const parsed = parseInt(trimmed, 10);
                    if (!isNaN(parsed) && parsed > 0) {
                      kiotVietId = parsed;
                      stringConversions++;
                    }
                  }
                } else if (typeof kiotVietIdRaw === 'number') {
                  kiotVietId = Math.floor(kiotVietIdRaw);
                }
              }

              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;

                // Debug first few conversions
                if (cacheBuilt <= 3) {
                  this.logger.debug(
                    `✅ Cached: "${kiotVietIdRaw}" (${typeof kiotVietIdRaw}) → ${kiotVietId} → ${record.record_id}`,
                  );
                }
              }

              // Also cache by customer code
              const customerCode =
                record.fields[LARK_CUSTOMER_FIELDS.CUSTOMER_CODE];
              if (customerCode) {
                this.customerCodeCache.set(
                  String(customerCode).trim(),
                  record.record_id,
                );
              }
            }

            totalLoaded += records.length;
            page_token = response.data.data?.page_token || '';

            this.logger.debug(
              `📥 Loaded ${records.length} records in ${loadTime}ms (total: ${totalLoaded}, cached: ${cacheBuilt})`,
            );

            // Progress update
            if (totalLoaded % 1000 === 0 || !page_token) {
              this.logger.log(
                `📊 Cache progress: ${cacheBuilt}/${totalLoaded} records processed (${stringConversions} string conversions)`,
              );
            }
          } else {
            throw new Error(
              `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
            );
          }
        } catch (error) {
          if (error.code === 'ECONNABORTED') {
            throw new Error(
              'Request timeout - LarkBase took too long to respond',
            );
          }
          if (error.response?.status === 400) {
            throw new Error(
              'Bad request - check table permissions and field names',
            );
          }
          throw error;
        }
      } while (page_token);

      this.cacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.customerCodeCache.size} by code (${successRate}% success, ${stringConversions} string→number conversions)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // IMPROVED CATEGORIZATION
  // ============================================================================

  private categorizeCustomers(customers: any[]): {
    newCustomers: any[];
    updateCustomers: any[];
  } {
    const newCustomers: any[] = [];
    const updateCustomers: any[] = [];

    for (const customer of customers) {
      // Convert BigInt to number for comparison
      const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);

      // Check by ID first
      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      // Fallback to code lookup
      if (!existingRecordId && customer.code) {
        existingRecordId = this.customerCodeCache.get(
          String(customer.code).trim(),
        );

        if (existingRecordId) {
          this.logger.debug(
            `Found record by code: ${customer.code} -> ${existingRecordId}`,
          );
        }
      }

      if (existingRecordId) {
        updateCustomers.push({
          ...customer,
          larkRecordId: existingRecordId,
        });
      } else {
        newCustomers.push(customer);
      }
    }

    return { newCustomers, updateCustomers };
  }

  // ============================================================================
  // PROCESS NEW CUSTOMERS
  // ============================================================================

  private async processNewCustomers(customers: any[]): Promise<void> {
    if (customers.length === 0) return;

    this.logger.log(`📝 Creating ${customers.length} new customers...`);

    const batches = this.chunkArray(customers, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `📊 Batch ${i + 1}/${batches.length}: Processing ${batch.length} customers`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateCustomers(batch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `📊 Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );

      // Small delay between batches to avoid rate limiting
      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  // ============================================================================
  // PROCESS UPDATES
  // ============================================================================

  private async processUpdateCustomers(customers: any[]): Promise<void> {
    if (customers.length === 0) return;

    this.logger.log(`📝 Updating ${customers.length} existing customers...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    // Process updates in smaller chunks to avoid timeouts
    const UPDATE_CHUNK_SIZE = 5;

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

      // Small delay between chunks
      if (i + UPDATE_CHUNK_SIZE < customers.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    // Process fallbacks
    if (createFallbacks.length > 0) {
      this.logger.log(
        `📝 Creating ${createFallbacks.length} customers that failed update...`,
      );
      await this.processNewCustomers(createFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  // ============================================================================
  // BATCH CREATE WITH PROPER DATA CONVERSION
  // ============================================================================

  private async batchCreateCustomers(customers: any[]): Promise<BatchResult> {
    const records = customers.map((customer) => ({
      fields: this.mapCustomerToLarkBase(customer),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();
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

          // Update cache with proper number conversion
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
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: customers };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: customers };
      }
    }

    return { successRecords: [], failedRecords: customers };
  }

  // ============================================================================
  // UPDATE SINGLE CUSTOMER
  // ============================================================================

  private async updateSingleCustomer(customer: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();
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
            `✅ Updated record ${customer.larkRecordId} for customer ${customer.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
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

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getCustomerHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000, // Increased timeout
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
    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'customer_lark_sync',
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();
      if (lockAge < 30 * 60 * 1000) {
        throw new Error('Another sync is already running');
      }
      this.logger.warn('🔓 Clearing stale lock');
    }

    await this.prismaService.syncControl.upsert({
      where: { name: 'customer_lark_sync' },
      create: {
        name: 'customer_lark_sync',
        entities: ['customer'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey }, // Sử dụng progress field thay vì metadata
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey }, // Sử dụng progress field thay vì metadata
      },
    });

    this.logger.debug(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    // Tìm record có lockKey trong progress
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'customer_lark_sync',
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
        where: { id: lockRecord.id },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: {}, // Clear progress sau khi hoàn thành
        },
      });

      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    }
  }

  private async forceTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;
      await this.larkAuthService.getCustomerHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async updateDatabaseStatus(
    customers: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (customers.length === 0) return;

    const customerIds = customers.map((c) => c.id);
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

    // Handle BigInt
    if (typeof value === 'bigint') {
      return Number(value);
    }

    // Handle number
    if (typeof value === 'number') {
      return Math.floor(value);
    }

    // Handle string
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return 0;
      const parsed = parseInt(trimmed, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    // Handle boolean
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    // Try to convert any other type
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

    // CRITICAL: Convert BigInt to number for kiotVietId
    fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = this.safeBigIntToNumber(
      customer.kiotVietId,
    );

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
      fields[LARK_CUSTOMER_FIELDS.STORE_ID] = '2svn';
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
      if (customer.branchId === 1) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.CUA_HANG_DIEP_TRA;
      } else if (customer.branchId === 2) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_HA_NOI;
      } else if (customer.branchId === 3) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_SAI_GON;
      } else if (customer.branchId == 4) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.VAN_PHONG_HA_NOI;
      }
    }

    if (customer.groups) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_GROUPS] = customer.groups || '';
    }

    if (customer.wardName) {
      fields[LARK_CUSTOMER_FIELDS.WARD_NAME] = customer.wardName || '';
    }

    if (customer.rewardPoint !== null && customer.rewardPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_POINTS] =
        this.safeBigIntToNumber(customer.rewardPoint) || 0;
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

    return fields;
  }

  // ============================================================================
  // MONITORING & HEALTH CHECK METHODS (Fixed BigInt serialization)
  // ============================================================================

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.customer.count();
    const synced = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.customer.count({
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

  async resetFailedCustomers(): Promise<{ resetCount: number }> {
    const result = await this.prismaService.customer.updateMany({
      where: { larkSyncStatus: 'FAILED' },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncRetries: 0,
      },
    });

    this.logger.log(`🔄 Reset ${result.count} FAILED customers to PENDING`);
    return { resetCount: result.count };
  }

  async getFailedCustomersReport(): Promise<any> {
    const failedCustomers = await this.prismaService.customer.findMany({
      where: { larkSyncStatus: 'FAILED' },
      select: {
        id: true,
        code: true,
        name: true,
        larkSyncRetries: true,
        larkSyncedAt: true,
        modifiedDate: true,
      },
      orderBy: { larkSyncRetries: 'desc' },
      take: 10,
    });

    return {
      totalFailed: await this.prismaService.customer.count({
        where: { larkSyncStatus: 'FAILED' },
      }),
      topFailures: failedCustomers.map((c) => ({
        ...c,
        larkSyncRetries: Number(c.larkSyncRetries), // Convert BigInt to number
      })),
      canReset: true,
    };
  }

  async performHealthCheck(): Promise<any> {
    const allIssues: string[] = [];
    const recommendations: string[] = [];

    // Data reconciliation
    this.logger.log('📊 [1/5] DATA RECONCILIATION');
    const dataReconciliation = await this.reconcileDataMismatch();
    allIssues.push(...dataReconciliation.recommendations);

    // Sync control health
    this.logger.log('🔄 [2/5] SYNC CONTROL HEALTH');
    const syncControlHealth = await this.checkSyncControlHealth();
    allIssues.push(...syncControlHealth.issues);

    // LarkBase connectivity
    this.logger.log('🌐 [3/5] LARKBASE CONNECTIVITY');
    const larkBaseConnectivity = await this.testLarkBaseConnectivity();
    if (!larkBaseConnectivity.connected) {
      allIssues.push(
        `LarkBase connectivity failed: ${larkBaseConnectivity.error}`,
      );
    }

    // Data quality
    this.logger.log('🔍 [4/5] DATA QUALITY CHECK');
    const dataQuality = await this.checkCustomerDataQuality();
    allIssues.push(...dataQuality.issues);

    // Cache health
    this.logger.log('📦 [5/5] CACHE HEALTH');
    const cacheHealth = {
      loaded: this.cacheLoaded,
      size: this.existingRecordsCache.size,
      codeMapSize: this.customerCodeCache.size,
      valid: this.isCacheValid(),
      age: this.lastCacheLoadTime
        ? Math.round(
            (Date.now() - this.lastCacheLoadTime.getTime()) / 1000 / 60,
          )
        : null,
    };

    if (!cacheHealth.valid && this.cacheLoaded) {
      recommendations.push('🔄 Refresh cache for accurate duplicate detection');
    }

    // Generate overall status
    let overallStatus: 'HEALTHY' | 'WARNING' | 'CRITICAL' = 'HEALTHY';

    const criticalIssues = allIssues.filter(
      (issue) =>
        issue.includes('🚨') ||
        issue.includes('CRITICAL') ||
        issue.includes('connectivity failed'),
    );

    const warningIssues = allIssues.filter(
      (issue) =>
        issue.includes('⚠️') ||
        issue.includes('WARNING') ||
        issue.includes('duplicate'),
    );

    if (criticalIssues.length > 0) {
      overallStatus = 'CRITICAL';
    } else if (warningIssues.length > 0 || allIssues.length > 0) {
      overallStatus = 'WARNING';
    }

    // Additional recommendations
    if (dataReconciliation.pendingSync > 0) {
      recommendations.push(
        `🚀 Sync ${dataReconciliation.pendingSync} pending customers`,
      );
    }

    if (dataReconciliation.failedSync > 0) {
      recommendations.push(
        `🔄 Retry ${dataReconciliation.failedSync} failed customers`,
      );
    }

    return {
      timestamp: new Date().toISOString(),
      overallStatus,
      components: {
        dataReconciliation: {
          ...dataReconciliation,
          syncedCount: Number(dataReconciliation.syncedCount),
        },
        syncControlHealth,
        larkBaseConnectivity,
        dataQuality: {
          ...dataQuality,
          duplicateKiotVietIds: Number(dataQuality.duplicateKiotVietIds),
          nullKiotVietIds: Number(dataQuality.nullKiotVietIds),
        },
        cacheHealth,
      },
      issues: allIssues,
      recommendations,
    };
  }

  private async reconcileDataMismatch(): Promise<any> {
    const databaseCount = await this.prismaService.customer.count();
    const pendingCount = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failedCount = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'FAILED' },
    });
    const syncedCount = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'SYNCED' },
    });

    let larkBaseCount = 0;
    let cacheLoadError = null;

    try {
      if (!this.isCacheValid()) {
        await this.loadExistingRecordsWithRetry();
      }
      larkBaseCount = this.existingRecordsCache.size;
    } catch (error) {
      cacheLoadError = error.message;
    }

    const mismatch = Math.abs(databaseCount - larkBaseCount);
    const recommendations: string[] = [];

    if (cacheLoadError) {
      recommendations.push(
        `🚨 CRITICAL: Cannot access LarkBase - ${cacheLoadError}`,
      );
    }

    if (mismatch > 1000) {
      recommendations.push(
        `🚨 CRITICAL: Major data mismatch - ${mismatch} records difference`,
      );
    } else if (mismatch > 100) {
      recommendations.push(
        `⚠️ WARNING: Significant mismatch - ${mismatch} records difference`,
      );
    }

    if (pendingCount > 0) {
      recommendations.push(`⏳ ${pendingCount} customers pending sync`);
    }

    if (failedCount > 100) {
      recommendations.push(
        `❌ ${failedCount} customers failed sync - investigation needed`,
      );
    }

    // Check for extra records in LarkBase
    if (larkBaseCount > databaseCount) {
      const extraRecords = larkBaseCount - databaseCount;
      recommendations.push(
        `📊 ${extraRecords} extra records in LarkBase - possible manual additions or sync status issues`,
      );
    }

    return {
      databaseCount,
      larkBaseCount,
      mismatch,
      pendingSync: pendingCount,
      failedSync: failedCount,
      syncedCount,
      recommendations,
    };
  }

  private async checkSyncControlHealth(): Promise<any> {
    const stuckSyncs = await this.prismaService.syncControl.findMany({
      where: {
        isRunning: true,
        startedAt: {
          lt: new Date(Date.now() - 60 * 60 * 1000), // Over 1 hour
        },
      },
    });

    const issues: string[] = [];

    if (stuckSyncs.length > 0) {
      issues.push(`⚠️ ${stuckSyncs.length} stuck sync processes detected`);
    }

    return {
      stuckSyncs: stuckSyncs.map((s) => ({
        name: s.name,
        startedAt: s.startedAt ? s.startedAt.toISOString() : 'unknown',
        duration: s.startedAt
          ? Math.round((Date.now() - s.startedAt.getTime()) / 1000 / 60)
          : 0,
      })),
      issues,
    };
  }

  private async testLarkBaseConnectivity(): Promise<any> {
    try {
      await this.testLarkBaseConnection();
      return { connected: true, error: null };
    } catch (error) {
      return { connected: false, error: error.message };
    }
  }

  private async checkCustomerDataQuality(): Promise<any> {
    const issues: string[] = [];

    // Check for duplicates by kiotVietId
    const duplicateKiotVietIds = await this.prismaService.$queryRaw<any[]>`
      SELECT "kiotVietId", COUNT(*) as count
      FROM "Customer"
      GROUP BY "kiotVietId"
      HAVING COUNT(*) > 1
      LIMIT 10
    `;

    if (duplicateKiotVietIds.length > 0) {
      issues.push(
        `⚠️ Found ${duplicateKiotVietIds.length} duplicate kiotVietIds in database`,
      );
    }

    // Check for null kiotVietIds
    const nullKiotVietIds = await this.prismaService.customer.count({
      where: { kiotVietId: null },
    });

    if (nullKiotVietIds > 0) {
      issues.push(`⚠️ ${nullKiotVietIds} customers with null kiotVietId`);
    }

    return {
      duplicateKiotVietIds: duplicateKiotVietIds.length,
      nullKiotVietIds,
      issues,
    };
  }

  // ============================================================================
  // DATA TYPE DEBUG (Fixed BigInt serialization)
  // ============================================================================

  async debugKiotVietIdDataTypes(): Promise<any> {
    this.logger.log(
      '🔍 Debugging kiotVietId data types between Database and LarkBase...',
    );

    // Get samples from database
    const dbSamples = await this.prismaService.customer.findMany({
      select: {
        kiotVietId: true,
        code: true,
      },
      take: 10,
      orderBy: { createdDate: 'desc' },
    });

    // Get samples from LarkBase cache
    const larkSamples: any[] = [];
    if (this.existingRecordsCache.size > 0) {
      let count = 0;
      for (const [
        kiotVietId,
        recordId,
      ] of this.existingRecordsCache.entries()) {
        larkSamples.push({ kiotVietId, recordId });
        count++;
        if (count >= 10) break;
      }
    }

    const conversionIssues: string[] = [];

    // Analyze database types
    const dbTypes = new Map<string, number>();
    dbSamples.forEach((sample) => {
      const type = typeof sample.kiotVietId;
      dbTypes.set(type, (dbTypes.get(type) || 0) + 1);
    });

    // Log findings
    this.logger.log('🔍 KiotVietId Data Type Analysis:');
    this.logger.log(`Database samples (${dbSamples.length}):`);
    dbSamples.forEach((sample, i) => {
      const type = typeof sample.kiotVietId;
      this.logger.log(
        `${i + 1}. ${sample.kiotVietId} (${type}) - ${sample.code}`,
      );
    });

    this.logger.log(`LarkBase samples (${larkSamples.length}):`);
    larkSamples.forEach((sample, i) => {
      this.logger.log(
        `${i + 1}. ${sample.kiotVietId} (string) - ${sample.recordId}`,
      );
    });

    this.logger.log('Type Distribution:');
    this.logger.log(`LarkBase: ${larkSamples.length} strings, 0 numbers`);
    this.logger.log(
      `Database: ${dbTypes.get('bigint') || 0} bigints, ${dbTypes.get('number') || 0} numbers`,
    );

    if (dbTypes.has('bigint') && larkSamples.length > 0) {
      conversionIssues.push(
        'Type mismatch: LarkBase stores kiotVietId as STRING but Database expects NUMBER/BIGINT',
      );
    }

    if (conversionIssues.length > 0) {
      this.logger.warn('⚠️ Conversion Issues Found:');
      conversionIssues.forEach((issue, i) => {
        this.logger.warn(`${i + 1}. ${issue}`);
      });
    }

    return {
      databaseTypes: Object.fromEntries(dbTypes),
      larkBaseType: 'string',
      samplesAnalyzed: {
        database: dbSamples.length,
        larkBase: larkSamples.length,
      },
      conversionIssues,
    };
  }

  // ============================================================================
  // DATA ANALYSIS METHODS
  // ============================================================================

  async analyzeMissingData(): Promise<{
    missing: any[];
    exists: any[];
    duplicates: any[];
    summary: any;
  }> {
    this.logger.log(
      '🔍 Analyzing missing data between Database and LarkBase...',
    );

    // Load cache để có data LarkBase
    await this.loadExistingRecordsWithRetry();

    // Get all database records
    const dbCustomers = await this.prismaService.customer.findMany({
      select: {
        id: true,
        kiotVietId: true,
        code: true,
        name: true,
        larkSyncStatus: true,
        larkSyncedAt: true,
      },
      orderBy: { kiotVietId: 'asc' },
    });

    const missing: any[] = [];
    const exists: any[] = [];
    const duplicates: Map<number, number> = new Map();

    // Analyze each database record
    for (const customer of dbCustomers) {
      const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
      const existsInLark = this.existingRecordsCache.has(kiotVietId);

      if (existsInLark) {
        exists.push({
          dbId: customer.id,
          kiotVietId,
          code: customer.code,
          name: customer.name,
          larkRecordId: this.existingRecordsCache.get(kiotVietId),
          syncStatus: customer.larkSyncStatus,
        });

        // Count occurrences for duplicate detection
        duplicates.set(kiotVietId, (duplicates.get(kiotVietId) || 0) + 1);
      } else {
        missing.push({
          dbId: customer.id,
          kiotVietId,
          code: customer.code,
          name: customer.name,
          syncStatus: customer.larkSyncStatus,
          lastSyncAttempt: customer.larkSyncedAt,
        });
      }
    }

    // Find actual duplicates
    const duplicatesList: any[] = [];
    for (const [kiotVietId, count] of duplicates.entries()) {
      if (count > 1) {
        duplicatesList.push({ kiotVietId, count });
      }
    }

    const summary = {
      totalDatabase: dbCustomers.length,
      totalLarkBase: this.existingRecordsCache.size,
      existsInBoth: exists.length,
      missingInLarkBase: missing.length,
      duplicatesFound: duplicatesList.length,
      syncStatusBreakdown: {
        SYNCED: missing.filter((m) => m.syncStatus === 'SYNCED').length,
        PENDING: missing.filter((m) => m.syncStatus === 'PENDING').length,
        FAILED: missing.filter((m) => m.syncStatus === 'FAILED').length,
      },
    };

    this.logger.log('📊 Analysis Summary:');
    this.logger.log(`- Total in Database: ${summary.totalDatabase}`);
    this.logger.log(`- Total in LarkBase: ${summary.totalLarkBase}`);
    this.logger.log(`- Exists in both: ${summary.existsInBoth}`);
    this.logger.log(`- Missing in LarkBase: ${summary.missingInLarkBase}`);
    this.logger.log(`- Duplicates found: ${summary.duplicatesFound}`);

    return {
      missing: missing.slice(0, 100), // First 100 for readability
      exists: exists.slice(0, 20), // Sample of existing
      duplicates: duplicatesList,
      summary,
    };
  }

  // ============================================================================
  // TARGETED SYNC FOR MISSING DATA
  // ============================================================================

  async syncMissingDataOnly(): Promise<{
    attempted: number;
    success: number;
    failed: number;
    details: any[];
  }> {
    this.logger.log('🚀 Starting targeted sync for missing data only...');

    // First, analyze what's missing
    const analysis = await this.analyzeMissingData();
    const missingCustomers = analysis.missing;

    if (missingCustomers.length === 0) {
      this.logger.log(
        '✅ No missing data found! Database and LarkBase are in sync.',
      );
      return {
        attempted: 0,
        success: 0,
        failed: 0,
        details: [],
      };
    }

    // Get full customer data for missing records
    const missingIds = missingCustomers.map((m) => m.dbId);
    const customersToSync = await this.prismaService.customer.findMany({
      where: { id: { in: missingIds } },
    });

    this.logger.log(
      `📋 Found ${customersToSync.length} missing customers to sync`,
    );

    // Reset their status to PENDING for fresh sync
    await this.prismaService.customer.updateMany({
      where: { id: { in: missingIds } },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncRetries: 0,
      },
    });

    // Sync in small batches
    const BATCH_SIZE = 25;
    let totalSuccess = 0;
    let totalFailed = 0;
    const syncDetails: any[] = [];

    for (let i = 0; i < customersToSync.length; i += BATCH_SIZE) {
      const batch = customersToSync.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(customersToSync.length / BATCH_SIZE);

      this.logger.log(
        `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} customers)`,
      );

      try {
        // Process this batch
        const { successRecords, failedRecords } =
          await this.batchCreateCustomers(batch);

        totalSuccess += successRecords.length;
        totalFailed += failedRecords.length;

        // Update database status
        if (successRecords.length > 0) {
          await this.updateDatabaseStatus(successRecords, 'SYNCED');
        }

        if (failedRecords.length > 0) {
          await this.updateDatabaseStatus(failedRecords, 'FAILED');
        }

        syncDetails.push({
          batch: batchNumber,
          success: successRecords.length,
          failed: failedRecords.length,
          failedCodes: failedRecords.map((f) => f.code),
        });

        // Small delay between batches
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`❌ Batch ${batchNumber} failed: ${error.message}`);
        totalFailed += batch.length;

        syncDetails.push({
          batch: batchNumber,
          success: 0,
          failed: batch.length,
          error: error.message,
        });
      }
    }

    this.logger.log('🎯 Missing data sync completed:');
    this.logger.log(`- Attempted: ${customersToSync.length}`);
    this.logger.log(`- Success: ${totalSuccess}`);
    this.logger.log(`- Failed: ${totalFailed}`);

    return {
      attempted: customersToSync.length,
      success: totalSuccess,
      failed: totalFailed,
      details: syncDetails,
    };
  }

  // ============================================================================
  // VERIFY SYNC COMPLETENESS
  // ============================================================================

  async verifySyncCompleteness(): Promise<{
    isComplete: boolean;
    discrepancies: any[];
    recommendations: string[];
  }> {
    this.logger.log('🔍 Verifying sync completeness...');

    // Reload cache to get latest LarkBase state
    this.clearCache();
    await this.loadExistingRecordsWithRetry();

    // Get counts
    const dbTotal = await this.prismaService.customer.count();
    const dbSynced = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const larkTotal = this.existingRecordsCache.size;

    // Check each SYNCED record actually exists in LarkBase
    const syncedButMissing: any[] = [];
    const syncedCustomers = await this.prismaService.customer.findMany({
      where: { larkSyncStatus: 'SYNCED' },
      select: {
        id: true,
        kiotVietId: true,
        code: true,
        name: true,
      },
    });

    for (const customer of syncedCustomers) {
      const kiotVietId = this.safeBigIntToNumber(customer.kiotVietId);
      if (!this.existingRecordsCache.has(kiotVietId)) {
        syncedButMissing.push({
          id: customer.id,
          kiotVietId,
          code: customer.code,
          name: customer.name,
        });
      }
    }

    const isComplete = dbTotal === larkTotal && syncedButMissing.length === 0;
    const discrepancies: any[] = [];
    const recommendations: string[] = [];

    if (dbTotal !== larkTotal) {
      discrepancies.push({
        type: 'COUNT_MISMATCH',
        database: dbTotal,
        larkBase: larkTotal,
        difference: Math.abs(dbTotal - larkTotal),
      });
    }

    if (syncedButMissing.length > 0) {
      discrepancies.push({
        type: 'SYNCED_BUT_MISSING',
        count: syncedButMissing.length,
        samples: syncedButMissing.slice(0, 5),
      });

      recommendations.push(
        `Reset ${syncedButMissing.length} incorrectly marked SYNCED records and re-sync`,
      );
    }

    if (dbSynced < dbTotal) {
      const pending = await this.prismaService.customer.count({
        where: { larkSyncStatus: 'PENDING' },
      });
      const failed = await this.prismaService.customer.count({
        where: { larkSyncStatus: 'FAILED' },
      });

      if (pending > 0) {
        recommendations.push(`Sync ${pending} PENDING customers`);
      }

      if (failed > 0) {
        recommendations.push(`Reset and retry ${failed} FAILED customers`);
      }
    }

    if (isComplete) {
      recommendations.push(
        '✅ Sync is complete! Database and LarkBase are fully synchronized.',
      );
    } else {
      recommendations.push('Run syncMissingDataOnly() to sync missing records');
    }

    return {
      isComplete,
      discrepancies,
      recommendations,
    };
  }
}
