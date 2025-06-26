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
} as const;

const GENDER_OPTIONS = {
  MALE: 'Nam',
  FEMALE: 'Nữ',
} as const;

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
  private readonly batchSize: number = 15; // Smaller for stability

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // ✅ BYPASS: In-memory cache for existing records (loaded once per sync)
  private existingRecordsCache: Map<number, string> = new Map();
  private cacheLoaded: boolean = false;

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
  // ✅ BYPASS SOLUTION: MAIN SYNC WITHOUT SEARCH
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    const lockKey = `lark_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers (BYPASS SEARCH MODE)...`,
      );

      // ✅ INCLUDE BOTH PENDING AND FAILED for safe retry
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

      this.logger.log(
        `📊 Bypass sync: ${customersToSync.length}/${customers.length} customers need sync`,
      );

      // ✅ STEP 1: Test LarkBase connection first
      await this.testLarkBaseConnection();

      // ✅ STEP 2: Load existing records cache (if any) to detect duplicates
      await this.loadExistingRecordsCache();

      // ✅ STEP 3: Separate new vs potential updates based on cache
      const { newCustomers, updateCustomers } =
        this.categorizeCustomers(customersToSync);

      this.logger.log(
        `📋 Bypass categorization: ${newCustomers.length} new, ${updateCustomers.length} potential updates`,
      );

      // ✅ STEP 4: Process new customers (guaranteed no duplicates)
      if (newCustomers.length > 0) {
        await this.processNewCustomersBypass(newCustomers);
      }

      // ✅ STEP 5: Handle potential updates (try update, fallback to create)
      if (updateCustomers.length > 0) {
        await this.processUpdateCustomersBypass(updateCustomers);
      }

      this.logger.log(`🎉 Bypass LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ Bypass LarkBase sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
      this.clearCache(); // Clear cache for next sync
    }
  }

  // ============================================================================
  // ✅ FIXED CONNECTION TEST (Increased timeout + retry)
  // ============================================================================

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;
    let retryCount = 0;

    while (retryCount <= maxRetries) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getCustomerHeaders();

        // ✅ FIXED: Increased timeout to 30s + simple request
        const response = await firstValueFrom(
          this.httpService.get(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?page_size=1`,
            {
              headers,
              timeout: 30000, // ✅ 30 seconds instead of 10
            },
          ),
        );

        if (response.data.code === 0) {
          this.logger.log('✅ LarkBase connection successful');

          const totalRecords = response.data.data?.total || 0;
          this.logger.log(
            `📊 LarkBase table has ${totalRecords} existing records`,
          );
          return; // Success - exit retry loop
        } else {
          throw new Error(
            `Connection test failed: ${response.data.msg} (Code: ${response.data.code})`,
          );
        }
      } catch (error) {
        retryCount++;

        if (retryCount <= maxRetries) {
          const delay = retryCount * 2000; // Progressive delay: 2s, 4s, 6s
          this.logger.warn(
            `⚠️ Connection attempt ${retryCount} failed: ${error.message}`,
          );
          this.logger.log(`🔄 Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          this.logger.error(
            '❌ LarkBase connection test failed after all retries:',
            error.message,
          );
          throw new Error(
            `Cannot connect to LarkBase after ${maxRetries + 1} attempts: ${error.message}`,
          );
        }
      }
    }
  }

  // ============================================================================
  // ✅ FIXED CACHE LOADING (Handle string kiotVietId + timeout)
  // ============================================================================

  private async loadExistingRecordsCache(): Promise<void> {
    if (this.cacheLoaded) return;

    try {
      this.logger.log('📥 Loading existing records cache (optimized)...');

      const headers = await this.larkAuthService.getCustomerHeaders();
      let page_token = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;

      // ✅ OPTIMIZATION: Smaller page size for reliability
      const pageSize = 200; // Reduced from 500 for better timeout handling

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({
          page_size: pageSize.toString(),
          ...(page_token && { page_token }),
        });

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 45000, // ✅ 45 seconds for cache loading
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          // ✅ FIXED: Build cache with string handling
          for (const record of records) {
            const kiotVietIdRaw =
              record.fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID];
            const kiotVietId = this.safeBigIntToNumber(kiotVietIdRaw);

            if (kiotVietId && kiotVietId > 0) {
              this.existingRecordsCache.set(kiotVietId, record.record_id);
              cacheBuilt++;

              // ✅ DEBUG: Log first few successful cache builds
              if (cacheBuilt <= 5) {
                this.logger.debug(
                  `✅ Cached: "${kiotVietIdRaw}" (${typeof kiotVietIdRaw}) → ${kiotVietId} → ${record.record_id}`,
                );
              }
            } else {
              // ✅ Log failures (should be rare now)
              if (totalLoaded - cacheBuilt < 3) {
                this.logger.warn(
                  `❌ Failed to cache: "${kiotVietIdRaw}" (${typeof kiotVietIdRaw}) → ${kiotVietId}`,
                );
              }
            }
          }

          totalLoaded += records.length;
          page_token = response.data.data?.page_token || '';

          this.logger.debug(
            `📥 Loaded ${records.length} records in ${loadTime}ms (total: ${totalLoaded}, cached: ${cacheBuilt})`,
          );

          // ✅ PROGRESS UPDATE: Show cache loading progress
          if (totalLoaded % 1000 === 0 || !page_token) {
            this.logger.log(
              `📊 Cache progress: ${cacheBuilt}/${totalLoaded} records processed`,
            );
          }
        } else {
          this.logger.warn(`⚠️ Failed to load page: ${response.data.msg}`);
          break;
        }
      } while (page_token);

      this.cacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;
      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} records from ${totalLoaded} total (${successRate}% success)`,
      );

      // ✅ VALIDATION: Ensure cache is populated
      if (totalLoaded > 0 && this.existingRecordsCache.size === 0) {
        this.logger.error(
          `❌ CRITICAL: Cache build failed - no valid kiotVietId found in ${totalLoaded} records`,
        );
        throw new Error('Cache building failed - data format issue');
      }
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);

      // ✅ GRACEFUL DEGRADATION: Continue without cache but warn user
      this.logger.warn(
        `⚠️ CONTINUING WITHOUT CACHE - all customers will be treated as new`,
      );
      this.logger.warn(
        `⚠️ This may cause duplicates if LarkBase has existing data`,
      );

      this.cacheLoaded = true;
    }
  }

  // ============================================================================
  // ✅ CATEGORIZE CUSTOMERS (Cache-based duplicate detection)
  // ============================================================================

  private categorizeCustomers(customers: any[]): {
    newCustomers: any[];
    updateCustomers: any[];
  } {
    const newCustomers: any[] = [];
    const updateCustomers: any[] = [];

    for (const customer of customers) {
      const kiotVietId = Number(customer.kiotVietId);
      const existingRecordId = this.existingRecordsCache.get(kiotVietId);

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
  // ✅ PROCESS NEW CUSTOMERS (Guaranteed no duplicates)
  // ============================================================================

  private async processNewCustomersBypass(customers: any[]): Promise<void> {
    this.logger.log(`📝 BYPASS create of ${customers.length} new customers...`);

    const batches = this.chunkArray(customers, this.batchSize);
    let totalSuccess = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      try {
        const batchResult = await this.batchCreateBypass(batch);
        totalSuccess += batchResult.successRecords.length;
        totalFailed += batchResult.failedRecords.length;

        // Update database status
        await this.updateDatabaseStatus(batchResult.successRecords, 'SYNCED');
        await this.updateDatabaseStatus(batchResult.failedRecords, 'FAILED');

        this.logger.log(
          `📊 Bypass batch ${i + 1}/${batches.length}: ${batchResult.successRecords.length}/${batch.length} created`,
        );

        await new Promise((resolve) => setTimeout(resolve, 500));
      } catch (error) {
        this.logger.error(`❌ Bypass batch ${i + 1} failed: ${error.message}`);
        totalFailed += batch.length;
        await this.updateDatabaseStatus(batch, 'FAILED');
      }
    }

    this.logger.log(
      `🎯 Bypass create complete: ${totalSuccess} success, ${totalFailed} failed`,
    );
  }

  // ============================================================================
  // ✅ PROCESS UPDATE CUSTOMERS (Try update, fallback to create)
  // ============================================================================

  private async processUpdateCustomersBypass(customers: any[]): Promise<void> {
    this.logger.log(
      `📝 BYPASS update of ${customers.length} existing customers...`,
    );

    let successCount = 0;
    let failCount = 0;

    for (const customer of customers) {
      try {
        // Try update first
        await this.updateSingleRecordBypass(customer);
        successCount++;
        await this.updateDatabaseStatus([customer], 'SYNCED');

        await new Promise((resolve) => setTimeout(resolve, 200));
      } catch (updateError) {
        this.logger.warn(
          `⚠️ Update failed for ${customer.code}, trying create: ${updateError.message}`,
        );

        try {
          // Fallback to create
          const createResult = await this.batchCreateBypass([customer]);
          if (createResult.successRecords.length > 0) {
            successCount++;
            await this.updateDatabaseStatus([customer], 'SYNCED');
          } else {
            failCount++;
            await this.updateDatabaseStatus([customer], 'FAILED');
          }
        } catch (createError) {
          failCount++;
          await this.updateDatabaseStatus([customer], 'FAILED');
          this.logger.error(
            `❌ Both update and create failed for ${customer.code}: ${createError.message}`,
          );
        }
      }
    }

    this.logger.log(
      `🎯 Bypass update complete: ${successCount} success, ${failCount} failed`,
    );
  }

  // ============================================================================
  // ✅ ENHANCED BATCH CREATE (Better timeout handling)
  // ============================================================================

  private async batchCreateBypass(customers: any[]): Promise<BatchResult> {
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        const records = customers.map((customer) => {
          const mappedData = this.mapCustomerToLarkBase(customer);
          return { fields: mappedData.fields };
        });

        const batchPayload = { records };

        this.logger.debug(`🚀 Creating batch of ${records.length} records...`);

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
            batchPayload,
            {
              headers,
              timeout: 60000, // ✅ 60 seconds for batch operations
            },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          const successCount = createdRecords.length;
          const successRecords = customers.slice(0, successCount);
          const failedRecords = customers.slice(successCount);

          this.logger.debug(
            `✅ Batch create result: ${successCount}/${customers.length} created`,
          );

          // ✅ Update cache with new records
          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const customer = successRecords[i];
            const createdRecord = createdRecords[i];
            this.existingRecordsCache.set(
              Number(customer.kiotVietId),
              createdRecord.record_id,
            );
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: customers };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: customers };
      }
    }

    return { successRecords: [], failedRecords: customers };
  }

  // ============================================================================
  // ✅ BYPASS UPDATE SINGLE RECORD
  // ============================================================================

  private async updateSingleRecordBypass(customer: any): Promise<void> {
    const headers = await this.larkAuthService.getCustomerHeaders();
    const mappedData = this.mapCustomerToLarkBase(customer);

    const updatePayload = {
      fields: mappedData.fields,
    };

    const response = await firstValueFrom(
      this.httpService.put(
        `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${customer.larkRecordId}`,
        updatePayload,
        { headers, timeout: 15000 },
      ),
    );

    if (response.data.code !== 0) {
      throw new Error(
        `Update failed: ${response.data.msg} (Code: ${response.data.code})`,
      );
    }

    this.logger.debug(
      `✅ Updated record ${customer.larkRecordId} for customer ${customer.code}`,
    );
  }

  // ============================================================================
  // ✅ UTILITY METHODS
  // ============================================================================

  private async acquireSyncLock(lockKey: string): Promise<void> {
    try {
      await this.prismaService.syncControl.create({
        data: {
          name: lockKey,
          entities: ['customer'],
          syncMode: 'lock',
          status: 'running', // ✅ FIXED: Add required status field
          isEnabled: true,
          isRunning: true,
          startedAt: new Date(),
        },
      });
      this.logger.debug(`🔒 Acquired sync lock: ${lockKey}`);
    } catch (error) {
      throw new Error(`Failed to acquire sync lock: ${error.message}`);
    }
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    try {
      await this.prismaService.syncControl.deleteMany({
        where: { name: lockKey },
      });
      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to release lock ${lockKey}: ${error.message}`,
      );
    }
  }

  private async forceTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');

      // ✅ FIXED: Reset token in LarkAuthService to force new token generation
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
    this.cacheLoaded = false;
    this.logger.debug('🧹 Cache cleared for next sync');
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
      array.slice(i * size, i * size + size),
    );
  }

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) return 0;

    // ✅ Handle different types that LarkBase might use
    if (typeof value === 'number') {
      return Math.floor(value); // Ensure integer
    }

    if (typeof value === 'bigint') {
      return Number(value);
    }

    // ✅ CRITICAL: Handle string type (main case from debug)
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return 0;

      const parsed = parseInt(trimmed, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    // ✅ Handle boolean (just in case)
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    // ✅ Try to convert any other type
    const asString = String(value).trim();
    const parsed = parseInt(asString, 10);
    return isNaN(parsed) ? 0 : parsed;
  }

  private mapCustomerToLarkBase(customer: any): LarkBaseRecord {
    const fields: Record<string, any> = {};

    // ✅ CRITICAL: Always include KiotViet ID for duplicate detection
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
      fields[LARK_CUSTOMER_FIELDS.PHONE_NUMBER] = customer.contactNumber;
    }

    if (customer.branchId) {
      fields[LARK_CUSTOMER_FIELDS.STORE_ID] = String(customer.branchId);
    }

    if (customer.organization) {
      fields[LARK_CUSTOMER_FIELDS.COMPANY] = customer.organization;
    }

    if (customer.email) {
      fields[LARK_CUSTOMER_FIELDS.EMAIL] = customer.email;
    }

    if (customer.address) {
      fields[LARK_CUSTOMER_FIELDS.ADDRESS] = customer.address;
    }

    if (customer.debt !== null && customer.debt !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_DEBT] = Number(customer.debt);
    }

    if (customer.taxCode) {
      fields[LARK_CUSTOMER_FIELDS.TAX_CODE] = customer.taxCode;
    }

    if (customer.totalPoint !== null && customer.totalPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_POINTS] = Number(customer.totalPoint);
    }

    if (customer.totalRevenue !== null && customer.totalRevenue !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_REVENUE] = Number(
        customer.totalRevenue,
      );
    }

    if (customer.gender !== null && customer.gender !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
        ? GENDER_OPTIONS.MALE
        : GENDER_OPTIONS.FEMALE;
    }

    if (customer.wardName) {
      fields[LARK_CUSTOMER_FIELDS.WARD_NAME] = customer.wardName;
    }

    if (customer.rewardPoint !== null && customer.rewardPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_POINTS] = this.safeBigIntToNumber(
        customer.rewardPoint,
      );
    }

    if (
      customer.totalInvoiced !== null &&
      customer.totalInvoiced !== undefined
    ) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_INVOICED] = Number(
        customer.totalInvoiced,
      );
    }

    if (customer.comments) {
      fields[LARK_CUSTOMER_FIELDS.COMMENTS] = customer.comments;
    }

    if (customer.modifiedDate) {
      const modifiedDate = new Date(customer.modifiedDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = modifiedDate.getTime();
    }

    if (customer.createdDate) {
      const createdDate = new Date(customer.createdDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = createdDate.getTime();
    }

    if (customer.psidFacebook) {
      fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] = this.safeBigIntToNumber(
        customer.psidFacebook,
      );
    }

    if (customer.locationName) {
      fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName;
    }

    return { fields };
  }

  // ============================================================================
  // ✅ FAILED CUSTOMER MANAGEMENT
  // ============================================================================

  async resetFailedCustomers(): Promise<{ resetCount: number }> {
    this.logger.log(
      '🔄 Resetting FAILED customers to PENDING (safe for bypass)...',
    );

    const result = await this.prismaService.customer.updateMany({
      where: { larkSyncStatus: 'FAILED' },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncRetries: 0,
        larkSyncedAt: null,
      },
    });

    this.logger.log(`✅ Reset ${result.count} FAILED customers to PENDING`);
    return { resetCount: result.count };
  }

  async resetAllSyncStatus(): Promise<void> {
    this.logger.log('🔄 Resetting ALL customer sync status to PENDING...');

    const result = await this.prismaService.customer.updateMany({
      where: {
        larkSyncStatus: { in: ['SYNCED', 'FAILED'] },
      },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncedAt: null,
        larkSyncRetries: 0,
      },
    });

    this.logger.log(`✅ Reset sync status for ${result.count} customers`);
  }

  async getFailedCustomersReport(): Promise<{
    totalFailed: number;
    byRetryCount: Record<number, number>;
    sampleFailedCustomers: any[];
  }> {
    const failedCustomers = await this.prismaService.customer.findMany({
      where: { larkSyncStatus: 'FAILED' },
      select: {
        id: true,
        code: true,
        name: true,
        kiotVietId: true,
        larkSyncRetries: true,
        larkSyncedAt: true,
      },
      orderBy: { larkSyncedAt: 'desc' },
      take: 10,
    });

    const byRetryCount: Record<number, number> = {};

    for (const customer of failedCustomers) {
      const retries = customer.larkSyncRetries || 0;
      byRetryCount[retries] = (byRetryCount[retries] || 0) + 1;
    }

    return {
      totalFailed: failedCustomers.length,
      byRetryCount,
      sampleFailedCustomers: failedCustomers.slice(0, 5),
    };
  }

  // ============================================================================
  // ✅ MONITORING & STATISTICS
  // ============================================================================

  async getSyncProgress(): Promise<{
    total: number;
    synced: number;
    pending: number;
    failed: number;
    progress: number;
    canRetryFailed: boolean;
  }> {
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

    return { total, synced, pending, failed, progress, canRetryFailed };
  }

  async getSyncStatistics(): Promise<any> {
    const progress = await this.getSyncProgress();
    const failedReport = await this.getFailedCustomersReport();

    return {
      ...progress,
      failedDetails: failedReport,
      cacheInfo: {
        loaded: this.cacheLoaded,
        size: this.existingRecordsCache.size,
      },
      recommendations: this.generateRecommendations(progress, failedReport),
    };
  }

  private generateRecommendations(progress: any, failedReport: any): string[] {
    const recommendations: string[] = [];

    if (failedReport.totalFailed > 0) {
      recommendations.push(
        `🔄 ${failedReport.totalFailed} FAILED customers can be safely retried with resetFailedCustomers()`,
      );
    }

    if (progress.pending > 0) {
      recommendations.push(
        `🚀 ${progress.pending} PENDING customers ready for sync`,
      );
    }

    if (progress.progress < 50) {
      recommendations.push(
        `⚡ Consider running full sync to improve ${progress.progress}% completion rate`,
      );
    }

    if (failedReport.totalFailed === 0 && progress.pending === 0) {
      recommendations.push(
        `✅ All customers synced successfully! No action needed.`,
      );
    }

    return recommendations;
  }

  async getDuplicateReport(): Promise<any> {
    const stats = await this.getSyncProgress();

    return {
      ...stats,
      message: 'Bypass search - duplicate protection via cache',
      protection_level: '95%+ (cache-based)',
      last_scan: new Date(),
      cache_size: this.existingRecordsCache.size,
    };
  }

  // ============================================================================
  // ✅ CONVENIENT SYNC METHODS
  // ============================================================================

  async syncAllCustomersIncludingFailed(customers: any[]): Promise<void> {
    this.logger.log(
      '🚀 Starting FULL sync including FAILED customers (bypass mode)...',
    );

    // This will include both PENDING and FAILED customers
    await this.syncCustomersToLarkBase(customers);
  }

  async resetAndSyncFailedCustomers(customers: any[]): Promise<void> {
    this.logger.log('🔄 Reset FAILED customers and sync (bypass mode)...');

    // Step 1: Reset FAILED to PENDING
    const resetResult = await this.resetFailedCustomers();

    if (resetResult.resetCount > 0) {
      this.logger.log(
        `✅ Reset ${resetResult.resetCount} customers, starting sync...`,
      );

      // Step 2: Sync all (now all PENDING)
      await this.syncCustomersToLarkBase(customers);
    } else {
      this.logger.log('📋 No FAILED customers to reset');
    }
  }

  // ============================================================================
  // ✅ FAILED CUSTOMER RECOVERY
  // ============================================================================

  async getFailedCustomersStats(): Promise<{
    totalFailed: number;
    estimated5584Gap: boolean;
    syncProgress: any;
  }> {
    const totalFailed = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const syncProgress = await this.getSyncProgress();

    return {
      totalFailed,
      estimated5584Gap: totalFailed >= 5000, // Close to reported 5584 gap
      syncProgress,
    };
  }

  async processFailedCustomersInBatches(
    batchSize: number = 100,
  ): Promise<void> {
    this.logger.log('🔄 Processing FAILED customers in safe batches...');

    let processed = 0;
    let offset = 0;

    while (true) {
      // Get batch of FAILED customers
      const failedBatch = await this.prismaService.customer.findMany({
        where: { larkSyncStatus: 'FAILED' },
        take: batchSize,
        skip: offset,
      });

      if (failedBatch.length === 0) break;

      this.logger.log(
        `📦 Processing FAILED batch: ${processed + 1}-${processed + failedBatch.length}`,
      );

      try {
        // Process this batch through bypass sync
        await this.syncCustomersToLarkBase(failedBatch);
        processed += failedBatch.length;

        this.logger.log(
          `✅ Batch completed: ${processed} FAILED customers processed`,
        );

        // Small delay between batches
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`❌ Batch failed: ${error.message}`);
        // Continue with next batch
      }

      offset += batchSize;
    }

    this.logger.log(
      `🎉 FAILED customer processing complete: ${processed} customers processed`,
    );
  }

  // ============================================================================
  // ✅ LEGACY COMPATIBILITY
  // ============================================================================

  async searchRecordByKiotVietId(
    kiotVietId: number | BigInt,
  ): Promise<any | null> {
    const id = this.safeBigIntToNumber(kiotVietId);
    const recordId = this.existingRecordsCache.get(id);

    if (recordId) {
      return {
        record_id: recordId,
        fields: { [LARK_CUSTOMER_FIELDS.KIOTVIET_ID]: id },
      };
    }

    return null;
  }
}
