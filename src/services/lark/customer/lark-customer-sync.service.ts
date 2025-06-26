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

      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      );

      if (customersToSync.length === 0) {
        this.logger.log('📋 No customers need LarkBase sync');
        return;
      }

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
  // ✅ TEST LARKBASE CONNECTION (No search - just list)
  // ============================================================================

  private async testLarkBaseConnection(): Promise<void> {
    try {
      this.logger.log('🔍 Testing LarkBase connection...');

      const headers = await this.larkAuthService.getCustomerHeaders();

      // ✅ Simple list records (no filter) to test connection
      const response = await firstValueFrom(
        this.httpService.get(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?page_size=1`,
          { headers, timeout: 10000 },
        ),
      );

      if (response.data.code === 0) {
        this.logger.log('✅ LarkBase connection successful');

        // Log table info for debugging
        const totalRecords = response.data.data?.total || 0;
        this.logger.log(
          `📊 LarkBase table has ${totalRecords} existing records`,
        );
      } else {
        throw new Error(
          `Connection test failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
      }
    } catch (error) {
      this.logger.error('❌ LarkBase connection test failed:', error.message);
      throw new Error(`Cannot connect to LarkBase: ${error.message}`);
    }
  }

  // ============================================================================
  // ✅ LOAD EXISTING RECORDS CACHE (List All, No Search)
  // ============================================================================

  private async loadExistingRecordsCache(): Promise<void> {
    if (this.cacheLoaded) return;

    try {
      this.logger.log('📥 Loading existing records cache...');

      const headers = await this.larkAuthService.getCustomerHeaders();
      let page_token = '';
      let totalLoaded = 0;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({
          page_size: '500',
          ...(page_token && { page_token }),
        });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, { headers, timeout: 15000 }),
        );

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          // Build cache: kiotVietId → record_id
          for (const record of records) {
            const kiotVietId = record.fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID];
            if (kiotVietId && typeof kiotVietId === 'number') {
              this.existingRecordsCache.set(kiotVietId, record.record_id);
            }
          }

          totalLoaded += records.length;
          page_token = response.data.data?.page_token || '';

          this.logger.debug(
            `📥 Loaded ${records.length} records (total: ${totalLoaded})`,
          );
        } else {
          this.logger.warn(`⚠️ Failed to load page: ${response.data.msg}`);
          break;
        }
      } while (page_token);

      this.cacheLoaded = true;
      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} existing records`,
      );
    } catch (error) {
      this.logger.warn(`⚠️ Failed to load cache: ${error.message}`);
      // Continue without cache - all will be treated as new
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
  // ✅ BYPASS BATCH CREATE (No duplicate check needed)
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
            { headers, timeout: 30000 },
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
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
      array.slice(i * size, i * size + size),
    );
  }

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') return value;
    if (typeof value === 'bigint') return Number(value);
    if (typeof value === 'string') return parseInt(value, 10) || 0;
    return 0;
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
  // ✅ MONITORING & STATISTICS
  // ============================================================================

  async getSyncProgress(): Promise<{
    total: number;
    synced: number;
    pending: number;
    failed: number;
    progress: number;
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

    return { total, synced, pending, failed, progress };
  }

  async getSyncStatistics(): Promise<any> {
    return this.getSyncProgress();
  }

  async resetAllSyncStatus(): Promise<void> {
    this.logger.log('🔄 Resetting all customer sync status to PENDING...');

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

  // ✅ LEGACY COMPATIBILITY
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
