// src/services/lark/customer/lark-customer-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// Field mappings - Based on Khách Hàng.rtf configuration
const LARK_CUSTOMER_FIELDS = {
  PRIMARY_NAME: 'fld71g8Gci',
  CUSTOMER_CODE: 'fld29zIB9D',
  PHONE_NUMBER: 'fldHo79lXi',
  STORE_ID: 'fld6M0YzOE',
  COMPANY: 'fldUubtChK',
  EMAIL: 'fldRXGBAzC',
  ADDRESS: 'fld17QvTM6',
  CURRENT_DEBT: 'fldEBifOyt',
  TAX_CODE: 'fldCDKr4yC',
  TOTAL_POINTS: 'fld9zfi74R',
  TOTAL_REVENUE: 'fldStZEptP',
  GENDER: 'fldLa1obN8',
  WARD_NAME: 'fldU0Vru4a',
  CURRENT_POINTS: 'fldujW0cpW',
  KIOTVIET_ID: 'fldN5NE17y',
  TOTAL_INVOICED: 'fld1gzrrvR',
  COMMENTS: 'fldRFEVYOn',
  MODIFIED_DATE: 'fldK8teGni',
  CREATED_DATE: 'flddDuUUEg',
  FACEBOOK_ID: 'fldh8TIi9K',
  LOCATION_NAME: 'fldU3fKuoa',
} as const;

const GENDER_OPTIONS = {
  MALE: 'optUmkTfdd',
  FEMALE: 'optcf5ndAC',
} as const;

interface LarkBaseRecord {
  record_id?: string;
  fields: Record<string, any>;
}

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
  };
}

interface DuplicateCheckResult {
  kiotVietId: number;
  larkRecordId: string | null;
  isDuplicate: boolean;
}

@Injectable()
export class LarkCustomerSyncService {
  private readonly logger = new Logger(LarkCustomerSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 25; // ✅ Reduced batch size for stability

  // ✅ AUTH ERROR CODES
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;
  private readonly MAX_INDIVIDUAL_RETRIES = 2;

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
  // ✅ ENHANCED MAIN SYNC WITH ROBUST DUPLICATE DETECTION
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers...`,
      );

      // ✅ Step 1: Pre-filter customers that are already synced
      const customersToCheck = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      );

      if (customersToCheck.length === 0) {
        this.logger.log(
          '📋 No customers need LarkBase sync - all already synced!',
        );
        return;
      }

      this.logger.log(
        `📊 Resuming sync: ${customersToCheck.length}/${customers.length} customers need sync`,
      );

      // ✅ Step 2: Batch duplicate check to identify existing records
      const duplicateCheckResults =
        await this.batchCheckDuplicates(customersToCheck);

      // ✅ Step 3: Separate new records vs updates
      const newCustomers = [];
      const updateCustomers = [];

      for (const customer of customersToCheck) {
        const checkResult = duplicateCheckResults.find(
          (r) => r.kiotVietId === customer.kiotVietId,
        );

        if (checkResult?.isDuplicate && checkResult.larkRecordId) {
          // Existing record - prepare for update
          updateCustomers.push({
            ...customer,
            larkRecordId: checkResult.larkRecordId,
          });
        } else {
          // New record - prepare for creation
          newCustomers.push(customer);
        }
      }

      this.logger.log(
        `📋 Duplicate check complete: ${newCustomers.length} new, ${updateCustomers.length} updates`,
      );

      // ✅ Step 4: Process new records in batches
      if (newCustomers.length > 0) {
        await this.processNewCustomers(newCustomers);
      }

      // ✅ Step 5: Process updates individually (more reliable)
      if (updateCustomers.length > 0) {
        await this.processUpdateCustomers(updateCustomers);
      }

      this.logger.log(`🎉 LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // ✅ ROBUST DUPLICATE DETECTION SYSTEM
  // ============================================================================

  async batchCheckDuplicates(
    customers: any[],
  ): Promise<DuplicateCheckResult[]> {
    this.logger.log(
      `🔍 Checking duplicates for ${customers.length} customers...`,
    );

    const results: DuplicateCheckResult[] = [];
    const batchSize = 50; // Check duplicates in smaller batches

    for (let i = 0; i < customers.length; i += batchSize) {
      const batch = customers.slice(i, i + batchSize);

      try {
        // ✅ Build complex filter for multiple KiotViet IDs
        const filters = batch.map((customer) => ({
          field_name: LARK_CUSTOMER_FIELDS.KIOTVIET_ID,
          operator: 'is',
          value: [customer.kiotVietId.toString()],
        }));

        // ✅ Use OR condition to check multiple IDs at once
        const searchFilter = {
          conjunction: 'or',
          conditions: filters,
        };

        const searchResults = await this.searchLarkBaseRecords(searchFilter);

        // ✅ Map results back to customers
        for (const customer of batch) {
          const existingRecord = searchResults.find(
            (record) =>
              record.fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] ===
              customer.kiotVietId.toString(),
          );

          results.push({
            kiotVietId: customer.kiotVietId,
            larkRecordId: existingRecord?.record_id || null,
            isDuplicate: !!existingRecord,
          });
        }

        this.logger.log(
          `✅ Batch ${Math.floor(i / batchSize) + 1}: Found ${searchResults.length} existing records`,
        );

        // Small delay between batches to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 200));
      } catch (error) {
        this.logger.warn(
          `⚠️ Duplicate check failed for batch ${Math.floor(i / batchSize) + 1}: ${error.message}`,
        );

        // ✅ Fallback: Treat as new records if check fails
        for (const customer of batch) {
          results.push({
            kiotVietId: customer.kiotVietId,
            larkRecordId: null,
            isDuplicate: false,
          });
        }
      }
    }

    return results;
  }

  // ============================================================================
  // ✅ ENHANCED SEARCH WITH RETRY LOGIC
  // ============================================================================

  private async searchLarkBaseRecords(filter: any): Promise<any[]> {
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        const searchPayload = {
          filter,
          page_size: 500, // Max page size for search
        };

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`,
            searchPayload,
            { headers },
          ),
        );

        if (response.data.code === 0) {
          return response.data.data?.items || [];
        }

        // ✅ Check for auth errors
        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          this.logger.warn(
            `🔄 Auth error in search: ${response.data.msg}. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        throw new Error(`Search failed: ${response.data.msg}`);
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.warn(`⚠️ Search error: ${error.message}`);
        throw error;
      }
    }

    throw new Error(`Search failed after ${this.MAX_AUTH_RETRIES} retries`);
  }

  // ============================================================================
  // ✅ PROCESS NEW CUSTOMERS (BATCH CREATE)
  // ============================================================================

  private async processNewCustomers(customers: any[]): Promise<void> {
    this.logger.log(
      `📝 Creating ${customers.length} new customers in LarkBase...`,
    );

    const batches = this.createBatches(customers, this.batchSize);
    let successCount = 0;
    let failedCount = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      try {
        this.logger.log(
          `📦 Creating batch ${i + 1}/${batches.length} (${batch.length} customers)`,
        );

        const batchResult = await this.createBatchWithRetry(batch);
        successCount += batchResult.successCount;
        failedCount += batchResult.failedCount;

        // ✅ Mark successful customers as SYNCED
        if (batchResult.successCount > 0) {
          await this.markCustomersAsSynced(
            batch.slice(0, batchResult.successCount),
          );
        }

        // ✅ Mark failed customers
        if (batchResult.failedCount > 0) {
          await this.markCustomersAsFailed(
            batch.slice(batchResult.successCount),
            'Batch creation failed',
          );
        }

        await new Promise((resolve) => setTimeout(resolve, 300));
      } catch (error) {
        this.logger.error(
          `❌ Batch ${i + 1} creation failed: ${error.message}`,
        );
        await this.markCustomersAsFailed(batch, error.message);
        failedCount += batch.length;
      }
    }

    this.logger.log(
      `✅ New customers processed: ${successCount} created, ${failedCount} failed`,
    );
  }

  // ============================================================================
  // ✅ PROCESS UPDATE CUSTOMERS (INDIVIDUAL UPDATES)
  // ============================================================================

  private async processUpdateCustomers(customers: any[]): Promise<void> {
    this.logger.log(
      `📝 Updating ${customers.length} existing customers in LarkBase...`,
    );

    let successCount = 0;
    let failedCount = 0;

    for (const customer of customers) {
      try {
        await this.updateSingleCustomer(customer);
        await this.markCustomersAsSynced([customer]);
        successCount++;

        this.logger.debug(`✅ Updated customer ${customer.code}`);

        // Small delay between individual updates
        await new Promise((resolve) => setTimeout(resolve, 100));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to update customer ${customer.code}: ${error.message}`,
        );

        await this.markCustomersAsFailed([customer], error.message);
        failedCount++;
      }
    }

    this.logger.log(
      `✅ Update processing complete: ${successCount} updated, ${failedCount} failed`,
    );
  }

  // ============================================================================
  // ✅ INDIVIDUAL CUSTOMER UPDATE
  // ============================================================================

  private async updateSingleCustomer(customer: any): Promise<void> {
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();
        const recordData = this.mapCustomerToLarkBase(customer);

        const updatePayload = {
          fields: recordData.fields,
        };

        const response = await firstValueFrom(
          this.httpService.put(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${customer.larkRecordId}`,
            updatePayload,
            { headers },
          ),
        );

        if (response.data.code === 0) {
          return; // Success
        }

        // ✅ Check for auth errors
        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          this.logger.warn(
            `🔄 Auth error updating customer ${customer.code}: ${response.data.msg}. Retry ${authRetries}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
            continue;
          }
        }

        throw new Error(`Update failed: ${response.data.msg}`);
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
            continue;
          }
        }

        throw error;
      }
    }

    throw new Error(`Update failed after ${this.MAX_AUTH_RETRIES} retries`);
  }

  // ============================================================================
  // ✅ BATCH CREATE WITH RETRY
  // ============================================================================

  private async createBatchWithRetry(customers: any[]): Promise<{
    successCount: number;
    failedCount: number;
  }> {
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        const records = customers.map((customer) => {
          const mappedData = this.mapCustomerToLarkBase(customer);
          return { fields: mappedData.fields };
        });

        const batchPayload = { records };

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
            batchPayload,
            { headers },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          return {
            successCount: createdRecords.length,
            failedCount: customers.length - createdRecords.length,
          };
        }

        // ✅ Check for auth errors
        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          this.logger.warn(
            `🔄 Auth error in batch create: ${response.data.msg}. Retry ${authRetries}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        throw new Error(`Batch create failed: ${response.data.msg}`);
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        throw error;
      }
    }

    throw new Error(
      `Batch create failed after ${this.MAX_AUTH_RETRIES} retries`,
    );
  }

  // ============================================================================
  // ✅ TOKEN MANAGEMENT
  // ============================================================================

  private async forceTokenRefresh(): Promise<void> {
    try {
      this.logger.log('🔄 Forcing LarkBase token refresh...');

      // Clear cached token in auth service
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;

      // Get fresh token
      await this.larkAuthService.getCustomerHeaders();

      this.logger.log('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh LarkBase token: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // ✅ DATABASE STATUS TRACKING
  // ============================================================================

  private async markCustomersAsSynced(customers: any[]): Promise<void> {
    try {
      const customerIds = customers.map((c) => c.id);

      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'SYNCED',
          larkSyncedAt: new Date(),
          larkSyncRetries: 0,
        },
      });

      this.logger.debug(`✅ Marked ${customers.length} customers as SYNCED`);
    } catch (error) {
      this.logger.error(
        `❌ Failed to mark customers as synced: ${error.message}`,
      );
    }
  }

  private async markCustomersAsFailed(
    customers: any[],
    error: string,
  ): Promise<void> {
    try {
      const customerIds = customers.map((c) => c.id);

      // ✅ Increment retry count and mark as failed if too many retries
      for (const customerId of customerIds) {
        const customer = await this.prismaService.customer.findUnique({
          where: { id: customerId },
          select: { larkSyncRetries: true },
        });

        const retryCount = (customer?.larkSyncRetries || 0) + 1;
        const status =
          retryCount >= this.MAX_INDIVIDUAL_RETRIES ? 'FAILED' : 'PENDING';

        await this.prismaService.customer.update({
          where: { id: customerId },
          data: {
            larkSyncStatus: status,
            larkSyncedAt: new Date(),
            larkSyncRetries: retryCount,
            larkSyncError: error.substring(0, 500),
          },
        });
      }

      this.logger.debug(`⚠️ Marked ${customers.length} customers with error`);
    } catch (dbError) {
      this.logger.error(
        `❌ Failed to mark customers as failed: ${dbError.message}`,
      );
    }
  }

  // ============================================================================
  // ✅ UTILITY METHODS
  // ============================================================================

  private createBatches<T>(items: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }

  private mapCustomerToLarkBase(customer: any): LarkBaseRecord {
    const fields: Record<string, any> = {};

    // ✅ Map all customer fields based on Khách Hàng.rtf configuration
    if (customer.name) {
      fields[LARK_CUSTOMER_FIELDS.PRIMARY_NAME] = customer.name;
    }

    if (customer.code) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_CODE] = customer.code;
    }

    if (customer.contactNumber) {
      fields[LARK_CUSTOMER_FIELDS.PHONE_NUMBER] = customer.contactNumber;
    }

    if (customer.email) {
      fields[LARK_CUSTOMER_FIELDS.EMAIL] = customer.email;
    }

    if (customer.address) {
      fields[LARK_CUSTOMER_FIELDS.ADDRESS] = customer.address;
    }

    if (customer.organization) {
      fields[LARK_CUSTOMER_FIELDS.COMPANY] = customer.organization;
    }

    if (customer.taxCode) {
      fields[LARK_CUSTOMER_FIELDS.TAX_CODE] = customer.taxCode;
    }

    if (customer.wardName) {
      fields[LARK_CUSTOMER_FIELDS.WARD_NAME] = customer.wardName;
    }

    if (customer.retailerId) {
      fields[LARK_CUSTOMER_FIELDS.STORE_ID] = customer.retailerId.toString();
    }

    if (customer.debt !== null && customer.debt !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_DEBT] = Number(customer.debt);
    }

    if (customer.totalRevenue !== null && customer.totalRevenue !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_REVENUE] = Number(
        customer.totalRevenue,
      );
    }

    if (customer.totalPoint !== null && customer.totalPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_POINTS] = Number(customer.totalPoint);
    }

    if (customer.rewardPoint !== null && customer.rewardPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_POINTS] = Number(
        customer.rewardPoint,
      );
    }

    if (customer.gender !== null && customer.gender !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
        ? GENDER_OPTIONS.MALE
        : GENDER_OPTIONS.FEMALE;
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
      const vietnamDate = new Date(customer.modifiedDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = vietnamDate.getTime();
    }

    if (customer.createdDate) {
      const vietnamDate = new Date(customer.createdDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = vietnamDate.getTime();
    }

    if (customer.psidFacebook) {
      fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] =
        customer.psidFacebook.toString();
    }

    if (customer.locationName) {
      fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName;
    }

    // ✅ CRITICAL: Always include KiotViet ID for duplicate detection
    fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = customer.kiotVietId.toString();

    return { fields };
  }

  // ============================================================================
  // ✅ SYNC STATISTICS & MONITORING
  // ============================================================================

  async getSyncStatistics(): Promise<any> {
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

  // ============================================================================
  // ✅ LEGACY COMPATIBILITY METHODS (if needed by other parts of system)
  // ============================================================================

  async searchRecordByKiotVietId(kiotVietId: number): Promise<any | null> {
    try {
      const filter = {
        field_name: LARK_CUSTOMER_FIELDS.KIOTVIET_ID,
        operator: 'is',
        value: [kiotVietId.toString()],
      };

      const results = await this.searchLarkBaseRecords({
        conditions: [filter],
      });
      return results.length > 0 ? results[0] : null;
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to search for KiotViet ID ${kiotVietId}: ${error.message}`,
      );
      return null;
    }
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
        larkSyncError: null,
      },
    });

    this.logger.log(`✅ Reset sync status for ${result.count} customers`);
  }
}
