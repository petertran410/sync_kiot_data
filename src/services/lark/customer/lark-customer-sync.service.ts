import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// Field mappings (same as before)
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

@Injectable()
export class LarkCustomerSyncService {
  private readonly logger = new Logger(LarkCustomerSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 50;

  // ✅ AUTH ERROR CODES
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

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
  // ✅ MAIN SYNC WITH RESUME CAPABILITY
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers...`,
      );

      // ✅ RESUME LOGIC: Filter customers already synced
      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      );

      if (customersToSync.length === 0) {
        this.logger.log(
          '📋 No customers need LarkBase sync - all already synced!',
        );
        return;
      }

      this.logger.log(
        `📊 Resuming sync: ${customersToSync.length}/${customers.length} customers need sync`,
      );

      // Process in batches with auth retry
      const batches = this.createBatches(customersToSync, this.batchSize);
      let syncedCount = 0;
      let failedCount = 0;

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        this.logger.log(
          `📦 Processing batch ${i + 1}/${batches.length} (${batch.length} customers)`,
        );

        try {
          const batchResults = await this.processBatchWithRetry(batch);
          syncedCount += batchResults.successCount;
          failedCount += batchResults.failedCount;

          this.logger.log(
            `✅ Batch ${i + 1} completed: ${batchResults.successCount} synced, ${batchResults.failedCount} failed`,
          );

          // ✅ UPDATE PROGRESS: Mark successful customers as SYNCED
          await this.markCustomersAsSynced(
            batch.slice(0, batchResults.successCount),
          );

          // Small delay between batches
          if (i < batches.length - 1) {
            await new Promise((resolve) => setTimeout(resolve, 200));
          }
        } catch (error) {
          this.logger.error(
            `❌ Batch ${i + 1} failed after retries: ${error.message}`,
          );

          // ✅ PARTIAL PROGRESS: Don't fail entire sync, mark batch as failed
          await this.markCustomersAsFailed(batch, error.message);
          failedCount += batch.length;
        }
      }

      this.logger.log(
        `🎉 LarkBase sync completed: ${syncedCount} synced, ${failedCount} failed`,
      );
    } catch (error) {
      this.logger.error(`❌ LarkBase sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // ✅ BATCH PROCESSING WITH AUTH RETRY
  // ============================================================================

  private async processBatchWithRetry(
    customers: any[],
  ): Promise<{ successCount: number; failedCount: number }> {
    const createRecords: LarkBaseRecord[] = [];
    const updateRecords: Array<{ recordId: string; record: LarkBaseRecord }> =
      [];
    let successCount = 0;
    let failedCount = 0;

    // Prepare records for sync
    for (const customer of customers) {
      try {
        this.logger.debug(
          `🔍 Processing customer: ${customer.code} (KiotViet ID: ${customer.kiotVietId})`,
        );

        const larkRecord = this.mapCustomerToLarkBase(customer);

        if (customer.larkRecordId) {
          updateRecords.push({
            recordId: customer.larkRecordId,
            record: larkRecord,
          });
        } else {
          // ✅ SAFE SEARCH: With auth retry
          const existingRecord = await this.findLarkRecordByKiotVietIdWithRetry(
            customer.kiotVietId,
          );

          if (existingRecord) {
            updateRecords.push({
              recordId: existingRecord.record_id,
              record: larkRecord,
            });

            // Update database with found larkRecordId
            await this.prismaService.customer.update({
              where: { id: customer.id },
              data: { larkRecordId: existingRecord.record_id },
            });
          } else {
            createRecords.push(larkRecord);
          }
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to process customer ${customer.code}: ${error.message}`,
        );
        failedCount++;
      }
    }

    // ✅ EXECUTE OPERATIONS WITH AUTH RETRY
    try {
      if (createRecords.length > 0) {
        this.logger.log(
          `📤 Creating ${createRecords.length} new LarkBase records...`,
        );
        const createdRecords =
          await this.batchCreateRecordsWithRetry(createRecords);

        await this.updateCustomersWithLarkRecordIds(
          customers.filter((c) => !c.larkRecordId),
          createdRecords,
        );

        successCount += createdRecords.length;
      }

      if (updateRecords.length > 0) {
        this.logger.log(
          `📤 Updating ${updateRecords.length} existing LarkBase records...`,
        );
        await this.batchUpdateRecordsWithRetry(updateRecords);
        successCount += updateRecords.length;
      }
    } catch (error) {
      this.logger.error(
        `❌ Batch operation failed after retries: ${error.message}`,
      );
      failedCount += createRecords.length + updateRecords.length;
    }

    return { successCount, failedCount };
  }

  // ============================================================================
  // ✅ LARKBASE API OPERATIONS WITH AUTO AUTH RETRY
  // ============================================================================

  private async batchCreateRecordsWithRetry(
    records: LarkBaseRecord[],
  ): Promise<Array<{ record_id: string; fields: any }>> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        this.logger.debug(
          `🔍 Creating ${records.length} LarkBase records (attempt ${authRetries + 1})`,
        );

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
            { records: records },
            { headers },
          ),
        );

        const result: LarkBatchResponse = response.data;

        if (result.code !== 0) {
          // ✅ CHECK FOR AUTH ERRORS
          if (this.AUTH_ERROR_CODES.includes(result.code)) {
            authRetries++;
            this.logger.warn(
              `⚠️ Auth error ${result.code}: ${result.msg}. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
            );

            // ✅ FORCE TOKEN REFRESH
            await this.forceTokenRefresh();

            if (authRetries < this.MAX_AUTH_RETRIES) {
              await new Promise((resolve) => setTimeout(resolve, 2000));
              continue;
            }
          }

          throw new Error(
            `LarkBase CREATE failed: Code ${result.code}, Message: ${result.msg}`,
          );
        }

        const createdRecords = result.data?.records || [];
        this.logger.log(
          `✅ Created ${createdRecords.length} LarkBase records successfully`,
        );

        return createdRecords;
      } catch (error) {
        // ✅ HANDLE HTTP ERRORS (401, 403, etc.)
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          this.logger.warn(
            `⚠️ HTTP ${error.response.status} error. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.error(`❌ LarkBase batch CREATE error: ${error.message}`);
        if (error.response) {
          this.logger.error(`📤 HTTP Status: ${error.response.status}`);
          this.logger.error(
            `📤 Response Data: ${JSON.stringify(error.response.data, null, 2)}`,
          );
        }
        throw error;
      }
    }

    throw new Error(
      `LarkBase CREATE failed after ${this.MAX_AUTH_RETRIES} auth retries`,
    );
  }

  private async batchUpdateRecordsWithRetry(
    updateRecords: Array<{ recordId: string; record: LarkBaseRecord }>,
  ): Promise<void> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        const records = updateRecords.map((ur) => ({
          record_id: ur.recordId,
          fields: ur.record.fields,
        }));

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_update`,
            { records: records },
            { headers },
          ),
        );

        const result: LarkBatchResponse = response.data;

        if (result.code !== 0) {
          if (this.AUTH_ERROR_CODES.includes(result.code)) {
            authRetries++;
            this.logger.warn(
              `⚠️ Auth error ${result.code}: ${result.msg}. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
            );

            await this.forceTokenRefresh();

            if (authRetries < this.MAX_AUTH_RETRIES) {
              await new Promise((resolve) => setTimeout(resolve, 2000));
              continue;
            }
          }

          throw new Error(`LarkBase UPDATE failed: ${result.msg}`);
        }

        this.logger.log(
          `✅ Updated ${updateRecords.length} LarkBase records successfully`,
        );
        return;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          this.logger.warn(
            `⚠️ HTTP ${error.response.status} error. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.error(`❌ LarkBase batch UPDATE error: ${error.message}`);
        throw error;
      }
    }

    throw new Error(
      `LarkBase UPDATE failed after ${this.MAX_AUTH_RETRIES} auth retries`,
    );
  }

  private async findLarkRecordByKiotVietIdWithRetry(
    kiotVietId: number,
  ): Promise<any | null> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getCustomerHeaders();

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`,
            {
              filter: {
                conjunction: 'and',
                conditions: [
                  {
                    field_name: LARK_CUSTOMER_FIELDS.KIOTVIET_ID,
                    operator: 'is',
                    value: [Number(kiotVietId)],
                  },
                ],
              },
              page_size: 1,
            },
            { headers },
          ),
        );

        const result = response.data;

        // ✅ CHECK FOR AUTH ERRORS
        if (result.code !== 0 && this.AUTH_ERROR_CODES.includes(result.code)) {
          authRetries++;
          this.logger.warn(
            `⚠️ Search auth error ${result.code}. Retry ${authRetries}/${this.MAX_AUTH_RETRIES}`,
          );

          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        if (result.code === 0 && result.data?.items?.length > 0) {
          return result.data.items[0];
        }
        return null;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();

          if (authRetries < this.MAX_AUTH_RETRIES) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }
        }

        this.logger.warn(
          `⚠️ Failed to search LarkBase for kiotVietId ${kiotVietId}: ${error.message}`,
        );
        return null;
      }
    }

    this.logger.warn(
      `⚠️ Search failed after ${this.MAX_AUTH_RETRIES} retries for kiotVietId ${kiotVietId}`,
    );
    return null;
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
  // ✅ PROGRESS TRACKING
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

      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncedAt: new Date(),
          larkSyncRetries: { increment: 1 },
        },
      });

      this.logger.debug(`❌ Marked ${customers.length} customers as FAILED`);
    } catch (dbError) {
      this.logger.error(
        `❌ Failed to mark customers as failed: ${dbError.message}`,
      );
    }
  }

  // ============================================================================
  // ✅ RESUME SYNC STATUS CHECK
  // ============================================================================

  async getSyncProgress(): Promise<{
    total: number;
    synced: number;
    pending: number;
    failed: number;
    progress: number;
  }> {
    const stats = await this.prismaService.customer.groupBy({
      by: ['larkSyncStatus'],
      _count: { larkSyncStatus: true },
    });

    const total = await this.prismaService.customer.count();

    const synced =
      stats.find((s) => s.larkSyncStatus === 'SYNCED')?._count
        ?.larkSyncStatus || 0;
    const pending =
      stats.find((s) => s.larkSyncStatus === 'PENDING')?._count
        ?.larkSyncStatus || 0;
    const failed =
      stats.find((s) => s.larkSyncStatus === 'FAILED')?._count
        ?.larkSyncStatus || 0;

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    return { total, synced, pending, failed, progress };
  }

  // ============================================================================
  // ✅ UTILITY METHODS (Keep existing mapping logic)
  // ============================================================================

  private createBatches<T>(items: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }

  private mapCustomerToLarkBase(customer: any): LarkBaseRecord {
    // Keep existing mapping logic
    const fields: Record<string, any> = {};

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

    if (customer.kiotVietId) {
      fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = Number(customer.kiotVietId);
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

    if (customer.locationName) {
      fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName;
    }

    if (customer.psidFacebook !== null && customer.psidFacebook !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] = Number(customer.psidFacebook);
    }

    if (customer.createdDate) {
      const vietnamDate = new Date(customer.createdDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = vietnamDate.getTime();
    }

    if (customer.modifiedDate) {
      const vietnamDate = new Date(customer.modifiedDate + '+07:00');
      fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = vietnamDate.getTime;
    }

    return { fields };
  }

  private async updateCustomersWithLarkRecordIds(
    customers: any[],
    larkRecords: Array<{ record_id: string; fields: any }>,
  ): Promise<void> {
    try {
      const customersWithoutLarkId = customers.filter((c) => !c.larkRecordId);

      for (
        let i = 0;
        i < customersWithoutLarkId.length && i < larkRecords.length;
        i++
      ) {
        const customer = customersWithoutLarkId[i];
        const larkRecord = larkRecords[i];

        await this.prismaService.customer.update({
          where: { id: customer.id },
          data: {
            larkRecordId: larkRecord.record_id,
            larkSyncStatus: 'SYNCED',
            larkSyncedAt: new Date(),
            larkSyncRetries: 0,
          },
        });
      }

      this.logger.log(
        `📝 Updated ${Math.min(customersWithoutLarkId.length, larkRecords.length)} customers with LarkBase record IDs`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to update customers with larkRecordIds: ${error.message}`,
      );
      throw error;
    }
  }
}
