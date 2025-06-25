// src/services/lark/customer/lark-customer-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// LarkBase Field IDs from "Khách Hàng.rtf"
const LARK_CUSTOMER_FIELDS = {
  PRIMARY_NAME: 'fld71g8Gci', // Tên Khách Hàng (primary) - from .env
  CUSTOMER_CODE: 'fld29zIB9D', // Mã Khách Hàng
  PHONE_NUMBER: 'fldHo79lXi', // Số Điện Thoại
  STORE_ID: 'fld6M0YzOE', // Id Cửa Hàng
  COMPANY: 'fldUubtChK', // Công Ty
  EMAIL: 'fldRXGBAzC', // Email của Khách Hàng
  ADDRESS: 'fld17QvTM6', // Địa Chỉ Khách Hàng
  CURRENT_DEBT: 'fldEBifOyt', // Nợ Hiện Tại
  TAX_CODE: 'fldCDKr4yC', // Mã Số Thuế
  TOTAL_POINTS: 'fld9zfi74R', // Tổng Điểm
  TOTAL_REVENUE: 'fldStZEptP', // Tổng Doanh Thu
  GENDER: 'fldLa1obN8', // Giới Tính (select)
  WARD_NAME: 'fldU0Vru4a', // Phường xã
  CURRENT_POINTS: 'fldujW0cpW', // Điểm Hiện Tại
  KIOTVIET_ID: 'fldNewKiotVietId', // ⭐ Need to create this field in LarkBase
} as const;

// ✅ CORRECT: Use actual LarkBase option IDs
const GENDER_OPTIONS = {
  MALE: 'optUmkTfdd', // Nam option ID
  FEMALE: 'optcf5ndAC', // Nữ option ID
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
  private readonly batchSize: number = 50; // Max 500 per LarkBase API, using 50 for safety

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
  // MAIN SYNC METHOD
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers...`,
      );

      // Process in batches
      const batches = this.createBatches(customers, this.batchSize);
      let syncedCount = 0;
      let failedCount = 0;

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        this.logger.log(
          `📦 Processing batch ${i + 1}/${batches.length} (${batch.length} customers)`,
        );

        try {
          const batchResults = await this.processBatch(batch);
          syncedCount += batchResults.successCount;
          failedCount += batchResults.failedCount;

          this.logger.log(
            `✅ Batch ${i + 1} completed: ${batchResults.successCount} synced, ${batchResults.failedCount} failed`,
          );

          // Small delay between batches to avoid rate limiting
          if (i < batches.length - 1) {
            await new Promise((resolve) => setTimeout(resolve, 200));
          }
        } catch (error) {
          this.logger.error(
            `❌ Batch ${i + 1} failed completely: ${error.message}`,
          );
          failedCount += batch.length;

          // STOP on batch failure to prevent data duplication
          throw new Error(`Batch ${i + 1} failed: ${error.message}`);
        }
      }

      if (failedCount > 0) {
        throw new Error(
          `LarkBase sync partially failed: ${syncedCount} synced, ${failedCount} failed`,
        );
      }

      this.logger.log(
        `🎉 LarkBase sync completed successfully: ${syncedCount} customers synced`,
      );
    } catch (error) {
      this.logger.error(`❌ LarkBase sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // BATCH PROCESSING
  // ============================================================================

  private async processBatch(
    customers: any[],
  ): Promise<{ successCount: number; failedCount: number }> {
    let successCount = 0;
    let failedCount = 0;

    const createRecords: LarkBaseRecord[] = [];
    const updateRecords: Array<{ recordId: string; record: LarkBaseRecord }> =
      [];

    for (const customer of customers) {
      try {
        const larkRecord = this.mapCustomerToLarkBase(customer);

        if (customer.larkRecordId) {
          // Strategy 1: Use existing larkRecordId mapping
          updateRecords.push({
            recordId: customer.larkRecordId,
            record: larkRecord,
          });
        } else {
          // Strategy 2: Check if record exists by kiotVietId
          const existingRecord = await this.findLarkRecordByKiotVietId(
            customer.kiotVietId,
          );

          if (existingRecord) {
            // Found existing record, update it and save larkRecordId
            updateRecords.push({
              recordId: existingRecord.record_id,
              record: larkRecord,
            });

            // Update database with found larkRecordId
            await this.prismaService.customer.update({
              where: { id: customer.id },
              data: { larkRecordId: existingRecord.record_id },
            });

            this.logger.log(
              `🔍 Found existing LarkBase record for customer ${customer.code}`,
            );
          } else {
            // Strategy 3: Create new record
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

    // Execute operations...
    return { successCount, failedCount };
  }

  private async findLarkRecordByKiotVietId(
    kiotVietId: number,
  ): Promise<any | null> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();

      // Search for existing record with matching kiotVietId
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
      if (result.code === 0 && result.data?.items?.length > 0) {
        return result.data.items[0];
      }
      return null;
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to search LarkBase for kiotVietId ${kiotVietId}: ${error.message}`,
      );
      return null;
    }
  }

  // ============================================================================
  // LARKBASE API OPERATIONS
  // ============================================================================

  private async batchCreateRecords(
    records: LarkBaseRecord[],
  ): Promise<Array<{ record_id: string; fields: any }>> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();

      // ⭐ LOG: Debug request
      this.logger.debug(`🔍 Creating ${records.length} LarkBase records`);
      this.logger.debug(
        `📋 Sample record: ${JSON.stringify(records[0], null, 2)}`,
      );

      const response = await firstValueFrom(
        this.httpService.post(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
          {
            records: records,
          },
          { headers },
        ),
      );

      const result: LarkBatchResponse = response.data;

      // ⭐ LOG: Debug response
      this.logger.debug(
        `📤 LarkBase API Response: ${JSON.stringify(result, null, 2)}`,
      );

      if (result.code !== 0) {
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
      this.logger.error(`❌ LarkBase batch CREATE error: ${error.message}`);
      // ⭐ LOG: Full error details
      if (error.response) {
        this.logger.error(`📤 HTTP Status: ${error.response.status}`);
        this.logger.error(
          `📤 Response Data: ${JSON.stringify(error.response.data, null, 2)}`,
        );
      }
      throw error;
    }
  }

  private async batchUpdateRecords(
    updateRecords: Array<{ recordId: string; record: LarkBaseRecord }>,
  ): Promise<void> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();

      const records = updateRecords.map((ur) => ({
        record_id: ur.recordId,
        fields: ur.record.fields,
      }));

      const response = await firstValueFrom(
        this.httpService.post(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_update`,
          {
            records: records,
          },
          { headers },
        ),
      );

      const result: LarkBatchResponse = response.data;

      if (result.code !== 0) {
        throw new Error(`LarkBase UPDATE failed: ${result.msg}`);
      }
    } catch (error) {
      this.logger.error(`LarkBase batch UPDATE error: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // FIELD MAPPING: Database → LarkBase
  // ============================================================================

  private mapCustomerToLarkBase(customer: any): LarkBaseRecord {
    try {
      const fields: Record<string, any> = {};

      if (customer.kiotVietId) {
        fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = Number(customer.kiotVietId);
      }

      // Required primary field
      if (customer.name) {
        fields[LARK_CUSTOMER_FIELDS.PRIMARY_NAME] = customer.name;
      }

      // Text fields
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

      // Number fields
      if (customer.retailerId) {
        fields[LARK_CUSTOMER_FIELDS.STORE_ID] = customer.retailerId.toString();
      }

      if (customer.debt !== null && customer.debt !== undefined) {
        fields[LARK_CUSTOMER_FIELDS.CURRENT_DEBT] = Number(customer.debt);
      }

      if (
        customer.totalRevenue !== null &&
        customer.totalRevenue !== undefined
      ) {
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

      // Gender select field
      if (customer.gender !== null && customer.gender !== undefined) {
        fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
          ? GENDER_OPTIONS.MALE
          : GENDER_OPTIONS.FEMALE;
      }

      return { fields };
    } catch (error) {
      this.logger.error(
        `Field mapping error for customer ${customer.code}: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // DATABASE UPDATES
  // ============================================================================

  private async updateCustomersWithLarkRecordIds(
    customers: any[],
    larkRecords: Array<{ record_id: string; fields: any }>,
  ): Promise<void> {
    try {
      // Map customers without larkRecordId to created records
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

  private async updateCustomersSyncStatus(
    customerIds: number[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    try {
      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: status,
          larkSyncedAt: new Date(),
          ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
        },
      });

      this.logger.log(
        `📝 Updated ${customerIds.length} customers sync status to ${status}`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to update customers sync status: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private createBatches<T>(items: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }

  // ============================================================================
  // DELETE SUPPORT (for future delete operations)
  // ============================================================================

  async deleteCustomersFromLarkBase(customerIds: number[]): Promise<void> {
    try {
      // Get customers with larkRecordId
      const customers = await this.prismaService.customer.findMany({
        where: {
          id: { in: customerIds },
          larkRecordId: { not: null },
        },
        select: { id: true, larkRecordId: true, code: true },
      });

      if (customers.length === 0) {
        this.logger.log('📋 No customers with LarkBase records to delete');
        return;
      }

      const recordIds = customers
        .map((c) => c.larkRecordId)
        .filter((id) => id !== null);

      if (recordIds.length > 0) {
        await this.batchDeleteRecords(recordIds);

        // Update database
        await this.prismaService.customer.deleteMany({
          where: { id: { in: customerIds } },
        });

        this.logger.log(
          `🗑️ Deleted ${customers.length} customers from LarkBase and database`,
        );
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to delete customers from LarkBase: ${error.message}`,
      );
      throw error;
    }
  }

  private async batchDeleteRecords(recordIds: string[]): Promise<void> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();

      const response = await firstValueFrom(
        this.httpService.post(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_delete`,
          {
            records: recordIds,
          },
          { headers },
        ),
      );

      const result: LarkBatchResponse = response.data;

      if (result.code !== 0) {
        throw new Error(`LarkBase DELETE failed: ${result.msg}`);
      }
    } catch (error) {
      this.logger.error(`LarkBase batch DELETE error: ${error.message}`);
      throw error;
    }
  }
}
