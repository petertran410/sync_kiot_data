// src/services/lark/customer/lark-customer-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// ✅ SOLUTION: Use FIELD NAMES instead of Field IDs for better reliability
const LARK_CUSTOMER_FIELDS = {
  // Core fields (using Vietnamese field names from LarkBase)
  PRIMARY_NAME: 'Tên Khách Hàng', // Primary field
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

  // ✅ New fields
  KIOTVIET_ID: 'kiotVietId',
  TOTAL_INVOICED: 'Tổng Bán',
  COMMENTS: 'Ghi Chú',
  MODIFIED_DATE: 'Thời Gian Cập Nhật',
  CREATED_DATE: 'Thời Gian Tạo',
  FACEBOOK_ID: 'Facebook Khách Hàng',
  LOCATION_NAME: 'Khu Vực',
} as const;

// ✅ Gender options (using Vietnamese option names)
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
          this.logger.error(`❌ Batch ${i + 1} failed: ${error.message}`);
          failedCount += batch.length;
        }
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

  private createBatches<T>(items: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }

  private async processBatch(
    customers: any[],
  ): Promise<{ successCount: number; failedCount: number }> {
    const createRecords: LarkBaseRecord[] = [];
    const updateRecords: Array<{ recordId: string; record: LarkBaseRecord }> =
      [];
    let successCount = 0;
    let failedCount = 0;

    // Process each customer in the batch
    for (const customer of customers) {
      try {
        this.logger.debug(
          `🔍 Processing customer: ${customer.code} (KiotViet ID: ${customer.kiotVietId})`,
        );

        const larkRecord = this.mapCustomerToLarkBase(customer);

        if (customer.larkRecordId) {
          // Strategy 1: Update existing record
          updateRecords.push({
            recordId: customer.larkRecordId,
            record: larkRecord,
          });
          this.logger.debug(
            `📝 Will update existing LarkBase record for customer ${customer.code}`,
          );
        } else {
          // Strategy 2: Check if record exists by kiotVietId
          const existingRecord = await this.findLarkRecordByKiotVietId(
            customer.kiotVietId,
          );

          if (existingRecord) {
            // Found existing record, update it
            updateRecords.push({
              recordId: existingRecord.record_id,
              record: larkRecord,
            });

            // Update our database with the found larkRecordId
            await this.prismaService.customer.update({
              where: { id: customer.id },
              data: { larkRecordId: existingRecord.record_id },
            });

            this.logger.debug(
              `🔄 Found and will update existing LarkBase record for customer ${customer.code}`,
            );
          } else {
            // Strategy 3: Create new record
            createRecords.push(larkRecord);
            this.logger.debug(
              `➕ Will create new LarkBase record for customer ${customer.code}`,
            );
          }
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to process customer ${customer.code}: ${error.message}`,
        );
        failedCount++;
      }
    }

    // Execute batch operations
    try {
      // Execute CREATE operations
      if (createRecords.length > 0) {
        this.logger.log(
          `📤 Creating ${createRecords.length} new LarkBase records...`,
        );
        const createdRecords = await this.batchCreateRecords(createRecords);

        // Update database with new larkRecordIds
        await this.updateCustomersWithLarkRecordIds(
          customers.filter((c) => !c.larkRecordId),
          createdRecords,
        );

        successCount += createdRecords.length;
      }

      // Execute UPDATE operations
      if (updateRecords.length > 0) {
        this.logger.log(
          `📤 Updating ${updateRecords.length} existing LarkBase records...`,
        );
        await this.batchUpdateRecords(updateRecords);
        successCount += updateRecords.length;
      }
    } catch (error) {
      this.logger.error(`❌ Batch operation failed: ${error.message}`);
      failedCount += createRecords.length + updateRecords.length;
    }

    return { successCount, failedCount };
  }

  private async findLarkRecordByKiotVietId(
    kiotVietId: number,
  ): Promise<any | null> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();

      // ✅ FIXED: Use field name for search instead of field ID
      const response = await firstValueFrom(
        this.httpService.post(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`,
          {
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_CUSTOMER_FIELDS.KIOTVIET_ID, // Using field name: 'kiotVietId'
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

      this.logger.log(
        `✅ Updated ${updateRecords.length} LarkBase records successfully`,
      );
    } catch (error) {
      this.logger.error(`❌ LarkBase batch UPDATE error: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // ✅ ENHANCED FIELD MAPPING: Database → LarkBase (Using Field Names)
  // ============================================================================

  private mapCustomerToLarkBase(customer: any): LarkBaseRecord {
    try {
      const fields: Record<string, any> = {};

      // ✅ CORE FIELDS (using field names)

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

      // Gender select field (using option names)
      if (customer.gender !== null && customer.gender !== undefined) {
        fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
          ? GENDER_OPTIONS.MALE
          : GENDER_OPTIONS.FEMALE;
      }

      // ✅ KiotViet ID
      if (customer.kiotVietId) {
        fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = Number(customer.kiotVietId);
      }

      // ✅ NEW: Additional fields mapping

      // Total Invoiced (Tổng Bán)
      if (
        customer.totalInvoiced !== null &&
        customer.totalInvoiced !== undefined
      ) {
        fields[LARK_CUSTOMER_FIELDS.TOTAL_INVOICED] = Number(
          customer.totalInvoiced,
        );
      }

      // Comments (Ghi Chú)
      if (customer.comments) {
        fields[LARK_CUSTOMER_FIELDS.COMMENTS] = customer.comments;
      }

      // Location Name (Khu Vực)
      if (customer.locationName) {
        fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName;
      }

      // Facebook ID (Facebook Khách Hàng)
      if (
        customer.psidFacebook !== null &&
        customer.psidFacebook !== undefined
      ) {
        fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] = Number(
          customer.psidFacebook,
        );
      }

      // Dates - Convert to ISO format for LarkBase
      if (customer.createdDate) {
        fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = new Date(
          customer.createdDate,
        ).getTime();
      }

      if (customer.modifiedDate) {
        fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = new Date(
          customer.modifiedDate,
        ).getTime();
      }

      this.logger.debug(
        `📋 Mapped customer ${customer.code} to LarkBase fields: ${Object.keys(fields).length} fields`,
      );

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

  private async updateFailedCustomers(
    customers: any[],
    error: string,
  ): Promise<void> {
    try {
      const customerIds = customers.map((c) => c.id);

      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncRetries: { increment: 1 },
        },
      });

      this.logger.log(
        `📝 Updated ${customers.length} customers as failed sync`,
      );
    } catch (updateError) {
      this.logger.error(
        `Failed to update failed customer status: ${updateError.message}`,
      );
    }
  }
}
