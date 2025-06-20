// src/services/lark/lark-base.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as lark from '@larksuiteoapi/node-sdk';

@Injectable()
export class LarkBaseService {
  private readonly logger = new Logger(LarkBaseService.name);
  private client: lark.Client;
  private readonly baseToken: string;
  private readonly tableId: string;

  constructor(private readonly configService: ConfigService) {
    const appId = this.configService.get<string>('LARK_CUSTOMER_SYNC_APP_ID');
    const appSecret = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_APP_SECRET',
    );
    const baseToken = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_TABLE_ID',
    );

    if (!baseToken) {
      throw new Error(
        'LARK_BASE_TOKEN environment vairable is not configution',
      );
    }
    if (!tableId) {
      throw new Error('LARK_TABLE_ID environment variable is not configuraion');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;

    if (!appId || !appSecret || !this.baseToken || !this.tableId) {
      throw new Error('Missing LarkSuite configuration');
    }

    this.client = new lark.Client({
      appId,
      appSecret,
      appType: lark.AppType.SelfBuild,
      domain: lark.Domain.Feishu,
    });
  }

  private mapKiotVietToLarkBase(customerData: any): any {
    const fields: any = {};

    // Primary field - Tên Khách Hàng
    if (customerData.name) {
      fields['fld71g8Gci'] = customerData.name;
    }

    // Mã Khách Hàng
    if (customerData.code) {
      fields['fld29zIB9D'] = customerData.code;
    }

    // Số Điện Thoại
    if (customerData.contactNumber) {
      fields['fldHo79lXi'] = customerData.contactNumber;
    }

    // Email của Khách Hàng
    if (customerData.email) {
      fields['fldRXGBAzC'] = customerData.email;
    }

    // Địa Chỉ Khách Hàng
    if (customerData.address) {
      fields['fld17QvTM6'] = customerData.address;
    }

    // kiotVietId
    if (customerData.id) {
      fields['fldN5NE17y'] = customerData.id;
    }

    // Nợ Hiện Tại
    if (customerData.debt !== null && customerData.debt !== undefined) {
      fields['fldEBifOyt'] = Number(customerData.debt);
    }

    // Tổng Bán
    if (
      customerData.totalInvoiced !== null &&
      customerData.totalInvoiced !== undefined
    ) {
      fields['fld1gzrrvR'] = Number(customerData.totalInvoiced);
    }

    // Tổng Doanh Thu
    if (
      customerData.totalRevenue !== null &&
      customerData.totalRevenue !== undefined
    ) {
      fields['fldStZEptP'] = Number(customerData.totalRevenue);
    }

    // Điểm Hiện Tại
    if (
      customerData.rewardPoint !== null &&
      customerData.rewardPoint !== undefined
    ) {
      fields['fldujW0cpW'] = Number(customerData.rewardPoint);
    }

    // Tổng Điểm
    if (
      customerData.totalPoint !== null &&
      customerData.totalPoint !== undefined
    ) {
      fields['fld9zfi74R'] = Number(customerData.totalPoint);
    }

    // Công Ty
    if (customerData.organization) {
      fields['fldUubtChK'] = customerData.organization;
    }

    // Ghi Chú
    if (customerData.comments) {
      fields['fldRFEVYOn'] = customerData.comments;
    }

    // Id Của Hàng
    if (customerData.retailerId) {
      fields['fld6M0YzOE'] = customerData.retailerId.toString();
    }

    // Thời Gian Tạo
    if (customerData.createdDate) {
      fields['flddDuUUEg'] = new Date(customerData.createdDate).getTime();
    }

    // Thời Gian Cập Nhật
    if (customerData.modifiedDate) {
      fields['fldK8teGni'] = new Date(customerData.modifiedDate).getTime();
    }

    // Giới Tính (Single Select)
    if (customerData.gender !== null && customerData.gender !== undefined) {
      fields['fldLa1obN8'] = customerData.gender ? 'optUmkTfdd' : 'optcf5ndAC'; // Nam : Nữ
    }

    // Khu Vực
    if (customerData.locationName) {
      fields['fldU3fKuoa'] = customerData.locationName;
    }

    // Phường xã
    if (customerData.wardName) {
      fields['fldU0Vru4a'] = customerData.wardName;
    }

    // Mã Số Thuế
    if (customerData.taxCode) {
      fields['fldCDKr4yC'] = customerData.taxCode;
    }

    // Facebook Khách Hàng
    if (
      customerData.psidFacebook !== null &&
      customerData.psidFacebook !== undefined
    ) {
      fields['fldh8TIi9K'] = Number(customerData.psidFacebook);
    }

    return { fields };
  }

  async getExistingRecords(
    kiotVietIds: string[],
  ): Promise<Map<string, string>> {
    try {
      const existingRecords = new Map<string, string>();
      const batchSize = 100;

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        const response = await this.client.bitable.appTableRecord.search({
          path: {
            app_token: this.baseToken,
            table_id: this.tableId,
          },
          data: {
            filter: {
              conjunction: 'or',
              conditions: batch.map((id) => ({
                field_name: 'fldN5NE17y',
                operator: 'is',
                value: [id],
              })),
            },
          },
        });

        if (response.data?.items) {
          response.data.items.forEach((record: any) => {
            const kiotVietId = record.fields?.fldN5NE17y?.toString();
            if (kiotVietId) {
              existingRecords.set(kiotVietId, record.record_id);
            }
          });
        }
      }

      return existingRecords;
    } catch (error) {
      this.logger.error(`Failed to get existing records: ${error.message}`);
      throw error;
    }
  }

  async batchCreateRecords(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      const records = customers.map((customer) =>
        this.mapKiotVietToLarkBase(customer),
      );

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.baseToken,
          table_id: this.tableId,
        },
        data: {
          records,
        },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = customers.length - successCount;

      this.logger.log(
        `LarkBase batch create: ${successCount} success, ${failedCount} failed`,
      );
      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`LarkBase batch create failed: ${error.message}`);
      return { success: 0, failed: customers.length };
    }
  }

  async batchUpdateRecords(
    customers: any[],
    existingRecords: Map<string, string>,
  ): Promise<{ success: number; failed: number }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      const records = customers
        .filter((customer) => existingRecords.has(customer.id.toString()))
        .map((customer) => ({
          record_id: existingRecords.get(customer.id.toString()),
          fields: this.mapKiotVietToLarkBase(customer).fields,
        }));

      if (!records.length) return { success: 0, failed: 0 };

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.baseToken,
          table_id: this.tableId,
        },
        data: {
          records,
        },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = records.length - successCount;

      this.logger.log(
        `LarkBase batch update: ${successCount} success, ${failedCount} failed`,
      );
      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`LarkBase batch update failed: ${error.message}`);
      return { success: 0, failed: customers.length };
    }
  }

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    if (!customers.length) return;

    try {
      const batchSize = 100; // Optimal batch size
      let totalProcessed = 0;

      for (let i = 0; i < customers.length; i += batchSize) {
        const batch = customers.slice(i, i + batchSize);

        // Get existing records
        const kiotVietIds = batch.map((c) => c.id.toString());
        const existingRecords = await this.getExistingRecords(kiotVietIds);

        // Separate into create and update batches
        const toCreate = batch.filter(
          (c) => !existingRecords.has(c.id.toString()),
        );
        const toUpdate = batch.filter((c) =>
          existingRecords.has(c.id.toString()),
        );

        // Process creates and updates in parallel
        const [createResult, updateResult] = await Promise.allSettled([
          toCreate.length > 0
            ? this.batchCreateRecords(toCreate)
            : Promise.resolve({ success: 0, failed: 0 }),
          toUpdate.length > 0
            ? this.batchUpdateRecords(toUpdate, existingRecords)
            : Promise.resolve({ success: 0, failed: 0 }),
        ]);

        const createSuccess =
          createResult.status === 'fulfilled' ? createResult.value.success : 0;
        const updateSuccess =
          updateResult.status === 'fulfilled' ? updateResult.value.success : 0;

        totalProcessed += createSuccess + updateSuccess;

        // Log any errors
        if (createResult.status === 'rejected') {
          this.logger.error(`Create batch failed: ${createResult.reason}`);
        }
        if (updateResult.status === 'rejected') {
          this.logger.error(`Update batch failed: ${updateResult.reason}`);
        }

        // Rate limiting delay
        if (i + batchSize < customers.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000)); // 1 second delay between batches
        }
      }

      this.logger.log(
        `LarkBase sync completed: ${totalProcessed} customers processed`,
      );
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      throw error;
    }
  }
}
