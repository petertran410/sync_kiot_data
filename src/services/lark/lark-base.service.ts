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

    // Primary field - Tên Khách Hàng (REQUIRED)
    if (customerData.name) {
      fields['Tên Khách Hàng'] = customerData.name;
    }

    // Mã Khách Hàng
    if (customerData.code) {
      fields['Mã Khách Hàng'] = customerData.code;
    }

    // Số Điện Thoại
    if (customerData.contactNumber) {
      fields['Số Điện Thoại'] = customerData.contactNumber;
    }

    // Email của Khách Hàng
    if (customerData.email) {
      fields['Email của Khách Hàng'] = customerData.email;
    }

    // Địa Chỉ Khách Hàng
    if (customerData.address) {
      fields['Địa Chỉ Khách Hàng'] = customerData.address;
    }

    // kiotvietId (IMPORTANT for deduplication)
    if (customerData.id) {
      fields['kiotvietId'] = Number(customerData.id);
    }

    // Nợ Hiện Tại
    if (customerData.debt !== null && customerData.debt !== undefined) {
      fields['Nợ Hiện Tại'] = Number(customerData.debt);
    }

    // Tổng Bán
    if (
      customerData.totalInvoiced !== null &&
      customerData.totalInvoiced !== undefined
    ) {
      fields['Tổng Bán'] = Number(customerData.totalInvoiced);
    }

    // Tổng Doanh Thu
    if (
      customerData.totalRevenue !== null &&
      customerData.totalRevenue !== undefined
    ) {
      fields['Tổng Doanh Thu'] = Number(customerData.totalRevenue);
    }

    // Điểm Hiện Tại
    if (
      customerData.rewardPoint !== null &&
      customerData.rewardPoint !== undefined
    ) {
      fields['Điểm Hiện Tại'] = Number(customerData.rewardPoint);
    }

    // Tổng Điểm
    if (
      customerData.totalPoint !== null &&
      customerData.totalPoint !== undefined
    ) {
      fields['Tổng Điểm'] = Number(customerData.totalPoint);
    }

    // Công Ty
    if (customerData.organization) {
      fields['Công Ty'] = customerData.organization;
    }

    // Ghi Chú
    if (customerData.comments) {
      fields['Ghi Chú'] = customerData.comments;
    }

    // Id Cửa Hàng
    if (customerData.retailerId) {
      fields['Id Cửa Hàng'] = customerData.retailerId.toString();
    }

    // Thời Gian Tạo
    if (customerData.createdDate) {
      try {
        const date = new Date(customerData.createdDate);
        fields['Thời Gian Tạo'] = Math.floor(date.getTime() / 1000);
      } catch (error) {
        this.logger.warn(
          `Invalid createdDate for customer ${customerData.code}: ${customerData.createdDate}`,
        );
      }
    }

    // Thời Gian Cập Nhật
    if (customerData.modifiedDate) {
      try {
        const date = new Date(customerData.modifiedDate);
        fields['Thời Gian Cập Nhật'] = Math.floor(date.getTime() / 1000);
      } catch (error) {
        this.logger.warn(
          `Invalid modifiedDate for customer ${customerData.code}: ${customerData.modifiedDate}`,
        );
      }
    }

    // Giới Tính (Single Select)
    if (customerData.gender !== null && customerData.gender !== undefined) {
      fields['Giới Tính'] = customerData.gender ? 'Nam' : 'Nữ';
    }

    // Khu Vực
    if (customerData.locationName) {
      fields['Khu Vực'] = customerData.locationName;
    }

    // Phường xã
    if (customerData.wardName) {
      fields['Phường xã'] = customerData.wardName;
    }

    // Mã Số Thuế
    if (customerData.taxCode) {
      fields['Mã Số Thuế'] = customerData.taxCode;
    }

    // Facebook Khách Hàng
    if (
      customerData.psidFacebook !== null &&
      customerData.psidFacebook !== undefined
    ) {
      fields['Facebook Khách Hàng'] = Number(customerData.psidFacebook);
    }

    return { fields };
  }

  async getTableFields(): Promise<void> {
    try {
      const response = await this.client.bitable.appTableField.list({
        path: {
          app_token: this.baseToken,
          table_id: this.tableId,
        },
      });

      this.logger.log('Actual LarkBase fields:');
      if (response.data?.items) {
        response.data.items.forEach((field) => {
          this.logger.log(
            `Field ID: ${field.field_id}, Name: ${field.field_name}, Type: ${field.type}`,
          );
        });
      }
    } catch (error) {
      this.logger.error(`Failed to get table fields: ${error.message}`);
      if (error.response?.data) {
        this.logger.error(
          'Error details:',
          JSON.stringify(error.response.data, null, 2),
        );
      }
    }
  }

  async getExistingRecords(
    kiotVietIds: string[],
  ): Promise<Map<string, string>> {
    try {
      const existingRecords = new Map<string, string>();
      const batchSize = 50;

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        for (const kiotVietIdStr of batch) {
          try {
            const kiotVietIdNum = Number(kiotVietIdStr);

            const response = await this.client.bitable.appTableRecord.search({
              path: {
                app_token: this.baseToken,
                table_id: this.tableId,
              },
              data: {
                filter: {
                  conjunction: 'and',
                  conditions: [
                    {
                      field_name: 'kiotvietId',
                      operator: 'is',
                      value: [kiotVietIdNum] as any,
                    },
                  ],
                },
              },
            });

            if (response.data?.items && response.data.items.length > 0) {
              const record = response.data.items[0];
              const recordKiotVietId =
                record.fields?.['kiotvietId']?.toString();
              const recordId = record.record_id;

              if (recordKiotVietId && recordId) {
                existingRecords.set(recordKiotVietId, recordId);
              }
            }
          } catch (searchError) {
            this.logger.warn(
              `Failed to search for kiotVietId ${kiotVietIdStr}: ${searchError.message}`,
            );
          }
        }

        if (i + batchSize < kiotVietIds.length) {
          await new Promise((resolve) => setTimeout(resolve, 500));
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
      const records = customers
        .map((customer) => this.mapKiotVietToLarkBase(customer))
        .filter((record) => record.fields['Tên Khách Hàng']); // Ensure primary field exists

      if (!records.length) {
        this.logger.warn('No valid records to create (missing primary field)');
        return { success: 0, failed: customers.length };
      }

      this.logger.debug(
        `Attempting to create ${records.length} records in LarkBase`,
      );
      this.logger.debug('Sample record:', JSON.stringify(records[0], null, 2));

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

      if (failedCount > 0) {
        this.logger.error(
          'LarkBase create response:',
          JSON.stringify(response, null, 2),
        );
      }

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`LarkBase batch create failed: ${error.message}`);
      this.logger.error('Error stack:', error.stack);
      if (error.response?.data) {
        this.logger.error(
          'LarkBase API Error:',
          JSON.stringify(error.response.data, null, 2),
        );
      }
      if (error.response?.status) {
        this.logger.error('HTTP Status:', error.response.status);
      }
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
        .map((customer) => {
          const recordId = existingRecords.get(customer.id.toString());
          if (!recordId) return null;

          return {
            record_id: recordId,
            fields: this.mapKiotVietToLarkBase(customer).fields,
          };
        })
        .filter((record) => record !== null && record.fields['Tên Khách Hàng'])
        .map((record) => record!);

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
      if (error.response?.data) {
        this.logger.error(
          'Error details:',
          JSON.stringify(error.response.data, null, 2),
        );
      }
      return { success: 0, failed: customers.length };
    }
  }

  async syncCustomersToLarkBase(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      await this.getTableFields();

      const batchSize = 50;
      let totalSuccess = 0;
      let totalFailed = 0;

      this.logger.log(
        `Starting LarkBase sync for ${customers.length} customers`,
      );

      for (let i = 0; i < customers.length; i += batchSize) {
        const batch = customers.slice(i, i + batchSize);

        const kiotVietIds = batch.map((c) => c.id.toString());
        const existingRecords = await this.getExistingRecords(kiotVietIds);

        const toCreate = batch.filter(
          (c) => !existingRecords.has(c.id.toString()),
        );
        const toUpdate = batch.filter((c) =>
          existingRecords.has(c.id.toString()),
        );

        this.logger.log(
          `Batch ${Math.floor(i / batchSize) + 1}: ${toCreate.length} to create, ${toUpdate.length} to update`,
        );

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
        const createFailed =
          createResult.status === 'fulfilled'
            ? createResult.value.failed
            : toCreate.length;
        const updateSuccess =
          updateResult.status === 'fulfilled' ? updateResult.value.success : 0;
        const updateFailed =
          updateResult.status === 'fulfilled'
            ? updateResult.value.failed
            : toUpdate.length;

        totalSuccess += createSuccess + updateSuccess;
        totalFailed += createFailed + updateFailed;

        if (createResult.status === 'rejected') {
          this.logger.error(`Create batch failed: ${createResult.reason}`);
        }
        if (updateResult.status === 'rejected') {
          this.logger.error(`Update batch failed: ${updateResult.reason}`);
        }

        if (i + batchSize < customers.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      this.logger.log(
        `LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: customers.length };
    }
  }
}
