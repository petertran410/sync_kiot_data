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

    // Use field names instead of field IDs
    if (customerData.name) {
      fields['Tên Khách Hàng'] = customerData.name;
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
      const batchSize = 50; // ✅ Reduced batch size

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        // ✅ Fixed search structure
        for (const kiotVietIdStr of batch) {
          try {
            const kiotVietIdNum: number = Number(kiotVietIdStr);

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
                      field_name: 'fldN5NE17y',
                      operator: 'is',
                      value: [kiotVietIdNum] as any,
                    },
                  ],
                },
              },
            });

            if (response.data?.items && response.data.items.length > 0) {
              const record = response.data.items[0];
              const recordKiotVietId = record.fields?.fldN5NE17y?.toString();
              const recordId = record.record_id; // ✅ Get record_id

              // ✅ Check if both values exist before adding to map
              if (recordKiotVietId && recordId) {
                existingRecords.set(recordKiotVietId, recordId);
              }
            }
          } catch (searchError) {
            this.logger.warn(
              `Failed to search for kiotVietId ${kiotVietIdStr}: ${searchError.message}`,
            );
            // Continue with next ID
          }
        }

        // Rate limiting delay
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
      // Test with just one record first
      const testRecord = {
        fields: {
          'Tên Khách Hàng': customers[0].name, // Use field name
        },
      };

      this.logger.debug('Testing single record create with field name');
      this.logger.debug('Test record:', JSON.stringify(testRecord, null, 2));

      const response = await this.client.bitable.appTableRecord.create({
        path: {
          app_token: this.baseToken,
          table_id: this.tableId,
        },
        data: testRecord,
      });

      this.logger.log(
        'Single record test result:',
        JSON.stringify(response, null, 2),
      );

      return { success: 1, failed: customers.length - 1 };
    } catch (error) {
      this.logger.error(`Single record test failed: ${error.message}`);
      if (error.response?.data) {
        this.logger.error(
          'Error details:',
          JSON.stringify(error.response.data, null, 2),
        );
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
          // ✅ Check if recordId exists before creating update object
          if (!recordId) {
            return null;
          }
          return {
            record_id: recordId,
            fields: this.mapKiotVietToLarkBase(customer).fields,
          };
        })
        .filter((record) => record !== null && record.fields['fld71g8Gci']) // ✅ Filter out null records and ensure primary field exists
        .map((record) => record!); // ✅ TypeScript assertion after filter

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
