// src/services/lark/lark-base.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as lark from '@larksuiteoapi/node-sdk';

interface InvoiceForLarkBase {
  invoiceData: any;
  branchName: string | null;
  customerName: string | null;
  userName: string | null;
}

interface OrderForLarkBase {
  orderData: any;
  branchName: string | null;
  customerName: string | null;
  userName: string | null;
}

@Injectable()
export class LarkBaseService {
  private readonly logger = new Logger(LarkBaseService.name);
  private client: lark.Client;

  // Customer
  private readonly customerBaseToken: string;
  private readonly customerTableId: string;

  // Invoice
  private readonly invoiceBaseToken: string;
  private readonly invoiceTableId: string;

  // Order
  private readonly orderBaseToken: string;
  private readonly orderTableId: string;

  constructor(private readonly configService: ConfigService) {
    const appId = this.configService.get<string>('LARK_CUSTOMER_SYNC_APP_ID');

    const appSecret = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_APP_SECRET',
    );

    const customerBaseToken = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_BASE_TOKEN',
    );

    const customerTableId = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_TABLE_ID',
    );

    const invoiceBaseToken = this.configService.get<string>(
      'LARK_INVOICE_SYNC_BASE_TOKEN',
    );

    const invoiceTableId = this.configService.get<string>(
      'LARK_INVOICE_SYNC_TABLE_ID',
    );

    const orderBaseToken = this.configService.get<string>(
      'LARK_ORDER_SYNC_BASE_TOKEN',
    );

    const orderTableId = this.configService.get<string>(
      'LARK_ORDER_SYNC_TABLE_ID',
    );

    if (!customerBaseToken || !customerTableId) {
      throw new Error('LARK customer environment vairable is not configution');
    }
    if (!invoiceBaseToken || !invoiceTableId) {
      throw new Error('LARK invoice environment variable is not configuraion');
    }
    if (!orderBaseToken || !orderTableId) {
      throw new Error('LARK order environment variable is not configution');
    }

    this.customerBaseToken = customerBaseToken;
    this.customerTableId = customerTableId;

    this.invoiceBaseToken = invoiceBaseToken;
    this.invoiceTableId = invoiceTableId;

    this.orderBaseToken = orderBaseToken;
    this.orderTableId = orderTableId;

    if (
      !appId ||
      !appSecret ||
      !this.customerBaseToken ||
      !this.customerTableId ||
      !this.invoiceBaseToken ||
      !this.invoiceTableId ||
      !this.orderBaseToken ||
      !this.orderTableId
    ) {
      throw new Error('Missing LarkSuite configuration');
    }

    this.client = new lark.Client({
      appId,
      appSecret,
      appType: lark.AppType.SelfBuild,
      domain: lark.Domain.Feishu,
    });
  }

  private mapCustomerToLarkBase(customerData: any): any {
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
      const vietnamDate = new Date(customerData.createdDate + '+07:00');
      fields['Thời Gian Tạo'] = vietnamDate.getTime();
    }

    // Thời Gian Cập Nhật
    if (customerData.modifiedDate) {
      const vietnamDate = new Date(customerData.modifiedDate + '+07:00');
      fields['Thời Gian Cập Nhật'] = vietnamDate.getTime();
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

  private mapInvoiceToLarkBase(
    invoiceData: any,
    branchName?: string | null,
    customerName?: string | null,
    userName?: string | null,
  ): any {
    const fields: any = {};

    // Primary field - Mã Hóa Đơn (REQUIRED)
    if (invoiceData.code) {
      fields['Mã Hoá Đơn'] = invoiceData.code;
    }

    // Mã Đơn Hàng
    if (invoiceData.orderCode) {
      fields['Mã Đơn Hàng'] = invoiceData.orderCode;
    }

    // Ngày Mua
    if (invoiceData.purchaseDate) {
      const vietnamDate = new Date(invoiceData.purchaseDate + '+07:00');
      fields['Ngày Mua'] = vietnamDate.getTime();
    }

    // Chi Nhánh - mapped from branchId to branchName
    if (branchName) {
      // FIXED: Only set if not null/undefined
      fields['Chi Nhánh'] = branchName;
    }

    // Tên Khách Hàng - customer name as string
    if (customerName) {
      // FIXED: Only set if not null/undefined
      fields['Tên Khách Hàng'] = customerName;
    }

    if (userName) {
      fields['Người Bán'] = userName;
    }

    // Tổng Sau Giảm Giá - total after discount
    if (invoiceData.total !== null && invoiceData.total !== undefined) {
      fields['Tổng Sau Giảm Giá'] = Number(invoiceData.total);
    }

    // Tổng Tiền Hàng - calculate total before discount
    if (invoiceData.total !== null && invoiceData.total !== undefined) {
      const total = Number(invoiceData.total);
      const discount = Number(invoiceData.discount || 0);
      const totalBeforeDiscount = total + discount;
      fields['Tổng Tiền Hàng'] = totalBeforeDiscount;
    }

    // Khách Đã Trả
    if (
      invoiceData.totalPayment !== null &&
      invoiceData.totalPayment !== undefined
    ) {
      fields['Khách Đã Trả'] = Number(invoiceData.totalPayment);
    }

    // Discount
    if (invoiceData.discount !== null && invoiceData.discount !== undefined) {
      fields['Discount'] = Number(invoiceData.discount);
    }

    // Mức Độ Giảm Giá
    if (
      invoiceData.discountRatio !== null &&
      invoiceData.discountRatio !== undefined
    ) {
      fields['Mức Độ Giảm Giá'] = Number(invoiceData.discountRatio);
    }

    // Ghi Chú
    if (invoiceData.description) {
      fields['Ghi Chú'] = invoiceData.description;
    }

    // Ngày Tạo
    if (invoiceData.createdDate) {
      const vietnamDate = new Date(invoiceData.createdDate + '+07:00');
      fields['Ngày Tạo'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật
    if (invoiceData.modifiedDate) {
      const vietnamDate = new Date(invoiceData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (invoiceData.id) {
      fields['kiotVietId'] = Number(invoiceData.id);
    }

    return { fields };
  }

  async getInvoiceTableFields(): Promise<void> {
    try {
      const response = await this.client.bitable.appTableField.list({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
      });

      this.logger.log('Invoice LarkBase fields:');
      if (response.data?.items) {
        response.data.items.forEach((field) => {
          this.logger.log(
            `Field ID: ${field.field_id}, Name: ${field.field_name}, Type: ${field.type}`,
          );
        });
      }
    } catch (error) {
      this.logger.error(`Failed to get invoice table fields: ${error.message}`);
    }
  }

  async getExistingInvoiceRecords(
    kiotVietIds: string[],
  ): Promise<Map<string, string>> {
    try {
      // Step 1: Verify kiotVietId field exists in LarkBase
      const hasKiotVietIdField = await this.verifyKiotVietIdField('invoice');
      if (!hasKiotVietIdField) {
        this.logger.warn(
          'Invoice: kiotVietId field not found in LarkBase - treating all records as new',
        );
        return new Map();
      }

      const existingRecords = new Map<string, string>();
      const batchSize = 100;
      let totalChecked = 0;
      let totalFound = 0;
      let totalErrors = 0;

      this.logger.log(
        `Invoice: Starting duplicate check for ${kiotVietIds.length} records`,
      );

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        for (const kiotVietIdStr of batch) {
          totalChecked++;
          try {
            const kiotVietIdNum = Number(kiotVietIdStr);

            const response = await this.client.bitable.appTableRecord.search({
              path: {
                app_token: this.invoiceBaseToken,
                table_id: this.invoiceTableId,
              },
              data: {
                filter: {
                  conjunction: 'and',
                  conditions: [
                    {
                      field_name: 'kiotVietId',
                      operator: 'is',
                      value: [kiotVietIdNum] as any,
                    },
                  ],
                },
              },
            });

            if (response.data?.items && response.data.items.length > 0) {
              const record = response.data.items[0];
              const recordKiotVietId = record.fields?.['kiotVietId'];
              if (recordKiotVietId === kiotVietIdNum) {
                existingRecords.set(kiotVietIdStr, record.record_id!);
                totalFound++;
                this.logger.debug(
                  `Invoice: DUPLICATE found - kiotVietId=${kiotVietIdStr}, recordId=${record.record_id}`,
                );
              }
            } else {
              this.logger.debug(
                `Invoice: NEW record - kiotVietId=${kiotVietIdStr}`,
              );
            }
          } catch (error) {
            totalErrors++;
            this.logger.warn(
              `Invoice: Failed to check kiotVietId ${kiotVietIdStr}: ${error.message} - treating as NEW record`,
            );
            // Continue processing instead of throwing - don't stop sync
          }
        }
      }

      this.logger.log(
        `Invoice duplicate check completed: ${totalChecked} checked, ${totalFound} duplicates found, ${totalErrors} errors, ${totalChecked - totalFound} new records`,
      );

      return existingRecords;
    } catch (error) {
      this.logger.error(
        `Invoice: Critical error in duplicate checking: ${error.message} - treating all records as NEW`,
      );
      // Return empty map instead of throwing - don't stop sync
      return new Map();
    }
  }

  async batchCreateInvoiceRecords(
    invoices: InvoiceForLarkBase[],
  ): Promise<{ success: number; failed: number }> {
    if (!invoices.length) return { success: 0, failed: 0 };

    try {
      const records = invoices
        .map((invoice) => {
          const mappedData = this.mapInvoiceToLarkBase(
            invoice.invoiceData,
            invoice.branchName,
            invoice.customerName,
            invoice.userName,
          );
          return mappedData.fields['Mã Hoá Đơn'] ? mappedData : null;
        })
        .filter((record): record is { fields: any } => record !== null)
        .map((record) => ({ fields: record.fields }));

      if (!records.length) return { success: 0, failed: 0 };

      this.logger.debug(
        `Attempting to create ${records.length} invoice records in LarkBase`,
      );
      this.logger.debug(
        'Sample invoice record:',
        JSON.stringify(records[0], null, 2),
      );

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = records.length - successCount;

      this.logger.log(
        `Invoice LarkBase batch create: ${successCount} success, ${failedCount} failed`,
      );

      // ADD: Log detailed response when there are failures
      if (failedCount > 0) {
        this.logger.error(
          'Invoice LarkBase create response:',
          JSON.stringify(response, null, 2),
        );
      }

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `Invoice LarkBase batch create failed: ${error.message}`,
      );
      this.logger.error('Error stack:', error.stack);
      if (error.response?.data) {
        this.logger.error(
          'Invoice LarkBase API Error:',
          JSON.stringify(error.response.data, null, 2),
        );
      }
      if (error.response?.status) {
        this.logger.error('HTTP Status:', error.response.status);
      }
      return { success: 0, failed: invoices.length };
    }
  }

  async batchUpdateInvoiceRecords(
    invoices: InvoiceForLarkBase[],
    existingRecords: Map<string, string>,
  ): Promise<{ success: number; failed: number }> {
    if (!invoices.length) return { success: 0, failed: 0 };

    try {
      const records = invoices
        .filter((invoice) =>
          existingRecords.has(invoice.invoiceData.id.toString()),
        )
        .map((invoice) => {
          const recordId = existingRecords.get(
            invoice.invoiceData.id.toString(),
          );
          if (!recordId) return null;

          const mappedData = this.mapInvoiceToLarkBase(
            invoice.invoiceData,
            invoice.branchName,
            invoice.customerName,
            invoice.userName,
          );

          return {
            record_id: recordId,
            fields: mappedData.fields,
          };
        })
        .filter(
          (record): record is { record_id: string; fields: any } =>
            record !== null && record.fields['Mã Hoá Đơn'],
        ); // FIXED: Type guard to ensure non-null records

      if (!records.length) return { success: 0, failed: 0 };

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = records.length - successCount;

      this.logger.log(
        `Invoice LarkBase batch update: ${successCount} success, ${failedCount} failed`,
      );
      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `Invoice LarkBase batch update failed: ${error.message}`,
      );
      return { success: 0, failed: invoices.length };
    }
  }

  async syncInvoicesToLarkBase(
    invoicesWithData: InvoiceForLarkBase[],
  ): Promise<{ success: number; failed: number }> {
    // return { success: 0, failed: 0 };

    if (!invoicesWithData.length) return { success: 0, failed: 0 };

    try {
      await this.getInvoiceTableFields();

      const batchSize = 100;
      let totalSuccess = 0;
      let totalFailed = 0;

      this.logger.log(
        `Starting Invoice LarkBase sync for ${invoicesWithData.length} invoices`,
      );

      for (let i = 0; i < invoicesWithData.length; i += batchSize) {
        const batch = invoicesWithData.slice(i, i + batchSize);

        const kiotVietIds = batch.map((item) => item.invoiceData.id.toString());
        const existingRecords =
          await this.getExistingInvoiceRecords(kiotVietIds);

        const toCreate = batch.filter(
          (item) => !existingRecords.has(item.invoiceData.id.toString()),
        );
        const toUpdate = batch.filter((item) =>
          existingRecords.has(item.invoiceData.id.toString()),
        );

        this.logger.log(
          `Invoice batch ${Math.floor(i / batchSize) + 1}: ${toCreate.length} to create, ${toUpdate.length} to update`,
        );

        const [createResult, updateResult] = await Promise.allSettled([
          toCreate.length > 0
            ? this.batchCreateInvoiceRecords(toCreate)
            : Promise.resolve({ success: 0, failed: 0 }),
          toUpdate.length > 0
            ? this.batchUpdateInvoiceRecords(toUpdate, existingRecords)
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

        if (i + batchSize < invoicesWithData.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      this.logger.log(
        `Invoice LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`Invoice LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: invoicesWithData.length };
    }
  }

  async getCustomerTableFields(): Promise<void> {
    try {
      const response = await this.client.bitable.appTableField.list({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
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

  async getExistingCustomerRecords(
    kiotVietIds: string[],
  ): Promise<Map<string, string>> {
    try {
      // Step 1: Verify kiotvietId field exists in LarkBase (note: customer uses 'kiotvietId', not 'kiotVietId')
      const hasKiotVietIdField = await this.verifyKiotVietIdField('customer');
      if (!hasKiotVietIdField) {
        this.logger.warn(
          'Customer: kiotvietId field not found in LarkBase - treating all records as new',
        );
        return new Map();
      }

      const existingRecords = new Map<string, string>();
      const batchSize = 100;
      let totalChecked = 0;
      let totalFound = 0;
      let totalErrors = 0;

      this.logger.log(
        `Customer: Starting duplicate check for ${kiotVietIds.length} records`,
      );

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        for (const kiotVietIdStr of batch) {
          totalChecked++;
          try {
            const kiotVietIdNum = Number(kiotVietIdStr);

            const response = await this.client.bitable.appTableRecord.search({
              path: {
                app_token: this.customerBaseToken,
                table_id: this.customerTableId,
              },
              data: {
                filter: {
                  conjunction: 'and',
                  conditions: [
                    {
                      field_name: 'kiotvietId', // Note: customer uses lowercase 'kiotvietId'
                      operator: 'is',
                      value: [kiotVietIdNum] as any,
                    },
                  ],
                },
              },
            });

            if (response.data?.items && response.data.items.length > 0) {
              const record = response.data.items[0];
              const recordKiotVietId = record.fields?.['kiotvietId'];
              if (recordKiotVietId === kiotVietIdNum) {
                existingRecords.set(kiotVietIdStr, record.record_id!);
                totalFound++;
                this.logger.debug(
                  `Customer: DUPLICATE found - kiotvietId=${kiotVietIdStr}, recordId=${record.record_id}`,
                );
              }
            } else {
              this.logger.debug(
                `Customer: NEW record - kiotvietId=${kiotVietIdStr}`,
              );
            }
          } catch (error) {
            totalErrors++;
            this.logger.warn(
              `Customer: Failed to check kiotvietId ${kiotVietIdStr}: ${error.message} - treating as NEW record`,
            );
            // Continue processing instead of throwing - don't stop sync
          }
        }

        // Add delay between batches to avoid rate limiting
        if (i + batchSize < kiotVietIds.length) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }

      this.logger.log(
        `Customer duplicate check completed: ${totalChecked} checked, ${totalFound} duplicates found, ${totalErrors} errors, ${totalChecked - totalFound} new records`,
      );

      return existingRecords;
    } catch (error) {
      this.logger.error(
        `Customer: Critical error in duplicate checking: ${error.message} - treating all records as NEW`,
      );
      // Return empty map instead of throwing - don't stop sync
      return new Map();
    }
  }

  async batchCreateRecords(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      const records = customers
        .map((customer) => this.mapCustomerToLarkBase(customer))
        .filter((record) => record.fields['Tên Khách Hàng']);

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
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
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
            fields: this.mapCustomerToLarkBase(customer).fields,
          };
        })
        .filter((record) => record !== null && record.fields['Tên Khách Hàng'])
        .map((record) => record!);

      if (!records.length) return { success: 0, failed: 0 };

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
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
    // return { success: 0, failed: 0 };

    if (!customers.length) return { success: 0, failed: 0 };

    try {
      await this.getCustomerTableFields();

      const batchSize = 100;
      let totalSuccess = 0;
      let totalFailed = 0;

      this.logger.log(
        `Starting LarkBase sync for ${customers.length} customers`,
      );

      for (let i = 0; i < customers.length; i += batchSize) {
        const batch = customers.slice(i, i + batchSize);

        const kiotVietIds = batch.map((c) => c.id.toString());
        const existingRecords =
          await this.getExistingCustomerRecords(kiotVietIds);

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

  private mapOrderToLarkBase(
    orderData: any,
    branchName?: string | null,
    customerName?: string | null,
    userName?: string | null,
  ): any {
    const fields: any = {};

    // Primary field - Mã Đặt Hàng (REQUIRED)
    if (orderData.code) {
      fields['Mã Đặt Hàng'] = orderData.code;
    }

    // Chi Nhánh - mapped from branchName
    if (branchName) {
      fields['Chi Nhánh'] = branchName;
    }

    // Id Khách Hàng
    if (orderData.customerId) {
      fields['Id Khách Hàng'] = Number(orderData.customerId);
    }

    // Tên Khách Hàng
    if (customerName) {
      fields['Tên Khách Hàng'] = customerName;
    }

    // Người bán
    if (userName) {
      fields['Người bán'] = userName;
    }

    // Tình Trạng Đặt Hàng - mapped from statusValue
    if (orderData.statusValue) {
      fields['Tình Trạng Đặt Hàng'] = orderData.statusValue;
    }

    // Thu Khác
    let thuKhac = 0;
    if (
      orderData.invoiceOrderSurcharges &&
      orderData.invoiceOrderSurcharges.length > 0
    ) {
      thuKhac = orderData.invoiceOrderSurcharges.reduce(
        (sum: any, surcharge: any) => {
          return sum + Number(surcharge.price || 0);
        },
        0,
      );
    }
    const tongThuKhac = Number(thuKhac || 0);
    fields['Thu Khác'] = tongThuKhac || 0;

    // Giảm Giá
    const giamGia = Number(orderData.discount || 0);
    fields['Giảm Giá'] = giamGia || 0;

    // Khách cần trả (Total)
    const khachCanTra = Number(orderData.total || 0);
    fields['Khách Cần Trả'] = khachCanTra || 0;

    // Khách Đã Trả
    if (
      orderData.totalPayment !== null &&
      orderData.totalPayment !== undefined
    ) {
      fields['Khách Đã Trả'] = Number(orderData.totalPayment || 0);
    }

    // Tổng tiền hàng = Khách cần trả ( Total ) + Giảm giá ( discounnt) - Thu khác ( Surcharge )
    const tongTienHang = khachCanTra + giamGia - tongThuKhac;
    fields['Tổng Tiền Hàng'] = tongTienHang;

    // Tổng sau giảm giá = Tổng tiền hàng - Giảm giá
    const tongSauGiamGia = tongTienHang - giamGia;
    fields['Tổng Sau Giảm Giá'] = tongSauGiamGia;

    // Mã Hoá Đơn - from invoices array
    if (orderData.invoices && orderData.invoices.length > 0) {
      const invoiceCodes = orderData.invoices
        .map((inv) => inv.invoiceCode)
        .join(', ');
      fields['Mã Hoá Đơn'] = invoiceCodes; // "HD076536, HD076536.01"
    }

    // Ghi Chú
    if (orderData.description) {
      fields['Ghi Chú'] = orderData.description;
    }

    // Ngày Mua - purchaseDate
    if (orderData.purchaseDate) {
      const vietnamDate = new Date(orderData.purchaseDate + '+07:00');
      fields['Ngày Mua'] = vietnamDate.getTime();
    }

    // Ngày Tạo Đơn - createdDate
    if (orderData.createdDate) {
      const vietnamDate = new Date(orderData.createdDate + '+07:00');
      fields['Ngày Tạo Đơn'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật - modifiedDate
    if (orderData.modifiedDate) {
      const vietnamDate = new Date(orderData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // Số Điện Thoại - from orderDelivery
    if (orderData.orderDelivery && orderData.orderDelivery.contactNumber) {
      fields['Số Điện Thoại'] = orderData.orderDelivery.contactNumber;
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (orderData.id) {
      fields['kiotVietId'] = Number(orderData.id);
    }

    return { fields };
  }

  async getOrderTableFields(): Promise<void> {
    try {
      const response = await this.client.bitable.appTableField.list({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
      });

      this.logger.log('Order LarkBase fields: ');
      if (response.data?.items) {
        response.data.items.forEach((field) => {
          this.logger.log(
            `Field ID: ${field.field_id}, Name: ${field.field_name}, Type: ${field.type}`,
          );
        });
      }
    } catch (error) {
      this.logger.log(`Failed to get order table fields: ${error.message}`);
    }
  }

  async getExistingOrderRecords(
    kiotVietIds: string[],
  ): Promise<Map<string, string>> {
    try {
      // Step 1: Verify kiotVietId field exists in LarkBase
      const hasKiotVietIdField = await this.verifyKiotVietIdField('order');
      if (!hasKiotVietIdField) {
        this.logger.warn(
          'Order: kiotVietId field not found in LarkBase - treating all records as new',
        );
        return new Map();
      }

      const existingRecords = new Map<string, string>();
      const batchSize = 100;
      let totalChecked = 0;
      let totalFound = 0;
      let totalErrors = 0;

      this.logger.log(
        `Order: Starting duplicate check for ${kiotVietIds.length} records`,
      );

      for (let i = 0; i < kiotVietIds.length; i += batchSize) {
        const batch = kiotVietIds.slice(i, i + batchSize);

        for (const kiotVietIdStr of batch) {
          totalChecked++;
          try {
            const kiotVietIdNum = Number(kiotVietIdStr);

            const response = await this.client.bitable.appTableRecord.search({
              path: {
                app_token: this.orderBaseToken,
                table_id: this.orderTableId,
              },
              data: {
                filter: {
                  conjunction: 'and',
                  conditions: [
                    {
                      field_name: 'kiotVietId',
                      operator: 'is',
                      value: [kiotVietIdNum] as any,
                    },
                  ],
                },
              },
            });

            if (response.data?.items && response.data.items.length > 0) {
              const record = response.data.items[0];
              const recordKiotVietId = record.fields?.['kiotVietId'];
              if (recordKiotVietId === kiotVietIdNum) {
                existingRecords.set(kiotVietIdStr, record.record_id!);
                totalFound++;
                this.logger.debug(
                  `Order: DUPLICATE found - kiotVietId=${kiotVietIdStr}, recordId=${record.record_id}`,
                );
              }
            } else {
              this.logger.debug(
                `Order: NEW record - kiotVietId=${kiotVietIdStr}`,
              );
            }
          } catch (error) {
            totalErrors++;
            this.logger.warn(
              `Order: Failed to check kiotVietId ${kiotVietIdStr}: ${error.message} - treating as NEW record`,
            );
            // Continue processing instead of throwing - don't stop sync
          }
        }
      }

      this.logger.log(
        `Order duplicate check completed: ${totalChecked} checked, ${totalFound} duplicates found, ${totalErrors} errors, ${totalChecked - totalFound} new records`,
      );

      return existingRecords;
    } catch (error) {
      this.logger.error(
        `Order: Critical error in duplicate checking: ${error.message} - treating all records as NEW`,
      );
      // Return empty map instead of throwing - don't stop sync
      return new Map();
    }
  }

  async batchCreateOrderRecords(
    orders: OrderForLarkBase[],
  ): Promise<{ success: number; failed: number }> {
    if (!orders.length) return { success: 0, failed: 0 };

    try {
      const records = orders
        .map((order) => {
          const mappedData = this.mapOrderToLarkBase(
            order.orderData,
            order.branchName,
            order.customerName,
            order.userName,
          );
          return mappedData.fields['Mã Đặt Hàng'] ? mappedData : null;
        })
        .filter((record): record is { fields: any } => record !== null)
        .map((record) => ({ fields: record.fields }));

      if (!records.length) return { success: 0, failed: 0 };

      this.logger.debug(
        `Attempting to create ${records.length} order records in LarkBase`,
      );
      this.logger.debug(
        'Sample order record:',
        JSON.stringify(records[0], null, 2),
      );

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = records.length - successCount;

      this.logger.log(
        `Order LarkBase batch create: ${successCount} success, ${failedCount} failed`,
      );

      if (failedCount > 0) {
        this.logger.error(
          'Order LarkBase create response:',
          JSON.stringify(response, null, 2),
        );
      }

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`Order LarkBase batch create failed: ${error.message}`);
      this.logger.error('Error stack:', error.stack);
      if (error.response?.data) {
        this.logger.error(
          'Order LarkBase API Error:',
          JSON.stringify(error.response.data, null, 2),
        );
      }
      if (error.response?.status) {
        this.logger.error('HTTP Status:', error.response.status);
      }
      return { success: 0, failed: orders.length };
    }
  }

  async batchUpdateOrderRecords(
    orders: OrderForLarkBase[],
    existingRecords: Map<string, string>,
  ): Promise<{ success: number; failed: number }> {
    if (!orders.length) return { success: 0, failed: 0 };

    try {
      const records = orders
        .filter((order) => existingRecords.has(order.orderData.id.toString()))
        .map((order) => {
          const recordId = existingRecords.get(order.orderData.id.toString());
          if (!recordId) return null;

          const mappedData = this.mapOrderToLarkBase(
            order.orderData,
            order.branchName,
            order.customerName,
            order.userName,
          );

          return {
            record_id: recordId,
            fields: mappedData.fields,
          };
        })
        .filter((record) => record !== null && record.fields['Mã Đặt Hàng'])
        .map((record) => record!);

      if (!records.length) return { success: 0, failed: 0 };

      this.logger.debug(
        `Attempting to update ${records.length} order records in LarkBase`,
      );

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = records.length - successCount;

      this.logger.log(
        `Order LarkBase batch update: ${successCount} success, ${failedCount} failed`,
      );
      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`Order LarkBase batch update failed: ${error.message}`);
      return { success: 0, failed: orders.length };
    }
  }

  async syncOrdersToLarkBase(
    ordersWithData: OrderForLarkBase[],
  ): Promise<{ success: number; failed: number }> {
    if (!ordersWithData.length) return { success: 0, failed: 0 };

    try {
      await this.getOrderTableFields();

      const batchSize = 100;
      let totalSuccess = 0;
      let totalFailed = 0;

      this.logger.log(
        `Order: Starting LarkBase sync for ${ordersWithData.length} orders`,
      );

      for (let i = 0; i < ordersWithData.length; i += batchSize) {
        const batch = ordersWithData.slice(i, i + batchSize);

        const kiotVietIds = batch.map((item) => item.orderData.id.toString());
        const existingRecords = await this.getExistingOrderRecords(kiotVietIds);

        const toCreate = batch.filter(
          (item) => !existingRecords.has(item.orderData.id.toString()),
        );
        const toUpdate = batch.filter((item) =>
          existingRecords.has(item.orderData.id.toString()),
        );

        this.logger.log(
          `Order batch ${Math.floor(i / batchSize) + 1}: ${toCreate.length} NEW records to create, ${toUpdate.length} DUPLICATE records to update`,
        );

        // Log specific records being created and updated
        if (toCreate.length > 0) {
          this.logger.debug(
            `Order: Creating NEW records with kiotVietIds: ${toCreate.map((item) => item.orderData.id).join(', ')}`,
          );
        }
        if (toUpdate.length > 0) {
          this.logger.debug(
            `Order: Updating DUPLICATE records with kiotVietIds: ${toUpdate.map((item) => item.orderData.id).join(', ')}`,
          );
        }

        const [createResult, updateResult] = await Promise.allSettled([
          toCreate.length > 0
            ? this.batchCreateOrderRecords(toCreate)
            : Promise.resolve({ success: 0, failed: 0 }),
          toUpdate.length > 0
            ? this.batchUpdateOrderRecords(toUpdate, existingRecords)
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
          this.logger.error(
            `Order: Create batch failed: ${createResult.reason}`,
          );
        }
        if (updateResult.status === 'rejected') {
          this.logger.error(
            `Order: Update batch failed: ${updateResult.reason}`,
          );
        }

        if (i + batchSize < ordersWithData.length) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      this.logger.log(
        `Order LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`Order LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: ordersWithData.length };
    }
  }

  private async verifyKiotVietIdField(
    entityType: 'customer' | 'order' | 'invoice',
  ): Promise<boolean> {
    try {
      let baseToken: string;
      let tableId: string;
      let fieldName: string;

      switch (entityType) {
        case 'customer':
          baseToken = this.customerBaseToken;
          tableId = this.customerTableId;
          fieldName = 'kiotvietId'; // Note: customer uses lowercase
          break;
        case 'order':
          baseToken = this.orderBaseToken;
          tableId = this.orderTableId;
          fieldName = 'kiotVietId';
          break;
        case 'invoice':
          baseToken = this.invoiceBaseToken;
          tableId = this.invoiceTableId;
          fieldName = 'kiotVietId';
          break;
      }

      const response = await this.client.bitable.appTableField.list({
        path: {
          app_token: baseToken,
          table_id: tableId,
        },
      });

      if (response.data?.items) {
        const hasField = response.data.items.some(
          (field) => field.field_name === fieldName,
        );
        if (hasField) {
          this.logger.debug(
            `${entityType}: ${fieldName} field verified in LarkBase`,
          );
          return true;
        } else {
          this.logger.error(
            `${entityType}: ${fieldName} field NOT FOUND in LarkBase! Available fields: ${response.data.items.map((f) => f.field_name).join(', ')}`,
          );
          return false;
        }
      }

      this.logger.error(
        `${entityType}: Could not retrieve field list from LarkBase`,
      );
      return false;
    } catch (error) {
      this.logger.error(
        `${entityType}: Failed to verify ${entityType === 'customer' ? 'kiotvietId' : 'kiotVietId'} field: ${error.message}`,
      );
      return false;
    }
  }
}
