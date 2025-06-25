// src/services/lark/lark-base.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as lark from '@larksuiteoapi/node-sdk';

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

  // ===== CUSTOMERS UPSERT METHODS =====
  async directCreateCustomers(
    customers: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      let totalSuccess = 0;
      let totalFailed = 0;
      let allRecords: any[] = [];

      // Process in smaller batches to avoid API limits
      const batchSize = 100;
      for (let i = 0; i < customers.length; i += batchSize) {
        const batch = customers.slice(i, i + batchSize);

        const { recordsToCreate, recordsToUpdate } =
          await this.separateCustomersForUpsert(batch);

        // CREATE new records
        if (recordsToCreate.length > 0) {
          const createResult = await this.batchCreateCustomers(recordsToCreate);
          totalSuccess += createResult.success;
          totalFailed += createResult.failed;
          if (createResult.records) allRecords.push(...createResult.records);
        }

        // UPDATE existing records
        if (recordsToUpdate.length > 0) {
          const updateResult = await this.batchUpdateCustomers(recordsToUpdate);
          totalSuccess += updateResult.success;
          totalFailed += updateResult.failed;
        }
      }

      this.logger.log(
        `Customer UPSERT: ${totalSuccess} success, ${totalFailed} failed`,
      );
      return {
        success: totalSuccess,
        failed: totalFailed,
        records: allRecords,
      };
    } catch (error) {
      this.logger.error(`Customer UPSERT failed: ${error.message}`);
      return { success: 0, failed: customers.length };
    }
  }

  private async separateCustomersForUpsert(customers: any[]): Promise<{
    recordsToCreate: any[];
    recordsToUpdate: any[];
  }> {
    const recordsToCreate: any[] = [];
    const recordsToUpdate: any[] = [];

    try {
      // Get all kiotVietIds from incoming customers
      const kiotVietIds = customers
        .map((c) => c.id || c.kiotVietId)
        .filter(Boolean);

      // Query LarkBase to find existing records by kiotVietId
      const existingRecords =
        await this.findExistingCustomersByKiotVietId(kiotVietIds);

      // Create lookup map of existing records
      const existingRecordsMap = new Map();
      existingRecords.forEach((record) => {
        const kiotVietId = record.fields?.kiotVietId;
        if (kiotVietId) {
          existingRecordsMap.set(kiotVietId.toString(), record);
        }
      });

      // Separate customers into create vs update
      for (const customer of customers) {
        const kiotVietId = (customer.id || customer.kiotVietId)?.toString();

        if (kiotVietId && existingRecordsMap.has(kiotVietId)) {
          // Record exists - add to update batch
          const existingRecord = existingRecordsMap.get(kiotVietId);
          recordsToUpdate.push({
            ...customer,
            larkRecordId: existingRecord.record_id,
          });
        } else {
          // Record doesn't exist - add to create batch
          recordsToCreate.push(customer);
        }
      }

      this.logger.debug(
        `Separated customers: ${recordsToCreate.length} to create, ${recordsToUpdate.length} to update`,
      );
    } catch (error) {
      this.logger.error(
        `Error separating customers for upsert: ${error.message}`,
      );
      // If error, default to create all (might cause duplicates but won't fail)
      recordsToCreate.push(...customers);
    }

    return { recordsToCreate, recordsToUpdate };
  }

  // ✅ FIXED: Moved page_size and page_token to params
  private async buildExistingRecordMap(
    kiotVietIds: any[],
  ): Promise<Map<string, string>> {
    const recordMap = new Map<string, string>();

    try {
      // FIXED: Use proper filter structure
      const filter = {
        conjunction: 'or' as const,
        conditions: kiotVietIds.map((id) => ({
          field_name: 'kiotVietId',
          operator: 'is' as const,
          value: [id.toString()],
        })),
      };

      let hasMore = true;
      let pageToken: string | undefined;

      while (hasMore) {
        const response = await this.client.bitable.appTableRecord.search({
          path: {
            app_token: this.customerBaseToken,
            table_id: this.customerTableId,
          },
          data: {
            filter,
            automatic_fields: false,
          },
          params: {
            page_size: 500, // ✅ FIXED: Moved to params
            page_token: pageToken, // ✅ FIXED: Moved to params
          },
        });

        if (response.data?.items) {
          response.data.items.forEach((record) => {
            const kiotVietId = record.fields?.kiotVietId?.toString();
            const recordId = record.record_id;

            if (kiotVietId && recordId) {
              recordMap.set(kiotVietId, recordId);
            }
          });
        }

        hasMore = response.data?.has_more || false;
        pageToken = response.data?.page_token;
      }
    } catch (error) {
      this.logger.warn(
        `Search failed, falling back to list scan: ${error.message}`,
      );
      return this.listAllAndFilter(kiotVietIds);
    }

    return recordMap;
  }

  private async listAllAndFilter(
    kiotVietIds: any[],
  ): Promise<Map<string, string>> {
    const recordMap = new Map<string, string>();
    let hasMore = true;
    let pageToken: string | undefined;

    while (hasMore) {
      const response = await this.client.bitable.appTableRecord.list({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        params: {
          page_size: 500,
          page_token: pageToken,
        },
      });

      if (response.data?.items) {
        response.data.items.forEach((record) => {
          const kiotVietId = record.fields?.kiotVietId?.toString();
          const recordId = record.record_id;

          if (
            kiotVietId &&
            recordId &&
            kiotVietIds.includes(parseInt(kiotVietId))
          ) {
            recordMap.set(kiotVietId, recordId);
          }
        });
      }

      hasMore = response.data?.has_more || false;
      pageToken = response.data?.page_token;
    }

    return recordMap;
  }

  private async findExistingCustomersByKiotVietId(
    kiotVietIds: any[],
  ): Promise<any[]> {
    try {
      const allRecords: any[] = [];
      let hasMore = true;
      let pageToken: string | undefined;

      while (hasMore) {
        const response = await this.client.bitable.appTableRecord.list({
          path: {
            app_token: this.customerBaseToken,
            table_id: this.customerTableId,
          },
          params: {
            page_size: 500,
            page_token: pageToken,
          },
        });

        if (response.data?.items) {
          const matchingRecords = response.data.items.filter((record) => {
            const recordKiotVietId = record.fields?.kiotVietId?.toString();
            return (
              recordKiotVietId &&
              kiotVietIds.includes(parseInt(recordKiotVietId))
            );
          });

          allRecords.push(...matchingRecords);
        }

        hasMore = response.data?.has_more || false;
        pageToken = response.data?.page_token;
      }

      return allRecords;
    } catch (error) {
      this.logger.error(`Error finding existing customers: ${error.message}`);
      return [];
    }
  }

  private async batchCreateCustomers(customers: any[]): Promise<{
    success: number;
    failed: number;
    records?: any[];
  }> {
    try {
      const records = customers
        .map((customer) => this.mapCustomerToLarkBase(customer))
        .filter((record) => record.fields['Tên Khách Hàng'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: customers.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = customers.length - successCount;

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(
        `LarkBase customer batch create failed: ${error.message}`,
      );
      return { success: 0, failed: customers.length };
    }
  }

  private async batchUpdateCustomers(customers: any[]): Promise<{
    success: number;
    failed: number;
  }> {
    try {
      const records = customers
        .filter((customer) => customer.larkRecordId)
        .map((customer) => ({
          record_id: customer.larkRecordId,
          fields: this.mapCustomerToLarkBase(customer).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: customers.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = customers.length - successCount;

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `LarkBase customer batch update failed: ${error.message}`,
      );
      return { success: 0, failed: customers.length };
    }
  }

  // ===== ORDERS UPSERT METHODS =====
  async directCreateOrders(
    orders: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!orders.length) return { success: 0, failed: 0 };

    try {
      let totalSuccess = 0;
      let totalFailed = 0;
      let allRecords: any[] = [];

      const batchSize = 100;
      for (let i = 0; i < orders.length; i += batchSize) {
        const batch = orders.slice(i, i + batchSize);

        const { recordsToCreate, recordsToUpdate } =
          await this.separateOrdersForUpsert(batch);

        if (recordsToCreate.length > 0) {
          const createResult = await this.batchCreateOrders(recordsToCreate);
          totalSuccess += createResult.success;
          totalFailed += createResult.failed;
          if (createResult.records) allRecords.push(...createResult.records);
        }

        if (recordsToUpdate.length > 0) {
          const updateResult = await this.batchUpdateOrders(recordsToUpdate);
          totalSuccess += updateResult.success;
          totalFailed += updateResult.failed;
        }
      }

      this.logger.log(
        `Order UPSERT: ${totalSuccess} success, ${totalFailed} failed`,
      );
      return {
        success: totalSuccess,
        failed: totalFailed,
        records: allRecords,
      };
    } catch (error) {
      this.logger.error(`Order UPSERT failed: ${error.message}`);
      return { success: 0, failed: orders.length };
    }
  }

  private async separateOrdersForUpsert(orders: any[]): Promise<{
    recordsToCreate: any[];
    recordsToUpdate: any[];
  }> {
    const recordsToCreate: any[] = [];
    const recordsToUpdate: any[] = [];

    try {
      const kiotVietIds = orders
        .map((o) => o.id || o.kiotVietId)
        .filter(Boolean);

      const existingRecords =
        await this.findExistingOrdersByKiotVietId(kiotVietIds);

      const existingRecordsMap = new Map();
      existingRecords.forEach((record) => {
        const kiotVietId = record.fields?.kiotVietId;
        if (kiotVietId) {
          existingRecordsMap.set(kiotVietId.toString(), record);
        }
      });

      for (const order of orders) {
        const kiotVietId = (order.id || order.kiotVietId)?.toString();

        if (kiotVietId && existingRecordsMap.has(kiotVietId)) {
          const existingRecord = existingRecordsMap.get(kiotVietId);
          recordsToUpdate.push({
            ...order,
            larkRecordId: existingRecord.record_id,
          });
        } else {
          recordsToCreate.push(order);
        }
      }

      this.logger.debug(
        `Separated orders: ${recordsToCreate.length} to create, ${recordsToUpdate.length} to update`,
      );
    } catch (error) {
      this.logger.error(`Error separating orders for upsert: ${error.message}`);
      recordsToCreate.push(...orders);
    }

    return { recordsToCreate, recordsToUpdate };
  }

  private async findExistingOrdersByKiotVietId(
    kiotVietIds: any[],
  ): Promise<any[]> {
    try {
      const allRecords: any[] = [];
      let hasMore = true;
      let pageToken: string | undefined;

      while (hasMore) {
        const response = await this.client.bitable.appTableRecord.list({
          path: {
            app_token: this.orderBaseToken,
            table_id: this.orderTableId,
          },
          params: {
            page_size: 500,
            page_token: pageToken,
          },
        });

        if (response.data?.items) {
          const matchingRecords = response.data.items.filter((record) => {
            const recordKiotVietId = record.fields?.kiotVietId?.toString();
            return (
              recordKiotVietId &&
              kiotVietIds.includes(parseInt(recordKiotVietId))
            );
          });

          allRecords.push(...matchingRecords);
        }

        hasMore = response.data?.has_more || false;
        pageToken = response.data?.page_token;
      }

      return allRecords;
    } catch (error) {
      this.logger.error(`Error finding existing orders: ${error.message}`);
      return [];
    }
  }

  private async batchCreateOrders(orders: any[]): Promise<{
    success: number;
    failed: number;
    records?: any[];
  }> {
    try {
      const records = orders
        .map((order) =>
          this.mapOrderToLarkBase(
            order,
            order.branch?.name || null,
            order.customer?.name || null,
            order.soldBy?.userName || null,
          ),
        )
        .filter((record) => record.fields['Mã Đặt Hàng'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: orders.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = orders.length - successCount;

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(`LarkBase order batch create failed: ${error.message}`);
      return { success: 0, failed: orders.length };
    }
  }

  private async batchUpdateOrders(orders: any[]): Promise<{
    success: number;
    failed: number;
  }> {
    try {
      const records = orders
        .filter((order) => order.larkRecordId)
        .map((order) => ({
          record_id: order.larkRecordId,
          fields: this.mapOrderToLarkBase(
            order,
            order.branch?.name || null,
            order.customer?.name || null,
            order.soldBy?.userName || null,
          ).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: orders.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = orders.length - successCount;

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(`LarkBase order batch update failed: ${error.message}`);
      return { success: 0, failed: orders.length };
    }
  }

  // ===== INVOICES UPSERT METHODS =====
  async directCreateInvoices(
    invoices: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!invoices.length) return { success: 0, failed: 0 };

    try {
      let totalSuccess = 0;
      let totalFailed = 0;
      let allRecords: any[] = [];

      const batchSize = 100;
      for (let i = 0; i < invoices.length; i += batchSize) {
        const batch = invoices.slice(i, i + batchSize);

        const { recordsToCreate, recordsToUpdate } =
          await this.separateInvoicesForUpsert(batch);

        if (recordsToCreate.length > 0) {
          const createResult = await this.batchCreateInvoices(recordsToCreate);
          totalSuccess += createResult.success;
          totalFailed += createResult.failed;
          if (createResult.records) allRecords.push(...createResult.records);
        }

        if (recordsToUpdate.length > 0) {
          const updateResult = await this.batchUpdateInvoices(recordsToUpdate);
          totalSuccess += updateResult.success;
          totalFailed += updateResult.failed;
        }
      }

      this.logger.log(
        `Invoice UPSERT: ${totalSuccess} success, ${totalFailed} failed`,
      );
      return {
        success: totalSuccess,
        failed: totalFailed,
        records: allRecords,
      };
    } catch (error) {
      this.logger.error(`Invoice UPSERT failed: ${error.message}`);
      return { success: 0, failed: invoices.length };
    }
  }

  private async separateInvoicesForUpsert(invoices: any[]): Promise<{
    recordsToCreate: any[];
    recordsToUpdate: any[];
  }> {
    const recordsToCreate: any[] = [];
    const recordsToUpdate: any[] = [];

    try {
      const kiotVietIds = invoices
        .map((i) => i.id || i.kiotVietId)
        .filter(Boolean);

      const existingRecords =
        await this.findExistingInvoicesByKiotVietId(kiotVietIds);

      const existingRecordsMap = new Map();
      existingRecords.forEach((record) => {
        const kiotVietId = record.fields?.kiotVietId;
        if (kiotVietId) {
          existingRecordsMap.set(kiotVietId.toString(), record);
        }
      });

      for (const invoice of invoices) {
        const kiotVietId = (invoice.id || invoice.kiotVietId)?.toString();

        if (kiotVietId && existingRecordsMap.has(kiotVietId)) {
          const existingRecord = existingRecordsMap.get(kiotVietId);
          recordsToUpdate.push({
            ...invoice,
            larkRecordId: existingRecord.record_id,
          });
        } else {
          recordsToCreate.push(invoice);
        }
      }

      this.logger.debug(
        `Separated invoices: ${recordsToCreate.length} to create, ${recordsToUpdate.length} to update`,
      );
    } catch (error) {
      this.logger.error(
        `Error separating invoices for upsert: ${error.message}`,
      );
      recordsToCreate.push(...invoices);
    }

    return { recordsToCreate, recordsToUpdate };
  }

  private async findExistingInvoicesByKiotVietId(
    kiotVietIds: any[],
  ): Promise<any[]> {
    try {
      const allRecords: any[] = [];
      let hasMore = true;
      let pageToken: string | undefined;

      while (hasMore) {
        const response = await this.client.bitable.appTableRecord.list({
          path: {
            app_token: this.invoiceBaseToken,
            table_id: this.invoiceTableId,
          },
          params: {
            page_size: 500,
            page_token: pageToken,
          },
        });

        if (response.data?.items) {
          const matchingRecords = response.data.items.filter((record) => {
            const recordKiotVietId = record.fields?.kiotVietId?.toString();
            return (
              recordKiotVietId &&
              kiotVietIds.includes(parseInt(recordKiotVietId))
            );
          });

          allRecords.push(...matchingRecords);
        }

        hasMore = response.data?.has_more || false;
        pageToken = response.data?.page_token;
      }

      return allRecords;
    } catch (error) {
      this.logger.error(`Error finding existing invoices: ${error.message}`);
      return [];
    }
  }

  private async batchCreateInvoices(invoices: any[]): Promise<{
    success: number;
    failed: number;
    records?: any[];
  }> {
    try {
      const records = invoices
        .map((invoice) =>
          this.mapInvoiceToLarkBase(
            invoice,
            invoice.branch?.name || null,
            invoice.customer?.name || null,
            invoice.soldBy?.userName || null,
          ),
        )
        .filter((record) => record.fields['Mã Hóa Đơn'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: invoices.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = invoices.length - successCount;

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(
        `LarkBase invoice batch create failed: ${error.message}`,
      );
      return { success: 0, failed: invoices.length };
    }
  }

  private async batchUpdateInvoices(invoices: any[]): Promise<{
    success: number;
    failed: number;
  }> {
    try {
      const records = invoices
        .filter((invoice) => invoice.larkRecordId)
        .map((invoice) => ({
          record_id: invoice.larkRecordId,
          fields: this.mapInvoiceToLarkBase(
            invoice,
            invoice.branch?.name || null,
            invoice.customer?.name || null,
            invoice.soldBy?.userName || null,
          ).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: invoices.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = invoices.length - successCount;

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `LarkBase invoice batch update failed: ${error.message}`,
      );
      return { success: 0, failed: invoices.length };
    }
  }

  // ===== DUPLICATE CLEANUP METHODS =====
  async deleteDuplicateCustomers(): Promise<{
    deleted: number;
    remaining: number;
  }> {
    try {
      this.logger.log('🔍 Scanning LarkBase for duplicate customers...');

      const allRecords = await this.getAllCustomerRecords();
      const duplicates = this.findDuplicatesByKiotVietId(allRecords);

      let deletedCount = 0;

      for (const [kiotVietId, records] of duplicates) {
        // Keep first record, delete the rest
        const toDelete = records.slice(1);

        if (toDelete.length > 0) {
          const recordIds = toDelete
            .map((r) => r.record_id)
            .filter((id): id is string => typeof id === 'string');

          if (recordIds.length > 0) {
            await this.batchDeleteCustomers(recordIds);
            deletedCount += recordIds.length;
            this.logger.debug(
              `Deleted ${recordIds.length} duplicates for kiotVietId: ${kiotVietId}`,
            );
          }
        }
      }

      this.logger.log(
        `🧹 Cleanup complete: ${deletedCount} duplicates removed, ${allRecords.length - deletedCount} remaining`,
      );

      return {
        deleted: deletedCount,
        remaining: allRecords.length - deletedCount,
      };
    } catch (error) {
      this.logger.error(`Duplicate cleanup failed: ${error.message}`);
      throw error;
    }
  }

  private async getAllCustomerRecords(): Promise<any[]> {
    const allRecords: any[] = [];
    let hasMore = true;
    let pageToken: string | undefined;

    while (hasMore) {
      const response = await this.client.bitable.appTableRecord.list({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        params: {
          page_size: 500,
          page_token: pageToken,
        },
      });

      if (response.data?.items) {
        allRecords.push(...response.data.items);
      }

      hasMore = response.data?.has_more || false;
      pageToken = response.data?.page_token;
    }

    return allRecords;
  }

  private findDuplicatesByKiotVietId(records: any[]): Map<string, any[]> {
    const kiotVietIdMap = new Map<string, any[]>();

    records.forEach((record) => {
      const kiotVietId = record.fields?.kiotVietId?.toString();
      if (kiotVietId) {
        if (!kiotVietIdMap.has(kiotVietId)) {
          kiotVietIdMap.set(kiotVietId, []);
        }
        kiotVietIdMap.get(kiotVietId)!.push(record);
      }
    });

    // Filter to only return groups with duplicates (more than 1 record)
    const duplicates = new Map<string, any[]>();
    for (const [kiotVietId, recordList] of kiotVietIdMap) {
      if (recordList.length > 1) {
        duplicates.set(kiotVietId, recordList);
      }
    }

    return duplicates;
  }

  private async batchDeleteCustomers(recordIds: string[]): Promise<void> {
    if (!recordIds.length) return;

    try {
      const batchSize = 500;
      for (let i = 0; i < recordIds.length; i += batchSize) {
        const batchIds = recordIds.slice(i, i + batchSize);

        await this.client.bitable.appTableRecord.batchDelete({
          path: {
            app_token: this.customerBaseToken,
            table_id: this.customerTableId,
          },
          data: {
            records: batchIds,
          },
        });
      }
    } catch (error) {
      this.logger.error(`Batch delete failed: ${error.message}`);
      throw error;
    }
  }

  // ===== MAPPING METHODS =====
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

    // Địa Chỉ
    if (customerData.address) {
      fields['Địa Chỉ'] = customerData.address;
    }

    // Email
    if (customerData.email) {
      fields['Email'] = customerData.email;
    }

    // Nợ Hiện Tại
    if (customerData.debt !== null && customerData.debt !== undefined) {
      fields['Nợ Hiện Tại'] = Number(customerData.debt);
    }

    // Tổng Bán (totalInvoiced) - ✅ Now properly mapped from KiotViet API
    if (
      customerData.totalInvoiced !== null &&
      customerData.totalInvoiced !== undefined
    ) {
      fields['Tổng Bán'] = Number(customerData.totalInvoiced);
    }

    // Ghi Chú
    if (customerData.comments) {
      fields['Ghi Chú'] = customerData.comments;
    }

    // Ngày Tạo
    if (customerData.createdDate) {
      const vietnamDate = new Date(customerData.createdDate + '+07:00');
      fields['Ngày Tạo'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật
    if (customerData.modifiedDate) {
      const vietnamDate = new Date(customerData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (customerData.id || customerData.kiotVietId) {
      fields['kiotVietId'] = Number(customerData.id || customerData.kiotVietId);
    }

    return { fields };
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

    // Chi Nhánh
    if (branchName) {
      fields['Chi Nhánh'] = branchName;
    }

    // Tên Khách Hàng
    if (customerName) {
      fields['Tên Khách Hàng'] = customerName;
    }

    // Người bán
    if (userName) {
      fields['Người bán'] = userName;
    }

    // Khách Cần Trả
    if (orderData.total !== null && orderData.total !== undefined) {
      fields['Khách Cần Trả'] = Number(orderData.total);
    }

    // Khách Đã Trả
    if (
      orderData.totalPayment !== null &&
      orderData.totalPayment !== undefined
    ) {
      fields['Khách Đã Trả'] = Number(orderData.totalPayment);
    }

    // Tình Trạng Đặt Hàng
    if (orderData.status !== null && orderData.status !== undefined) {
      const statusMap = {
        1: 'Hoàn thành',
        2: 'Đã hủy',
        3: 'Đang xử lý',
        4: 'Chờ xử lý',
        5: 'Không giao được',
      };
      fields['Tình Trạng Đặt Hàng'] =
        statusMap[orderData.status] || 'Không xác định';
    }

    // Ngày Tạo
    if (orderData.createdDate) {
      const vietnamDate = new Date(orderData.createdDate + '+07:00');
      fields['Ngày Tạo'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật
    if (orderData.modifiedDate) {
      const vietnamDate = new Date(orderData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (orderData.id || orderData.kiotVietId) {
      fields['kiotVietId'] = Number(orderData.id || orderData.kiotVietId);
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
      fields['Mã Hóa Đơn'] = invoiceData.code;
    }

    // Chi Nhánh
    if (branchName) {
      fields['Chi Nhánh'] = branchName;
    }

    // Tên Khách Hàng
    if (customerName) {
      fields['Tên Khách Hàng'] = customerName;
    }

    // Người bán
    if (userName) {
      fields['Người bán'] = userName;
    }

    // Khách Cần Trả
    if (invoiceData.total !== null && invoiceData.total !== undefined) {
      fields['Khách Cần Trả'] = Number(invoiceData.total);
    }

    // Tổng Tiền Hàng
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
    if (invoiceData.id || invoiceData.kiotVietId) {
      fields['kiotVietId'] = Number(invoiceData.id || invoiceData.kiotVietId);
    }

    return { fields };
  }

  // ===== LEGACY METHODS (Keep for backward compatibility) =====
  async syncCustomersToLarkBase(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateCustomers(customers);
  }

  async syncOrdersToLarkBase(
    orders: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateOrders(orders);
  }

  async syncInvoicesToLarkBase(
    invoices: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateInvoices(invoices);
  }
}
