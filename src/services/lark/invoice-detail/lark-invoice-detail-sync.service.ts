import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_INVOICE_DETAIL_FIELDS = {
  UNIQUE_KEY: 'UniqueKey',
  INVOICE_KIOTVIET_ID: 'invoiceKiotVietId',
  PRODUCT_KIOTVIET_ID: 'productKiotVietId',
  PRODUCT_NAME: 'Tên Sản Phẩm',
  PRODUCT_CODE: 'Mã Sản Phẩm',
  QUANTITY: 'Số Lượng',
  PRICE: 'Giá Bán',
  SUB_TOTAL: 'Cần Thu',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá',
  NOTE: 'Ghi Chú',
};

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

@Injectable()
export class LarkInvoiceDetailSyncService {
  private readonly logger = new Logger(LarkInvoiceDetailSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private existingRecordsCache = new Map<string, string>();
  private cacheLoaded = false;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_INVOICE_DETAIL_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_INVOICE_DETAIL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase invoice detail configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  private async searchRecordByUniqueKey(
    uniqueKey: string,
  ): Promise<string | null> {
    try {
      const headers = await this.larkAuthService.getInvoiceDetailHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            field_names: [LARK_INVOICE_DETAIL_FIELDS.UNIQUE_KEY],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_INVOICE_DETAIL_FIELDS.UNIQUE_KEY,
                  operator: 'is',
                  value: [uniqueKey],
                },
              ],
            },
          },
          {
            headers,
            timeout: 10000,
          },
        ),
      );

      if (response.data.code === 0) {
        const items = response.data.data?.items || [];
        if (items.length > 0) {
          return items[0].record_id;
        }
      }

      return null;
    } catch (error) {
      this.logger.warn(
        `Search invoice detail by uniqueKey failed: ${error.message}`,
      );
      return null;
    }
  }

  async syncSingleInvoiceDetailDirect(detail: any): Promise<void> {
    try {
      this.logger.log(
        `🔄 Syncing invoice detail ${detail.uniqueKey} to Lark...`,
      );

      let existingRecordId: string | null = null;

      if (detail.larkRecordId) {
        existingRecordId = detail.larkRecordId;
        this.logger.log(`Found existing larkRecordId: ${existingRecordId}`);
      } else {
        existingRecordId = await this.searchRecordByUniqueKey(detail.uniqueKey);
      }

      const larkData = this.mapDetailToLarkBase(detail);
      const headers = await this.larkAuthService.getInvoiceDetailHeaders();

      if (existingRecordId) {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        try {
          await firstValueFrom(
            this.httpService.put(
              url,
              { fields: larkData },
              { headers, timeout: 10000 },
            ),
          );

          this.logger.log(
            `✅ Updated invoice detail ${detail.uniqueKey} in Lark`,
          );

          if (!detail.larkRecordId) {
            await this.prismaService.invoiceDetail.update({
              where: { uniqueKey: detail.uniqueKey },
              data: {
                larkRecordId: existingRecordId,
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
              },
            });
          } else {
            await this.prismaService.invoiceDetail.update({
              where: { uniqueKey: detail.uniqueKey },
              data: {
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
              },
            });
          }
        } catch (updateError) {
          const isRecordNotFound =
            updateError.response?.status === 404 ||
            updateError.response?.data?.code === 1254034 ||
            updateError.response?.data?.msg?.includes('record') ||
            updateError.response?.data?.msg?.includes('not found');

          if (isRecordNotFound) {
            this.logger.warn(
              `⚠️ Record ${existingRecordId} not found on Lark, creating new record...`,
            );

            await this.prismaService.invoiceDetail.update({
              where: { uniqueKey: detail.uniqueKey },
              data: { larkRecordId: null },
            });

            const createUrl = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

            const createResponse = await firstValueFrom(
              this.httpService.post(
                createUrl,
                { fields: larkData },
                { headers, timeout: 10000 },
              ),
            );

            this.logger.log(
              `✅ Re-created invoice detail ${detail.uniqueKey} in Lark`,
            );

            const newRecordId = createResponse.data?.data?.record_id;
            if (newRecordId) {
              await this.prismaService.invoiceDetail.update({
                where: { uniqueKey: detail.uniqueKey },
                data: {
                  larkRecordId: newRecordId,
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                },
              });
            } else {
              await this.prismaService.invoiceDetail.update({
                where: { uniqueKey: detail.uniqueKey },
                data: {
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                },
              });
            }
          } else {
            throw updateError;
          }
        }
      } else {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const response = await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(
          `✅ Created invoice detail ${detail.uniqueKey} in Lark`,
        );

        const newRecordId = response.data?.data?.record_id;
        if (newRecordId) {
          await this.prismaService.invoiceDetail.update({
            where: { uniqueKey: detail.uniqueKey },
            data: {
              larkRecordId: newRecordId,
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
            },
          });
        } else {
          await this.prismaService.invoiceDetail.update({
            where: { uniqueKey: detail.uniqueKey },
            data: {
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Sync detail ${detail.uniqueKey} failed: ${error.message}`,
      );

      await this.prismaService.invoiceDetail.update({
        where: { uniqueKey: detail.uniqueKey },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncRetries: { increment: 1 },
        },
      });

      throw error;
    }
  }

  async syncInvoiceDetailsToLarkBase(): Promise<void> {
    const lockKey = `lark_invoice_detail_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log('Starting invoice detail sync...');

      const invoiceDetails = await this.prismaService.invoiceDetail.findMany({
        where: {
          larkSyncStatus: 'PENDING',
        },
        take: 500,
      });

      if (invoiceDetails.length === 0) {
        this.logger.log('No invoice details to sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      this.logger.log(
        `Found ${invoiceDetails.length} invoice details with "thanh lý"`,
      );

      await this.loadExistingRecords();

      const BATCH_SIZE = 500;
      let totalSuccess = 0;
      let totalFailed = 0;

      for (let i = 0; i < invoiceDetails.length; i += BATCH_SIZE) {
        const batch = invoiceDetails.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(invoiceDetails.length / BATCH_SIZE);

        this.logger.log(
          `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} invoicesDetail)`,
        );

        for (const invoiceDetail of batch) {
          try {
            await this.syncSingleInvoiceDetailDirect(invoiceDetail);
            totalSuccess++;
          } catch (error) {
            this.logger.error(
              `Failed to sync invoice detail: ${error.message}`,
            );
            totalFailed++;
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      // const { newDetails, updateDetails } =
      //   this.categorizeDetails(invoiceDetails);

      // this.logger.log(
      //   `New: ${newDetails.length}, Updates: ${updateDetails.length}`,
      // );

      // if (newDetails.length > 0) {
      //   await this.processNewDetails(newDetails);
      // }

      // if (updateDetails.length > 0) {
      //   await this.processUpdateDetails(updateDetails);
      // }

      // await this.releaseSyncLock(lockKey);
      // this.logger.log('Invoice detail sync completed!');

      this.logger.log('🎯 Batch sync completed!');
      this.logger.log(`✅ Success: ${totalSuccess}`);
      this.logger.log(`❌ Failed: ${totalFailed}`);
    } catch (error) {
      this.logger.error(`Sync failed: ${error.message}`);
      await this.releaseSyncLock(lockKey);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) return 0;

    if (typeof value === 'bigint') {
      return Number(value);
    }

    if (typeof value === 'number') {
      return Math.floor(value);
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return 0;
      const parsed = parseInt(trimmed, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    try {
      const asString = String(value).trim();
      const parsed = parseInt(asString, 10);
      return isNaN(parsed) ? 0 : parsed;
    } catch {
      return 0;
    }
  }

  async syncInvoiceDetailsByInvoiceId(invoiceId: number): Promise<void> {
    try {
      this.logger.log(
        `🔄 Starting realtime sync for invoice details of invoice ID: ${invoiceId}`,
      );

      const invoiceDetails = await this.prismaService.invoiceDetail.findMany({
        where: {
          invoiceId: invoiceId,
          larkSyncStatus: 'PENDING',
        },
      });

      if (invoiceDetails.length === 0) {
        this.logger.log(
          `⏭️  No PENDING invoice details to sync for invoice ID: ${invoiceId}`,
        );
        return;
      }

      this.logger.log(
        `📦 Found ${invoiceDetails.length} PENDING invoice details for invoice ID: ${invoiceId}`,
      );

      let successCount = 0;
      let failCount = 0;

      for (const detail of invoiceDetails) {
        try {
          await this.syncSingleInvoiceDetailDirect(detail);
          successCount++;

          await new Promise((resolve) => setTimeout(resolve, 300));
        } catch (error) {
          this.logger.error(
            `❌ Failed to sync detail ${detail.uniqueKey}: ${error.message}`,
          );
          failCount++;
        }
      }

      this.logger.log(
        `✅ Realtime sync completed for invoice ID: ${invoiceId} - Success: ${successCount}, Failed: ${failCount}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Realtime sync failed for invoice ID ${invoiceId}: ${error.message}`,
      );
      throw error;
    }
  }

  private categorizeDetails(details: any[]): {
    newDetails: any[];
    updateDetails: any[];
  } {
    const newDetails: any[] = [];
    const updateDetails: any[] = [];

    for (const detail of details) {
      if (!detail.uniqueKey) continue;

      if (this.existingRecordsCache.has(detail.uniqueKey)) {
        updateDetails.push(detail);
      } else {
        newDetails.push(detail);
      }
    }

    return { newDetails, updateDetails };
  }

  private async processNewDetails(details: any[]): Promise<void> {
    this.logger.log(`Creating ${details.length} new records...`);

    for (let i = 0; i < details.length; i += this.batchSize) {
      const batch = details.slice(i, i + this.batchSize);
      const result = await this.batchCreateDetails(batch);

      await this.updateDatabaseStatus(
        result.successRecords.map((r) => r.uniqueKey),
        'SYNCED',
      );

      // if (i + this.batchSize < details.length) {
      //   await new Promise((resolve) => setTimeout(resolve, 100));
      // }
    }
  }

  private async processUpdateDetails(details: any[]): Promise<void> {
    this.logger.log(`Updating ${details.length} records...`);

    for (const detail of details) {
      try {
        const larkRecordId = this.existingRecordsCache.get(detail.uniqueKey);
        if (!larkRecordId) continue;

        await this.updateSingleDetail(detail, larkRecordId);
        await this.updateDatabaseStatus([detail.uniqueKey], 'SYNCED');
      } catch (error) {
        this.logger.warn(
          `Update failed for ${detail.uniqueKey}: ${error.message}`,
        );
      }
    }
  }

  private async batchCreateDetails(
    details: any[],
  ): Promise<{ successRecords: any[]; failedRecords: any[] }> {
    const records = details.map((detail) => ({
      fields: this.mapDetailToLarkBase(detail),
    }));

    const headers = await this.larkAuthService.getInvoiceDetailHeaders();
    const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

    try {
      const response = await firstValueFrom(
        this.httpService.post<LarkBatchResponse>(
          url,
          { records },
          { headers, timeout: 30000 },
        ),
      );

      if (response.data.code !== 0) {
        this.logger.error('❌ LarkBase API Error:', {
          code: response.data.code,
          msg: response.data.msg,
          data: response.data.data,
        });
        throw new Error(`Lark API error: ${response.data.msg}`);
      }

      if (response.data.data?.records) {
        for (let i = 0; i < response.data.data.records.length; i++) {
          const larkRecord = response.data.data.records[i];
          const detail = details[i];

          if (larkRecord.record_id && detail.uniqueKey) {
            await this.prismaService.invoiceDetail.update({
              where: { uniqueKey: detail.uniqueKey },
              data: {
                larkRecordId: larkRecord.record_id,
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Saved larkRecordId ${larkRecord.record_id} for ${detail.uniqueKey}`,
            );
          }
        }
      }

      return {
        successRecords: details,
        failedRecords: [],
      };
    } catch (error) {
      this.logger.error('❌ Batch create error details:', {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        message: error.message,
      });

      if (records && records.length > 0) {
        this.logger.error(
          'Sample record being sent:',
          JSON.stringify(records[0], null, 2),
        );
      }

      throw error;
    }
  }

  private async updateSingleDetail(
    detail: any,
    recordId: string,
  ): Promise<void> {
    const headers = await this.larkAuthService.getInvoiceDetailHeaders();
    const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${recordId}`;

    await firstValueFrom(
      this.httpService.put(
        url,
        { fields: this.mapDetailToLarkBase(detail) },
        { headers, timeout: 30000 },
      ),
    );

    await this.prismaService.invoiceDetail.update({
      where: { uniqueKey: detail.uniqueKey },
      data: {
        larkRecordId: recordId,
        larkSyncStatus: 'SYNCED',
        larkSyncedAt: new Date(),
      },
    });
  }

  private async loadExistingRecords(): Promise<void> {
    this.logger.log('Loading existing records from LarkBase...');

    let pageToken: string | undefined;
    let totalLoaded = 0;

    do {
      const headers = await this.larkAuthService.getInvoiceDetailHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

      const params: any = { page_size: 500 };
      if (pageToken) params.page_token = pageToken;

      const response = await firstValueFrom(
        this.httpService.get<LarkBatchResponse>(url, { headers, params }),
      );

      if (response.data.code === 0 && response.data.data?.records) {
        for (const record of response.data.data.records) {
          const uniqueKey =
            record.fields[LARK_INVOICE_DETAIL_FIELDS.UNIQUE_KEY];
          if (uniqueKey) {
            this.existingRecordsCache.set(uniqueKey, record.record_id);
            totalLoaded++;
          }
        }
      }

      pageToken = response.data.data?.page_token;
    } while (pageToken);

    this.cacheLoaded = true;
    this.logger.log(`Loaded ${totalLoaded} existing records`);
  }

  private mapDetailToLarkBase(detail: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (detail.uniqueKey) {
      fields[LARK_INVOICE_DETAIL_FIELDS.UNIQUE_KEY] = detail.uniqueKey;
    }

    if (detail.invoiceKiotVietId) {
      fields[LARK_INVOICE_DETAIL_FIELDS.INVOICE_KIOTVIET_ID] =
        this.safeBigIntToNumber(detail.invoiceKiotVietId);
    }

    if (detail.productKiotVietId) {
      fields[LARK_INVOICE_DETAIL_FIELDS.PRODUCT_KIOTVIET_ID] =
        this.safeBigIntToNumber(detail.productKiotVietId);
    }

    if (detail.productName) {
      fields[LARK_INVOICE_DETAIL_FIELDS.PRODUCT_NAME] = detail.productName;
    }

    if (detail.productCode) {
      fields[LARK_INVOICE_DETAIL_FIELDS.PRODUCT_CODE] = detail.productCode;
    }

    if (detail.quantity !== null) {
      fields[LARK_INVOICE_DETAIL_FIELDS.QUANTITY] = Number(detail.quantity);
    }

    if (detail.price !== null) {
      fields[LARK_INVOICE_DETAIL_FIELDS.PRICE] = Number(detail.price);
    }

    if (detail.subTotal !== null) {
      fields[LARK_INVOICE_DETAIL_FIELDS.SUB_TOTAL] = Number(detail.subTotal);
    }

    if (detail.discount !== null) {
      fields[LARK_INVOICE_DETAIL_FIELDS.DISCOUNT] = Number(detail.discount);
    }

    if (detail.discountRatio !== null) {
      fields[LARK_INVOICE_DETAIL_FIELDS.DISCOUNT_RATIO] = Number(
        detail.discountRatio,
      );
    }

    if (detail.note) {
      fields[LARK_INVOICE_DETAIL_FIELDS.NOTE] = detail.note;
    }

    return fields;
  }

  private async updateDatabaseStatus(
    uniqueKeys: string[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    await this.prismaService.invoiceDetail.updateMany({
      where: { uniqueKey: { in: uniqueKeys } },
      data: {
        larkSyncStatus: status,
        larkSyncedAt: status === 'SYNCED' ? new Date() : undefined,
      },
    });
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name: 'invoice_detail_lark_sync' },
      create: {
        name: 'invoice_detail_lark_sync',
        entities: ['invoice_detail'],
        syncMode: 'lark',
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
      },
      update: {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
      },
    });
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    await this.prismaService.syncControl.update({
      where: { name: 'invoice_detail_lark_sync' },
      data: {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
      },
    });
  }
}
