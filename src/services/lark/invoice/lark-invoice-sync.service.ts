// src/services/lark/invoice/lark-invoice-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// ✅ EXACT field names from Hoá Đơn.rtf
const LARK_INVOICE_FIELDS = {
  PRIMARY_CODE: 'Mã Hoá Đơn',
  ORDER_CODE: 'Mã Đơn Hàng',
  KIOTVIET_ID: 'kiotVietId',
  PURCHASE_DATE: 'Ngày Mua',
  BRANCH: 'Branch',
  SELLER: 'Người Bán',
  CUSTOMER_NEED_PAY: 'Khách Cần Trả',
  CUSTOMER_PAID: 'Khách Đã Trả',
  TOTAL_AMOUNT: 'Tổng Tiền Hàng',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá',
  STATUS: 'Tình Trạng',
  SALE_CHANNEL: 'Phương Thức',
  APPLY_VOUCHER: 'Áp Mã Voucher',
  CREATED_DATE: 'Ngày Tạo',
  MODIFIED_DATE: 'Ngày Cập Nhật',
} as const;

const BRANCH_OPTIONS = {
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
};

const STATUS_OPTIONS = {
  COMPLETED: 'Hoàn Thành',
  CANCELLED: 'Đã Huỷ',
  PROCESSING: 'Đang Xử Lý',
  DELIVERY_FAILED: 'Không Giao Được',
};

const SALE_CHANNEL_OPTIONS = {
  DIRECT: 'Trực Tiếp',
  DELIVERY: 'Giao Hàng',
  ONLINE: 'Online',
  OTHER: 'Khác',
};

const VOUCHER_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
};

interface LarkBaseRecord {
  record_id?: string;
  fields: Record<string, any>;
}

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    items?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

interface BatchResult {
  successRecords: any[];
  failedRecords: any[];
}

@Injectable()
export class LarkInvoiceSyncService {
  private readonly logger = new Logger(LarkInvoiceSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // Cache management
  private existingRecordsCache: Map<number, string> = new Map();
  private invoiceCodeCache: Map<string, string> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 30;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_INVOICE_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_INVOICE_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase invoice configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ============================================================================
  // MAIN SYNC METHOD
  // ============================================================================

  async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    const lockKey = `lark_invoice_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${invoices.length} invoices...`,
      );

      const invoicesToSync = invoices.filter(
        (i) => i.larkSyncStatus === 'PENDING' || i.larkSyncStatus === 'FAILED',
      );

      if (invoicesToSync.length === 0) {
        this.logger.log('📋 No invoices need LarkBase sync');
        return;
      }

      const pendingCount = invoices.filter(
        (i) => i.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = invoices.filter(
        (i) => i.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED invoices`,
      );

      // Test LarkBase connection
      await this.testLarkBaseConnection();

      // Load cache with retry mechanism
      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn(
          '⚠️ Cache loading failed - will use alternative duplicate detection',
        );
      }

      // Categorize invoices
      const { newInvoices, updateInvoices } =
        this.categorizeInvoices(invoicesToSync);

      this.logger.log(
        `📋 Categorization: ${newInvoices.length} new, ${updateInvoices.length} updates`,
      );

      // Process in smaller batches
      const BATCH_SIZE_FOR_SYNC = 50;

      // Process new invoices
      if (newInvoices.length > 0) {
        for (let i = 0; i < newInvoices.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newInvoices.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new invoices batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newInvoices.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewInvoices(batch);
        }
      }

      // Process updates
      if (updateInvoices.length > 0) {
        for (let i = 0; i < updateInvoices.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateInvoices.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateInvoices.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateInvoices(batch);
        }
      }

      this.logger.log(`🎉 LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync failed: ${error.message}`);

      // Only mark as failed for non-connection errors
      if (
        !error.message.includes('connect') &&
        !error.message.includes('400')
      ) {
        await this.updateDatabaseStatus(invoices, 'FAILED');
      }

      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  // ============================================================================
  // CACHE LOADING WITH RETRY
  // ============================================================================

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(
          `📥 Loading cache (attempt ${attempt}/${maxRetries})...`,
        );

        if (this.isCacheValid()) {
          this.logger.log('✅ Using existing valid cache');
          return true;
        }

        this.clearCache();
        await this.loadExistingRecordsCache();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `✅ Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`⚠️ Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.error(
          `❌ Cache loading attempt ${attempt} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = attempt * 3000;
          this.logger.log(`⏳ Waiting ${delay / 1000}s before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    return false;
  }

  private isCacheValid(): boolean {
    if (!this.cacheLoaded || !this.lastCacheLoadTime) {
      return false;
    }

    const cacheAge = Date.now() - this.lastCacheLoadTime.getTime();
    const maxAge = this.CACHE_VALIDITY_MINUTES * 60 * 1000;

    return cacheAge < maxAge && this.existingRecordsCache.size > 0;
  }

  private async loadExistingRecordsCache(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getInvoiceHeaders();
      let page_token = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      let stringConversions = 0;
      const pageSize = 100;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({
          page_size: pageSize.toString(),
          ...(page_token && { page_token }),
        });

        const startTime = Date.now();

        try {
          const response = await firstValueFrom(
            this.httpService.get<LarkBatchResponse>(`${url}?${params}`, {
              headers,
              timeout: 60000,
            }),
          );

          const loadTime = Date.now() - startTime;

          if (response.data.code === 0) {
            const records = response.data.data?.items || [];

            for (const record of records) {
              // Handle string kiotVietId
              const kiotVietIdRaw =
                record.fields[LARK_INVOICE_FIELDS.KIOTVIET_ID];

              let kiotVietId = 0;

              if (kiotVietIdRaw !== null && kiotVietIdRaw !== undefined) {
                if (typeof kiotVietIdRaw === 'string') {
                  const trimmed = kiotVietIdRaw.trim();
                  if (trimmed !== '') {
                    const parsed = parseInt(trimmed, 10);
                    if (!isNaN(parsed) && parsed > 0) {
                      kiotVietId = parsed;
                      stringConversions++;
                    }
                  }
                } else if (typeof kiotVietIdRaw === 'number') {
                  kiotVietId = Math.floor(kiotVietIdRaw);
                }
              }

              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }

              // Also cache by invoice code
              const invoiceCode =
                record.fields[LARK_INVOICE_FIELDS.PRIMARY_CODE];
              if (invoiceCode) {
                this.invoiceCodeCache.set(
                  String(invoiceCode).trim(),
                  record.record_id,
                );
              }
            }

            totalLoaded += records.length;
            page_token = response.data.data?.page_token || '';

            this.logger.debug(
              `📥 Loaded ${records.length} records in ${loadTime}ms (total: ${totalLoaded}, cached: ${cacheBuilt})`,
            );

            if (totalLoaded % 1000 === 0 || !page_token) {
              this.logger.log(
                `📊 Cache progress: ${cacheBuilt}/${totalLoaded} records processed (${stringConversions} string conversions)`,
              );
            }
          } else {
            throw new Error(
              `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
            );
          }
        } catch (error) {
          if (error.code === 'ECONNABORTED') {
            throw new Error(
              'Request timeout - LarkBase took too long to respond',
            );
          }
          if (error.response?.status === 400) {
            throw new Error(
              'Bad request - check table permissions and field names',
            );
          }
          throw error;
        }
      } while (page_token);

      this.cacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.invoiceCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // CATEGORIZATION
  // ============================================================================

  private categorizeInvoices(invoices: any[]): {
    newInvoices: any[];
    updateInvoices: any[];
  } {
    const newInvoices: any[] = [];
    const updateInvoices: any[] = [];

    for (const invoice of invoices) {
      const kiotVietId = this.safeBigIntToNumber(invoice.kiotVietId);

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && invoice.code) {
        existingRecordId = this.invoiceCodeCache.get(
          String(invoice.code).trim(),
        );
      }

      if (existingRecordId) {
        updateInvoices.push({
          ...invoice,
          larkRecordId: existingRecordId,
        });
      } else {
        newInvoices.push(invoice);
      }
    }

    return { newInvoices, updateInvoices };
  }

  // ============================================================================
  // PROCESS NEW INVOICES
  // ============================================================================

  private async processNewInvoices(invoices: any[]): Promise<void> {
    if (invoices.length === 0) return;

    this.logger.log(`📝 Creating ${invoices.length} new invoices...`);

    const batches = this.chunkArray(invoices, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `📊 Batch ${i + 1}/${batches.length}: Processing ${batch.length} invoices`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateInvoices(batch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `📊 Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  // ============================================================================
  // PROCESS UPDATES
  // ============================================================================

  private async processUpdateInvoices(invoices: any[]): Promise<void> {
    if (invoices.length === 0) return;

    this.logger.log(`📝 Updating ${invoices.length} existing invoices...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < invoices.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = invoices.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (invoice) => {
          try {
            const updated = await this.updateSingleInvoice(invoice);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([invoice], 'SYNCED');
            } else {
              createFallbacks.push(invoice);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${invoice.code}: ${error.message}`,
            );
            createFallbacks.push(invoice);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < invoices.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `📝 Creating ${createFallbacks.length} invoices that failed update...`,
      );
      await this.processNewInvoices(createFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  // ============================================================================
  // BATCH CREATE
  // ============================================================================

  private async batchCreateInvoices(invoices: any[]): Promise<BatchResult> {
    const records = invoices.map((invoice) => ({
      fields: this.mapInvoiceToLarkBase(invoice),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getInvoiceHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          const successCount = createdRecords.length;
          const successRecords = invoices.slice(0, successCount);
          const failedRecords = invoices.slice(successCount);

          // Update cache
          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const invoice = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(invoice.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (invoice.code) {
              this.invoiceCodeCache.set(
                String(invoice.code).trim(),
                createdRecord.record_id,
              );
            }
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: invoices };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: invoices };
      }
    }

    return { successRecords: [], failedRecords: invoices };
  }

  // ============================================================================
  // UPDATE SINGLE INVOICE
  // ============================================================================

  private async updateSingleInvoice(invoice: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getInvoiceHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${invoice.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapInvoiceToLarkBase(invoice) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `✅ Updated record ${invoice.larkRecordId} for invoice ${invoice.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${invoice.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getInvoiceHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data.code === 0) {
          const totalRecords = response.data.data?.total || 0;
          this.logger.log(`✅ LarkBase connection successful`);
          this.logger.log(
            `📊 LarkBase table has ${totalRecords} existing records`,
          );
          return;
        }

        throw new Error(`Connection test failed: ${response.data.msg}`);
      } catch (error) {
        if (retryCount < maxRetries) {
          const delay = (retryCount + 1) * 2000;
          this.logger.warn(
            `⚠️ Connection attempt ${retryCount + 1} failed: ${error.message}`,
          );
          this.logger.log(`🔄 Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          this.logger.error(
            '❌ LarkBase connection test failed after all retries',
          );
          throw new Error(`Cannot connect to LarkBase: ${error.message}`);
        }
      }
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'invoice_lark_sync',
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();
      if (lockAge < 30 * 60 * 1000) {
        throw new Error('Another sync is already running');
      }
      this.logger.warn('🔓 Clearing stale lock');
    }

    await this.prismaService.syncControl.upsert({
      where: { name: 'invoice_lark_sync' },
      create: {
        name: 'invoice_lark_sync',
        entities: ['invoice'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey },
      },
    });

    this.logger.debug(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'invoice_lark_sync',
        isRunning: true,
      },
    });

    if (
      lockRecord &&
      lockRecord.progress &&
      typeof lockRecord.progress === 'object' &&
      'lockKey' in lockRecord.progress &&
      lockRecord.progress.lockKey === lockKey
    ) {
      await this.prismaService.syncControl.update({
        where: {
          id: lockRecord.id,
        },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: {},
        },
      });

      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    }
  }

  private async updateDatabaseStatus(
    invoices: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (invoices.length === 0) return;

    const invoiceIds = invoices.map((i) => i.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.invoice.updateMany({
      where: { id: { in: invoiceIds } },
      data: updateData,
    });
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.invoiceCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
      array.slice(i * size, i * size + size),
    );
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

  private mapInvoiceToLarkBase(invoice: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // Required fields
    fields[LARK_INVOICE_FIELDS.KIOTVIET_ID] = this.safeBigIntToNumber(
      invoice.kiotVietId,
    );

    if (invoice.code) {
      fields[LARK_INVOICE_FIELDS.PRIMARY_CODE] = invoice.code;
    }

    if (invoice.orderCode) {
      fields[LARK_INVOICE_FIELDS.ORDER_CODE] = invoice.orderCode || '';
    }

    if (invoice.purchaseDate) {
      fields[LARK_INVOICE_FIELDS.PURCHASE_DATE] =
        invoice.purchaseDate.getTime();
    }

    // Branch mapping
    if (invoice.branchId !== null && invoice.branchId !== undefined) {
      // Get branch name from database
      const branchMapping = {
        1: BRANCH_OPTIONS.CUA_HANG_DIEP_TRA,
        2: BRANCH_OPTIONS.KHO_HA_NOI,
        3: BRANCH_OPTIONS.KHO_SAI_GON,
        4: BRANCH_OPTIONS.VAN_PHONG_HA_NOI,
      };
      fields[LARK_INVOICE_FIELDS.BRANCH] =
        branchMapping[invoice.branchId] || '';
    }

    // Seller name - need to look up from soldById
    if (invoice.soldByName) {
      fields[LARK_INVOICE_FIELDS.SELLER] = invoice.soldByName;
    }

    // Financial fields
    if (invoice.totalPayment !== null && invoice.totalPayment !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_NEED_PAY] = Number(
        invoice.totalPayment || 0,
      );
    }

    if (invoice.totalPayment !== null && invoice.totalPayment !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_PAID] = Number(
        invoice.totalPayment || 0,
      );
    }

    if (invoice.total !== null && invoice.total !== undefined) {
      fields[LARK_INVOICE_FIELDS.TOTAL_AMOUNT] = Number(invoice.total || 0);
    }

    if (invoice.discount !== null && invoice.discount !== undefined) {
      fields[LARK_INVOICE_FIELDS.DISCOUNT] = Number(invoice.discount || 0);
    }

    if (invoice.discountRatio !== null && invoice.discountRatio !== undefined) {
      fields[LARK_INVOICE_FIELDS.DISCOUNT_RATIO] = Number(
        invoice.discountRatio || 0,
      );
    }

    // Status
    if (invoice.status) {
      fields[LARK_INVOICE_FIELDS.STATUS] =
        STATUS_OPTIONS[invoice.status] || STATUS_OPTIONS.COMPLETED;
    }

    // Sale channel - need custom mapping
    if (invoice.saleChannelId) {
      // You might need to map saleChannelId to proper options
      fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.DIRECT;
    }

    // Voucher
    fields[LARK_INVOICE_FIELDS.APPLY_VOUCHER] = invoice.isApplyVoucher
      ? VOUCHER_OPTIONS.YES
      : VOUCHER_OPTIONS.NO;

    // Dates
    if (invoice.createdDate) {
      fields[LARK_INVOICE_FIELDS.CREATED_DATE] = invoice.createdDate.getTime();
    }

    if (invoice.modifiedDate) {
      fields[LARK_INVOICE_FIELDS.MODIFIED_DATE] =
        invoice.modifiedDate.getTime();
    }

    return fields;
  }

  // ============================================================================
  // MONITORING METHODS
  // ============================================================================

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.invoice.count();
    const synced = await this.prismaService.invoice.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.invoice.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.invoice.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    return {
      total,
      synced,
      pending,
      failed,
      progress,
      summary: `${synced}/${total} synced (${progress}%)`,
    };
  }

  async resetFailedInvoices(): Promise<{ resetCount: number }> {
    const result = await this.prismaService.invoice.updateMany({
      where: { larkSyncStatus: 'FAILED' },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncRetries: 0,
      },
    });

    this.logger.log(`🔄 Reset ${result.count} FAILED invoices to PENDING`);
    return { resetCount: result.count };
  }
}
