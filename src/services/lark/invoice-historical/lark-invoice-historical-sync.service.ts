import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_INVOICE_FIELDS = {
  PRIMARY_CODE: 'Mã Hoá Đơn',
  ORDER_CODE: 'Mã Đơn Hàng',
  KIOTVIET_ID: 'kiotVietId',
  BRANCH: 'Branch',
  CUSTOMER_NAME: 'Tên Khách Hàng',
  CUSTOMER_CODE: 'Mã Khách Hàng',
  SELLER: 'Người Bán',
  CUSTOMER_NEED_PAY: 'Khách Cần Trả',
  CUSTOMER_PAID: 'Khách Đã Trả',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá',
  STATUS: 'Tình Trạng',
  COMMENT: 'Ghi Chú',
  APPLY_COD: 'Sử Dụng COD',
  SALE_CHANNEL: 'Kênh Bán',
  APPLY_VOUCHER: 'Áp Mã Voucher',
  CREATED_DATE: 'Ngày Tạo',
  PURCHASE_DATE: 'Ngày Mua',
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

const SALE_NAME = {
  LE_ANH_TUAN: 'Lê Anh Tuấn',
  NGUYEN_THI_PHUONG: 'Nguyễn Thị Phương',
  LINH_THUY_DUONG: 'Linh Thuỳ Dương',
  VU_HUYEN_TRANG: 'Vũ Huyền Trang',
  NGUYEN_THI_THUONG: 'Nguyễn Thị Thương',
  NGUYEN_THI_NGAN: 'Nguyễn Thị Ngân',
  NGUYEN_HUYEN_TRANG: 'Nguyễn Huyền Trang',
  MAI_THI_VAN_ANH: 'Mai Thị Vân Anh',
  BANG_ANH_VU: 'Bàng Anh Vũ',
  PHI_THI_PHUONG_THANH: 'Phí Thị Phương Thanh',
  LE_THI_HONG_LIEN: 'Lê Thị Hồng Liên',
  TRAN_XUAN_PHUONG: 'Trần Xuân Phương',
  DINH_THI_LY_LY: 'Đinh Thị Ly Ly',
  ADMIN: 'Admin',
  LE_XUAN_TUNG: 'Lê Xuân Tùng',
  TA_THI_TRANG: 'Tạ Thị Trang',
  LINH_THU_TRANG: 'Linh Thu Trang',
  LY_THI_HONG_DAO: 'Lý Thị Hồng Đào',
  NGUYEN_HUU_TOAN: 'Nguyễn Hữu Toàn',
  LE_BICH_NGOC: 'Lê Bích Ngọc',
  NGUYEN_THI_LOAN: 'Nguyễn Thị Loan',
  NGUYEN_VIET_NAM: 'Nguyễn Viết Nam',
  CUA_HANG_DIEP_TRA_ANH_TUAN: 'Cửa Hàng Diệp Trà Anh Tuấn',
  DO_THI_THUONG: 'Đỗ Thị Thương',
  NGUYEN_THI_BICH_NGOC: 'Nguyễn Thị Bích Ngọc',
  LE_BAO_NGAN: 'Lê Bảo Ngân',
  HUYNH_MAN_NHI: 'Huỳnh Mẫn Nhi',
  NGO_TRANG_NHUNG: 'Ngô Trang Nhung',
};

const SALE_CHANNEL_OPTIONS = {
  DIRECT: 'Bán Trực Tiếp',
  LERMAO_SANH_AN: 'LerMao - Sành Ăn Như Gấu',
  DIEP_TRA_PHA_CHE: 'Diệp Trà - Nguyên Liệu Pha Chế',
  FACEBOOK: 'Facebook',
  INSTAGRAM: 'Instagram',
  DIEPTRA: 'DiepTra',
  DIEPTRA_OFFICIAL: 'DiepTraOfficial',
  TIKTOK_LIVE: 'Tiktok Live',
  DIEPTRA_ROYAL: 'DIỆP TRÀ - ROYALTEA',
  DIEPTRA_TONGKHO_NGUYENLIEU: 'DIỆP TRÀ  Tổng Kho Nguyên Liệu',
  DIEPTRA_TONGKHO_NGUYENLIEU_2: 'DIỆP TRÀ  Tổng Kho Nguyên Liệu',
  DIEPTRA_CHINHANH_MIENNAM: 'Diệp Trà Chi Nhánh Miền Nam',
  DIEPTRA_CHINHANH_MIENNAM_2: 'Diệp Trà Chi Nhánh Miền Nam',
  DIEPTRA_SAIGON: 'DIỆP TRÀ SÀI GÒN',
  MAOMAO_THICH_TRASUA: 'Mao Mao thích Tà Xữa',
  SHOPEE: 'Shopee',
  DIEPTRA_ROYAL_2: 'DIỆP TRÀ - ROYALTEA',
  DIEPTRA_SAIGON_2: 'DIỆP TRÀ SÀI GÒN',
  WEBSITE: 'Website',
  OTHER: 'Khác',
};

const VOUCHER_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
};

const COD_APPLY = {
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
export class LarkInvoiceHistoricalSyncService {
  private readonly logger = new Logger(LarkInvoiceHistoricalSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<number, string> = new Map();
  private invoiceCodeCache: Map<string, string> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_INVOICE_HISTORICAL_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_INVOICE_HISTORICAL_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase invoice configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    const lockKey = `lark_invoice_historical_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${invoices.length} invoices...`,
      );

      const invoicesToSync = invoices.filter(
        (i) => i.larkSyncStatus === 'PENDING' || i.larkSyncStatus === 'FAILED',
      );

      if (invoicesToSync.length === 0) {
        this.logger.log('No invoices need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (invoicesToSync.length < 5) {
        this.logger.log(
          `Small sync (${invoicesToSync.length} invoices) - using lightweight mode`,
        );
        await this.syncWithoutCache(invoicesToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = invoices.filter(
        (i) => i.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = invoices.filter(
        (i) => i.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingCount} PENDING + ${failedCount} FAILED invoices`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(invoicesToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const { newInvoices, updateInvoices } =
        await this.categorizeInvoices(invoicesToSync);

      this.logger.log(
        `Categorization: ${newInvoices.length} new, ${updateInvoices.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newInvoices.length > 0) {
        for (let i = 0; i < newInvoices.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newInvoices.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new invoices batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newInvoices.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewInvoices(batch);
        }
      }

      if (updateInvoices.length > 0) {
        for (let i = 0; i < updateInvoices.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateInvoices.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update invoices batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateInvoices.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateInvoices(batch);
        }
      }

      this.logger.log('LarkBase invoice sync completed successfully');
    } catch (error) {
      this.logger.error(`LarkBase invoice sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (this.isCacheValid() && this.existingRecordsCache.size > 5000) {
          this.logger.log(
            `Large cache available (${this.existingRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastCacheLoadTime) {
          const cacheAgeMinutes =
            (Date.now() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
          if (cacheAgeMinutes < 45 && this.existingRecordsCache.size > 500) {
            this.logger.log(
              `Recent cache (${cacheAgeMinutes.toFixed(1)}min old, ${this.existingRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearCache();
        await this.loadExistingRecordsCache();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.error(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = attempt * 1500;
          this.logger.log(`Waiting ${delay / 1000}s before retry...`);
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
    const maxAge = this.CACHE_VALIDITY_MINUTES * 100 * 1000;

    return cacheAge < maxAge && this.existingRecordsCache.size > 0;
  }

  private async loadExistingRecordsCache(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getInvoiceHeaders();
      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheBuilt = 0;
      const pageSize = 500;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params = new URLSearchParams({
          page_size: String(pageSize),
        });

        if (pageToken) {
          params.append('page_token', pageToken);
        }

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 90000,
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          for (const record of records) {
            const kiotVietIdField =
              record.fields[LARK_INVOICE_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const invoiceCodeField =
              record.fields[LARK_INVOICE_FIELDS.PRIMARY_CODE];
            if (invoiceCodeField) {
              this.invoiceCodeCache.set(
                String(invoiceCodeField).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token;

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheBuilt}/${totalLoaded} records (${loadTime}ms/page)`,
            );
          }
        } else {
          throw new Error(
            `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
          );
        }
      } while (pageToken);

      this.cacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `Invoice cache loaded: ${this.existingRecordsCache.size} by ID, ${this.invoiceCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Invoice cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeInvoices(invoices: any[]): Promise<any> {
    const newInvoices: any[] = [];
    const updateInvoices: any[] = [];

    for (const invoice of invoices) {
      const kiotVietId = invoice.kiotVietId
        ? typeof invoice.kiotVietId === 'bigint'
          ? Number(invoice.kiotVietId)
          : Number(invoice.kiotVietId)
        : 0;

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Invoices ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && invoice.code) {
        existingRecordId = this.invoiceCodeCache.get(
          String(invoice.code).trim(),
        );
      }

      if (existingRecordId) {
        updateInvoices.push({ ...invoice, larkRecordId: existingRecordId });
      } else {
        newInvoices.push(invoice);
      }
    }

    return { newInvoices, updateInvoices };
  }

  private async syncWithoutCache(invoices: any[]): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingInvoices = await this.prismaService.invoice.findMany({
      where: {
        kiotVietId: { in: invoices.map((i) => i.kiotVietId) },
      },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingInvoices.forEach((i) => {
      if (i.larkRecordId) {
        quickCache.set(Number(i.kiotVietId), i.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newInvoices, updateInvoices } =
        await this.categorizeInvoices(invoices);

      if (newInvoices.length > 0) {
        await this.processNewInvoices(newInvoices);
      }

      if (updateInvoices.length > 0) {
        await this.processUpdateInvoices(updateInvoices);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewInvoices(invoices: any[]): Promise<void> {
    if (invoices.length === 0) return;

    this.logger.log(`Creating ${invoices.length} new invoices...`);

    const batches = this.chunkArray(invoices, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const invoice of batch) {
        const kiotVietId = this.safeBigIntToNumber(invoice.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(invoice);
        } else {
          this.logger.warn(
            `Skipping duplicate invoice ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all invoices already exist`);
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} invoices)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateInvoices(verifiedBatch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');

        successRecords.forEach((record) => {
          const kiotVietId = this.safeBigIntToNumber(record.kiotVietId);
          this.pendingCreation.delete(kiotVietId);
        });
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );

      // if (i < batches.length - 1) {
      //   await new Promise((resolve) => setTimeout(resolve, 500));
      // }
    }

    this.logger.log(
      `Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processUpdateInvoices(invoices: any[]): Promise<void> {
    if (invoices.length === 0) return;

    this.logger.log(`Updating ${invoices.length} existing invoices...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 20;

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

      // if (i + UPDATE_CHUNK_SIZE < invoices.length) {
      //   await new Promise((resolve) => setTimeout(resolve, 300));
      // }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `Creating ${createFallbacks.length} invoices that failed update...`,
      );
      await this.processNewInvoices(createFallbacks);
    }

    this.logger.log(
      `Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

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

            if (createdRecord.record_id && invoice.id) {
              await this.prismaService.invoice.update({
                where: { id: invoice.id },
                data: {
                  larkRecordId: createdRecord.record_id,
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                  larkSyncRetries: 0,
                },
              });
            }
          }

          successRecords.forEach((invoice) => {
            const kiotVietId = this.safeBigIntToNumber(invoice.kiotVietId);
            this.pendingCreation.delete(kiotVietId);
          });

          failedRecords.forEach((invoice) => {
            const kiotVietId = this.safeBigIntToNumber(invoice.kiotVietId);
            this.pendingCreation.delete(kiotVietId);
          });

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: invoices };
      } catch (error) {
        this.logger.error('Batch create error details:', {
          status: error.response?.status,
          statusText: error.response?.statusText,
          data: error.response?.data,
          config: {
            url: error.config?.url,
            method: error.config?.method,
            data: JSON.parse(error.config?.data || '{}'),
          },
        });

        if (records && records.length > 0) {
          this.logger.error(
            'Sample record being sent:',
            JSON.stringify(records[0], null, 2),
          );
        }

        return { successRecords: [], failedRecords: invoices };
      }
    }

    return { successRecords: [], failedRecords: invoices };
  }

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
            `Updated record ${invoice.larkRecordId} for invoice ${invoice.code}`,
          );
          await this.prismaService.invoice.update({
            where: { id: invoice.id },
            data: {
              larkRecordId: invoice.larkRecordId,
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshInvoiceToken();
          await new Promise((resolve) => setTimeout(resolve, 500));
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

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

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
          const delay = (retryCount + 1) * 500;
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
    const syncName = 'invoice_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: syncName,
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();

      if (lockAge < 10 * 60 * 1000) {
        const isProcessActive = await this.isLockProcessActive(existingLock);

        if (isProcessActive) {
          throw new Error('Another sync is already running');
        } else {
          this.logger.warn(
            `🔓 Clearing inactive lock (age: ${Math.round(lockAge / 1000)}s)`,
          );
          await this.forceReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['invoice'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
    });

    this.logger.debug(
      `🔒 Acquired sync lock: ${lockKey} (PID: ${process.pid})`,
    );
  }

  private async isLockProcessActive(lockRecord: any): Promise<boolean> {
    try {
      if (!lockRecord.progress?.processId) {
        return false;
      }

      const currentHostname = require('os').hostname();
      if (lockRecord.progress.hostname !== currentHostname) {
        return false;
      }

      const lockAge = Date.now() - lockRecord.startedAt.getTime();
      if (lockAge > 5 * 60 * 1000) {
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Could not verify lock process: ${error.message}`);
      return false;
    }
  }

  private async waitForLockAvailability(
    syncName: string,
    maxWaitMs: number = 30000,
  ): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const existingLock = await this.prismaService.syncControl.findFirst({
        where: { name: syncName, isRunning: true },
      });

      if (!existingLock) {
        return; // Lock is available
      }

      this.logger.debug(
        `⏳ Waiting for lock release... (${Math.round((Date.now() - startTime) / 1000)}s)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    throw new Error(`Lock wait timeout after ${maxWaitMs / 1000}s`);
  }

  private async forceReleaseLock(syncName: string): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: syncName },
      data: {
        isRunning: false,
        status: 'force_released',
        error: 'Lock force released due to inactivity',
        completedAt: new Date(),
        progress: {},
      },
    });
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
      fields[LARK_INVOICE_FIELDS.PURCHASE_DATE] = new Date(
        invoice.purchaseDate,
      ).getTime();
    }

    if (invoice.branchId !== null && invoice.branchId !== undefined) {
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
    if (invoice.soldById !== null && invoice.soldById !== undefined) {
      const sellerMapping: Record<number, string> = {
        1015579: SALE_NAME.ADMIN,
        1031177: SALE_NAME.DINH_THI_LY_LY,
        1015592: SALE_NAME.TRAN_XUAN_PHUONG,
        1015596: SALE_NAME.LE_THI_HONG_LIEN,
        1015604: SALE_NAME.PHI_THI_PHUONG_THANH,
        1015610: SALE_NAME.LE_XUAN_TUNG,
        1015613: SALE_NAME.TA_THI_TRANG,
        1015698: SALE_NAME.BANG_ANH_VU,
        1015722: SALE_NAME.MAI_THI_VAN_ANH,
        1015729: SALE_NAME.LINH_THU_TRANG,
        1015746: SALE_NAME.LY_THI_HONG_DAO,
        1015761: SALE_NAME.NGUYEN_HUYEN_TRANG,
        1015764: SALE_NAME.NGUYEN_THI_NGAN,
        1015777: SALE_NAME.NGUYEN_THI_THUONG,
        1015781: SALE_NAME.VU_HUYEN_TRANG,
        1015788: SALE_NAME.LINH_THUY_DUONG,
        1016818: SALE_NAME.NGUYEN_THI_PHUONG,
        383855: SALE_NAME.NGUYEN_HUU_TOAN,
        1032906: SALE_NAME.LE_BICH_NGOC,
        1032972: SALE_NAME.NGUYEN_THI_LOAN,
        1034030: SALE_NAME.NGUYEN_VIET_NAM,
        1030913: SALE_NAME.CUA_HANG_DIEP_TRA_ANH_TUAN,
        1034176: SALE_NAME.DO_THI_THUONG,
        1034250: SALE_NAME.NGUYEN_THI_BICH_NGOC,
        1034266: SALE_NAME.LE_BAO_NGAN,
        1033767: SALE_NAME.HUYNH_MAN_NHI,
        1042325: SALE_NAME.NGO_TRANG_NHUNG,
      };

      fields[LARK_INVOICE_FIELDS.SELLER] =
        sellerMapping[invoice.soldById] || '';
    }

    if (invoice.customerCode !== null && invoice.customerCode !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_CODE] = invoice.customerCode || '';
    }

    if (invoice.customerName !== null && invoice.customerName !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_NAME] = invoice.customerName || '';
    }

    // Financial fields
    if (invoice.total !== null && invoice.total !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_NEED_PAY] = Number(
        invoice.total || 0,
      );
    }

    if (invoice.totalPayment !== null && invoice.totalPayment !== undefined) {
      fields[LARK_INVOICE_FIELDS.CUSTOMER_PAID] = Number(
        invoice.totalPayment || 0,
      );
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
      if (invoice.status === 1) {
        fields[LARK_INVOICE_FIELDS.STATUS] = STATUS_OPTIONS.COMPLETED;
      } else if (invoice.status === 2) {
        fields[LARK_INVOICE_FIELDS.STATUS] = STATUS_OPTIONS.CANCELLED;
      } else if (invoice.status === 3) {
        fields[LARK_INVOICE_FIELDS.STATUS] = STATUS_OPTIONS.PROCESSING;
      } else if (invoice.status === 4) {
        fields[LARK_INVOICE_FIELDS.STATUS] = STATUS_OPTIONS.DELIVERY_FAILED;
      }
    }

    if (invoice.saleChannelId) {
      if (invoice.saleChannelId === 1) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.DIRECT;
      }
      if (invoice.saleChannelId === 2) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.LERMAO_SANH_AN;
      }
      if (invoice.saleChannelId === 3) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEP_TRA_PHA_CHE;
      }
      if (invoice.saleChannelId === 4) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.FACEBOOK;
      }
      if (invoice.saleChannelId === 5) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.INSTAGRAM;
      }
      if (invoice.saleChannelId === 6) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.DIEPTRA;
      }
      if (invoice.saleChannelId === 7) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_OFFICIAL;
      }
      if (invoice.saleChannelId === 8) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.TIKTOK_LIVE;
      }
      if (invoice.saleChannelId === 9) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_ROYAL;
      }
      if (invoice.saleChannelId === 10) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_TONGKHO_NGUYENLIEU;
      }
      if (invoice.saleChannelId === 11) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_TONGKHO_NGUYENLIEU_2;
      }
      if (invoice.saleChannelId === 12) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_CHINHANH_MIENNAM;
      }
      if (invoice.saleChannelId === 13) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_CHINHANH_MIENNAM_2;
      }
      if (invoice.saleChannelId === 14) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_SAIGON;
      }
      if (invoice.saleChannelId === 15) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.MAOMAO_THICH_TRASUA;
      }
      if (invoice.saleChannelId === 16) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.SHOPEE;
      }
      if (invoice.saleChannelId === 17) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_ROYAL_2;
      }
      if (invoice.saleChannelId === 18) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] =
          SALE_CHANNEL_OPTIONS.DIEPTRA_SAIGON_2;
      }
      if (invoice.saleChannelId === 19) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.WEBSITE;
      }
      if (invoice.saleChannelId === 20) {
        fields[LARK_INVOICE_FIELDS.SALE_CHANNEL] = SALE_CHANNEL_OPTIONS.OTHER;
      }
    }

    fields[LARK_INVOICE_FIELDS.APPLY_VOUCHER] = invoice.isApplyVoucher
      ? VOUCHER_OPTIONS.YES
      : VOUCHER_OPTIONS.NO;

    fields[LARK_INVOICE_FIELDS.APPLY_COD] = invoice.usingCod
      ? COD_APPLY.YES
      : COD_APPLY.NO;

    if (invoice.description) {
      fields[LARK_INVOICE_FIELDS.COMMENT] = invoice.description || '';
    }

    if (invoice.createdDate) {
      fields[LARK_INVOICE_FIELDS.CREATED_DATE] = new Date(
        invoice.createdDate,
      ).getTime();
    }

    return fields;
  }
}
