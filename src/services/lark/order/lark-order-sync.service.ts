import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_ORDER_FIELDS = {
  PRIMARY_CODE: 'Mã Đơn Hàng',
  KIOTVIET_ID: 'kiotVietId',
  BRANCH: 'Chi Nhánh',
  CUSTOMER_NAME: 'Tên Khách Hàng',
  CUSTOMER_CODE: 'Mã Khách Hàng',
  SELLER: 'Người Bán',
  CUSTOMER_NEED_PAY: 'Khách Cần Trả',
  CUSTOMER_PAID: 'Khách Đã Trả',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá (%)',
  STATUS: 'Tình Trạng',
  COMMENT: 'Ghi Chú',
  ORDER_DATE: 'Ngày Đặt Hàng',
  CREATED_DATE: 'Thời Gian Tạo',
  MODIFIED_DATE: 'Thời Gian Cập Nhật',
  SALE_CHANNEL: 'Kênh Bán',
} as const;

const BRANCH_OPTIONS = {
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
};

const STATUS_OPTIONS = {
  PHIEU_TAM: 'Phiếu Tạm',
  DANG_GIAO_HANG: 'Đang Giao Hàng',
  HOAN_THANH: 'Hoàn Thành',
  DA_HUY: 'Đã Hủy',
  DA_XAC_NHAN: 'Đã Xác Nhận',
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
export class LarkOrderSyncService {
  private readonly logger = new Logger(LarkOrderSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();

  private existingRecordsCache = new Map<number, string>();
  private orderCodeCache = new Map<string, string>();
  private cacheLoaded = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 180;
  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_ORDER_TEST_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_ORDER_TEST_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    const lockKey = `lark_order_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${orders.length} orders...`,
      );

      const ordersToSync = orders.filter(
        (o) => o.larkSyncStatus === 'PENDING' || o.larkSyncStatus === 'FAILED',
      );

      if (ordersToSync.length === 0) {
        this.logger.log('📋 No orders need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (ordersToSync.length < 15) {
        this.logger.log(
          `🏃‍♂️ Small sync (${ordersToSync.length} orders) - using lightweight mode`,
        );
        await this.syncWithoutCache(ordersToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = orders.filter(
        (o) => o.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = orders.filter(
        (o) => o.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED orders`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('⚠️ Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(ordersToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const { newOrders, updateOrders } = this.categorizeOrders(ordersToSync);

      this.logger.log(
        `📋 Categorization: ${newOrders.length} new, ${updateOrders.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newOrders.length > 0) {
        for (let i = 0; i < newOrders.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewOrders(batch);
        }
      }

      if (updateOrders.length > 0) {
        for (let i = 0; i < updateOrders.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateOrders(batch);
        }
      }

      this.logger.log('🎉 LarkBase order sync completed!');
    } catch (error) {
      this.logger.error(`💥 LarkBase order sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(
          `📥 Loading cache (attempt ${attempt}/${maxRetries})...`,
        );

        if (this.isCacheValid() && this.existingRecordsCache.size > 3000) {
          this.logger.log(
            `✅ Large cache available (${this.existingRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastCacheLoadTime) {
          const cacheAgeMinutes =
            (Date.now() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
          if (cacheAgeMinutes < 45 && this.existingRecordsCache.size > 500) {
            this.logger.log(
              `✅ Recent cache (${cacheAgeMinutes.toFixed(1)}min old, ${this.existingRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearCache();
        await this.loadExistingRecords();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `✅ Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`⚠️ Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
          `❌ Cache loading attempt ${attempt} failed: ${error.message}`,
        );
        if (attempt < maxRetries) {
          const delay = attempt * 2000;
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

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getOrderHeaders();
      let pageToken = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      const pageSize = 1000;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params: any = {
          page_size: pageSize,
          ...(pageToken && { page_token: pageToken }),
        };

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(url, {
            headers,
            params,
            timeout: 15000,
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          for (const record of records) {
            const kiotVietIdField =
              record.fields[LARK_ORDER_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const orderCodeField =
              record.fields[LARK_ORDER_FIELDS.PRIMARY_CODE];
            if (orderCodeField) {
              this.orderCodeCache.set(
                String(orderCodeField).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token || '';

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `📊 Cache progress: ${cacheBuilt}/${totalLoaded} records (${loadTime}ms/page)`,
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
        `✅ Order cache loaded: ${this.existingRecordsCache.size} by ID, ${this.orderCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Order cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeOrders(orders: any[]): Promise<any> {
    const newOrders: any[] = [];
    const updateOrders: any[] = [];

    const duplicateDetected = orders.filter((order) => {
      const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
      return this.existingRecordsCache.has(kiotVietId);
    });

    if (duplicateDetected.length > 0) {
      this.logger.warn(
        `🚨 Detected ${duplicateDetected.length} orders already in cache: ${duplicateDetected
          .map((o) => o.kiotVietId)
          .slice(0, 5)
          .join(', ')}`,
      );
    }

    for (const order of orders) {
      const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `⚠️ Order ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && order.code) {
        existingRecordId = this.orderCodeCache.get(String(order.code).trim());
      }

      if (existingRecordId) {
        updateOrders.push({ ...order, larkRecordId: existingRecordId });
      } else {
        this.pendingCreation.add(kiotVietId);
        newOrders.push(order);
      }
    }

    return { newOrders, updateOrders };
  }

  private async syncWithoutCache(orders: any[]): Promise<void> {
    this.logger.log(`🏃‍♂️ Running lightweight sync without full cache...`);

    const existingOrders = await this.prismaService.order.findMany({
      where: {
        kiotVietId: { in: orders.map((o) => o.kiotVietId) },
      },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingOrders.forEach((o) => {
      if (o.larkRecordId) {
        quickCache.set(Number(o.kiotVietId), o.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newOrders, updateOrders } = this.categorizeOrders(orders);

      if (newOrders.length > 0) {
        await this.processNewOrders(newOrders);
      }

      if (updateOrders.length > 0) {
        await this.processUpdateOrders(updateOrders);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewOrders(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    this.logger.log(`📝 Creating ${orders.length} new orders...`);

    const batches = this.chunkArray(orders, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const order of batch) {
        const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(order);
        } else {
          this.logger.warn(
            `⚠️ Skipping duplicate order ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`✅ Batch ${i + 1} skipped - all orders already exist`);
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} orders)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateOrders(verifiedBatch);

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

  private async processUpdateOrders(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    this.logger.log(`📝 Updating ${orders.length} existing orders...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < orders.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = orders.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (order) => {
          try {
            const updated = await this.updateSingleOrder(order);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([order], 'SYNCED');
            } else {
              createFallbacks.push(order);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${order.code}: ${error.message}`,
            );
            createFallbacks.push(order);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < orders.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    // Process fallbacks as new orders
    if (createFallbacks.length > 0) {
      this.logger.log(
        `🔄 Processing ${createFallbacks.length} update fallbacks as new orders...`,
      );
      await this.processNewOrders(createFallbacks);
    }

    this.logger.log(
      `📝 Update complete: ${successCount} updated, ${createFallbacks.length} fallback to create`,
    );
  }

  private async batchCreateOrders(orders: any[]): Promise<BatchResult> {
    const records = orders.map((order) => ({
      fields: this.mapOrderToLarkBase(order),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
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
          const successRecords = orders.slice(0, successCount);
          const failedRecords = orders.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const order = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            successRecords.forEach((order) => {
              const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            failedRecords.forEach((order) => {
              const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            if (order.code) {
              this.orderCodeCache.set(
                String(order.code).trim(),
                createdRecord.record_id,
              );
            }
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: orders };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: orders };
      }
    }

    return { successRecords: [], failedRecords: orders };
  }

  private async updateSingleOrder(order: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${order.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapOrderToLarkBase(order) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `✅ Updated record ${order.larkRecordId} for order ${order.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${order.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  async analyzeMissingData(): Promise<{
    missing: any[];
    exists: any[];
    duplicates: any[];
    summary: any;
  }> {
    this.logger.log(
      '🔍 Analyzing missing data between Database and LarkBase...',
    );

    // Load cache để có data LarkBase
    await this.loadExistingRecordsWithRetry();

    // Get all database records
    const dbOrders = await this.prismaService.order.findMany({
      select: {
        id: true,
        kiotVietId: true,
        code: true,
        larkSyncStatus: true,
        larkSyncedAt: true,
      },
      orderBy: { kiotVietId: 'asc' },
    });

    const missing: any[] = [];
    const exists: any[] = [];
    const duplicates: Map<number, number> = new Map();

    // Analyze each database record
    for (const order of dbOrders) {
      const kiotVietId = this.safeBigIntToNumber(order.kiotVietId);
      const existsInLark = this.existingRecordsCache.has(kiotVietId);

      if (existsInLark) {
        exists.push({
          dbId: order.id,
          kiotVietId,
          code: order.code,
          larkRecordId: this.existingRecordsCache.get(kiotVietId),
          syncStatus: order.larkSyncStatus,
        });

        // Count occurrences for duplicate detection
        duplicates.set(kiotVietId, (duplicates.get(kiotVietId) || 0) + 1);
      } else {
        missing.push({
          dbId: order.id,
          kiotVietId,
          code: order.code,
          syncStatus: order.larkSyncStatus,
          lastSyncAttempt: order.larkSyncedAt,
        });
      }
    }

    // Find actual duplicates
    const duplicatesList: any[] = [];
    for (const [kiotVietId, count] of duplicates.entries()) {
      if (count > 1) {
        duplicatesList.push({ kiotVietId, count });
      }
    }

    const summary = {
      totalDatabase: dbOrders.length,
      totalLarkBase: this.existingRecordsCache.size,
      existsInBoth: exists.length,
      missingInLarkBase: missing.length,
      duplicatesFound: duplicatesList.length,
      syncStatusBreakdown: {
        SYNCED: missing.filter((m) => m.syncStatus === 'SYNCED').length,
        PENDING: missing.filter((m) => m.syncStatus === 'PENDING').length,
        FAILED: missing.filter((m) => m.syncStatus === 'FAILED').length,
      },
    };

    this.logger.log('📊 Analysis Summary:');
    this.logger.log(`- Total in Database: ${summary.totalDatabase}`);
    this.logger.log(`- Total in LarkBase: ${summary.totalLarkBase}`);
    this.logger.log(`- Exists in both: ${summary.existsInBoth}`);
    this.logger.log(`- Missing in LarkBase: ${summary.missingInLarkBase}`);
    this.logger.log(`- Duplicates found: ${summary.duplicatesFound}`);

    return {
      missing: missing.slice(0, 100), // First 100 for readability
      exists: exists.slice(0, 20), // Sample of existing
      duplicates: duplicatesList,
      summary,
    };
  }

  private mapOrderToLarkBase(order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (order.code) {
      fields[LARK_ORDER_FIELDS.PRIMARY_CODE] = order.code;
    }

    if (order.kiotVietId !== null && order.kiotVietId !== undefined) {
      fields[LARK_ORDER_FIELDS.KIOTVIET_ID] = Number(order.kiotVietId);
    }

    if (order.branchId !== null && order.branchId !== undefined) {
      const branchMapping = {
        1: BRANCH_OPTIONS.CUA_HANG_DIEP_TRA,
        2: BRANCH_OPTIONS.KHO_HA_NOI,
        3: BRANCH_OPTIONS.KHO_SAI_GON,
        4: BRANCH_OPTIONS.VAN_PHONG_HA_NOI,
      };

      fields[LARK_ORDER_FIELDS.BRANCH] = branchMapping[order.branchId] || '';
    }

    // Seller mapping
    if (order.soldById !== null && order.soldById !== undefined) {
      const sellerMapping = {
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
      };

      fields[LARK_ORDER_FIELDS.SELLER] = sellerMapping[order.soldById] || '';
    }

    if (order.customerCode) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_CODE] = order.customerCode;
    }

    if (order.saleChannelName) {
      fields[LARK_ORDER_FIELDS.SALE_CHANNEL] = order.saleChannelName || '';
    }

    if (order.customerName) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NAME] = order.customerName;
    }

    // Financial fields
    if (order.total !== null && order.total !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NEED_PAY] = Number(order.total || 0);
    }

    if (order.totalPayment !== null && order.totalPayment !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_PAID] = Number(order.totalPayment || 0);
    }

    if (order.discount !== null && order.discount !== undefined) {
      fields[LARK_ORDER_FIELDS.DISCOUNT] = Number(order.discount || 0);
    }

    if (order.discountRatio !== null && order.discountRatio !== undefined) {
      fields[LARK_ORDER_FIELDS.DISCOUNT_RATIO] = Number(
        order.discountRatio || 0,
      );
    }

    // Status mapping
    if (order.status) {
      const statusMapping = {
        1: STATUS_OPTIONS.PHIEU_TAM,
        2: STATUS_OPTIONS.DANG_GIAO_HANG,
        3: STATUS_OPTIONS.HOAN_THANH,
        4: STATUS_OPTIONS.DA_HUY,
        5: STATUS_OPTIONS.DA_XAC_NHAN,
      };

      fields[LARK_ORDER_FIELDS.STATUS] =
        statusMapping[order.status] || STATUS_OPTIONS.PHIEU_TAM;
    }

    // Comment
    if (order.description !== null && order.description !== undefined) {
      fields[LARK_ORDER_FIELDS.COMMENT] = order.description || '';
    }

    // Date fields
    if (order.purchaseDate) {
      fields[LARK_ORDER_FIELDS.ORDER_DATE] = new Date(
        order.purchaseDate,
      ).getTime();
    }

    if (order.createdDate) {
      fields[LARK_ORDER_FIELDS.CREATED_DATE] = new Date(
        order.createdDate,
      ).getTime();
    }

    if (order.modifiedDate !== null && order.modifiedDate !== undefined) {
      fields[LARK_ORDER_FIELDS.MODIFIED_DATE] = new Date(
        order.modifiedDate,
      ).getTime();
    }

    return fields;
  }

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.order.count();
    const synced = await this.prismaService.order.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.order.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.order.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    const canRetryFailed = failed > 0;

    return {
      total,
      synced,
      pending,
      failed,
      progress,
      canRetryFailed,
      summary: `${synced}/${total} synced (${progress}%)`,
    };
  }

  // ============================================================================
  // UTILITY METHODS - COPY FROM INVOICE
  // ============================================================================

  private async updateDatabaseStatus(
    orders: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (orders.length === 0) return;

    const orderIds = orders.map((i) => i.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.order.updateMany({
      where: { id: { in: orderIds } },
      data: updateData,
    });
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.orderCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getOrderHeaders();
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
    const syncName = 'order_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: syncName,
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();

      // 🆕 ENHANCED: More aggressive stale lock cleanup
      if (lockAge < 10 * 60 * 1000) {
        // Reduced from 30min to 10min
        // 🆕 ADDED: Check if the process is actually active
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

    // 🆕 ADDED: Wait for any competing processes
    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['order'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid, // 🆕 ADDED: Track process ID
          hostname: require('os').hostname(), // 🆕 ADDED: Track hostname
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

  // 🆕 ADD these new methods after acquireSyncLock:

  private async isLockProcessActive(lockRecord: any): Promise<boolean> {
    try {
      if (!lockRecord.progress?.processId) {
        return false; // No process ID stored = inactive
      }

      const currentHostname = require('os').hostname();
      if (lockRecord.progress.hostname !== currentHostname) {
        return false; // Different machine = inactive
      }

      // Check if process exists (simple heuristic)
      const lockAge = Date.now() - lockRecord.startedAt.getTime();
      if (lockAge > 5 * 60 * 1000) {
        // 5 minutes without update = likely stuck
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Could not verify lock process: ${error.message}`);
      return false; // Assume inactive if can't verify
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
      await new Promise((resolve) => setTimeout(resolve, 2000)); // Check every 2 seconds
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
        name: 'order_lark_sync',
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

  // ============================================================================
  // ADDITIONAL UTILITY METHODS LIKE INVOICE
  // ============================================================================

  async retryFailedOrderSyncs(): Promise<void> {
    this.logger.log('🔄 Retrying failed order syncs...');

    const failedOrders = await this.prismaService.order.findMany({
      where: {
        larkSyncStatus: 'FAILED',
        larkSyncRetries: { lt: 3 },
      },
      take: 100,
      include: {
        customer: {
          select: {
            code: true,
            name: true,
          },
        },
      },
    });

    if (failedOrders.length === 0) {
      this.logger.log('✅ No failed orders to retry');
      return;
    }

    // Reset to PENDING to trigger sync
    await this.prismaService.order.updateMany({
      where: { id: { in: failedOrders.map((o) => o.id) } },
      data: { larkSyncStatus: 'PENDING' },
    });

    await this.syncOrdersToLarkBase(failedOrders);
  }

  async getOrderSyncStats(): Promise<{
    pending: number;
    synced: number;
    failed: number;
    total: number;
  }> {
    const [pending, synced, failed, total] = await Promise.all([
      this.prismaService.order.count({ where: { larkSyncStatus: 'PENDING' } }),
      this.prismaService.order.count({ where: { larkSyncStatus: 'SYNCED' } }),
      this.prismaService.order.count({ where: { larkSyncStatus: 'FAILED' } }),
      this.prismaService.order.count(),
    ]);

    return { pending, synced, failed, total };
  }

  async syncMissingDataOnly(): Promise<{
    attempted: number;
    success: number;
    failed: number;
    details: any[];
  }> {
    this.logger.log('🔍 Starting missing data sync...');

    // Get orders that exist in database but not in LarkBase
    const analysis = await this.analyzeMissingData();
    const missingOrders = analysis.missing;

    if (missingOrders.length === 0) {
      this.logger.log(
        '✅ No missing data found! Database and LarkBase are in sync.',
      );
      return {
        attempted: 0,
        success: 0,
        failed: 0,
        details: [],
      };
    }

    // Get full order data for missing records
    const missingIds = missingOrders.map((m) => m.dbId);
    const ordersToSync = await this.prismaService.order.findMany({
      where: { id: { in: missingIds } },
    });

    this.logger.log(`📋 Found ${ordersToSync.length} missing order to sync`);

    // Reset their status to PENDING for fresh sync
    await this.prismaService.order.updateMany({
      where: { id: { in: missingIds } },
      data: {
        larkSyncStatus: 'PENDING',
        larkSyncRetries: 0,
      },
    });

    // Sync in small batches
    const BATCH_SIZE = 25;
    let totalSuccess = 0;
    let totalFailed = 0;
    const syncDetails: any[] = [];

    for (let i = 0; i < ordersToSync.length; i += BATCH_SIZE) {
      const batch = ordersToSync.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(ordersToSync.length / BATCH_SIZE);

      this.logger.log(
        `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} orders)`,
      );

      try {
        // Process this batch
        const { successRecords, failedRecords } =
          await this.batchCreateOrders(batch);

        totalSuccess += successRecords.length;
        totalFailed += failedRecords.length;

        // Update database status
        if (successRecords.length > 0) {
          await this.updateDatabaseStatus(successRecords, 'SYNCED');
        }

        if (failedRecords.length > 0) {
          await this.updateDatabaseStatus(failedRecords, 'FAILED');
        }

        syncDetails.push({
          batch: batchNumber,
          success: successRecords.length,
          failed: failedRecords.length,
          failedCodes: failedRecords.map((f) => f.code),
        });

        // Small delay between batches
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        this.logger.error(`❌ Batch ${batchNumber} failed: ${error.message}`);
        totalFailed += batch.length;

        syncDetails.push({
          batch: batchNumber,
          success: 0,
          failed: batch.length,
          error: error.message,
        });
      }
    }

    this.logger.log('🎯 Missing data sync completed:');
    this.logger.log(`- Attempted: ${ordersToSync.length}`);
    this.logger.log(`- Success: ${totalSuccess}`);
    this.logger.log(`- Failed: ${totalFailed}`);

    return {
      attempted: ordersToSync.length,
      success: totalSuccess,
      failed: totalFailed,
      details: syncDetails,
    };
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
}
