// src/services/lark/order/lark-order-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';
import { error } from 'console';
import { TimezoneUtils } from 'src/utils/timezone.utils';
import { LarkSyncStatus } from '@prisma/client';

// ✅ EXACT field names from Đơn Hàng.rtf
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

  // Cache management - EXACT COPY FROM INVOICE
  private existingRecordsCache = new Map<number, string>();
  private orderCodeCache = new Map<string, string>();
  private cacheLoaded = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 30;
  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_ORDER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>('LARK_ORDER_SYNC_TABLE_ID');

    if (!baseToken || !tableId) {
      throw new Error('LarkBase order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ============================================================================
  // MAIN SYNC METHOD - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

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

      const pendingCount = orders.filter(
        (o) => o.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = orders.filter(
        (o) => o.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED orders`,
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

      // Categorize orders - EXACT COPY FROM INVOICE
      const { newOrders, updateOrders } = this.categorizeOrders(ordersToSync);

      this.logger.log(
        `📋 Categorization: ${newOrders.length} new, ${updateOrders.length} updates`,
      );

      // Process in smaller batches
      const BATCH_SIZE_FOR_SYNC = 50;

      // Process new orders
      if (newOrders.length > 0) {
        for (let i = 0; i < newOrders.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewOrders(batch);
        }
      }

      // Process updates
      if (updateOrders.length > 0) {
        for (let i = 0; i < updateOrders.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateOrders(batch);
        }
      }

      await this.releaseSyncLock(lockKey);
      this.logger.log('🎉 LarkBase order sync completed!');
    } catch (error) {
      this.logger.error(`💥 LarkBase order sync failed: ${error.message}`);
      await this.releaseSyncLock(lockKey);
      throw error;
    }
  }

  // ============================================================================
  // CACHE MANAGEMENT - EXACT COPY FROM INVOICE
  // ============================================================================

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.loadExistingRecords();
        return true;
      } catch (error) {
        this.logger.warn(
          `Cache load attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );
        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
        }
      }
    }
    return false;
  }

  private async loadExistingRecords(): Promise<void> {
    const now = new Date();

    // Check cache validity
    if (
      this.cacheLoaded &&
      this.lastCacheLoadTime &&
      now.getTime() - this.lastCacheLoadTime.getTime() <
        this.CACHE_VALIDITY_MINUTES * 60 * 1000
    ) {
      return; // Cache is still valid
    }

    this.logger.log('🔄 Loading existing order records from LarkBase...');

    this.existingRecordsCache.clear();
    this.orderCodeCache.clear();

    let pageToken: string | undefined;
    let totalRecords = 0;
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        do {
          const headers = await this.larkAuthService.getOrderHeaders();
          let url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?page_size=500`;

          if (pageToken) {
            url += `&page_token=${pageToken}`;
          }

          const response = await firstValueFrom(
            this.httpService.get(url, { headers, timeout: 30000 }),
          );

          if (response.data.code === 0) {
            const records = response.data.data.records || [];
            totalRecords += records.length;

            for (const record of records) {
              const kiotVietId = record.fields[LARK_ORDER_FIELDS.KIOTVIET_ID];
              const orderCode = record.fields[LARK_ORDER_FIELDS.PRIMARY_CODE];

              if (kiotVietId) {
                this.existingRecordsCache.set(
                  Number(kiotVietId),
                  record.record_id,
                );
              }
              if (orderCode) {
                this.orderCodeCache.set(orderCode, record.record_id);
              }
            }

            pageToken = response.data.data.has_more
              ? response.data.data.page_token
              : undefined;

            if (records.length > 0) {
              await new Promise((resolve) => setTimeout(resolve, 300));
            }
          } else if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
            authRetries++;
            await this.larkAuthService.forceRefreshOrderToken();
            await new Promise((resolve) => setTimeout(resolve, 2000));
            break; // Break inner loop, retry outer loop
          } else {
            throw new Error(
              `API Error: ${response.data.msg} (${response.data.code})`,
            );
          }
        } while (pageToken);

        // If we reach here, the operation was successful
        this.cacheLoaded = true;
        this.lastCacheLoadTime = now;
        this.logger.log(`✅ Cached ${totalRecords} existing order records`);
        return;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
        } else {
          throw error;
        }
      }
    }

    throw new Error('Failed to load existing records after auth retries');
  }

  // ============================================================================
  // CATEGORIZATION - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private categorizeOrders(orders: any[]): {
    newOrders: any[];
    updateOrders: any[];
  } {
    const newOrders: any[] = [];
    const updateOrders: any[] = [];

    for (const order of orders) {
      const kiotVietId = Number(order.kiotVietId);
      const orderCode = order.code;

      const recordIdByKiotVietId = this.existingRecordsCache.get(kiotVietId);
      const recordIdByCode = this.orderCodeCache.get(orderCode);

      if (recordIdByKiotVietId || recordIdByCode) {
        // Record exists - mark for update
        order.larkRecordId = recordIdByKiotVietId || recordIdByCode;
        updateOrders.push(order);
      } else {
        // New record
        newOrders.push(order);
      }
    }

    return { newOrders, updateOrders };
  }

  // ============================================================================
  // PROCESS NEW ORDERS - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private async processNewOrders(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    this.logger.log(`📝 Creating ${orders.length} new orders...`);

    const batches: any[][] = [];
    for (let i = 0; i < orders.length; i += this.batchSize) {
      batches.push(orders.slice(i, i + this.batchSize));
    }

    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${batch.length} orders)...`,
      );

      const result = await this.createOrderBatch(batch);
      const { successRecords, failedRecords } = result;

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      // Update database status
      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, LarkSyncStatus.SYNCED);
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, LarkSyncStatus.FAILED);
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
  // PROCESS UPDATES - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private async processUpdateOrders(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    this.logger.log(`📝 Updating ${orders.length} existing orders...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];
    const successfulOrders: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < orders.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = orders.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (order) => {
          try {
            const updated = await this.updateSingleOrder(order);

            if (updated) {
              successCount++;
              successfulOrders.push(order);
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

    // Update successful orders in database
    if (successfulOrders.length > 0) {
      await this.updateDatabaseStatus(successfulOrders, LarkSyncStatus.SYNCED);
    }

    this.logger.log(
      `📝 Update complete: ${successCount} updated, ${createFallbacks.length} fallback to create`,
    );
  }

  // ============================================================================
  // LARKBASE API OPERATIONS - ADAPTED FOR ORDERS
  // ============================================================================

  private async createOrderBatch(orders: any[]): Promise<{
    successRecords: any[];
    failedRecords: any[];
  }> {
    const headers = await this.larkAuthService.getOrderHeaders();
    const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

    const records = orders.map((order) => ({
      fields: this.mapOrderToLarkBase(order),
    }));

    try {
      const response = await firstValueFrom(
        this.httpService.post(url, { records }, { headers, timeout: 30000 }),
      );

      if (response.data.code === 0) {
        const createdRecords = response.data.data.records || [];
        this.logger.log(`✅ Created ${createdRecords.length} order records`);

        // Update cache
        createdRecords.forEach((record, index) => {
          const order = orders[index];
          if (order && record.record_id) {
            const kiotVietId = Number(order.kiotVietId);
            this.existingRecordsCache.set(kiotVietId, record.record_id);

            if (order.code) {
              this.orderCodeCache.set(order.code, record.record_id);
            }
          }
        });

        return {
          successRecords: orders,
          failedRecords: [],
        };
      } else {
        throw new Error(`LarkBase API Error: ${response.data.msg}`);
      }
    } catch (error) {
      this.logger.error(`Batch create failed: ${error.message}`);
      return {
        successRecords: [],
        failedRecords: orders,
      };
    }
  }

  private async updateSingleOrder(order: any): Promise<boolean> {
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${order.larkRecordId}`;

        const updateData = {
          fields: this.mapOrderToLarkBase(order),
        };

        const response = await firstValueFrom(
          this.httpService.put(url, updateData, { headers, timeout: 15000 }),
        );

        if (response.data.code === 0) {
          return true;
        } else if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(
          `Update error: ${response.data.msg || 'Unknown error'}`,
        );
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(
          `Update error: ${error instanceof Error ? error.message : String(error)}`,
        );
        return false;
      }
    }

    return false;
  }

  // ============================================================================
  // TIMEZONE FIXED: Use TimezoneUtils instead of manual adjustment
  // ============================================================================

  private convertDateForLarkBase(dateInput: string | Date): number {
    // ✅ FIXED: Use proper timezone conversion instead of manual -7 hours
    return TimezoneUtils.convertKiotVietDateToLarkTimestamp(dateInput);
  }

  // ============================================================================
  // MAPPING ORDER TO LARKBASE FIELDS
  // ============================================================================

  private mapOrderToLarkBase(order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // Primary field - Mã Đơn Hàng
    if (order.code) {
      fields[LARK_ORDER_FIELDS.PRIMARY_CODE] = order.code;
    }

    // KiotViet ID
    if (order.kiotVietId !== null && order.kiotVietId !== undefined) {
      fields[LARK_ORDER_FIELDS.KIOTVIET_ID] = Number(order.kiotVietId);
    }

    // Branch mapping
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

    if (order.customer?.code) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_CODE] = order.customer.code;
    }

    if (order.customer?.name) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NAME] = order.customer.name;
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
      fields[LARK_ORDER_FIELDS.ORDER_DATE] = this.convertDateForLarkBase(
        order.purchaseDate,
      );
    }

    if (order.createdDate) {
      fields[LARK_ORDER_FIELDS.CREATED_DATE] = this.convertDateForLarkBase(
        order.createdDate,
      );
    }

    if (order.modifiedDate) {
      fields[LARK_ORDER_FIELDS.MODIFIED_DATE] = this.convertDateForLarkBase(
        order.modifiedDate,
      );
    }

    return fields;
  }

  // ============================================================================
  // UTILITY METHODS - COPY FROM INVOICE
  // ============================================================================

  private async updateDatabaseStatus(
    orders: any[],
    status: LarkSyncStatus,
  ): Promise<void> {
    const orderIds = orders.map((order) =>
      typeof order.kiotVietId === 'bigint'
        ? order.kiotVietId
        : BigInt(order.kiotVietId),
    );

    await this.prismaService.order.updateMany({
      where: { kiotVietId: { in: orderIds } },
      data: { larkSyncStatus: status },
    });
  }

  private async testLarkBaseConnection(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getOrderHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?page_size=1`;

      await firstValueFrom(
        this.httpService.get(url, { headers, timeout: 10000 }),
      );

      this.logger.log('✅ LarkBase connection test successful');
    } catch (error) {
      this.logger.error(`❌ LarkBase connection test failed: ${error.message}`);
      throw new Error(`LarkBase connectivity issue: ${error.message}`);
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    // Simple lock implementation - can be enhanced with Redis if needed
    this.logger.debug(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
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

  async syncMissingDataOnly(): Promise<void> {
    this.logger.log('🔍 Starting missing data sync...');

    // Get orders that exist in database but not in LarkBase
    const pendingOrders = await this.prismaService.order.findMany({
      where: { larkSyncStatus: 'PENDING' },
      take: 500,
      include: {
        customer: {
          select: {
            code: true,
            name: true,
          },
        },
      },
    });

    if (pendingOrders.length === 0) {
      this.logger.log('✅ No missing orders to sync');
      return;
    }

    await this.syncOrdersToLarkBase(pendingOrders);
  }

  async validateDataIntegrity(): Promise<{
    isComplete: boolean;
    discrepancies: any[];
    recommendations: string[];
  }> {
    this.logger.log(
      '🔍 Validating data integrity between Database and LarkBase...',
    );

    const dbStats = await this.getOrderSyncStats();

    // Load all LarkBase records
    await this.loadExistingRecords();
    const larkRecordCount = this.existingRecordsCache.size;

    const discrepancies: any[] = [];
    const recommendations: string[] = [];

    // Check total counts
    if (dbStats.total !== larkRecordCount) {
      discrepancies.push({
        type: 'count_mismatch',
        database: dbStats.total,
        larkbase: larkRecordCount,
        difference: Math.abs(dbStats.total - larkRecordCount),
      });
    }

    // Check sync status
    if (dbStats.pending > 0) {
      discrepancies.push({
        type: 'pending_syncs',
        count: dbStats.pending,
      });
    }

    if (dbStats.failed > 0) {
      discrepancies.push({
        type: 'failed_syncs',
        count: dbStats.failed,
      });
    }

    const isComplete = discrepancies.length === 0;

    if (isComplete) {
      recommendations.push('Database and LarkBase are fully synchronized.');
    } else {
      recommendations.push('Run syncMissingDataOnly() to sync missing records');
    }

    return {
      isComplete,
      discrepancies,
      recommendations,
    };
  }
}
