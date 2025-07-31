import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_ORDER_SUPPLIER_FIELDS = {
  ORDER_SUPPLIER_CODE: 'Mã Đặt Hàng Nhập',
  kiotVietId: 'kiotVietId',
  ORDER_DATE: 'Ngày Đặt Hàng Nhập',
  BRANCH: 'Branch',
  RETAILER: 'Retailer',
  USER_ID: 'Người Đặt',
  DESCRIPTION: 'Ghi Chú',
  STATUS: 'Tình Trạng',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá',
  PRODUCT_QTY: 'Số Lượng',
  DISCOUNT: 'Giảm Giá',
  CREATED_DATE: 'Ngày Tạo',
  CREATED_BY: 'Người Tạo',
  TOTAL: 'Cần Trả NCC',
  EX_RETURN_SUPPLIER: 'Tiền Nhà Cung Cấp Trả Lại',
  EX_RETURN_THIRD_PARTY: 'Chi Phí Nhập Khác',
  TOTAL_AMOUNT: 'Tổng Tiền Hàng',
  TOTAL_QUANTITY: 'Tổng Số Lượng',
  TOTAL_PRODUCT_TYPE: 'Tổng Số Lượng Mặt Hàng',
  PAID_AMOUNT: 'Tiền Đã Trả NCC',
  PURCHASE_ORDER_CODE: 'Mã Nhập Hàng',
};

const LARK_ORDER_SUPPLIER_DETAIL_FIELDS = {
  kiotVietId: 'kiotVietId',
  ORDER_SUPPLIER_CODE: 'Mã Đặt Hàng Nhập',
  PRODUCT_CODE: 'Mã Sản Phẩm',
  PRODUCT_NAME: 'Tên Sản Phẩm',
  QUANTITY: 'Số Lượng',
  PRICE: 'Đơn Giá',
  DISCOUNT: 'Discount',
  ALLOCATION: 'Phân Bổ',
  DESCRIPTION: 'Mô Tả',
  ORDER_BY_NUMBER: 'Đặt Hàng Theo Số',
  ALLOCATION_SUPPLIERS: 'Phân Bổ Theo Nhà Cung Cấp',
  ALLOCATION_THIRD_PARTY: 'Phân Bổ Từ Bên Thứ 3',
  ORDER_QUANTITY: 'Số Lượng Đặt Hàng',
  SUB_TOTAL: 'Tổng Tiền Đặt Hàng Theo Sản Phẩm',
  CREATED_DATE: 'Ngày Tạo',
};

const BRANCH_NAME = {
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
} as const;

const USER_OPTION = {
  NGUYEN_THI_NGAN: 'Nguyễn Thị Ngân',
};

const STATUS_OPTION = {
  PHIEU_TAM: 'Phiếu Tạm',
  DA_XAC_NHAN: 'Đã Xác Nhận NCC',
  NHAP_MOT_PHAN: 'Nhập Một Phần',
  HOAN_THANH: 'Hoàn Thành',
  DA_HUY: 'Đã Huỷ',
};

const CREATOR = {
  NGUYEN_THI_NGAN: 'Nguyễn Thị Ngân',
};

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

interface BatchDetailResult {
  successDetailRecords: any[];
  failedDetailRecords: any[];
}

@Injectable()
export class LarkOrderSupplierSyncService {
  private readonly logger = new Logger(LarkOrderSupplierSyncService.name);

  private readonly baseToken: string;
  private readonly tableId: string;

  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;

  private readonly batchSize = 100;

  private existingRecordsCache = new Map<number, string>();
  private orderSupplierCodeCache = new Map<string, string>();

  private existingDetailRecordsCache = new Map<number, string>();
  private orderSupplierDetailCodeCache = new Map<string, string>();

  private cacheLoaded = false;
  private cacheDetailLoaded = false;

  private lastCacheLoadTime: Date | null = null;
  private lastDetailCacheLoadTime: Date | null = null;

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
      'LARK_ORDER_SUPPLIER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_ORDER_SUPPLIER_SYNC_TABLE_ID',
    );

    const baseTokenDetail = this.configService.get<string>(
      'LARK_ORDER_SUPPLIER_DETAIL_SYNC_BASE_TOKEN',
    );
    const tableIdDetail = this.configService.get<string>(
      'LARK_ORDER_SUPPLIER_DETAIL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId || !baseTokenDetail || !tableIdDetail) {
      throw new Error('LarkBase order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
    this.baseTokenDetail = baseTokenDetail;
    this.tableIdDetail = tableIdDetail;
  }

  async syncOrderSuppliersToLarkBase(order_suppliers: any[]): Promise<void> {
    const lockKey = `lark_order_supplier_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${order_suppliers.length} order_suppliers...`,
      );

      const orderSuppliersToSync = order_suppliers.filter(
        (o) => o.larkSyncStatus === 'PENDING' || o.larkSyncStatus === 'FAILED',
      );

      if (orderSuppliersToSync.length === 0) {
        this.logger.log('📋 No order_supplier need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = order_suppliers.filter(
        (o) => o.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = order_suppliers.filter(
        (o) => o.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED order_suppliers`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn(
          '⚠️ Cache loading failed - will use alternative duplicate detection',
        );
      }

      const { newOrderSuppliers, updateOrderSuppliers } =
        this.categorizeOrderSuppliers(orderSuppliersToSync);

      this.logger.log(
        `📋 Categorization: ${newOrderSuppliers.length} new, ${updateOrderSuppliers.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 20;

      if (newOrderSuppliers.length > 0) {
        for (
          let i = 0;
          i < newOrderSuppliers.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = newOrderSuppliers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new order_suppliers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newOrderSuppliers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewOrderSuppliers(batch);
        }
      }

      if (updateOrderSuppliers.length > 0) {
        for (
          let i = 0;
          i < updateOrderSuppliers.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = updateOrderSuppliers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update order_suppliers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateOrderSuppliers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateOrderSuppliers(batch);
        }
      }

      await this.releaseSyncLock(lockKey);
      this.logger.log('🎉 LarkBase order_supplier sync completed!');
    } catch (error) {
      this.logger.error(
        `💥 LarkBase order_supplier sync failed: ${error.message}`,
      );
      await this.releaseSyncLock(lockKey);
      throw error;
    }
  }

  async syncOrderSupplierDetailsToLarkBase(): Promise<void> {
    const lockKey = `lark_order_supplier_detail_sync_lock_${Date.now()}`;

    try {
      await this.acquireDetailSyncLock(lockKey);

      this.logger.log('🚀 Starting LarkBase sync for OrderSupplierDetails...');

      const orderSupplierDetailsToSync =
        await this.prismaService.orderSupplierDetail.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      this.logger.log(
        `📋 Found ${orderSupplierDetailsToSync.length} order_supplier_details to sync`,
      );

      if (orderSupplierDetailsToSync.length === 0) {
        this.logger.log('📋 No order_supplier_details need LarkBase sync');
        await this.releaseDetailSyncLock(lockKey);
        return;
      }

      const pendingDetailCount = orderSupplierDetailsToSync.filter(
        (d) => d.larkSyncStatus === 'PENDING',
      ).length;
      const failedDetailCount = orderSupplierDetailsToSync.filter(
        (d) => d.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingDetailCount} PENDING + ${failedDetailCount} FAILED order_supplier_details`,
      );

      await this.testLarkBaseDetailConnection();

      const cacheDetailLoaded = await this.loadExistingDetailRecordsWithRetry();

      if (!cacheDetailLoaded) {
        this.logger.warn(
          '⚠️ OrderSupplierDetail cache loading failed - will use alternative duplicate detection',
        );
      }

      const { newOrderSuppliersDetail, updateOrderSuppliersDetail } =
        this.categorizeOrderSuppliersDetail(orderSupplierDetailsToSync);

      this.logger.log(
        `📋 OrderSupplierDetail Categorization: ${newOrderSuppliersDetail.length} new, ${updateOrderSuppliersDetail.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 50;

      if (newOrderSuppliersDetail.length > 0) {
        for (
          let i = 0;
          i < newOrderSuppliersDetail.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = newOrderSuppliersDetail.slice(
            i,
            i + BATCH_SIZE_FOR_SYNC,
          );
          this.logger.log(
            `Processing new order_supplier_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newOrderSuppliersDetail.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewOrderSuppliersDetail(batch);
        }
      }

      if (updateOrderSuppliersDetail.length > 0) {
        for (
          let i = 0;
          i < updateOrderSuppliersDetail.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = updateOrderSuppliersDetail.slice(
            i,
            i + BATCH_SIZE_FOR_SYNC,
          );
          this.logger.log(
            `Processing update order_supplier_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateOrderSuppliersDetail.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateOrderSuppliersDetail(batch);
        }
      }

      await this.releaseDetailSyncLock(lockKey);
      this.logger.log('🎉 LarkBase OrderSupplierDetail sync completed!');
    } catch (error) {
      this.logger.error(
        `❌ OrderSupplierDetail sync failed: ${error.message}`,
        error.stack,
      );
      await this.releaseDetailSyncLock(lockKey);
      throw error;
    }
  }

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
          const delay = attempt * 3000;
          this.logger.log(`⏳ Waiting ${delay / 1000}s before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }
    return false;
  }

  private async loadExistingDetailRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(
          `📥 Loading cache (attempt ${attempt}/${maxRetries})...`,
        );

        if (this.isDetailCacheValid()) {
          this.logger.log('✅ Using existing valid cache');
          return true;
        }

        this.clearDetailCache();

        await this.loadExistingDetailRecords();

        if (this.existingDetailRecordsCache.size > 0) {
          this.logger.log(
            `✅ Cache loaded successfully: ${this.existingDetailRecordsCache.size} records`,
          );
          this.lastDetailCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`⚠️ Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
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

  private isDetailCacheValid(): boolean {
    if (!this.cacheDetailLoaded || !this.lastDetailCacheLoadTime) {
      return false;
    }

    const cacheDetailAge = Date.now() - this.lastDetailCacheLoadTime.getTime();
    const maxAge = this.CACHE_VALIDITY_MINUTES * 60 * 1000;

    return cacheDetailAge < maxAge && this.existingDetailRecordsCache.size > 0;
  }

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getOrderSupplierHeaders();
      let page_token = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      let stringConversions = 0;
      const pageSize = 50;

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
              const kiotVietIdRaw =
                record.fields[LARK_ORDER_SUPPLIER_FIELDS.kiotVietId];

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

              const orderSupplierCode =
                record.fields[LARK_ORDER_SUPPLIER_FIELDS.ORDER_SUPPLIER_CODE];
              if (orderSupplierCode) {
                this.orderSupplierCodeCache.set(
                  String(orderSupplierCode).trim(),
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
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.orderSupplierCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async loadExistingDetailRecords(): Promise<void> {
    try {
      const headers =
        await this.larkAuthService.getOrderSupplierDetailHeaders();
      let page_token = '';
      let totalDetailLoaded = 0;
      let cacheDetailBuilt = 0;
      let stringConversions = 0;
      const pageSize = 50;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records`;
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
              const kiotVietIdRaw =
                record.fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.kiotVietId];

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
                this.existingDetailRecordsCache.set(
                  kiotVietId,
                  record.record_id,
                );
                cacheDetailBuilt++;
              }

              const orderSupplierDetailCode =
                record.fields[
                  LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ORDER_SUPPLIER_CODE
                ];
              if (orderSupplierDetailCode) {
                this.orderSupplierDetailCodeCache.set(
                  String(orderSupplierDetailCode).trim(),
                  record.record_id,
                );
              }
            }

            totalDetailLoaded += records.length;
            page_token = response.data.data?.page_token || '';

            this.logger.debug(
              `📥 Loaded ${records.length} records in ${loadTime}ms (total: ${totalDetailLoaded}, cached: ${cacheDetailBuilt})`,
            );

            if (totalDetailLoaded % 1000 === 0 || !page_token) {
              this.logger.log(
                `📊 Cache progress: ${cacheDetailBuilt}/${totalDetailLoaded} records processed (${stringConversions} string conversions)`,
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

      this.cacheDetailLoaded = true;

      const successRate =
        totalDetailLoaded > 0
          ? Math.round((cacheDetailBuilt / totalDetailLoaded) * 100)
          : 0;

      this.logger.log(
        `✅ Cache loaded: ${this.existingDetailRecordsCache.size} by ID, ${this.orderSupplierDetailCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private categorizeOrderSuppliers(order_suppliers: any[]): {
    newOrderSuppliers: any[];
    updateOrderSuppliers: any[];
  } {
    const newOrderSuppliers: any[] = [];
    const updateOrderSuppliers: any[] = [];

    for (const order_supplier of order_suppliers) {
      const kiotVietId = this.safeBigIntToNumber(order_supplier.kiotVietId);

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && order_supplier.code) {
        existingRecordId = this.orderSupplierCodeCache.get(
          String(order_supplier.code).trim(),
        );
      }

      if (existingRecordId) {
        updateOrderSuppliers.push({
          ...order_supplier,
          larkRecordId: existingRecordId,
        });
      } else {
        newOrderSuppliers.push(order_supplier);
      }
    }

    return { newOrderSuppliers, updateOrderSuppliers };
  }

  private categorizeOrderSuppliersDetail(order_suppliers_detail: any[]): {
    newOrderSuppliersDetail: any[];
    updateOrderSuppliersDetail: any[];
  } {
    const newOrderSuppliersDetail: any[] = [];
    const updateOrderSuppliersDetail: any[] = [];

    for (const order_supplier_detail of order_suppliers_detail) {
      const kiotVietId = this.safeBigIntToNumber(
        order_supplier_detail.kiotVietId,
      );

      let existingDetailRecordId =
        this.existingDetailRecordsCache.get(kiotVietId);

      if (!existingDetailRecordId && order_supplier_detail.code) {
        existingDetailRecordId = this.orderSupplierDetailCodeCache.get(
          String(order_supplier_detail.code).trim(),
        );
      }

      if (existingDetailRecordId) {
        updateOrderSuppliersDetail.push({
          ...order_supplier_detail,
          larkRecordId: existingDetailRecordId,
        });
      } else {
        newOrderSuppliersDetail.push(order_supplier_detail);
      }
    }

    return { newOrderSuppliersDetail, updateOrderSuppliersDetail };
  }

  private async processNewOrderSuppliers(
    order_suppliers: any[],
  ): Promise<void> {
    if (order_suppliers.length === 0) return;

    this.logger.log(
      `📝 Creating ${order_suppliers.length} new order_suppliers...`,
    );

    const batches = this.chunkArray(order_suppliers, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${batch.length} order_suppliers)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateOrderSuppliers(batch);

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

  private async processNewOrderSuppliersDetail(
    order_suppliers_detail: any[],
  ): Promise<void> {
    if (order_suppliers_detail.length === 0) return;

    this.logger.log(
      `📝 Creating ${order_suppliers_detail.length} new order_suppliers_detail...`,
    );

    const batches = this.chunkArray(order_suppliers_detail, this.batchSize);
    let totalDetailCreated = 0;
    let totalDetailFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${batch.length} order_suppliers_detail)...`,
      );

      const { successDetailRecords, failedDetailRecords } =
        await this.batchCreateOrderSuppliersDetail(batch);

      totalDetailCreated += successDetailRecords.length;
      totalDetailFailed += failedDetailRecords.length;

      if (successDetailRecords.length > 0) {
        await this.updateDetailDatabaseStatus(successDetailRecords, 'SYNCED');
      }

      if (failedDetailRecords.length > 0) {
        await this.updateDetailDatabaseStatus(failedDetailRecords, 'FAILED');
      }

      this.logger.log(
        `📊 Batch ${i + 1}/${batches.length}: ${successDetailRecords.length}/${batch.length} created`,
      );

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalDetailCreated} success, ${totalDetailFailed} failed`,
    );
  }

  private async processUpdateOrderSuppliers(
    order_suppliers: any[],
  ): Promise<void> {
    if (order_suppliers.length === 0) return;

    this.logger.log(
      `📝 Updating ${order_suppliers.length} existing order_suppliers...`,
    );

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < order_suppliers.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = order_suppliers.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (order_supplier) => {
          try {
            const updated =
              await this.updateSingleOrderSupplier(order_supplier);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([order_supplier], 'SYNCED');
            } else {
              createFallbacks.push(order_supplier);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${order_supplier.code}: ${error.message}`,
            );
            createFallbacks.push(order_supplier);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < order_suppliers.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `📝 Creating ${createFallbacks.length} order_suppliers that failed update...`,
      );
      await this.processNewOrderSuppliers(createFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  private async processUpdateOrderSuppliersDetail(
    order_suppliers_detail: any[],
  ): Promise<void> {
    if (order_suppliers_detail.length === 0) return;

    this.logger.log(
      `📝 Updating ${order_suppliers_detail.length} existing order_suppliers_detail...`,
    );

    let successDetailCount = 0;
    let failedDetailCount = 0;
    const createDetailFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < order_suppliers_detail.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = order_suppliers_detail.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (order_supplier_detail) => {
          try {
            const updated = await this.updateSingleOrderSupplierDetail(
              order_supplier_detail,
            );

            if (updated) {
              successDetailCount++;
              await this.updateDetailDatabaseStatus(
                [order_supplier_detail],
                'SYNCED',
              );
            } else {
              createDetailFallbacks.push(order_supplier_detail);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${order_supplier_detail.code}: ${error.message}`,
            );
            createDetailFallbacks.push(order_supplier_detail);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < order_suppliers_detail.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createDetailFallbacks.length > 0) {
      this.logger.log(
        `📝 Creating ${createDetailFallbacks.length} order_suppliers_detail that failed update...`,
      );
      await this.processNewOrderSuppliersDetail(createDetailFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successDetailCount} success, ${failedDetailCount} failed`,
    );
  }

  private async batchCreateOrderSuppliers(
    order_suppliers: any[],
  ): Promise<BatchResult> {
    const records = order_suppliers.map((order_supplier) => ({
      fields: this.mapOrderSupplierToLarkBase(order_supplier),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderSupplierHeaders();
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
          const successRecords = order_suppliers.slice(0, successCount);
          const failedRecords = order_suppliers.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const order_supplier = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(
              order_supplier.kiotVietId,
            );
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (order_supplier.code) {
              this.orderSupplierCodeCache.set(
                String(order_supplier.code).trim(),
                createdRecord.record_id,
              );
            }
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: order_suppliers };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: order_suppliers };
      }
    }

    return { successRecords: [], failedRecords: order_suppliers };
  }

  private async batchCreateOrderSuppliersDetail(
    order_suppliers_detail: any[],
  ): Promise<BatchDetailResult> {
    const records = order_suppliers_detail.map((order_supplier_detail) => ({
      fields: this.mapOrderSupplierDetailToLarkBase(order_supplier_detail),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getOrderSupplierDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdDetailRecords = response.data.data?.records || [];
          const successDetailCount = createdDetailRecords.length;
          const successDetailRecords = order_suppliers_detail.slice(
            0,
            successDetailCount,
          );
          const failedDetailRecords =
            order_suppliers_detail.slice(successDetailCount);

          for (
            let i = 0;
            i <
            Math.min(successDetailRecords.length, createdDetailRecords.length);
            i++
          ) {
            const order_supplier_detail = successDetailRecords[i];
            const createdDetailRecord = createdDetailRecords[i];

            const kiotVietId = this.safeBigIntToNumber(
              order_supplier_detail.kiotVietId,
            );
            if (kiotVietId > 0) {
              this.existingDetailRecordsCache.set(
                kiotVietId,
                createdDetailRecord.record_id,
              );
            }

            if (order_supplier_detail.code) {
              this.orderSupplierDetailCodeCache.set(
                String(order_supplier_detail.code).trim(),
                createdDetailRecord.record_id,
              );
            }
          }

          return { successDetailRecords, failedDetailRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceDetailTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return {
          successDetailRecords: [],
          failedDetailRecords: order_suppliers_detail,
        };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceDetailTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return {
          successDetailRecords: [],
          failedDetailRecords: order_suppliers_detail,
        };
      }
    }

    return {
      successDetailRecords: [],
      failedDetailRecords: order_suppliers_detail,
    };
  }

  private async updateSingleOrderSupplier(
    order_supplier: any,
  ): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderSupplierHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${order_supplier.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapOrderSupplierToLarkBase(order_supplier) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `✅ Updated record ${order_supplier.larkRecordId} for order_supplier ${order_supplier.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${order_supplier.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private async updateSingleOrderSupplierDetail(
    order_supplier_detail: any,
  ): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getOrderSupplierDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/${order_supplier_detail.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            {
              fields: this.mapOrderSupplierDetailToLarkBase(
                order_supplier_detail,
              ),
            },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `✅ Updated record ${order_supplier_detail.larkRecordId} for order_supplier_detail ${order_supplier_detail.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceDetailTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(
            `Record not found: ${order_supplier_detail.larkRecordId}`,
          );
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

        const headers = await this.larkAuthService.getOrderSupplierHeaders();
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

  private async testLarkBaseDetailConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers =
          await this.larkAuthService.getOrderSupplierDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records`;
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
    const syncName = 'order_supplier_lark_sync';

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
        entities: ['order_supplier'],
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

  private async acquireDetailSyncLock(lockKey: string): Promise<void> {
    const syncName = 'order_supplier_detail_lark_sync';

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
          await this.forceDetailReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceDetailReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['order_supplier_detail'],
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
        return;
      }

      this.logger.debug(
        `⏳ Waiting for lock release... (${Math.round((Date.now() - startTime) / 1000)}s)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 2000));
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

  private async forceDetailReleaseLock(syncName: string): Promise<void> {
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
        name: 'order_supplier_lark_sync',
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
        where: { id: lockRecord.id },
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

  private async releaseDetailSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'order_supplier_detail_lark_sync',
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
        where: { id: lockRecord.id },
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

  private async forceTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;
      await this.larkAuthService.getOrderSupplierHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async forceDetailTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;
      await this.larkAuthService.getOrderSupplierDetailHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async updateDatabaseStatus(
    order_suppliers: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (order_suppliers.length === 0) return;

    const orderSupplierIds = order_suppliers.map((c) => c.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.orderSupplier.updateMany({
      where: { id: { in: orderSupplierIds } },
      data: updateData,
    });
  }

  private async updateDetailDatabaseStatus(
    order_suppliers_detail: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (order_suppliers_detail.length === 0) return;

    const orderSupplierDetailIds = order_suppliers_detail.map((c) => c.id);
    const updateDetailData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.orderSupplierDetail.updateMany({
      where: { id: { in: orderSupplierDetailIds } },
      data: updateDetailData,
    });
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.orderSupplierCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  private clearDetailCache(): void {
    this.existingDetailRecordsCache.clear();
    this.orderSupplierDetailCodeCache.clear();
    this.cacheDetailLoaded = false;
    this.lastDetailCacheLoadTime = null;
    this.logger.debug('🧹 Cache Detail cleared');
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

  private mapOrderSupplierToLarkBase(order_supplier: any): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_ORDER_SUPPLIER_FIELDS.kiotVietId] = this.safeBigIntToNumber(
      order_supplier.kiotVietId,
    );

    if (order_supplier.code) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.ORDER_SUPPLIER_CODE] =
        order_supplier.code;
    }

    if (order_supplier.orderDate) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.ORDER_DATE] = new Date(
        order_supplier.orderDate,
      ).getTime();
    }

    if (
      order_supplier.branchId !== null &&
      order_supplier.branchId !== undefined
    ) {
      if (order_supplier.branchId === 1) {
        fields[LARK_ORDER_SUPPLIER_FIELDS.BRANCH] =
          BRANCH_NAME.CUA_HANG_DIEP_TRA;
      } else if (order_supplier.branchId === 2) {
        fields[LARK_ORDER_SUPPLIER_FIELDS.BRANCH] = BRANCH_NAME.KHO_HA_NOI;
      } else if (order_supplier.branchId === 3) {
        fields[LARK_ORDER_SUPPLIER_FIELDS.BRANCH] = BRANCH_NAME.KHO_SAI_GON;
      } else if (order_supplier.branchId == 4) {
        fields[LARK_ORDER_SUPPLIER_FIELDS.BRANCH] =
          BRANCH_NAME.VAN_PHONG_HA_NOI;
      }
    }

    if (order_supplier.retailerId) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.RETAILER] = '2svn';
    }

    if (order_supplier.userId !== null && order_supplier.userId !== undefined) {
      const userMapping = {
        13: USER_OPTION.NGUYEN_THI_NGAN,
      };

      fields[LARK_ORDER_SUPPLIER_FIELDS.USER_ID] =
        userMapping[order_supplier.userId];
    }

    if (order_supplier.description) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.DESCRIPTION] =
        order_supplier.description || '';
    }

    if (order_supplier.status !== null && order_supplier.status !== undefined) {
      const statusMapping = {
        1: STATUS_OPTION.DA_XAC_NHAN,
        2: STATUS_OPTION.NHAP_MOT_PHAN,
        3: STATUS_OPTION.HOAN_THANH,
        4: STATUS_OPTION.DA_HUY,
        5: STATUS_OPTION.PHIEU_TAM,
      };

      fields[LARK_ORDER_SUPPLIER_FIELDS.STATUS] =
        statusMapping[order_supplier.status];
    }

    if (order_supplier.discountRatio) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.DISCOUNT_RATIO] = Number(
        order_supplier.discountRatio || 0,
      );
    }

    if (order_supplier.productQty) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.PRODUCT_QTY] =
        order_supplier.productQty;
    }

    if (order_supplier.discount) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.DISCOUNT] = Number(
        order_supplier.discount || 0,
      );
    }

    if (order_supplier.createdDate) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.CREATED_DATE] = new Date(
        order_supplier.createdDate,
      ).getTime();
    }

    if (order_supplier.createdBy) {
      const createdByMapping = {
        13: CREATOR.NGUYEN_THI_NGAN,
      };
      fields[LARK_ORDER_SUPPLIER_FIELDS.CREATED_BY] =
        createdByMapping[order_supplier.createdBy];
    }

    if (order_supplier.total) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.TOTAL] = Number(
        order_supplier.total || 0,
      );
    }

    if (order_supplier.exReturnSuppliers) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.EX_RETURN_SUPPLIER] = Number(
        order_supplier.exReturnSuppliers || 0,
      );
    }

    if (order_supplier.exReturnThirdParty) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.EX_RETURN_THIRD_PARTY] = Number(
        order_supplier.exReturnThirdParty || 0,
      );
    }

    if (order_supplier.totalAmt) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.TOTAL_AMOUNT] = Number(
        order_supplier.totalAmt || 0,
      );
    }

    if (order_supplier.totalQty) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.TOTAL_QUANTITY] = Number(
        order_supplier.totalQty || 0,
      );
    }

    if (order_supplier.totalProductType) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.TOTAL_PRODUCT_TYPE] = Number(
        order_supplier.totalProductType || 0,
      );
    }

    if (order_supplier.paidAmount) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.PAID_AMOUNT] = Number(
        order_supplier.paidAmount || 0,
      );
    }

    if (order_supplier.purchaseOrderCodes) {
      fields[LARK_ORDER_SUPPLIER_FIELDS.PURCHASE_ORDER_CODE] =
        order_supplier.purchaseOrderCodes;
    }

    return fields;
  }

  private mapOrderSupplierDetailToLarkBase(
    order_supplier_detail: any,
  ): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.kiotVietId] =
      this.safeBigIntToNumber(order_supplier_detail.kiotVietId);

    if (order_supplier_detail.orderSupplierCode) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ORDER_SUPPLIER_CODE] =
        order_supplier_detail.orderSupplierCode;
    }

    if (order_supplier_detail.productCode) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.PRODUCT_CODE] =
        order_supplier_detail.productCode;
    }

    if (order_supplier_detail.productName) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.PRODUCT_NAME] =
        order_supplier_detail.productName;
    }

    if (
      order_supplier_detail.quantity &&
      order_supplier_detail.quantity !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.QUANTITY] = Number(
        order_supplier_detail.quantity || 0,
      );
    }

    if (
      order_supplier_detail.price &&
      order_supplier_detail.price !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.PRICE] = Number(
        order_supplier_detail.price || 0,
      );
    }

    if (
      order_supplier_detail.discount &&
      order_supplier_detail.discount !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.DISCOUNT] = Number(
        order_supplier_detail.discount || 0,
      );
    }

    if (
      order_supplier_detail.allocation &&
      order_supplier_detail.allocation !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ALLOCATION] = Number(
        order_supplier_detail.allocation || 0,
      );
    }

    if (order_supplier_detail.description) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.DESCRIPTION] =
        order_supplier_detail.description || '';
    }

    if (
      order_supplier_detail.orderByNumber &&
      order_supplier_detail.orderByNumber !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ORDER_BY_NUMBER] = Number(
        order_supplier_detail.orderByNumber || 0,
      );
    }

    if (
      order_supplier_detail.allocationSuppliers &&
      order_supplier_detail.allocationSuppliers !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ALLOCATION_SUPPLIERS] = Number(
        order_supplier_detail.allocationSuppliers || 0,
      );
    }

    if (
      order_supplier_detail.allocationThirdParty &&
      order_supplier_detail.allocationThirdParty !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ALLOCATION_THIRD_PARTY] = Number(
        order_supplier_detail.allocationThirdParty || 0,
      );
    }

    if (
      order_supplier_detail.orderQuantity &&
      order_supplier_detail.orderQuantity !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.ORDER_QUANTITY] = Number(
        order_supplier_detail.orderQuantity || 0,
      );
    }

    if (
      order_supplier_detail.subTotal &&
      order_supplier_detail.subTotal !== undefined
    ) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.SUB_TOTAL] = Number(
        order_supplier_detail.subTotal || 0,
      );
    }

    if (order_supplier_detail.createdDate) {
      fields[LARK_ORDER_SUPPLIER_DETAIL_FIELDS.CREATED_DATE] = new Date(
        order_supplier_detail.createdDate,
      ).getTime();
    }

    return fields;
  }
}
