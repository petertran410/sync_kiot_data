import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_PURCHASE_ORDER_FIELDS = {
  PURCHASE_ORDER_CODE: 'Mã Nhập Hàng',
  KIOTVIET_ID: 'kiotVietId',
  RETAILER: 'Gian Hàng',
  DESCRIPTION: 'Mô Tả',
  BRANCH_NAME: 'Tên Chi Nhánh',
  PURCHASE_DATE: 'Ngày Mua Hàng',
  DISCOUNT: 'Discount',
  DISCOUNT_RATIO: 'Mức Độ Discount',
  TOTAL: 'Giá Trị Nhập Hàng',
  TOTAL_PAYMENT: 'Tiền Đã Trả Hàng',
  CREATED_DATE: 'Ngày Tạo',
  SUPPLIER_NAME: 'Tên Nhà Cung Cấp',
  PURCHASE_BY_NAME: 'Người Nhập',
};

const LARK_PURCHASE_ORDER_DETAIL_FIELDS = {
  PRIMARY_PURCHASE_ORDER_CODE: 'Mã Nhập Hàng',
  PRODUCT_CODE: 'Mã Sản Phẩm',
  PRODUCT_NAME: 'Tên Sản Phẩm',
  QUANTITY: 'Số Lượng',
  DISCOUNT: 'Giảm Giá',
  UNIT_PRICE: 'Đơn Giá',
  LINE_NUMBER: 'lineNumber',
  PURCHASE_ORDER_ID: 'Id Nhập Hàng',
  UNIQUE_KEY: 'uniqueKey',
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
  successDetailsRecords: any[];
  failedDetailsRecords: any[];
}

@Injectable()
export class LarkPurchaseOrderSyncService {
  private readonly logger = new Logger(LarkPurchaseOrderSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();
  private readonly pendingDetailCreation = new Set<number>();

  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  private existingRecordsCache = new Map<number, string>();
  private purchaseOrderCodeCache = new Map<string, string>();

  private existingDetailRecordsCache = new Map<string, string>();
  private purchaseOrderDetailCache = new Map<string, string>();

  private cacheLoaded = false;
  private cacheDetailLoaded = false;

  private lastCacheLoadTime: Date | null = null;
  private lastDetailCacheLoadTime: Date | null = null;

  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_PURCHASE_ORDER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_PURCHASE_ORDER_SYNC_TABLE_ID',
    );

    const baseTokenDetail = this.configService.get<string>(
      'LARK_PURCHASE_ORDER_DETAIL_SYNC_BASE_TOKEN',
    );
    const tableIdDetail = this.configService.get<string>(
      'LARK_PURCHASE_ORDER_DETAIL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId || !baseTokenDetail || !tableIdDetail) {
      throw new Error('LarkBase purchase_order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
    this.baseTokenDetail = baseTokenDetail;
    this.tableIdDetail = tableIdDetail;
  }

  async syncPurchaseOrdersToLarkBase(purchase_orders: any[]): Promise<void> {
    const lockKey = `lark_purchase_order_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${purchase_orders.length} purchase_orders...`,
      );

      const purchaseOrdersToSync = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (purchaseOrdersToSync.length === 0) {
        this.logger.log('No purchase_orders need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (purchaseOrdersToSync.length < 5) {
        this.logger.log(
          `Small sync (${purchaseOrdersToSync.length} purchaseOrder) - using lightweight mode`,
        );
        await this.syncWithoutCache(purchaseOrdersToSync);
        await this.releaseDetailSyncLock(lockKey);
        return;
      }

      const pendingCount = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingCount} PENDING + ${failedCount} FAILED purchase_orders`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(purchaseOrdersToSync);
        await this.releaseSyncLock(lockKey);
      }

      const { newPurchaseOrders, updatePurchaseOrders } =
        await this.categorizePurchaseOrders(purchaseOrdersToSync);

      this.logger.log(
        `Categorization: ${newPurchaseOrders.length} new, ${updatePurchaseOrders.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newPurchaseOrders.length > 0) {
        for (
          let i = 0;
          i < newPurchaseOrders.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = newPurchaseOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new purchase_orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newPurchaseOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewPurchaseOrders(batch);
        }
      }

      if (updatePurchaseOrders.length > 0) {
        for (
          let i = 0;
          i < updatePurchaseOrders.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = updatePurchaseOrders.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update purchase_orders batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updatePurchaseOrders.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdatePurchaseOrders(batch);
        }
      }

      this.logger.log('LarkBase purchase_order sync completed!');
    } catch (error) {
      this.logger.error(
        `Purchase order sync failed: ${error.message}`,
        error.stack,
      );
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  async syncPurchaseOrderDetailsToLarkBase(): Promise<void> {
    this.logger.log('DEBUG: syncPurchaseOrderDetailsToLarkBase called');
    const lockKey = `lark_purchase_order_detail_sync_lock_${Date.now()}`;

    try {
      await this.acquireDetailSyncLock(lockKey);
      this.logger.log('DEBUG: Lock acquired successfully');

      this.logger.log(`Starting LarkBase sync for purchase_orders_details`);

      const purchaseOrderDetailsToSync =
        await this.prismaService.purchaseOrderDetail.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      this.logger.log(
        `DEBUG: Found ${purchaseOrderDetailsToSync.length} records with PENDING/FAILED status`,
      );

      if (purchaseOrderDetailsToSync.length === 0) {
        this.logger.log('DEBUG: No records to sync - checking database status');

        const statusCounts =
          await this.prismaService.purchaseOrderDetail.groupBy({
            by: ['larkSyncStatus'],
            _count: { larkSyncStatus: true },
          });
        this.logger.log('DEBUG: Current status distribution:', statusCounts);
        return;
      }

      if (purchaseOrderDetailsToSync.length < 5) {
        this.logger.log(
          `Small sync (${purchaseOrderDetailsToSync.length} purchaseOrderDetails) - using lightweight mode`,
        );
        await this.syncWithoutDetailCache(purchaseOrderDetailsToSync);
        return;
      }

      const pendingDetailCount = purchaseOrderDetailsToSync.filter(
        (s) => s.larkSyncStatus === 'PENDING',
      ).length;
      const failedDetailCount = purchaseOrderDetailsToSync.filter(
        (s) => s.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingDetailCount} PENDING + ${failedDetailCount} FAILED purchase_orders_details`,
      );

      await this.testLarkBaseDetailConnection();

      const cacheDetailLoaded = await this.loadExistingDetailRecordsWithRetry();

      if (!cacheDetailLoaded) {
        this.logger.warn(
          'PurchaseOrderDetail cache loading failed - will use alternative duplicate detection',
        );
        await this.syncWithoutDetailCache(purchaseOrderDetailsToSync);
        return;
      }

      const { newPurchaseOrdersDetails, updatePurchaseOrdersDetails } =
        await this.categorizePurchaseOrderDetails(purchaseOrderDetailsToSync);

      this.logger.log(
        `PurchaseOrderDetail Categorization: ${newPurchaseOrdersDetails.length} new, ${updatePurchaseOrdersDetails.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newPurchaseOrdersDetails.length > 0) {
        for (
          let i = 0;
          i < newPurchaseOrdersDetails.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = newPurchaseOrdersDetails.slice(
            i,
            i + BATCH_SIZE_FOR_SYNC,
          );
          this.logger.log(
            `Processing new purchase_orders_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newPurchaseOrdersDetails.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewPurchaseOrderDetails(batch);
        }
      }

      if (updatePurchaseOrdersDetails.length > 0) {
        for (
          let i = 0;
          i < updatePurchaseOrdersDetails.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = updatePurchaseOrdersDetails.slice(
            i,
            i + BATCH_SIZE_FOR_SYNC,
          );
          this.logger.log(
            `Processing update purchase_orders_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updatePurchaseOrdersDetails.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdatePurchaseOrderDetails(batch);
        }
      }

      this.logger.log('LarkBase PurchaseOrderDetail sync completed!');
    } catch (error) {
      this.logger.error(`PurchaseOrderDetail sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseDetailSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (this.isCacheValid() && this.existingRecordsCache.size > 5000) {
          this.logger.log(
            `Using cache available (${this.existingRecordsCache.size} records) - skipping reload`,
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
        await this.loadExistingRecords();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );
        if (attempt < maxRetries) {
          const delay = attempt * 2000;
          this.logger.log(`Waiting ${delay / 1000}s before retry...`);
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
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (
          this.isDetailCacheValid() &&
          this.existingDetailRecordsCache.size > 5000
        ) {
          this.logger.log(
            `Using cache detail available (${this.existingDetailRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastDetailCacheLoadTime) {
          const detailCacheAgeMinutes =
            (Date.now() - this.lastDetailCacheLoadTime.getTime()) / (1000 * 60);
          if (
            detailCacheAgeMinutes < 45 &&
            this.existingDetailRecordsCache.size > 500
          ) {
            this.logger.log(
              `Recent detail cache (${detailCacheAgeMinutes.toFixed(1)}min old, ${this.existingDetailRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearDetailCache();
        await this.loadExistingDetailRecords();

        if (this.existingDetailRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingDetailRecordsCache.size} records`,
          );
          this.lastDetailCacheLoadTime = new Date();
          return true;
        }
        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );
        if (attempt < maxRetries) {
          const delay = attempt * 2000;
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
      const headers = await this.larkAuthService.getPurchaseOrderHeaders();
      let pageToken = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      let stringConversions = 0;
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
              record.fields[LARK_PURCHASE_ORDER_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            // let kiotVietId = 0;

            // if (kiotVietIdRaw !== null && kiotVietIdRaw !== undefined) {
            //   if (typeof kiotVietIdRaw === 'string') {
            //     const trimmed = kiotVietIdRaw.trim();
            //     if (trimmed !== '') {
            //       const parsed = parseInt(trimmed, 10);
            //       if (!isNaN(parsed) && parsed > 0) {
            //         kiotVietId = parsed;
            //         stringConversions++;
            //       }
            //     }
            //   } else if (typeof kiotVietIdRaw === 'number') {
            //     kiotVietId = Math.floor(kiotVietIdRaw);
            //   }
            // }

            // if (kiotVietId > 0) {
            //   this.existingRecordsCache.set(kiotVietId, record.record_id);
            //   cacheBuilt++;
            // }

            const purchaseOrderCode =
              record.fields[LARK_PURCHASE_ORDER_FIELDS.PURCHASE_ORDER_CODE];
            if (purchaseOrderCode) {
              this.purchaseOrderCodeCache.set(
                String(purchaseOrderCode).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token || '';

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheBuilt}/${totalLoaded} records processed (${stringConversions} string conversions) (${loadTime}ms/page)`,
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
        `Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.purchaseOrderCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async loadExistingDetailRecords(): Promise<void> {
    try {
      const headers =
        await this.larkAuthService.getPurchaseOrderDetailHeaders();
      let pageToken = '';
      let totalLoaded = 0;
      let cacheDetailBuilt = 0;
      let stringConversions = 0;
      const pageSize = 1000;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records`;

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
            const uniqueKeyRaw =
              record.fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.UNIQUE_KEY];

            let uniqueKey = '';

            if (uniqueKeyRaw !== null && uniqueKeyRaw !== undefined) {
              if (typeof uniqueKeyRaw === 'string') {
                const trimmed = uniqueKeyRaw.trim();
                if (trimmed !== '') {
                  uniqueKey = trimmed;
                  stringConversions++;
                }
              } else if (typeof uniqueKeyRaw === 'number') {
                uniqueKey = String(uniqueKeyRaw).trim();
                if (uniqueKey !== '') {
                  stringConversions++;
                }
              }
            }

            if (uniqueKey && record.record_id) {
              this.existingDetailRecordsCache.set(uniqueKey, record.record_id);
              cacheDetailBuilt++;
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token || '';

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheDetailBuilt}/${totalLoaded} records processed (${stringConversions} string conversions) (${loadTime}ms/page)`,
            );
          }
        } else {
          throw new Error(
            `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
          );
        }
      } while (pageToken);

      this.cacheDetailLoaded = true;

      const successRate =
        totalLoaded > 0
          ? Math.round((cacheDetailBuilt / totalLoaded) * 100)
          : 0;

      this.logger.log(
        `Cache loaded: ${this.existingDetailRecordsCache.size} records (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to load existing detail records: ${error.message}`,
      );
      throw error;
    }
  }

  private async categorizePurchaseOrders(purchase_orders: any[]): Promise<any> {
    const newPurchaseOrders: any[] = [];
    const updatePurchaseOrders: any[] = [];

    for (const purchase_order of purchase_orders) {
      const kiotVietId = purchase_order.kiotVietId
        ? typeof purchase_order.kiotVietId === 'bigint'
          ? Number(purchase_order.kiotVietId)
          : Number(purchase_order.kiotVietId)
        : 0;

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Purchase Order ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && purchase_order.code) {
        existingRecordId = this.purchaseOrderCodeCache.get(
          String(purchase_order.code).trim(),
        );
      }

      if (existingRecordId) {
        updatePurchaseOrders.push({
          ...purchase_order,
          larkRecordId: existingRecordId,
        });
      } else {
        newPurchaseOrders.push(purchase_order);
      }
    }

    return { newPurchaseOrders, updatePurchaseOrders };
  }

  private async categorizePurchaseOrderDetails(
    purchase_order_details: any[],
  ): Promise<any> {
    const newPurchaseOrdersDetails: any[] = [];
    const updatePurchaseOrdersDetails: any[] = [];

    const duplicateDetected = purchase_order_details.filter(
      (purchase_order_detail) => {
        const uniqueKey = purchase_order_detail.uniqueKey;
        return this.existingDetailRecordsCache.has(uniqueKey);
      },
    );

    if (duplicateDetected.length > 0) {
      this.logger.warn(
        `Detectd ${duplicateDetected.length} purchase_order_detail already in cache: ${duplicateDetected.map((o) => o.uniqueKey)}`,
      );
    }

    for (const purchase_order_detail of purchase_order_details) {
      const uniqueKey = purchase_order_detail.uniqueKey;

      if (this.pendingDetailCreation.has(uniqueKey)) {
        this.logger.warn(
          `Purchase Order Detail ${uniqueKey} is pending creation, skipping`,
        );
        continue;
      }

      let existingDetailRecordId =
        this.existingDetailRecordsCache.get(uniqueKey);

      // if(!existingDetailRecordId) {
      //   existingDetailRecordId = this.purchaseOrderDetailCache
      // }

      if (existingDetailRecordId) {
        updatePurchaseOrdersDetails.push({
          ...purchase_order_detail,
          larkRecordId: existingDetailRecordId,
        });
      } else {
        this.pendingDetailCreation.add(uniqueKey);
        newPurchaseOrdersDetails.push(purchase_order_detail);
      }
    }

    return { newPurchaseOrdersDetails, updatePurchaseOrdersDetails };
  }

  private async syncWithoutCache(purchase_orders: any[]): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingPurchaseOrders =
      await this.prismaService.purchaseOrder.findMany({
        where: { kiotVietId: { in: purchase_orders.map((o) => o.kiotVietId) } },
        select: { kiotVietId: true, larkRecordId: true },
      });

    const quickCache = new Map<number, string>();
    existingPurchaseOrders.forEach((o) => {
      if (o.larkRecordId) {
        quickCache.set(Number(o.kiotVietId), o.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newPurchaseOrders, updatePurchaseOrders } =
        await this.categorizePurchaseOrders(purchase_orders);

      if (newPurchaseOrders.length > 0) {
        await this.processNewPurchaseOrders(newPurchaseOrders);
      }

      if (updatePurchaseOrders.length > 0) {
        await this.processUpdatePurchaseOrders(updatePurchaseOrders);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async syncWithoutDetailCache(
    purchase_orders_detail: any[],
  ): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingPurchaseOrdersDetail =
      await this.prismaService.purchaseOrderDetail.findMany({
        where: {
          uniqueKey: { in: purchase_orders_detail.map((o) => o.uniqueKey) },
        },
        select: { uniqueKey: true, larkRecordId: true },
      });

    const quickDetailCache = new Map<string, string>();
    existingPurchaseOrdersDetail.forEach((o) => {
      if (o.larkRecordId) {
        quickDetailCache.set(String(o.uniqueKey), o.larkRecordId);
      }
    });

    const originalDetailCache = this.existingDetailRecordsCache;
    this.existingDetailRecordsCache = quickDetailCache;

    try {
      const { newPurchaseOrdersDetails, updatePurchaseOrdersDetails } =
        await this.categorizePurchaseOrderDetails(purchase_orders_detail);

      if (newPurchaseOrdersDetails.length > 0) {
        await this.processNewPurchaseOrderDetails(newPurchaseOrdersDetails);
      }

      if (updatePurchaseOrdersDetails.length > 0) {
        await this.processUpdatePurchaseOrderDetails(
          updatePurchaseOrdersDetails,
        );
      }
    } finally {
      this.existingDetailRecordsCache = originalDetailCache;
    }
  }

  private async processNewPurchaseOrders(
    purchase_orders: any[],
  ): Promise<void> {
    if (purchase_orders.length === 0) return;

    this.logger.log(
      `Creating ${purchase_orders.length} new purchase_orders...`,
    );

    const batches = this.chunkArray(purchase_orders, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const purchaseOrder of batch) {
        const kiotVietId = this.safeBigIntToNumber(purchaseOrder.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(purchaseOrder);
        } else {
          this.logger.warn(
            `Skipping duplicate purchase_order ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(
          `Batch ${i + 1} skipped - all purchase_order already exist`,
        );
        continue;
      }
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} purchase_orders)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreatePurchaseOrders(verifiedBatch);

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

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processNewPurchaseOrderDetails(details: any[]): Promise<void> {
    if (details.length === 0) return;

    this.logger.log(
      `Creating ${details.length} new purchase order details details...`,
    );

    const batches = this.chunkArray(details, this.batchSize);
    let totalDetailsCreated = 0;
    let totalDetailsFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const purchaseOrderDetail of batch) {
        const uniqueKey = purchaseOrderDetail.uniqueKey;
        if (!this.existingDetailRecordsCache.has(uniqueKey)) {
          verifiedBatch.push(purchaseOrderDetail);
        } else {
          this.logger.warn(
            `Skipping duplicate purchase_order_detail ${uniqueKey} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(
          `Batch ${i + 1} skipped - all purchase_order_detail already exist`,
        );
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} purchase_orders_details)...`,
      );

      const { successDetailsRecords, failedDetailsRecords } =
        await this.batchCreatePurchaseOrderDetails(verifiedBatch);

      totalDetailsCreated += successDetailsRecords.length;
      totalDetailsFailed += failedDetailsRecords.length;

      if (successDetailsRecords.length > 0) {
        await this.updateDetailDatabaseStatus(successDetailsRecords, 'SYNCED');

        successDetailsRecords.forEach((detail) => {
          this.pendingDetailCreation.delete(detail.uniqueKey);
        });
      }

      if (failedDetailsRecords.length > 0) {
        await this.updateDetailDatabaseStatus(failedDetailsRecords, 'FAILED');
      }

      this.logger.log(
        `Batch ${i + 1}/${batches.length}: ${successDetailsRecords.length}/${batch.length} created`,
      );

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }

      this.logger.log(
        `Create complete: ${totalDetailsCreated} success, ${totalDetailsFailed} failed`,
      );
    }
  }

  private async processUpdatePurchaseOrders(
    purchase_orders: any[],
  ): Promise<void> {
    if (purchase_orders.length === 0) return;

    this.logger.log(
      `Updating ${purchase_orders.length} existing purchase_orders...`,
    );

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < purchase_orders.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = purchase_orders.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (purchase_order) => {
          try {
            const updated =
              await this.updateSinglePurchaseOrder(purchase_order);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([purchase_order], 'SYNCED');
            } else {
              createFallbacks.push(purchase_order);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${purchase_order.code}: ${error.message}`,
            );
            createFallbacks.push(purchase_order);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < purchase_orders.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `Processing ${createFallbacks.length} update fallbacks as new purchase_orders...`,
      );
      await this.processNewPurchaseOrders(createFallbacks);
    }

    this.logger.log(
      `Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  private async processUpdatePurchaseOrderDetails(
    purchase_orders_detail: any[],
  ): Promise<void> {
    if (purchase_orders_detail.length === 0) return;

    this.logger.log(
      `Updating ${purchase_orders_detail.length} existing purchase order purchase_orders_detail...`,
    );

    let successDetailCount = 0;
    let failedDetailCount = 0;
    const createDetailFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < purchase_orders_detail.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = purchase_orders_detail.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (purchase_order_detail) => {
          try {
            const updated = await this.updateSinglePurchaseOrderDetail(
              purchase_order_detail,
            );

            if (updated) {
              successDetailCount++;
              await this.updateDetailDatabaseStatus(
                [purchase_order_detail],
                'SYNCED',
              );
            } else {
              createDetailFallbacks.push(purchase_order_detail);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for purchase_order_detail ${purchase_order_detail.uniqueKey}: ${error.message}`,
            );
            createDetailFallbacks.push(purchase_order_detail);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < purchase_orders_detail.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createDetailFallbacks.length > 0) {
      this.logger.log(
        `Processing ${createDetailFallbacks.length} update fallbacks as new detail records...`,
      );
      await this.processNewPurchaseOrderDetails(createDetailFallbacks);
    }

    this.logger.log(
      `Update complete: ${successDetailCount} success, ${failedDetailCount} failed`,
    );
  }

  private async batchCreatePurchaseOrders(
    purchase_orders: any[],
  ): Promise<BatchResult> {
    const records = purchase_orders.map((purchase_order) => ({
      fields: this.mapPurchaseOrderToLarkBase(purchase_order),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
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
          const successRecords = purchase_orders.slice(0, successCount);
          const failedRecords = purchase_orders.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const purchase_order = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(
              purchase_order.kiotVietId,
            );
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (purchase_order.code) {
              this.purchaseOrderCodeCache.set(
                String(purchase_order.code).trim(),
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
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: purchase_orders };
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
        return { successRecords: [], failedRecords: purchase_orders };
      }
    }

    return { successRecords: [], failedRecords: purchase_orders };
  }

  private async batchCreatePurchaseOrderDetails(
    details: any[],
  ): Promise<BatchDetailResult> {
    const records = details.map((detail) => ({
      fields: this.mapPurchaseOrderDetailToLarkBase(detail),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdDetailsRecords = response.data.data?.records || [];
          const successDetailCount = createdDetailsRecords.length;
          const successDetailsRecords = details.slice(0, successDetailCount);
          const failedDetailsRecords = details.slice(successDetailCount);

          for (
            let i = 0;
            i <
            Math.min(
              successDetailsRecords.length,
              createdDetailsRecords.length,
            );
            i++
          ) {
            const purchase_order_detail = successDetailsRecords[i];
            const createdDetailRecord = createdDetailsRecords[i];

            if (purchase_order_detail.uniqueKey) {
              this.existingDetailRecordsCache.set(
                purchase_order_detail.uniqueKey,
                createdDetailRecord.record_id,
              );
            }
          }

          return { successDetailsRecords, failedDetailsRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceDetailTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );

        return { successDetailsRecords: [], failedDetailsRecords: details };
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
        return { successDetailsRecords: [], failedDetailsRecords: details };
      }
    }

    return { successDetailsRecords: [], failedDetailsRecords: details };
  }

  private async updateSinglePurchaseOrder(
    purchase_order: any,
  ): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${purchase_order.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapPurchaseOrderToLarkBase(purchase_order) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${purchase_order.larkRecordId} for purchase_order ${purchase_order.code}`,
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
          this.logger.warn(`Record not found: ${purchase_order.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private async updateSinglePurchaseOrderDetail(detail: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/${detail.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            {
              fields: this.mapPurchaseOrderDetailToLarkBase(detail),
            },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${detail.larkRecordId} for purchase_order_detail ${detail.code}`,
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

        if (error?.response?.status === 404) {
          this.logger.warn(`Record not found: ${detail.larkRecordId}`);
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

        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
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
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
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
    const syncName = 'purchase_order_lark_sync';

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
        entities: ['purchase_order'],
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
    const syncName = 'purchase_order_detail_lark_sync';

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
        entities: ['purchase_order_detail'],
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
        name: 'purchase_order_lark_sync',
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
        name: 'purchase_order_detail_lark_sync',
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
      await this.larkAuthService.getPurchaseOrderHeaders();
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
      await this.larkAuthService.getPurchaseOrderDetailHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async updateDatabaseStatus(
    purchase_orders: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (purchase_orders.length === 0) return;

    const purchaseOrderIds = purchase_orders.map((c) => c.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.purchaseOrder.updateMany({
      where: { id: { in: purchaseOrderIds } },
      data: updateData,
    });
  }

  private async updateDetailDatabaseStatus(
    details: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    const detailIds = details
      .map((detail) => detail.id)
      .filter((id) => id !== undefined);

    if (detailIds.length === 0) return;

    try {
      await this.prismaService.purchaseOrderDetail.updateMany({
        where: { id: { in: detailIds } },
        data: {
          larkSyncStatus: status,
          larkSyncedAt: new Date(),
          larkSyncRetries: status === 'FAILED' ? { increment: 1 } : 0,
        },
      });

      this.logger.debug(
        `✅ Updated ${detailIds.length} purchase order details to ${status}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to update detail database status: ${error.message}`,
      );
    }
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.purchaseOrderCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  clearDetailCache(): void {
    this.existingDetailRecordsCache.clear();
    this.purchaseOrderDetailCache.clear();
    this.cacheDetailLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.log('🗑️ Detail cache cleared');
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

  private mapPurchaseOrderToLarkBase(purchase_order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_PURCHASE_ORDER_FIELDS.KIOTVIET_ID] = this.safeBigIntToNumber(
      purchase_order.kiotVietId,
    );

    if (purchase_order.code) {
      fields[LARK_PURCHASE_ORDER_FIELDS.PURCHASE_ORDER_CODE] =
        purchase_order.code;
    }

    if (purchase_order.retailerId) {
      fields[LARK_PURCHASE_ORDER_FIELDS.RETAILER] = '2svn';
    }

    if (purchase_order.description) {
      fields[LARK_PURCHASE_ORDER_FIELDS.DESCRIPTION] =
        purchase_order.description || '';
    }

    if (purchase_order.branchName) {
      fields[LARK_PURCHASE_ORDER_FIELDS.BRANCH_NAME] =
        purchase_order.branchName || '';
    }

    if (purchase_order.purchaseDate) {
      fields[LARK_PURCHASE_ORDER_FIELDS.PURCHASE_DATE] = new Date(
        purchase_order.purchaseDate,
      ).getTime();
    }

    if (purchase_order.discount) {
      fields[LARK_PURCHASE_ORDER_FIELDS.DISCOUNT] = Number(
        purchase_order.discount || 0,
      );
    }

    if (purchase_order.discountRatio) {
      fields[LARK_PURCHASE_ORDER_FIELDS.DISCOUNT_RATIO] = Number(
        purchase_order.discountRatio || 0,
      );
    }

    if (purchase_order.total) {
      fields[LARK_PURCHASE_ORDER_FIELDS.TOTAL] = Number(
        purchase_order.total || 0,
      );
    }

    if (purchase_order.totalPayment) {
      fields[LARK_PURCHASE_ORDER_FIELDS.TOTAL_PAYMENT] = Number(
        purchase_order.totalPayment || 0,
      );
    }

    if (purchase_order.createdDate) {
      fields[LARK_PURCHASE_ORDER_FIELDS.CREATED_DATE] = new Date(
        purchase_order.createdDate,
      ).getTime();
    }

    if (purchase_order.supplierName) {
      fields[LARK_PURCHASE_ORDER_FIELDS.SUPPLIER_NAME] =
        purchase_order.supplierName || '';
    }

    if (purchase_order.purchaseName) {
      fields[LARK_PURCHASE_ORDER_FIELDS.PURCHASE_BY_NAME] =
        purchase_order.purchaseName || '';
    }

    return fields;
  }

  private mapPurchaseOrderDetailToLarkBase(detail: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (!detail) {
      this.logger.warn('⚠️ Received undefined/null detail object');
      return fields;
    }

    if (detail.uniqueKey) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.UNIQUE_KEY] = detail.uniqueKey;
    }

    if (detail.purchaseOrderCode) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.PRIMARY_PURCHASE_ORDER_CODE] =
        detail.purchaseOrderCode;
    }

    if (detail.productCode) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.PRODUCT_CODE] =
        detail.productCode;
    }

    if (detail.productName) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.PRODUCT_NAME] =
        detail.productName;
    }

    if (detail.quantity !== null && detail.quantity !== undefined) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.QUANTITY] = Number(
        detail.quantity || 0,
      );
    }

    if (detail.discount !== null && detail.discount !== undefined) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.DISCOUNT] = Number(
        detail.discount || 0,
      );
    }

    if (detail.price !== null && detail.price !== undefined) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.UNIT_PRICE] = Number(
        detail.price || 0,
      );
    }

    if (detail.lineNumber) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.LINE_NUMBER] = Number(
        detail.lineNumber,
      );
    }

    if (detail.purchaseOrderId) {
      fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.PURCHASE_ORDER_ID] = Number(
        detail.purchaseOrderId,
      );
    }

    return fields;
  }
}
