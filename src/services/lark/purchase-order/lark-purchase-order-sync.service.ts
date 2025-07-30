import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { async, firstValueFrom, max } from 'rxjs';
import { url } from 'inspector';

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

@Injectable()
export class LarkPurchaseOrderSyncService {
  private readonly logger = new Logger(LarkPurchaseOrderSyncService.name);

  private readonly baseToken: string;
  private readonly tableId: string;

  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;

  private readonly batchSize = 100;

  private existingRecordsCache = new Map<number, string>();
  private purchaseOrderCodeCache = new Map<string, string>();

  private existingDetailRecordsCache = new Map<string, string>();
  private purchaseOrderDetailCache = new Map<string, string>();

  private cacheLoaded = false;
  private detailCacheLoaded = false;

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
        `🚀 Starting LarkBase sync for ${purchase_orders.length} purchase_orders`,
      );

      const purchaseOrdersToSync = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (purchaseOrdersToSync.length === 0) {
        this.logger.log('📋 No purchase_orders need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = purchase_orders.filter(
        (p) => p.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED purchase_orders`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn(
          '⚠️ PurchaseOrder cache loading failed - will use alternative duplicate detection',
        );
      }

      const { newPurchaseOrders, updatePurchaseOrders } =
        this.categorizePurchaseOrders(purchaseOrdersToSync);

      this.logger.log(
        `📋 PurchaseOrder Categorization: ${newPurchaseOrders.length} new, ${updatePurchaseOrders.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 50;

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

      await this.syncPurchaseOrderDetailsToLarkBase(purchaseOrdersToSync);

      await this.releaseSyncLock(lockKey);
      this.logger.log('🎉 LarkBase purchase_order and details sync completed!');
    } catch (error) {
      this.logger.error(
        `❌ Purchase order sync failed: ${error.message}`,
        error.stack,
      );
      await this.releaseSyncLock(lockKey);
      throw error;
    }
  }

  async syncPurchaseOrderDetailsToLarkBase(
    purchase_orders_detail: any[],
  ): Promise<void> {
    const lockKey = `lark_purchase_order_detail_sync_lock_${Date.now()}`;

    try {
      await this.acquireDetailSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${purchase_orders_detail.length} purchase_orders_detail`,
      );

      const purchaseOrdersDetailsToSync = purchase_orders_detail.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (purchaseOrdersDetailsToSync.length === 0) {
        this.logger.log('📋 No purchase_orders_details need LarkBase sync');
        await this.releaseDetailSyncLock(lockKey);
        return;
      }

      const pendingDetailCount = purchase_orders_detail.filter(
        (p) => p.larkSyncStatus === 'PENDING',
      ).length;
      const failedDetailCount = purchase_orders_detail.filter(
        (p) => p.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingDetailCount} PENDING + ${failedDetailCount} FAILED purchase_orders_details`,
      );

      await this.testLarkBaseDetailConnection();

      const cacheDetailLoaded = await this.loadExistingDetailRecordsWithRetry();

      if (!cacheDetailLoaded) {
        this.logger.warn(
          '⚠️ PurchaseOrderDetail cache loading failed - will use alternative duplicate detection',
        );
      }

      const { newPurchaseOrdersDetails, updatePurchaseOrdersDetails } =
        this.categorizePurchaseOrderDetails(purchaseOrdersDetailsToSync);

      this.logger.log(
        `📋 PurchaseOrderDetail Categorization: ${newPurchaseOrdersDetails.length} new, ${updatePurchaseOrdersDetails.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 50;

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
          await this.processNewPurchaseOrders(batch);
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

      await this.releaseDetailSyncLock(lockKey);
      this.logger.log('🎉 LarkBase PurchaseOrderDetail sync completed!');
    } catch (error) {
      this.logger.error(
        `❌ PurchaseOrderDetail sync failed: ${error.message}`,
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
        return false;
      }
    }
    return false;
  }

  private isCacheValid(): boolean {
    if (!this.cacheLoaded || !this.lastCacheLoadTime) {
      return false;
    }

    const now = new Date();
    const diffMinutes =
      (now.getTime() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
    return diffMinutes < this.CACHE_VALIDITY_MINUTES;
  }

  private isDetailCacheValid(): boolean {
    if (!this.detailCacheLoaded || !this.lastDetailCacheLoadTime) {
      return false;
    }

    const now = new Date();
    const diffMinutes =
      (now.getTime() - this.lastDetailCacheLoadTime.getTime()) / (1000 * 60);
    return diffMinutes < this.CACHE_VALIDITY_MINUTES;
  }

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getPurchaseOrderHeaders();
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
                record.fields[LARK_PURCHASE_ORDER_FIELDS.KIOTVIET_ID];

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
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.purchaseOrderCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async loadExistingDetailRecords(): Promise<void> {
    try {
      const headers =
        await this.larkAuthService.getPurchaseOrderDetailHeaders();
      let page_token = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
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
              // const fields = record.fields;
              const uniqueKeyRaw =
                record.fields[LARK_PURCHASE_ORDER_DETAIL_FIELDS.UNIQUE_KEY];
            }

            totalLoaded += records.length;
            page_token = response.data.data?.page_token || '';

            this.logger.debug(
              `📄 Loaded page: ${records.length} records, total: ${totalLoaded}, hasNext: ${!!page_token}`,
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

      this.detailCacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.purchaseOrderCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to load existing detail records: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  private categorizePurchaseOrders(purchase_orders: any[]): {
    newPurchaseOrders: any[];
    updatePurchaseOrders: any[];
  } {
    const newPurchaseOrders: any[] = [];
    const updatePurchaseOrders: any[] = [];

    for (const purchase_order of purchase_orders) {
      const kiotVietId = this.safeBigIntToNumber(purchase_order.kiotVietId);

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

  private categorizePurchaseOrderDetails(details: any[]): {
    newPurchaseOrdersDetails: any[];
    updatePurchaseOrdersDetails: any[];
  } {
    const newPurchaseOrdersDetails: any[] = [];
    const updatePurchaseOrdersDetails: any[] = [];

    for (const detail of details) {
      const compositeKey = this.generateDetailCompositeKey(detail);

      if (this.existingDetailRecordsCache.has(compositeKey)) {
        const existingRecordId =
          this.existingDetailRecordsCache.get(compositeKey);
        detail.larkRecordId = existingRecordId;
        updatePurchaseOrdersDetails.push(detail);
      } else {
        newPurchaseOrdersDetails.push(detail);
      }
    }

    return { newPurchaseOrdersDetails, updatePurchaseOrdersDetails };
  }

  private generateDetailCompositeKey(detail: any): string {
    const purchaseOrderCode =
      detail.purchaseOrderCode || detail.purchaseOrder?.code || '';
    const lineNumber = detail.lineNumber || 0;

    const sanitizedPOCode = String(purchaseOrderCode).trim();
    const sanitizedLineNumber = Number(lineNumber) || 0;

    const compositeKey = `${sanitizedPOCode}-${sanitizedLineNumber}`;

    this.logger.debug(
      `Generated composite key: ${compositeKey} for detail ID: ${detail.id}`,
    );

    return compositeKey;
  }

  hasExistingDetailRecord(detail: any): boolean {
    const compositeKey = this.generateDetailCompositeKey(detail);
    return this.existingDetailRecordsCache.has(compositeKey);
  }

  getExistingDetailRecordId(detail: any): string | undefined {
    const compositeKey = this.generateDetailCompositeKey(detail);
    return this.existingDetailRecordsCache.get(compositeKey);
  }

  private async processNewPurchaseOrders(
    purchase_orders: any[],
  ): Promise<void> {
    if (purchase_orders.length === 0) return;

    this.logger.log(
      `📝 Creating ${purchase_orders.length} new purchase_orders...`,
    );

    const batches = this.chunkArray(purchase_orders, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${batch.length} purchase_orders)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreatePurchaseOrders(batch);

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

  private async processNewPurchaseOrderDetails(details: any[]): Promise<void> {
    if (details.length === 0) return;

    this.logger.log(
      `📝 Creating ${details.length} new purchase order details...`,
    );

    let totalCreated = 0;
    let totalFailed = 0;

    const CREATE_CHUNK_SIZE = 100;

    const batches: any[] = [];
    for (let i = 0; i < details.length; i += CREATE_CHUNK_SIZE) {
      batches.push(details.slice(i, i + CREATE_CHUNK_SIZE));
    }

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      try {
        const result = await this.batchCreatePurchaseOrderDetails(batch);

        await this.updateDetailDatabaseStatus(result.successRecords, 'SYNCED');
        await this.updateDetailDatabaseStatus(result.failedRecords, 'FAILED');

        totalCreated += result.successRecords.length;
        totalFailed += result.failedRecords.length;

        this.logger.log(
          `📊 Batch ${i + 1}/${batches.length}: ${result.successRecords.length}/${batch.length} created`,
        );
      } catch (error) {
        this.logger.error(`❌ Batch ${i + 1} failed: ${error.message}`);
        await this.updateDetailDatabaseStatus(batch, 'FAILED');
        totalFailed += batch.length;
      }

      // Rate limiting delay
      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processUpdatePurchaseOrders(
    purchase_orders: any[],
  ): Promise<void> {
    if (purchase_orders.length === 0) return;

    this.logger.log(
      `📝 Updating ${purchase_orders.length} existing purchase_orders...`,
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
        `📝 Creating ${createFallbacks.length} purchase_orders that failed update...`,
      );
      await this.processNewPurchaseOrders(createFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  private async processUpdatePurchaseOrderDetails(
    details: any[],
  ): Promise<void> {
    if (details.length === 0) return;

    this.logger.log(
      `📝 Updating ${details.length} existing purchase order details...`,
    );

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 100; // Even smaller chunks for updates

    for (let i = 0; i < details.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = details.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (detail) => {
          try {
            const updated = await this.updateSinglePurchaseOrderDetail(detail);

            if (updated) {
              successCount++;
              await this.updateDetailDatabaseStatus([detail], 'SYNCED');
            } else {
              createFallbacks.push(detail);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for detail ${detail.productCode} in PO ${detail.purchaseOrderCode}: ${error.message}`,
            );
            createFallbacks.push(detail);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < details.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    // Process fallbacks as new records
    if (createFallbacks.length > 0) {
      this.logger.log(
        `🔄 Processing ${createFallbacks.length} update fallbacks as new detail records...`,
      );
      await this.processNewPurchaseOrderDetails(createFallbacks);
    }

    this.logger.log(
      `📝 Update complete: ${successCount} updated, ${createFallbacks.length} fallback to create`,
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
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: purchase_orders };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: purchase_orders };
      }
    }

    return { successRecords: [], failedRecords: purchase_orders };
  }

  private async batchCreatePurchaseOrderDetails(
    details: any[],
  ): Promise<BatchResult> {
    const records = details.map((detail) => ({
      fields: this.mapPurchaseOrderDetailToLarkBase(detail),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const token = await this.larkAuthService.getPurchaseOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post(
            url,
            { records },
            {
              headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
            },
          ),
        );

        const larkResponse: LarkBatchResponse = response.data;

        if (larkResponse.code === 0 && larkResponse.data?.records) {
          const successRecords = larkResponse.data.records.map(
            (record, index) => ({
              ...details[index],
              larkRecordId: record.record_id,
            }),
          );

          // Update cache with new records
          for (let i = 0; i < larkResponse.data.records.length; i++) {
            const record = larkResponse.data.records[i];
            const detail = details[i];
            const compositeKey = this.generateDetailCompositeKey(detail);
            this.existingDetailRecordsCache.set(compositeKey, record.record_id);
            this.purchaseOrderDetailCache.set(record.record_id, compositeKey);
          }

          return {
            successRecords,
            failedRecords: [],
          };
        } else {
          this.logger.error(
            `❌ LarkBase batch create failed: ${larkResponse.msg} (Code: ${larkResponse.code})`,
          );
          return {
            successRecords: [],
            failedRecords: details,
          };
        }
      } catch (error: any) {
        if (this.AUTH_ERROR_CODES.includes(error?.response?.data?.code)) {
          authRetries++;
          if (authRetries < this.MAX_AUTH_RETRIES) {
            this.logger.warn(
              `🔄 Auth error in batch create (attempt ${authRetries}/${this.MAX_AUTH_RETRIES}). Refreshing token...`,
            );
            await this.larkAuthService.forceRefreshPurchaseOrderToken();
            await new Promise((resolve) =>
              setTimeout(resolve, 1000 * authRetries),
            );
            continue;
          }
        }

        this.logger.error(
          `❌ Batch create failed: ${error.message}`,
          error.stack,
        );
        return {
          successRecords: [],
          failedRecords: details,
        };
      }
    }

    return {
      successRecords: [],
      failedRecords: details,
    };
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
            `✅ Updated record ${purchase_order.larkRecordId} for purchase_order ${purchase_order.code}`,
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
            `✅ Updated record ${detail.larkRecordId} for purchase_order_detail ${detail.code}`,
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
      await this.larkAuthService.getPurchaseOrderHeaders();
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
      .filter((id) => id !== undefined && id !== null);

    if (detailIds.length === 0) return;

    try {
      const updateData: any = {
        larkSyncStatus: status,
      };

      if (status === 'SYNCED') {
        updateData.larkSyncedAt = new Date();
        updateData.larkSyncRetries = 0;

        // Set larkRecordId for successful syncs
        for (const detail of details) {
          if (detail.id && detail.larkRecordId) {
            await this.prismaService.purchaseOrderDetail.update({
              where: { id: detail.id },
              data: {
                ...updateData,
                larkRecordId: detail.larkRecordId,
              },
            });
          }
        }
      } else if (status === 'FAILED') {
        for (const detail of details) {
          if (detail.id) {
            await this.prismaService.purchaseOrderDetail.update({
              where: { id: detail.id },
              data: {
                larkSyncStatus: 'FAILED',
                larkSyncRetries: (detail.larkSyncRetries || 0) + 1,
              },
            });
          }
        }
      }

      this.logger.debug(
        `Updated ${detailIds.length} detail records to ${status}`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to update detail database status: ${error.message}`,
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
    this.detailCacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.log('🗑️ Detail cache cleared');
  }

  getDetailCacheStats(): {
    isLoaded: boolean;
    lastLoadTime: Date | null;
    isValid: boolean;
    compositeKeysCount: number;
    recordIdsCount: number;
  } {
    return {
      isLoaded: this.detailCacheLoaded,
      lastLoadTime: this.lastCacheLoadTime,
      isValid: this.isDetailCacheValid(),
      compositeKeysCount: this.existingDetailRecordsCache.size,
      recordIdsCount: this.purchaseOrderDetailCache.size,
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

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.purchaseOrder.count();
    const synced = await this.prismaService.purchaseOrder.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.purchaseOrder.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.purchaseOrder.count({
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
}
