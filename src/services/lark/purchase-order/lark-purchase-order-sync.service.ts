import { Inject, Injectable, Logger } from '@nestjs/common';
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
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá',
  TOTAL: 'Giá Trị Nhập Hàng',
  TOTAL_PAYMENT: 'Tiền Đã Trả Hàng',
  CREATED_DATE: 'Ngày Tạo',
  SUPPLIER_NAME: 'Tên Nhà Cung Cấp',
  PURCHASE_BY_NAME: 'Người Nhập',
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
  private readonly batchSize = 100;

  private existingRecordsCache = new Map<number, string>();
  private purchaseOrderCodeCache = new Map<string, string>();
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
      'LARK_PURCHASE_ORDER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_PURCHASE_ORDER_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase purchase_order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
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
          '⚠️ Cache loading failed - will use alternative duplicate detection',
        );
      }

      const { newPurchaseOrders, updatePurchaseOrders } =
        this.categorizePurchaseOrders(purchaseOrdersToSync);

      this.logger.log(
        `📋 Categorization: ${newPurchaseOrders.length} new, ${updatePurchaseOrders.length} updates`,
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

      await this.releaseSyncLock(lockKey);
      this.logger.log('🎉 LarkBase purchase_order sync completed!');
    } catch (error) {
      this.logger.error(
        `💥 LarkBase purchase_order sync failed: ${error.message}`,
      );
      await this.releaseSyncLock(lockKey);
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

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.purchaseOrderCodeCache.clear();
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
