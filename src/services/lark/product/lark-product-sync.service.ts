import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { async, firstValueFrom } from 'rxjs';
import { LarkSyncStatus } from '@prisma/client';

const LARK_PRODUCT_FIELDS = {
  PRIMARY_CODE: 'Mã Hàng Hoá',
  PRODUCT_ID: 'Id Hàng Hoá',
  CREATED_DATE: 'Ngày Tạo',
  MODIFIED_DATE: 'Ngày Cập Nhật',
  TRADEMARK: 'Thương Hiệu',
  PRODUCT_NAME: 'Tên Hàng Hoá',
  FULL_NAME: 'Tên Đầy Đủ',
  TYPE: 'Loại',
  ALLOWS_SALE: 'Cho Phép Bán',
  WEIGHT: 'Cân Nặng',
  UNIT: 'Đơn Vị',
  PRODUCT_BUSINESS: 'Hàng Kinh Doanh',
  BASE_PRICE: 'Bảng Giá Chung',
  DESCRIPTION: 'Mô Tả',

  SOURCE: 'Nguồn Gốc',
  PRODUCTS_TYPE: 'Loại Hàng',
  SUB_CATEGORY: 'Danh Mục',

  COST_PRICE_CUA_HANG_DIEP_TRA: 'Giá Vốn (Cửa Hàng Diệp Trà)',
  COST_PRICE_KHO_HA_NOI: 'Giá Vốn (Kho Hà Nội)',
  COST_PRICE_KHO_HA_NOI_2: 'Giá Vốn (Kho Hà Nội 2)',
  COST_PRICE_KHO_SAI_GON: 'Giá Vốn (Kho Sài Gòn)',
  COST_PRICE_VAN_PHONG_HA_NOI: 'Giá Vốn (Văn Phòng Hà Nội)',
  COST_PRICE_KHO_BAN_HANG: 'Giá Vốn (Kho Bán Hàng)',

  TON_KHO_CUA_HANG_DIEP_TRA: 'Tồn Kho (Cửa Hàng Diệp Trà)',
  TON_KHO_KHO_HA_NOI: 'Tồn Kho (Kho Hà Nội)',
  TON_KHO_KHO_HA_NOI_2: 'Tồn Kho (Kho Hà Nội 2)',
  TON_KHO_KHO_SAI_GON: 'Tồn Kho (Kho Sài Gòn)',
  TON_KHO_VAN_PHONG_HA_NOI: 'Tồn Kho (Văn Phòng Hà Nội)',
  TON_KHO_KHO_BAN_HANG: 'Tồn Kho (Kho Bán Hàng)',

  PRICE_HOC_VIEN_CAFE: 'Bảng Giá Học Viện Cafe',
  PRICE_HOANG_QUAN_HN: 'Bảng Giá Hoàng Quân Hà Nội',
  PRICE_LE_HCM: 'Bảng Giá Lẻ HCM',
  PRICE_DO_MINH_TAN: 'Bảng Giá Đỗ Minh Tân',
  PRICE_DO_MINH_TAN_8: 'Bảng Giá Đỗ Minh Tân 8%',
  PRICE_SHOPEE: 'Bảng Giá Shopee',
  PRICE_CHEESE_COFFEE: 'Bảng Giá Cheese Coffee',
  PRICE_CING_HU_TANG: 'Bảng Giá Cing Hu Tang',
  PRICE_CHIEN_LUOC: 'Bảng Giá Chiến Lược',
  PRICE_BUON_HN: 'Bảng Giá Buôn HN',
  PRICE_BUON_HCM: 'Bảng Giá Buôn HCM',
  PRICE_CHUOI_LABOONG: 'Bảng Giá Chuỗi Laboong',
  PRICE_CHUOI_SHANCHA: 'Bảng Giá Chuỗi ShanCha',
  PRICE_CONG_TAC_VIEN: 'Bảng Giá Cộng Tác Viên',
  PRICE_EM_HOAI_ROYALTEA: 'Bảng Giá Em Hoài RoyalTea',
  PRICE_KAFFA: 'Bảng Giá Kaffa',
  PRICE_LASIMI_SAI_GON: 'Bảng Giá Lasimi Sài Gòn',
  PRICE_SUB_D: 'Bảng Giá SUB-D',
  PRICE_DO_DO: 'Bảng giá chuỗi Đô Đô',
  PRICE_SUNDAY_BASIC: 'Bảng giá chuỗi Sunday Basic',
  PRICE_HADILAO: 'Bảng Giá Hadilao Việt Nam',
  PRICE_TRA_NON: 'Chuỗi Lá Trà Non',
} as const;

const ALLOWS_SALE_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
} as const;

const PRODUCT_TYPE_OPTIONS = {
  REGULAR: 'Hàng Hoá',
  SERVICE: 'Dịch Vụ',
} as const;

const PRODUCT_BUSINESS_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
};

const PRICEBOOK_FIELD_MAPPING: Record<number, string> = {
  1: LARK_PRODUCT_FIELDS.PRICE_LE_HCM,
  2: LARK_PRODUCT_FIELDS.PRICE_BUON_HCM,
  3: LARK_PRODUCT_FIELDS.PRICE_CHIEN_LUOC,
  4: LARK_PRODUCT_FIELDS.PRICE_LASIMI_SAI_GON,
  5: LARK_PRODUCT_FIELDS.PRICE_BUON_HN,
  6: LARK_PRODUCT_FIELDS.PRICE_EM_HOAI_ROYALTEA,
  7: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN,
  8: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN_8,
  9: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HN,
  10: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE,
  11: LARK_PRODUCT_FIELDS.PRICE_CHUOI_LABOONG,
  12: LARK_PRODUCT_FIELDS.PRICE_CONG_TAC_VIEN,
  13: LARK_PRODUCT_FIELDS.PRICE_SUB_D,
  14: LARK_PRODUCT_FIELDS.PRICE_CHEESE_COFFEE,
  15: LARK_PRODUCT_FIELDS.PRICE_CHUOI_SHANCHA,
  16: LARK_PRODUCT_FIELDS.PRICE_SHOPEE,
  17: LARK_PRODUCT_FIELDS.PRICE_KAFFA,
  18: LARK_PRODUCT_FIELDS.PRICE_CING_HU_TANG,
  19: LARK_PRODUCT_FIELDS.PRICE_DO_DO,
  20: LARK_PRODUCT_FIELDS.PRICE_SUNDAY_BASIC,
  21: LARK_PRODUCT_FIELDS.PRICE_HADILAO,
  22: LARK_PRODUCT_FIELDS.PRICE_TRA_NON,
} as const;

const BRANCH_COST_MAPPING: Record<number, string> = {
  635934: LARK_PRODUCT_FIELDS.COST_PRICE_CUA_HANG_DIEP_TRA,
  154833: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI,
  402819: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_SAI_GON,
  631164: LARK_PRODUCT_FIELDS.COST_PRICE_VAN_PHONG_HA_NOI,
  631163: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI_2,
  635935: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_BAN_HANG,
} as const;

const BRANCH_INVENTORY_MAPPING: Record<number, string> = {
  635934: LARK_PRODUCT_FIELDS.TON_KHO_CUA_HANG_DIEP_TRA,
  154833: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI,
  402819: LARK_PRODUCT_FIELDS.TON_KHO_KHO_SAI_GON,
  631164: LARK_PRODUCT_FIELDS.TON_KHO_VAN_PHONG_HA_NOI,
  631163: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI_2,
  635935: LARK_PRODUCT_FIELDS.TON_KHO_KHO_BAN_HANG,
} as const;

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
export class LarkProductSyncService {
  private readonly logger = new Logger(LarkProductSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 100;
  private readonly pendingCreation = new Set<number>();

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<number, string> = new Map();
  private productCodeCache: Map<string, string> = new Map();
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
      'LARK_PRODUCT_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_PRODUCT_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase product configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    const lockKey = `lark_product_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${products.length} products...`,
      );

      const productsToSync = products.filter(
        (o) => o.larkSyncStatus === 'PENDING' || o.larkSyncStatus === 'FAILED',
      );

      if (productsToSync.length === 0) {
        this.logger.log('No product need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (productsToSync.length < 5) {
        this.logger.log(
          `Small sync (${productsToSync.length} products) - using lightweight mode`,
        );
        await this.syncWithoutCache(productsToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = products.filter(
        (o) => o.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = products.filter(
        (o) => o.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `Including: ${pendingCount} PENDING + ${failedCount} FAILED products`,
      );

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn(
          'Cache loading failed - will use alternative duplicate detection',
        );
        await this.syncWithoutCache(productsToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      const { newProducts, updateProducts } =
        await this.categorizeProducts(productsToSync);

      this.logger.log(
        `Categorization: ${newProducts.length} new, ${updateProducts.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 100;

      if (newProducts.length > 0) {
        for (let i = 0; i < newProducts.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newProducts.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new products batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newProducts.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewProducts(batch);
        }
      }

      if (updateProducts.length > 0) {
        for (let i = 0; i < updateProducts.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateProducts.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update products batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateProducts.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateProducts(batch);
        }
      }

      this.logger.log('LarkBase product sync completed!');
    } catch (error) {
      this.logger.error(`LarkBase product sync failed: ${error.message}`);
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

        if (this.isCacheValid()) {
          this.logger.log('Using existing valid cache');
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
      const headers = await this.larkAuthService.getProductHeaders();
      let pageToken = '';
      let totalLoaded = 0;
      let cacheBuilt = 0;
      const pageSize = 100;

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
            const records = response.data.data?.items || [];

            for (const record of records) {
              const kiotVietIdField =
                record.fields[LARK_PRODUCT_FIELDS.PRODUCT_ID];

              if (kiotVietIdField) {
                const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
                if (kiotVietId > 0) {
                  this.existingRecordsCache.set(kiotVietId, record.record_id);
                  cacheBuilt++;
                }
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
            //         cacheBuilt++;
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

            const productCodeField =
              record.fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE];
            if (productCodeField) {
              this.productCodeCache.set(
                String(productCodeField).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token || '';

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
        `Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.productCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeProducts(products: any[]): Promise<any> {
    const newProducts: any[] = [];
    const updateProducts: any[] = [];

    const duplicateDetected = products.filter((product) => {
      const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
      return this.existingRecordsCache.has(kiotVietId);
    });

    if (duplicateDetected.length > 0) {
      this.logger.warn(
        `Detected ${duplicateDetected.length} products already in cache: ${duplicateDetected
          .map((o) => o.kiotVietId)
          .slice(0, 5)
          .join(', ')}`,
      );
    }

    for (const product of products) {
      const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(`Product ${kiotVietId} is pending creation, skipping`);
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && product.code) {
        existingRecordId = this.productCodeCache.get(
          String(product.code).trim(),
        );
      }

      if (existingRecordId) {
        updateProducts.push({
          ...product,
          larkRecordId: existingRecordId,
        });
      } else {
        this.pendingCreation.add(kiotVietId);
        newProducts.push(product);
      }
    }

    return { newProducts, updateProducts };
  }

  private async syncWithoutCache(products: any[]): Promise<void> {
    this.logger.log(`Running lightweight sync without full cache...`);

    const existingProducts = await this.prismaService.product.findMany({
      where: {
        kiotVietId: { in: products.map((p) => p.kiotVietId) },
      },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingProducts.forEach((p) => {
      if (p.larkRecordId) {
        quickCache.set(Number(p.kiotVietId), p.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newProducts, updateProducts } =
        await this.categorizeProducts(products);

      if (newProducts.length > 0) {
        await this.processNewProducts(newProducts);
      }

      if (updateProducts.length > 0) {
        await this.processUpdateProducts(updateProducts);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`Creating ${products.length} new products...`);

    const batches = this.chunkArray(products, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const product of batch) {
        const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(product);
        } else {
          this.logger.warn(
            `Skipping duplicate product ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all products already exist`);
        continue;
      }

      this.logger.log(
        `Creating batch ${i + 1}/${batch.length} (${verifiedBatch.length} products)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateProducts(batch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');
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

  private async processUpdateProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`Updating ${products.length} existing products...`);

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 5;

    for (let i = 0; i < products.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = products.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (product) => {
          try {
            const updated = await this.updateSingleProduct(product);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([product], 'SYNCED');
            } else {
              createFallbacks.push(product);
            }
          } catch (error) {
            this.logger.warn(
              `Update failed for ${product.code}: ${error.message}`,
            );
            createFallbacks.push(product);
          }
        }),
      );

      if (i + UPDATE_CHUNK_SIZE < products.length) {
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    }

    if (createFallbacks.length > 0) {
      this.logger.log(
        `Creating ${createFallbacks.length} products that failed update...`,
      );
      await this.processNewProducts(createFallbacks);
    }

    this.logger.log(
      `Update complete: ${successCount} success, ${failedCount} failed`,
    );
  }

  private async batchCreateProducts(products: any[]): Promise<BatchResult> {
    const records = products.map((product) => ({
      fields: this.mapProductToLarkBase(product),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getProductHeaders();
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
          const successRecords = products.slice(0, successCount);
          const failedRecords = products.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const product = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            successRecords.forEach((product) => {
              const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            failedRecords.forEach((product) => {
              const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
              this.pendingCreation.delete(kiotVietId);
            });

            if (product.code) {
              this.productCodeCache.set(
                String(product.code).trim(),
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
        return { successRecords: [], failedRecords: products };
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

        return { successRecords: [], failedRecords: products };
      }
    }

    return { successRecords: [], failedRecords: products };
  }

  private async updateSingleProduct(product: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getProductHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${product.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapProductToLarkBase(product) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${product.larkRecordId} for product ${product.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshProductToken();
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
          this.logger.warn(`Record not found: ${product.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private mapProductToLarkBase(product: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (product.code) {
      fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE] = product.code;
    }

    if (product.kiotVietId !== null && product.kiotVietId !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_ID] = this.safeBigIntToNumber(
        product.kiotVietId,
      );
    }

    if (product.description !== null && product.description !== undefined) {
      fields[LARK_PRODUCT_FIELDS.DESCRIPTION] = product.description;
    }

    if (product.createdDate) {
      fields[LARK_PRODUCT_FIELDS.CREATED_DATE] = new Date(
        product.createdDate,
      ).getTime();
    }

    if (product.modifiedDate) {
      fields[LARK_PRODUCT_FIELDS.MODIFIED_DATE] = new Date(
        product.modifiedDate,
      ).getTime();
    }

    if (product.tradeMarkName !== null && product.tradeMarkName !== undefined) {
      fields[LARK_PRODUCT_FIELDS.TRADEMARK] = product.tradeMarkName;
    }

    if (product.name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_NAME] = product.name;
    }

    if (product.fullName) {
      fields[LARK_PRODUCT_FIELDS.FULL_NAME] = product.fullName;
    }

    if (product.allowsSale !== null && product.allowsSale !== undefined) {
      fields[LARK_PRODUCT_FIELDS.ALLOWS_SALE] = product.allowsSale
        ? ALLOWS_SALE_OPTIONS.YES
        : ALLOWS_SALE_OPTIONS.NO;
    }

    if (product.isActive !== null && product.isActive !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_BUSINESS] = product.isActive
        ? PRODUCT_BUSINESS_OPTIONS.YES
        : PRODUCT_BUSINESS_OPTIONS.NO;
    }

    if (product.type !== null && product.type !== undefined) {
      const typeMapping = {
        2: PRODUCT_TYPE_OPTIONS.REGULAR,
        3: PRODUCT_TYPE_OPTIONS.SERVICE,
      };
      fields[LARK_PRODUCT_FIELDS.TYPE] = typeMapping[product.type] || null;
    }

    if (product.priceBooks && product.priceBooks.length > 0) {
      for (const priceBook of product.priceBooks) {
        const priceBookId = priceBook.priceBookId;

        const priceBookMapping = {
          1: LARK_PRODUCT_FIELDS.PRICE_LE_HCM,
          2: LARK_PRODUCT_FIELDS.PRICE_BUON_HCM,
          3: LARK_PRODUCT_FIELDS.PRICE_CHIEN_LUOC,
          4: LARK_PRODUCT_FIELDS.PRICE_LASIMI_SAI_GON,
          5: LARK_PRODUCT_FIELDS.PRICE_BUON_HN,
          6: LARK_PRODUCT_FIELDS.PRICE_EM_HOAI_ROYALTEA,
          7: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN,
          8: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN_8,
          9: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HN,
          10: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE,
          11: LARK_PRODUCT_FIELDS.PRICE_CHUOI_LABOONG,
          12: LARK_PRODUCT_FIELDS.PRICE_CONG_TAC_VIEN,
          13: LARK_PRODUCT_FIELDS.PRICE_SUB_D,
          14: LARK_PRODUCT_FIELDS.PRICE_CHEESE_COFFEE,
          15: LARK_PRODUCT_FIELDS.PRICE_CHUOI_SHANCHA,
          16: LARK_PRODUCT_FIELDS.PRICE_SHOPEE,
          17: LARK_PRODUCT_FIELDS.PRICE_KAFFA,
          18: LARK_PRODUCT_FIELDS.PRICE_CING_HU_TANG,
          19: LARK_PRODUCT_FIELDS.PRICE_DO_DO,
          20: LARK_PRODUCT_FIELDS.PRICE_SUNDAY_BASIC,
          21: LARK_PRODUCT_FIELDS.PRICE_HADILAO,
          22: LARK_PRODUCT_FIELDS.PRICE_TRA_NON,
        };

        const larkField = priceBookMapping[priceBookId];
        if (larkField && larkField !== 'undefined') {
          fields[larkField] = Number(priceBook.price) || 0;
        }
      }
    }

    if (product.inventories && product.inventories.length > 0) {
      for (const inventory of product.inventories) {
        const branchId = inventory.branchId;

        const branchCostMapping = {
          635934: LARK_PRODUCT_FIELDS.COST_PRICE_CUA_HANG_DIEP_TRA,
          154833: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI,
          402819: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_SAI_GON,
          631164: LARK_PRODUCT_FIELDS.COST_PRICE_VAN_PHONG_HA_NOI,
          631163: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI_2,
          635935: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_BAN_HANG,
        };

        const branchInventoryMapping = {
          635934: LARK_PRODUCT_FIELDS.TON_KHO_CUA_HANG_DIEP_TRA,
          154833: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI,
          402819: LARK_PRODUCT_FIELDS.TON_KHO_KHO_SAI_GON,
          631164: LARK_PRODUCT_FIELDS.TON_KHO_VAN_PHONG_HA_NOI,
          631163: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI_2,
          635935: LARK_PRODUCT_FIELDS.TON_KHO_KHO_BAN_HANG,
        };

        const costField = branchCostMapping[branchId];
        if (costField) {
          fields[costField] = Number(inventory.cost) || 0;
        }

        const inventoryField = branchInventoryMapping[branchId];
        if (inventoryField) {
          fields[inventoryField] = Number(inventory.onHand) || 0;
        }
      }
    }

    if (product.basePrice) {
      fields[LARK_PRODUCT_FIELDS.BASE_PRICE] = Number(product.basePrice);
    }

    if (product.weight) {
      fields[LARK_PRODUCT_FIELDS.WEIGHT] = Number(product.weight) || null;
    }

    if (product.unit) {
      fields[LARK_PRODUCT_FIELDS.UNIT] = product.unit || null;
    }

    if (product.parent_name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCTS_TYPE] = product.parent_name || null;
    }

    if (product.child_name) {
      fields[LARK_PRODUCT_FIELDS.SOURCE] = product.child_name || null;
    }

    if (product.branch_name) {
      fields[LARK_PRODUCT_FIELDS.SUB_CATEGORY] = product.branch_name || null;
    }

    return fields;
  }

  private safeBigIntToNumber(value: any): number {
    if (typeof value === 'bigint') {
      return Number(value);
    }
    if (typeof value === 'number') {
      return value;
    }
    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
  }

  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  private async updateDatabaseStatus(
    products: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (products.length === 0) return;

    const productIds = products.map((p) => p.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.product.updateMany({
      where: { id: { in: productIds } },
      data: updateData,
    });
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getProductHeaders();
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
    const syncName = 'product_lark_sync';

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
        entities: ['product'],
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
        name: 'product_lark_sync',
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
      await this.larkAuthService.getProductHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.productCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }
}
