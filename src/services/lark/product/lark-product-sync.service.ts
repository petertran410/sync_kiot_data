// src/services/lark/product/lark-product-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';
import { LarkSyncStatus } from '@prisma/client';

// ✅ EXTRACTED từ product.rtf - LarkBase Product Field Mapping
const LARK_PRODUCT_FIELDS = {
  // Primary & Core Fields
  PRIMARY_CODE: 'Mã Hàng Hoá', // fldU0X6CW5 (Primary)
  PRODUCT_ID: 'Id Hàng Hoá', // fld6I7AvWH
  CREATED_DATE: 'Ngày Tạo', // fldhfxFsTa
  TRADEMARK: 'Thương Hiệu', // fld8rFauSn
  PRODUCT_NAME: 'Tên Hàng Hoá', // fldFANpN5f
  FULL_NAME: 'Tên Đầy Đủ', // fldhYfS0Sz
  TYPE: 'Loại', // fldpriGtiy
  ALLOWS_SALE: 'Cho Phép Bán', // fldXnGFbh6
  PRODUCT_TYPE: 'Loại Hàng Hoá', // fldHLOYoKM

  // Cost Price Fields
  COST_PRICE_DIEP_TRA: 'Giá Vốn (Cửa Hàng Diệp Trà)', // fldpijwGUd
  COST_PRICE_WAREHOUSE: 'Giá Vốn (Kho Bán Hàng)', // fld2Gll93e

  // Price Book Fields
  PRICE_STRATEGIC: 'Bảng Giá Chiến Lược', // fldFiZ0Ufn
  PRICE_WHOLESALE_HN: 'Bảng Giá Buôn HN', // fldtGkhkFZ
  PRICE_WHOLESALE_HCM: 'Bảng Giá Buôn HCM', // fld7yImXrJ
  PRICE_LABOONG_CHAIN: 'Bảng Giá Chuỗi Laboong', // fldTJBkzvq
  PRICE_SHANCHA_CHAIN: 'Bảng Giá Chuỗi ShanCha', // fldw7uJour
  PRICE_CAFE_ACADEMY: 'Bảng Giá Học Viện Cafe', // fldALYEhYi
  PRICE_HOANG_QUAN_HN: 'Bảng Giá Hoàng Quân Hà Nội', // fldmPPYQZI
  PRICE_RETAIL_HCM: 'Bảng Giá Lẻ HCM', // fldZwlDjcW
} as const;

// Options Mapping
const ALLOWS_SALE_OPTIONS = {
  YES: 'Có', // optSDsIdAM
  NO: 'Không', // optnZaq1cn
} as const;

const PRODUCT_TYPE_OPTIONS = {
  REGULAR: 'Hàng Hoá Thường', // optRT63nW3
  MANUFACTURED: 'Hàng Hoá Sản Xuất', // opttT8hnTl
  SERVICE: 'Dịch Vụ', // optoHK5n9S
} as const;

// PriceBook ID to Field Mapping (based on KiotViet PriceBook structure)
const PRICEBOOK_FIELD_MAPPING = {
  1: LARK_PRODUCT_FIELDS.PRICE_STRATEGIC, // Bảng Giá Chiến Lược
  2: LARK_PRODUCT_FIELDS.PRICE_WHOLESALE_HN, // Bảng Giá Buôn HN
  3: LARK_PRODUCT_FIELDS.PRICE_WHOLESALE_HCM, // Bảng Giá Buôn HCM
  4: LARK_PRODUCT_FIELDS.PRICE_LABOONG_CHAIN, // Bảng Giá Chuỗi Laboong
  5: LARK_PRODUCT_FIELDS.PRICE_SHANCHA_CHAIN, // Bảng Giá Chuỗi ShanCha
  6: LARK_PRODUCT_FIELDS.PRICE_CAFE_ACADEMY, // Bảng Giá Học Viện Cafe
  7: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HN, // Bảng Giá Hoàng Quân Hà Nội
  8: LARK_PRODUCT_FIELDS.PRICE_RETAIL_HCM, // Bảng Giá Lẻ HCM
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
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // Cache management
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

  // ============================================================================
  // MAIN SYNC METHOD WITH IMPROVED ERROR HANDLING
  // ============================================================================

  async syncProductsToLarkBase(products?: any[]): Promise<void> {
    const lockKey = `lark_product_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      let productsToSync = products;

      if (!productsToSync) {
        this.logger.log(
          '🔍 Fetching products from database for LarkBase sync...',
        );

        productsToSync = await this.prismaService.product.findMany({
          where: {
            larkSyncStatus: {
              in: ['PENDING', 'FAILED'],
            },
          },
          include: {
            tradeMark: true,
            category: true,
            inventories: {
              include: {
                branch: true,
              },
            },
            priceBookDetails: {
              include: {
                priceBook: true,
              },
            },
          },
          orderBy: {
            createdDate: 'desc',
          },
        });
      }

      this.logger.log(
        `🚀 Starting LarkBase sync for ${productsToSync.length} products (ENHANCED MODE)...`,
      );

      if (productsToSync.length === 0) {
        this.logger.log('📋 No products need LarkBase sync');
        return;
      }

      // Load existing records cache
      await this.loadExistingRecordsCache();

      // Categorize products
      const { newProducts, existingProducts } =
        await this.categorizeProducts(productsToSync);

      this.logger.log(
        `📊 Sync breakdown: ${newProducts.length} new, ${existingProducts.length} existing`,
      );

      // Process new products
      if (newProducts.length > 0) {
        await this.processNewProducts(newProducts);
      }

      // Process existing products
      if (existingProducts.length > 0) {
        await this.processUpdateProducts(existingProducts);
      }

      this.logger.log('✅ LarkBase product sync completed successfully');
    } catch (error) {
      this.logger.error(`❌ LarkBase product sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  // ============================================================================
  // CACHE MANAGEMENT
  // ============================================================================

  private async loadExistingRecordsCache(): Promise<void> {
    const now = new Date();
    const cacheAge = this.lastCacheLoadTime
      ? (now.getTime() - this.lastCacheLoadTime.getTime()) / (1000 * 60)
      : Infinity;

    if (this.cacheLoaded && cacheAge < this.CACHE_VALIDITY_MINUTES) {
      this.logger.log(
        `📋 Using cached records (age: ${cacheAge.toFixed(1)}min)`,
      );
      return;
    }

    this.logger.log('🔄 Loading existing LarkBase records...');

    try {
      this.existingRecordsCache.clear();
      this.productCodeCache.clear();

      const allRecords = await this.fetchAllExistingRecords();

      for (const record of allRecords) {
        const productId = record.fields[LARK_PRODUCT_FIELDS.PRODUCT_ID];
        const productCode = record.fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE];

        if (productId) {
          this.existingRecordsCache.set(Number(productId), record.record_id);
        }

        if (productCode) {
          this.productCodeCache.set(productCode, record.record_id);
        }
      }

      this.cacheLoaded = true;
      this.lastCacheLoadTime = now;

      this.logger.log(
        `✅ Loaded ${this.existingRecordsCache.size} existing records into cache`,
      );
    } catch (error) {
      this.logger.error(`Failed to load existing records: ${error.message}`);
      throw error;
    }
  }

  private async fetchAllExistingRecords(): Promise<LarkBaseRecord[]> {
    const allRecords: LarkBaseRecord[] = [];
    let pageToken: string | undefined = undefined;

    do {
      const response = await this.fetchRecordsPage(pageToken);

      if (response.data?.items) {
        allRecords.push(...response.data.items);
      }

      pageToken = response.data?.page_token;

      if (pageToken) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    } while (pageToken);

    return allRecords;
  }

  private async fetchRecordsPage(
    pageToken?: string,
  ): Promise<LarkBatchResponse> {
    const token = await this.larkAuthService.getAccessToken('product');

    const params = new URLSearchParams({
      page_size: '500',
      field_names: JSON.stringify([
        LARK_PRODUCT_FIELDS.PRODUCT_ID,
        LARK_PRODUCT_FIELDS.PRIMARY_CODE,
      ]),
    });

    if (pageToken) {
      params.append('page_token', pageToken);
    }

    const response = await firstValueFrom(
      this.httpService.get(
        `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?${params}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
          timeout: 30000,
        },
      ),
    );

    return response.data;
  }

  // ============================================================================
  // PRODUCT CATEGORIZATION
  // ============================================================================

  private async categorizeProducts(
    products: any[],
  ): Promise<{ newProducts: any[]; existingProducts: any[] }> {
    const newProducts: any[] = [];
    const existingProducts: any[] = [];

    for (const product of products) {
      const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);
      const recordId =
        this.existingRecordsCache.get(kiotVietId) ||
        this.productCodeCache.get(product.code);

      if (recordId) {
        product._larkRecordId = recordId;
        existingProducts.push(product);
      } else {
        newProducts.push(product);
      }
    }

    return { newProducts, existingProducts };
  }

  // ============================================================================
  // PROCESS NEW PRODUCTS
  // ============================================================================

  private async processNewProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`📝 Creating ${products.length} new products...`);

    const batches = this.createBatches(products, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      try {
        const result = await this.batchCreateProducts(batch);
        const successCount = result.successRecords.length;
        const failedCount = result.failedRecords.length;

        totalCreated += successCount;
        totalFailed += failedCount;

        if (successCount > 0) {
          await this.updateDatabaseStatus(result.successRecords, 'SYNCED');
        }

        if (failedCount > 0) {
          await this.updateDatabaseStatus(result.failedRecords, 'FAILED');
        }

        this.logger.log(
          `📊 Batch ${i + 1}/${batches.length}: ${successCount}/${batch.length} created`,
        );

        if (i < batches.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      } catch (error) {
        this.logger.error(`Failed to process batch ${i + 1}: ${error.message}`);
        await this.updateDatabaseStatus(batch, 'FAILED');
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  // ============================================================================
  // PROCESS UPDATE PRODUCTS
  // ============================================================================

  private async processUpdateProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`📝 Updating ${products.length} existing products...`);

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

    // Process fallbacks as new products
    if (createFallbacks.length > 0) {
      this.logger.log(
        `🔄 Processing ${createFallbacks.length} update fallbacks as new products...`,
      );
      await this.processNewProducts(createFallbacks);
    }

    this.logger.log(
      `📝 Update complete: ${successCount} updated, ${createFallbacks.length} fallback to create`,
    );
  }

  private async batchCreateProducts(products: any[]): Promise<BatchResult> {
    const records = products.map((product) => ({
      fields: this.mapProductToLarkBase(product),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const token = await this.larkAuthService.getAccessToken('product');

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
            { records },
            {
              headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
              timeout: 60000,
            },
          ),
        );

        const responseData: LarkBatchResponse = response.data;

        if (responseData.code !== 0) {
          if (this.AUTH_ERROR_CODES.includes(responseData.code)) {
            authRetries++;
            this.logger.warn(
              `Auth error ${responseData.code}, retrying... (${authRetries}/${this.MAX_AUTH_RETRIES})`,
            );
            await this.larkAuthService.refreshProductToken('product');
            continue;
          }

          throw new Error(`LarkBase API error: ${responseData.msg}`);
        }

        const successRecords = responseData.data?.records || [];
        const failedRecords = products.slice(successRecords.length);

        return {
          successRecords: products.slice(0, successRecords.length),
          failedRecords,
        };
      } catch (error) {
        if (
          error.response?.data?.code &&
          this.AUTH_ERROR_CODES.includes(error.response.data.code)
        ) {
          authRetries++;
          this.logger.warn(
            `Auth error, retrying... (${authRetries}/${this.MAX_AUTH_RETRIES})`,
          );
          await this.larkAuthService.refreshToken('product');
          continue;
        }

        throw error;
      }
    }

    throw new Error('Max auth retries exceeded');
  }

  private async updateSingleProduct(product: any): Promise<boolean> {
    try {
      const token = await this.larkAuthService.getAccessToken('product');
      const recordId = product._larkRecordId;

      if (!recordId) {
        this.logger.warn(`No record ID for product ${product.code}`);
        return false;
      }

      const response = await firstValueFrom(
        this.httpService.put(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${recordId}`,
          {
            fields: this.mapProductToLarkBase(product),
          },
          {
            headers: {
              Authorization: `Bearer ${token}`,
              'Content-Type': 'application/json',
            },
            timeout: 30000,
          },
        ),
      );

      const responseData: LarkBatchResponse = response.data;

      if (responseData.code !== 0) {
        this.logger.warn(
          `Update failed for ${product.code}: ${responseData.msg}`,
        );
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Update error for ${product.code}: ${error.message}`);
      return false;
    }
  }

  // ============================================================================
  // MAPPING PRODUCT TO LARKBASE FIELDS
  // ============================================================================

  private mapProductToLarkBase(product: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // Primary field - Mã Hàng Hoá
    if (product.code) {
      fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE] = product.code;
    }

    // Product ID
    if (product.kiotVietId !== null && product.kiotVietId !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_ID] = this.safeBigIntToNumber(
        product.kiotVietId,
      );
    }

    // Created Date
    if (product.createdDate) {
      fields[LARK_PRODUCT_FIELDS.CREATED_DATE] = this.formatDateForLark(
        product.createdDate,
      );
    }

    // Trademark
    if (product.tradeMark?.name) {
      fields[LARK_PRODUCT_FIELDS.TRADEMARK] = product.tradeMark.name;
    }

    // Product Name
    if (product.name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_NAME] = product.name;
    }

    // Full Name
    if (product.fullName) {
      fields[LARK_PRODUCT_FIELDS.FULL_NAME] = product.fullName;
    }

    // Type (category or other type information)
    if (product.category?.name) {
      fields[LARK_PRODUCT_FIELDS.TYPE] = product.category.name;
    }

    // Allows Sale
    if (product.allowsSale !== null && product.allowsSale !== undefined) {
      fields[LARK_PRODUCT_FIELDS.ALLOWS_SALE] = product.allowsSale
        ? ALLOWS_SALE_OPTIONS.YES
        : ALLOWS_SALE_OPTIONS.NO;
    }

    // Product Type mapping
    if (product.type !== null && product.type !== undefined) {
      const typeMapping = {
        1: PRODUCT_TYPE_OPTIONS.REGULAR, // Combo
        2: PRODUCT_TYPE_OPTIONS.REGULAR, // Regular
        3: PRODUCT_TYPE_OPTIONS.SERVICE, // Service
      };
      fields[LARK_PRODUCT_FIELDS.PRODUCT_TYPE] =
        typeMapping[product.type] || PRODUCT_TYPE_OPTIONS.REGULAR;
    }

    // Cost Prices from Inventories
    if (product.inventories && product.inventories.length > 0) {
      for (const inventory of product.inventories) {
        if (inventory.branch) {
          // Map branch-specific costs
          if (inventory.branch.kiotVietId === 1) {
            // Cửa Hàng Diệp Trà
            fields[LARK_PRODUCT_FIELDS.COST_PRICE_DIEP_TRA] = Number(
              inventory.cost || 0,
            );
          } else if (
            inventory.branch.kiotVietId === 2 ||
            inventory.branch.kiotVietId === 3
          ) {
            // Warehouses
            fields[LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE] = Number(
              inventory.cost || 0,
            );
          }
        }
      }
    }

    // Price Books mapping
    if (product.priceBookDetails && product.priceBookDetails.length > 0) {
      for (const priceDetail of product.priceBookDetails) {
        const priceBookId = priceDetail.priceBook?.kiotVietId;
        const fieldName = PRICEBOOK_FIELD_MAPPING[priceBookId];

        if (fieldName && priceDetail.price) {
          fields[fieldName] = Number(priceDetail.price);
        }
      }
    }

    return fields;
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) {
      return 0;
    }

    if (typeof value === 'bigint') {
      return Number(value);
    }

    if (typeof value === 'number') {
      return isNaN(value) ? 0 : value;
    }

    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    try {
      const asString = String(value).trim();
      const parsed = parseInt(asString, 10);
      return isNaN(parsed) ? 0 : parsed;
    } catch {
      return 0;
    }
  }

  private formatDateForLark(date: Date | string): number {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    return Math.floor(dateObj.getTime() / 1000);
  }

  private createBatches<T>(items: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < items.length; i += batchSize) {
      batches.push(items.slice(i, i + batchSize));
    }
    return batches;
  }

  private async updateDatabaseStatus(
    products: any[],
    status: LarkSyncStatus,
  ): Promise<void> {
    if (products.length === 0) return;

    try {
      const productIds = products
        .map((p) => p.id)
        .filter((id) => id !== undefined);

      if (productIds.length > 0) {
        await this.prismaService.product.updateMany({
          where: { id: { in: productIds } },
          data: { larkSyncStatus: status },
        });
      }
    } catch (error) {
      this.logger.error(`Failed to update database status: ${error.message}`);
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    // Implement distributed lock if needed
    this.logger.log(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    // Release distributed lock if needed
    this.logger.log(`🔓 Released sync lock: ${lockKey}`);
  }
}
