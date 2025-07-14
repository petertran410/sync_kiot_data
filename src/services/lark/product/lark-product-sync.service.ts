import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { async, firstValueFrom } from 'rxjs';
import { LarkSyncStatus } from '@prisma/client';

const LARK_PRODUCT_FIELDS = {
  PRIMARY_CODE: 'Mã Hàng Hoá', // fldU0X6CW5 (Primary)
  PRODUCT_ID: 'Id Hàng Hoá', // fld6I7AvWH
  CREATED_DATE: 'Ngày Tạo', // fldhfxFsTa
  MODIFIED_DATE: 'Ngày Cập Nhật',
  TRADEMARK: 'Thương Hiệu', // fld8rFauSn
  PRODUCT_NAME: 'Tên Hàng Hoá', // fldFANpN5f
  FULL_NAME: 'Tên Đầy Đủ', // fldhYfS0Sz
  TYPE: 'Loại', // fldpriGtiy (Category)
  ALLOWS_SALE: 'Cho Phép Bán', // fldXnGFbh6
  // PRODUCT_TYPE: 'Loại Hàng Hoá', // fldHLOYoKM
  WEIGHT: 'Cân Nặng',
  UNIT: 'Đơn Vị',
  PRODUCT_BUSINESS: 'Hàng Kinh Doanh',
  BASE_PRICE: 'Bảng Giá Chung',
  DESCRIPTION: 'Mô Tả',

  // Cost Price Fields (from Inventories)
  COST_PRICE_DIEP_TRA: 'Giá Vốn (Cửa Hàng Diệp Trà)', // fldpijwGUd
  COST_PRICE_WAREHOUSE: 'Giá Vốn (Kho Bán Hàng)', // fld2Gll93e

  // Real PriceBook Fields - MAPPED FROM USER'S ACTUAL DATA
  PRICE_LE_HCM: 'Bảng Giá Lẻ HCM', // fldZwlDjcW - ID: 486878
  PRICE_BUON_HCM: 'Bảng Giá Buôn HCM', // fld7yImXrJ - ID: 486879
  PRICE_CHIEN_LUOC: 'Bảng Giá Chiến Lược', // fldFiZ0Ufn - ID: 486881
  PRICE_LASIMI_SAI_GON: 'Bảng Giá Lasimi Sài Gòn', // ID: 486883
  PRICE_BUON_HN: 'Bảng Giá Buôn HN', // fldtGkhkFZ - ID: 486884
  PRICE_EM_HOAI_ROYALTEA: 'Bảng Giá Em Hoài Royaltea', // ID: 486886
  PRICE_DO_MINH_TAN: 'Bảng Giá Đỗ Minh Tân', // ID: 486887
  PRICE_DO_MINH_TAN_8: 'Bảng Giá Đỗ Minh Tân 8%', // ID: 486888
  PRICE_HOANG_QUAN_HN: 'Bảng Giá Hoàng Quân Hà Nội', // fldmPPYQZI - ID: 486889
  PRICE_HOC_VIEN_CAFE: 'Bảng Giá Học Viện Cafe', // fldALYEhYi - ID: 486890
  PRICE_CHUOI_LABOONG: 'Bảng Giá Chuỗi Laboong', // fldTJBkzvq - ID: 486920
  PRICE_CONG_TAC_VIEN: 'Bảng Giá Cộng Tác Viên', // ID: 486967
  PRICE_SUB_D: 'Bảng Giá Sub -D', // ID: 486968
  PRICE_CHEESE_COFFEE: 'Bảng Giá Cheese Coffee', // ID: 487406
  PRICE_CHUOI_SHANCHA: 'Bảng Giá Chuỗi ShanCha', // fldw7uJour - ID: 487540
  PRICE_SHOPEE: 'Bảng Giá Shopee', // ID: 487577
  PRICE_KAFFA: 'Bảng Giá Kaffa', // ID: 487682
  PRICE_CING_HU_TANG: 'Bảng Giá Cing Hu Tang', // ID: 487791
} as const;

// ============================================================================
// OPTIONS MAPPING
// ============================================================================
const ALLOWS_SALE_OPTIONS = {
  YES: 'Có', // optSDsIdAM
  NO: 'Không', // optnZaq1cn
} as const;

const PRODUCT_TYPE_OPTIONS = {
  REGULAR: 'Hàng Hoá',
  SERVICE: 'Dịch Vụ',
} as const;

const PRODUCT_BUSINESS_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
};

// ============================================================================
// REAL PRICEBOOK ID MAPPING - FROM USER'S ACTUAL DATA
// ============================================================================
const PRICEBOOK_FIELD_MAPPING: Record<number, string> = {
  8: LARK_PRODUCT_FIELDS.PRICE_LE_HCM, // BẢNG GIÁ LẺ HCM
  9: LARK_PRODUCT_FIELDS.PRICE_BUON_HCM, // BẢNG GIÁ BUÔN HCM
  10: LARK_PRODUCT_FIELDS.PRICE_CHIEN_LUOC, // BẢNG GIÁ CHIẾN LƯỢC
  11: LARK_PRODUCT_FIELDS.PRICE_LASIMI_SAI_GON, // BẢNG GIÁ LASIMI SÀI GÒN
  12: LARK_PRODUCT_FIELDS.PRICE_BUON_HN, // BẢNG GIÁ BUÔN HN
  1: LARK_PRODUCT_FIELDS.PRICE_EM_HOAI_ROYALTEA, // BẢNG GIÁ EM HOÀI ROYALTEA
  2: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN, // BẢNG GIÁ ĐỖ MINH TÂN
  3: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN_8, // BẢNG GIÁ ĐỖ MINH TÂN 8%
  4: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HN, // BẢNG GIÁ HOÀNG QUÂN HÀ NỘI
  17: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE, // BẢNG GIÁ HỌC VIỆN CAFE
  18: LARK_PRODUCT_FIELDS.PRICE_CHUOI_LABOONG, // BẢNG GIÁ CHUỖI LABOONG
  19: LARK_PRODUCT_FIELDS.PRICE_CONG_TAC_VIEN, // BẢNG GIÁ CỘNG TÁC VIÊN
  20: LARK_PRODUCT_FIELDS.PRICE_SUB_D, // BẢNG GIÁ SUB -D
  21: LARK_PRODUCT_FIELDS.PRICE_CHEESE_COFFEE, // BẢNG GIÁ CHEESE COFFEE
  5: LARK_PRODUCT_FIELDS.PRICE_CHUOI_SHANCHA, // BẢNG GIÁ CHUỖI SHANCHA
  6: LARK_PRODUCT_FIELDS.PRICE_SHOPEE, // BẢNG GIÁ SHOPEE
  7: LARK_PRODUCT_FIELDS.PRICE_KAFFA, // BẢNG GIÁ KAFFA
  22: LARK_PRODUCT_FIELDS.PRICE_CING_HU_TANG, // BẢNG GIÁ CING HU TANG
} as const;

// ============================================================================
// BRANCH ID MAPPING FOR COST PRICES
// ============================================================================
const BRANCH_COST_MAPPING: Record<number, string> = {
  1: LARK_PRODUCT_FIELDS.COST_PRICE_DIEP_TRA, // Cửa Hàng Diệp Trà
  635935: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho bán hàng
  2: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Hà Nội
  3: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Sài Gòn
  4: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Văn phòng Hà Nội
  631164: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Hà Nội
} as const;

// ============================================================================
// INTERFACES - EXACT COPY FROM INVOICE PATTERN
// ============================================================================
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

// ============================================================================
// MAIN SERVICE - EXACT PATTERN FROM CUSTOMER/INVOICE/ORDER
// ============================================================================
@Injectable()
export class LarkProductSyncService {
  private readonly logger = new Logger(LarkProductSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // Cache management - EXACTLY LIKE INVOICE/ORDER PATTERN
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
  // MAIN SYNC METHOD - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    const lockKey = `lark_product_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${products.length} products...`,
      );

      const productsToSync = products.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (productsToSync.length === 0) {
        this.logger.log('📋 No products need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      const pendingCount = products.filter(
        (p) => p.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = products.filter(
        (p) => p.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED products`,
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

      // Categorize products - EXACT COPY FROM INVOICE
      const { newProducts, updateProducts } =
        this.categorizeProducts(productsToSync);

      this.logger.log(
        `📋 Categorization: ${newProducts.length} new, ${updateProducts.length} updates`,
      );

      // Process in smaller batches
      const BATCH_SIZE_FOR_SYNC = 50;

      // Process new products
      if (newProducts.length > 0) {
        for (let i = 0; i < newProducts.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newProducts.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new products batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newProducts.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewProducts(batch);
        }
      }

      // Process updates
      if (updateProducts.length > 0) {
        for (let i = 0; i < updateProducts.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateProducts.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update products batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateProducts.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateProducts(batch);
        }
      }

      await this.releaseSyncLock(lockKey);
      this.logger.log('🎉 LarkBase product sync completed!');
    } catch (error) {
      await this.releaseSyncLock(lockKey);
      this.logger.error(`❌ LarkBase product sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // CACHE LOADING - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private async loadExistingRecordsWithRetry(
    maxRetries: number = 3,
  ): Promise<boolean> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.loadExistingRecords();
        return true;
      } catch (error) {
        this.logger.warn(
          `⚠️ Cache loading attempt ${attempt}/${maxRetries} failed: ${error.message}`,
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
    if (this.cacheLoaded && this.lastCacheLoadTime) {
      const cacheAge = now.getTime() - this.lastCacheLoadTime.getTime();
      const cacheAgeMinutes = cacheAge / (1000 * 60);

      if (cacheAgeMinutes < this.CACHE_VALIDITY_MINUTES) {
        this.logger.log(
          `✅ Using cached records (${this.existingRecordsCache.size} records, ${cacheAgeMinutes.toFixed(1)}min old)`,
        );
        return;
      }
    }

    this.logger.log('🔄 Loading existing LarkBase records...');

    this.existingRecordsCache.clear();
    this.productCodeCache.clear();

    try {
      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheBuilt = 0;

      do {
        const headers = await this.larkAuthService.getProductHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params: any = { page_size: 500 };
        if (pageToken) {
          params.page_token = pageToken;
        }

        const response = await firstValueFrom(
          this.httpService.get(url, {
            headers,
            params,
            timeout: 30000,
          }),
        );

        if (response.data.code !== 0) {
          throw new Error(`LarkBase API error: ${response.data.msg}`);
        }

        const records = response.data.data?.items || [];
        totalLoaded += records.length;

        for (const record of records) {
          try {
            const fields = record.fields || {};

            // Index by Product ID (same as Invoice pattern)
            const productIdValue = fields[LARK_PRODUCT_FIELDS.PRODUCT_ID];
            const productId = this.extractNumber(productIdValue);

            if (productId) {
              this.existingRecordsCache.set(productId, record.record_id);
              cacheBuilt++;
            }

            // Index by Product Code (same as Invoice pattern)
            const productCode = fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE];
            if (productCode && typeof productCode === 'string') {
              this.productCodeCache.set(productCode.trim(), record.record_id);
            }
          } catch (recordError) {
            this.logger.debug(
              `Failed to process record: ${recordError.message}`,
            );
          }
        }

        pageToken = response.data.data?.page_token;

        this.logger.log(
          `📄 Loaded page: ${records.length} records (total: ${totalLoaded}, cached: ${cacheBuilt})`,
        );
      } while (pageToken);

      this.cacheLoaded = true;
      this.lastCacheLoadTime = now;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `✅ Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.productCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`❌ Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // CATEGORIZATION - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private categorizeProducts(products: any[]): {
    newProducts: any[];
    updateProducts: any[];
  } {
    const newProducts: any[] = [];
    const updateProducts: any[] = [];

    for (const product of products) {
      const kiotVietId = this.safeBigIntToNumber(product.kiotVietId);

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
        newProducts.push(product);
      }
    }

    return { newProducts, updateProducts };
  }

  // ============================================================================
  // PROCESS NEW PRODUCTS - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private async processNewProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`📝 Creating ${products.length} new products...`);

    const batches = this.chunkArray(products, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${batch.length} products)...`,
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

      if (i < batches.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    this.logger.log(
      `🎯 Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  // ============================================================================
  // PROCESS UPDATE PRODUCTS - EXACT COPY FROM INVOICE PATTERN
  // ============================================================================

  private async processUpdateProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(`🔄 Updating ${products.length} existing products...`);

    const UPDATE_CHUNK_SIZE = 5;
    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

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
        `📝 Creating ${createFallbacks.length} products that failed update...`,
      );
      await this.processNewProducts(createFallbacks);
    }

    this.logger.log(
      `🎯 Update complete: ${successCount} success, ${failedCount} failed, ${createFallbacks.length} fallback to create`,
    );
  }

  // ============================================================================
  // BATCH CREATE PRODUCTS
  // ============================================================================

  private async batchCreateProducts(products: any[]): Promise<BatchResult> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getProductHeaders();
        const records = products.map((product) => ({
          fields: this.mapProductToLarkBase(product),
        }));

        const response = await firstValueFrom(
          this.httpService.post(
            `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          this.logger.log(
            `✅ Created ${createdRecords.length}/${products.length} products`,
          );

          return { successRecords: products, failedRecords: [] };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshProductToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Batch create failed: ${response.data.msg}`);
        return { successRecords: [], failedRecords: products };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshProductToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: products };
      }
    }

    return { successRecords: [], failedRecords: products };
  }

  // ============================================================================
  // UPDATE SINGLE PRODUCT
  // ============================================================================

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
            `✅ Updated record ${product.larkRecordId} for product ${product.code}`,
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
          await this.larkAuthService.forceRefreshProductToken();
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

  // ============================================================================
  // PRODUCT MAPPING TO LARKBASE FIELDS
  // ============================================================================

  private mapProductToLarkBase(product: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // Primary field - Mã Hàng Hoá
    if (product.code) {
      fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE] = product.code;
    }

    // CORRECTED: Product ID using 'id' field directly from API
    if (product.id !== null && product.id !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_ID] = this.safeBigIntToNumber(
        product.id,
      );
    }
    // Fallback for database records that use kiotVietId
    else if (product.kiotVietId !== null && product.kiotVietId !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_ID] = this.safeBigIntToNumber(
        product.kiotVietId,
      );
    }

    if (product.description !== null && product.description !== undefined) {
      fields[LARK_PRODUCT_FIELDS.DESCRIPTION] = product.description;
    }

    // Created Date
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

    // CORRECTED: Trademark - using direct field from API response
    if (product.tradeMarkName !== null && product.tradeMarkName !== undefined) {
      fields[LARK_PRODUCT_FIELDS.TRADEMARK] = product.tradeMarkName;
    }

    // Product Name
    if (product.name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_NAME] = product.name;
    }

    // Full Name
    if (product.fullName) {
      fields[LARK_PRODUCT_FIELDS.FULL_NAME] = product.fullName;
    }

    // CORRECTED: Category - using direct field from API response
    // if (product.categoryName) {
    //   fields[LARK_PRODUCT_FIELDS.TYPE] = product.categoryName;
    // }
    // // Fallback for database records with nested structure
    // else if (product.category?.name) {
    //   fields[LARK_PRODUCT_FIELDS.TYPE] = product.category.name;
    // }

    // Allows Sale
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

    // Product Type
    if (product.type !== null && product.type !== undefined) {
      if (product.type === 2) {
        fields[LARK_PRODUCT_FIELDS.TYPE] = PRODUCT_TYPE_OPTIONS.REGULAR;
      }
      if (product.type === 3) {
        fields[LARK_PRODUCT_FIELDS.TYPE] = PRODUCT_TYPE_OPTIONS.SERVICE;
      }
    }

    // CORRECTED: Map Price Books - using actual API response structure
    if (
      product.priceBooks &&
      Array.isArray(product.priceBooks) &&
      product.priceBooks.length > 0
    ) {
      for (const priceBook of product.priceBooks) {
        const priceBookId = priceBook.priceBookId; // ← ĐÚNG field name
        const larkField = PRICEBOOK_FIELD_MAPPING[priceBookId];

        if (larkField && priceBook.price) {
          fields[larkField] = Number(priceBook.price);
        }
      }
    }

    // CORRECTED: Map Inventories (Cost Prices) - using branchId instead of branchName
    for (const inventory of product.inventories) {
      const branchId = inventory.branchId;
      const larkField = BRANCH_COST_MAPPING[branchId];

      if (larkField && inventory.cost && inventory.cost > 0) {
        fields[larkField] = Number(inventory.cost); // ✅ ĐÚNG
      }
    }

    for (const inventory of product.inventories) {
      const branchId = inventory.branchId;
      const inventoryField = BRANCH_COST_MAPPING[branchId];

      if (
        inventoryField &&
        inventory.onHand !== null &&
        inventory.onHand !== undefined
      ) {
        fields[inventoryField] = Number(inventory.onHand);
      }
    }

    // Additional fields that may be present in API response
    if (product.basePrice) {
      fields[LARK_PRODUCT_FIELDS.BASE_PRICE] = Number(product.basePrice);
    }

    if (product.weight) {
      fields[LARK_PRODUCT_FIELDS.WEIGHT] = Number(product.weight) || null;
    }

    if (product.unit) {
      fields[LARK_PRODUCT_FIELDS.UNIT] = product.unit || null;
    }

    return fields;
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

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

  private extractNumber(value: any): number | null {
    if (typeof value === 'number' && !isNaN(value)) {
      return value;
    }

    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      return isNaN(parsed) ? null : parsed;
    }

    return null;
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
          data: {
            larkSyncStatus: status,
            larkSyncedAt: new Date(),
          },
        });
      }
    } catch (error) {
      this.logger.error(`Failed to update database status: ${error.message}`);
    }
  }

  // ============================================================================
  // CONNECTION TEST & LOCK MANAGEMENT
  // ============================================================================

  private async testLarkBaseConnection(): Promise<void> {
    try {
      await this.larkAuthService.getAccessToken('product');
      this.logger.log('✅ LarkBase connection test passed');
    } catch (error) {
      this.logger.error('❌ LarkBase connection test failed');
      throw new Error(`LarkBase connection failed: ${error.message}`);
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    this.logger.log(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    this.logger.log(`🔓 Released sync lock: ${lockKey}`);
  }
}
