// src/services/lark/product/lark-product-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';
import { LarkSyncStatus } from '@prisma/client';

// ============================================================================
// LARKBASE PRODUCT FIELD MAPPING - COMPLETE WITH REAL DATA
// ============================================================================
const LARK_PRODUCT_FIELDS = {
  // Primary & Core Fields
  PRIMARY_CODE: 'Mã Hàng Hoá', // fldU0X6CW5 (Primary)
  PRODUCT_ID: 'Id Hàng Hoá', // fld6I7AvWH
  CREATED_DATE: 'Ngày Tạo', // fldhfxFsTa
  TRADEMARK: 'Thương Hiệu', // fld8rFauSn
  PRODUCT_NAME: 'Tên Hàng Hoá', // fldFANpN5f
  FULL_NAME: 'Tên Đầy Đủ', // fldhYfS0Sz
  TYPE: 'Loại', // fldpriGtiy (Category)
  ALLOWS_SALE: 'Cho Phép Bán', // fldXnGFbh6
  PRODUCT_TYPE: 'Loại Hàng Hoá', // fldHLOYoKM

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
  REGULAR: 'Hàng Hoá Thường', // optRT63nW3
  MANUFACTURED: 'Hàng Hoá Sản Xuất', // opttT8hnTl
  SERVICE: 'Dịch Vụ', // optoHK5n9S
} as const;

// ============================================================================
// REAL PRICEBOOK ID MAPPING - FROM USER'S ACTUAL DATA
// ============================================================================
const PRICEBOOK_FIELD_MAPPING: Record<number, string> = {
  486878: LARK_PRODUCT_FIELDS.PRICE_LE_HCM, // BẢNG GIÁ LẺ HCM
  486879: LARK_PRODUCT_FIELDS.PRICE_BUON_HCM, // BẢNG GIÁ BUÔN HCM
  486881: LARK_PRODUCT_FIELDS.PRICE_CHIEN_LUOC, // BẢNG GIÁ CHIẾN LƯỢC
  486883: LARK_PRODUCT_FIELDS.PRICE_LASIMI_SAI_GON, // BẢNG GIÁ LASIMI SÀI GÒN
  486884: LARK_PRODUCT_FIELDS.PRICE_BUON_HN, // BẢNG GIÁ BUÔN HN
  486886: LARK_PRODUCT_FIELDS.PRICE_EM_HOAI_ROYALTEA, // BẢNG GIÁ EM HOÀI ROYALTEA
  486887: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN, // BẢNG GIÁ ĐỖ MINH TÂN
  486888: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN_8, // BẢNG GIÁ ĐỖ MINH TÂN 8%
  486889: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HN, // BẢNG GIÁ HOÀNG QUÂN HÀ NỘI
  486890: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE, // BẢNG GIÁ HỌC VIỆN CAFE
  486920: LARK_PRODUCT_FIELDS.PRICE_CHUOI_LABOONG, // BẢNG GIÁ CHUỖI LABOONG
  486967: LARK_PRODUCT_FIELDS.PRICE_CONG_TAC_VIEN, // BẢNG GIÁ CỘNG TÁC VIÊN
  486968: LARK_PRODUCT_FIELDS.PRICE_SUB_D, // BẢNG GIÁ SUB -D
  487406: LARK_PRODUCT_FIELDS.PRICE_CHEESE_COFFEE, // BẢNG GIÁ CHEESE COFFEE
  487540: LARK_PRODUCT_FIELDS.PRICE_CHUOI_SHANCHA, // BẢNG GIÁ CHUỖI SHANCHA
  487577: LARK_PRODUCT_FIELDS.PRICE_SHOPEE, // BẢNG GIÁ SHOPEE
  487682: LARK_PRODUCT_FIELDS.PRICE_KAFFA, // BẢNG GIÁ KAFFA
  487791: LARK_PRODUCT_FIELDS.PRICE_CING_HU_TANG, // BẢNG GIÁ CING HU TANG
} as const;

// ============================================================================
// BRANCH ID MAPPING FOR COST PRICES
// ============================================================================
const BRANCH_COST_MAPPING: Record<number, string> = {
  635934: LARK_PRODUCT_FIELDS.COST_PRICE_DIEP_TRA, // Cửa Hàng Diệp Trà
  635935: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho bán hàng
  154833: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Hà Nội
  402819: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Sài Gòn
  631163: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Văn phòng Hà Nội
  631164: LARK_PRODUCT_FIELDS.COST_PRICE_WAREHOUSE, // Kho Hà Nội
} as const;

// ============================================================================
// INTERFACES
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
// MAIN SERVICE
// ============================================================================
@Injectable()
export class LarkProductSyncService {
  private readonly logger = new Logger(LarkProductSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  // Cache management - EXACTLY LIKE INVOICE
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

      this.logger.log('✅ LarkBase product sync completed successfully');
      await this.releaseSyncLock(lockKey);
    } catch (error) {
      this.logger.error(`❌ LarkBase product sync failed: ${error.message}`);
      await this.releaseSyncLock(lockKey);
      throw error;
    }
  }

  // ============================================================================
  // CACHE LOADING WITH RETRY - EXACT COPY FROM INVOICE
  // ============================================================================

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
        await this.loadExistingRecordsCache();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `✅ Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`⚠️ Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.error(
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

    const now = new Date();
    const ageMinutes =
      (now.getTime() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
    return ageMinutes < this.CACHE_VALIDITY_MINUTES;
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.productCodeCache.clear();
    this.cacheLoaded = false;
  }

  private async loadExistingRecordsCache(): Promise<void> {
    try {
      this.logger.log('📥 Loading existing LarkBase records cache...');

      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheBuilt = 0;

      do {
        const response = await this.fetchExistingRecordsPage(pageToken);
        const records = response.data?.items || response.data?.records || [];

        totalLoaded += records.length;

        for (const record of records) {
          try {
            const productId = this.extractNumber(
              record.fields?.[LARK_PRODUCT_FIELDS.PRODUCT_ID],
            );
            const productCode =
              record.fields?.[LARK_PRODUCT_FIELDS.PRIMARY_CODE];

            if (productId) {
              this.existingRecordsCache.set(productId, record.record_id);
              cacheBuilt++;
            }

            if (productCode && typeof productCode === 'string') {
              this.productCodeCache.set(productCode.trim(), record.record_id);
            }
          } catch (error) {
            this.logger.debug(`Skipped invalid record: ${error.message}`);
          }
        }

        pageToken = response.data?.page_token;
      } while (pageToken);

      this.cacheLoaded = true;
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

  private async fetchExistingRecordsPage(
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
  // CATEGORIZATION - EXACT COPY FROM INVOICE
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
        `📊 Batch ${i + 1}/${batches.length}: Processing ${batch.length} products`,
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

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
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

    // Type (category)
    if (product.category?.name) {
      fields[LARK_PRODUCT_FIELDS.TYPE] = product.category.name;
    }

    // Allows Sale
    if (product.allowsSale !== null && product.allowsSale !== undefined) {
      fields[LARK_PRODUCT_FIELDS.ALLOWS_SALE] = product.allowsSale
        ? ALLOWS_SALE_OPTIONS.YES
        : ALLOWS_SALE_OPTIONS.NO;
    }

    // Product Type
    if (product.type !== null && product.type !== undefined) {
      switch (product.type) {
        case 1:
          fields[LARK_PRODUCT_FIELDS.PRODUCT_TYPE] =
            PRODUCT_TYPE_OPTIONS.REGULAR;
          break;
        case 2:
          fields[LARK_PRODUCT_FIELDS.PRODUCT_TYPE] =
            PRODUCT_TYPE_OPTIONS.MANUFACTURED;
          break;
        case 3:
          fields[LARK_PRODUCT_FIELDS.PRODUCT_TYPE] =
            PRODUCT_TYPE_OPTIONS.SERVICE;
          break;
      }
    }

    // Map Price Books
    if (product.priceBookDetails && product.priceBookDetails.length > 0) {
      for (const priceDetail of product.priceBookDetails) {
        const priceBookName = priceDetail.priceBook?.name;
        const larkField = PRICEBOOK_FIELD_MAPPING[priceBookName];

        if (larkField && priceDetail.price) {
          fields[larkField] = Number(priceDetail.price);
        }
      }
    }

    // Map Inventories (Cost Prices)
    if (product.inventories && product.inventories.length > 0) {
      for (const inventory of product.inventories) {
        const branchName = inventory.branch?.name;
        const larkField = BRANCH_COST_MAPPING[branchName];

        if (larkField && inventory.cost) {
          fields[larkField] = Number(inventory.cost);
        }
      }
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

  private formatDateForLark(date: Date | string): number {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    return Math.floor(dateObj.getTime() / 1000);
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
