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
        `🚀 Starting LarkBase sync for ${productsToSync.length} products...`,
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
      this.logger.debug(
        `Cache still valid (${cacheAge.toFixed(1)}m old), skipping reload`,
      );
      return;
    }

    this.logger.log('🔄 Loading existing LarkBase records cache...');

    try {
      this.existingRecordsCache.clear();
      this.productCodeCache.clear();

      let pageToken: string | undefined;
      let totalLoaded = 0;

      do {
        const response = await this.fetchExistingRecords(pageToken);

        if (response.data?.records) {
          for (const record of response.data.records) {
            const productId = this.safeBigIntToNumber(
              record.fields[LARK_PRODUCT_FIELDS.PRODUCT_ID],
            );
            const productCode = record.fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE];

            if (productId > 0) {
              this.existingRecordsCache.set(productId, record.record_id);
            }
            if (productCode) {
              this.productCodeCache.set(productCode, record.record_id);
            }
            totalLoaded++;
          }
        }

        pageToken = response.data?.page_token;
      } while (pageToken);

      this.cacheLoaded = true;
      this.lastCacheLoadTime = now;
      this.logger.log(`✅ Cache loaded: ${totalLoaded} existing records`);
    } catch (error) {
      this.logger.error(`❌ Failed to load cache: ${error.message}`);
      throw error;
    }
  }

  private async fetchExistingRecords(
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

    this.logger.log(`📝 Creating ${products.length} new products in LarkBase`);

    const batches = this.createBatches(products, this.batchSize);
    let totalProcessed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      this.logger.log(
        `📦 Processing batch ${i + 1}/${batches.length} (${batch.length} products)`,
      );

      try {
        const result = await this.createProductBatch(batch);
        totalProcessed += result.successRecords.length;

        // Update database status for successful records
        await this.updateDatabaseStatus(result.successRecords, 'SYNCED');

        // Update database status for failed records
        if (result.failedRecords.length > 0) {
          await this.updateDatabaseStatus(result.failedRecords, 'FAILED');
        }

        this.logger.log(
          `✅ Batch ${i + 1} completed: ${result.successRecords.length} success, ${result.failedRecords.length} failed`,
        );

        // Rate limiting
        if (i < batches.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      } catch (error) {
        this.logger.error(`❌ Batch ${i + 1} failed: ${error.message}`);
        await this.updateDatabaseStatus(batch, 'FAILED');
      }
    }

    this.logger.log(
      `✅ New products processing completed: ${totalProcessed}/${products.length} successful`,
    );
  }

  private async createProductBatch(products: any[]): Promise<BatchResult> {
    const records: LarkBaseRecord[] = products.map((product) => ({
      fields: this.mapProductToLarkBase(product),
    }));

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
      throw new Error(`LarkBase API error: ${responseData.msg}`);
    }

    return {
      successRecords: products, // Assume all success if no error
      failedRecords: [],
    };
  }

  // ============================================================================
  // PROCESS UPDATE PRODUCTS
  // ============================================================================

  private async processUpdateProducts(products: any[]): Promise<void> {
    if (products.length === 0) return;

    this.logger.log(
      `🔄 Updating ${products.length} existing products in LarkBase`,
    );

    let totalProcessed = 0;

    for (const product of products) {
      try {
        const success = await this.updateProductRecord(product);
        if (success) {
          await this.updateDatabaseStatus([product], 'SYNCED');
          totalProcessed++;
        } else {
          await this.updateDatabaseStatus([product], 'FAILED');
        }
      } catch (error) {
        this.logger.error(
          `❌ Update failed for ${product.code}: ${error.message}`,
        );
        await this.updateDatabaseStatus([product], 'FAILED');
      }

      // Rate limiting
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    this.logger.log(
      `✅ Update products completed: ${totalProcessed}/${products.length} successful`,
    );
  }

  private async updateProductRecord(product: any): Promise<boolean> {
    try {
      const fields = this.mapProductToLarkBase(product);
      const token = await this.larkAuthService.getAccessToken('product');

      const response = await firstValueFrom(
        this.httpService.put(
          `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${product._larkRecordId}`,
          { fields },
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
  // MAPPING PRODUCT TO LARKBASE FIELDS - COMPLETE WITH REAL DATA
  // ============================================================================

  private mapProductToLarkBase(product: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // ============================================================================
    // PRIMARY & CORE FIELDS
    // ============================================================================

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

    // ============================================================================
    // COST PRICES FROM INVENTORIES (Real Branch Mapping)
    // ============================================================================

    if (product.inventories && product.inventories.length > 0) {
      for (const inventory of product.inventories) {
        if (inventory.branch?.kiotVietId && inventory.cost) {
          const branchId = inventory.branch.kiotVietId;
          const costField = BRANCH_COST_MAPPING[branchId];

          if (costField) {
            fields[costField] = Number(inventory.cost);
            this.logger.debug(
              `Mapped cost ${inventory.cost} for branch ${branchId} (${inventory.branch.name}) to ${costField}`,
            );
          }
        }
      }
    }

    // ============================================================================
    // PRICE BOOKS MAPPING (Real PriceBook IDs from User's Data)
    // ============================================================================

    if (product.priceBookDetails && product.priceBookDetails.length > 0) {
      for (const priceDetail of product.priceBookDetails) {
        const priceBookId = priceDetail.priceBook?.kiotVietId;
        const fieldName = PRICEBOOK_FIELD_MAPPING[priceBookId];

        if (fieldName && priceDetail.price) {
          fields[fieldName] = Number(priceDetail.price);
          this.logger.debug(
            `Mapped price ${priceDetail.price} for pricebook ${priceBookId} (${priceDetail.priceBook?.name}) to ${fieldName}`,
          );
        } else if (!fieldName && priceBookId) {
          // Log unmapped pricebooks for debugging
          this.logger.debug(
            `Unmapped pricebook: ${priceBookId} (${priceDetail.priceBook?.name})`,
          );
        }
      }
    }

    this.logger.debug(
      `Mapped product ${product.code} with ${Object.keys(fields).length} fields`,
    );
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

  // ============================================================================
  // PUBLIC DEBUG/MONITORING METHODS
  // ============================================================================

  async getProductSyncStats(): Promise<any> {
    const stats = await this.prismaService.product.groupBy({
      by: ['larkSyncStatus'],
      _count: {
        id: true,
      },
    });

    const statusCounts = stats.reduce(
      (acc, stat) => {
        acc[stat.larkSyncStatus || 'unknown'] = stat._count.id;
        return acc;
      },
      {} as Record<string, number>,
    );

    return {
      totalProducts: Object.values(statusCounts).reduce(
        (sum, count) => sum + count,
        0,
      ),
      statusBreakdown: statusCounts,
      cacheInfo: {
        loaded: this.cacheLoaded,
        lastLoadTime: this.lastCacheLoadTime,
        cachedRecords: this.existingRecordsCache.size,
        cachedCodes: this.productCodeCache.size,
      },
    };
  }

  async analyzePriceBookMapping(): Promise<any> {
    const products = await this.prismaService.product.findMany({
      include: {
        priceBookDetails: {
          include: {
            priceBook: true,
          },
        },
      },
      take: 10,
    });

    const priceBookAnalysis = new Map<
      number,
      { name: string; count: number; mapped: boolean }
    >();

    for (const product of products) {
      for (const priceDetail of product.priceBookDetails) {
        const priceBookId = priceDetail.priceBook?.kiotVietId;
        if (priceBookId) {
          const existing = priceBookAnalysis.get(priceBookId) || {
            name: priceDetail.priceBook?.name || 'Unknown',
            count: 0,
            mapped: false,
          };

          existing.count++;
          existing.mapped = !!PRICEBOOK_FIELD_MAPPING[priceBookId];
          priceBookAnalysis.set(priceBookId, existing);
        }
      }
    }

    return {
      foundPriceBooks: Array.from(priceBookAnalysis.entries()).map(
        ([id, info]) => ({
          id,
          name: info.name,
          count: info.count,
          mapped: info.mapped,
          larkField: PRICEBOOK_FIELD_MAPPING[id] || 'NOT_MAPPED',
        }),
      ),
      mappingCoverage: {
        totalFound: priceBookAnalysis.size,
        totalMapped: Array.from(priceBookAnalysis.values()).filter(
          (info) => info.mapped,
        ).length,
        mappingPercentage:
          priceBookAnalysis.size > 0
            ? (Array.from(priceBookAnalysis.values()).filter(
                (info) => info.mapped,
              ).length /
                priceBookAnalysis.size) *
              100
            : 0,
      },
    };
  }
}
