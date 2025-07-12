// src/services/kiot-viet/product/product.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import { LarkProductSyncService } from '../../lark/product/lark-product-sync.service';

interface KiotVietProduct {
  id: number;
  code: string;
  barCode?: string;
  name: string;
  fullName: string;
  categoryId?: number;
  categoryName?: string;
  tradeMarkId?: number;
  type?: number;
  description?: string;
  allowsSale?: boolean;
  hasVariants?: boolean;
  basePrice?: number;
  unit?: string;
  masterProductId?: number;
  masterUnitId?: number;
  conversionValue?: number;
  weight?: number;
  isLotSerialControl?: boolean;
  isBatchExpireControl?: boolean;
  orderTemplate?: string;
  minQuantity?: number;
  maxQuantity?: number;
  isRewardPoint?: boolean;
  isActive?: boolean;
  retailerId?: number;
  modifiedDate?: string;
  createdDate?: string;
  attributes?: Array<{
    productId: number;
    attributeName: string;
    attributeValue: string;
  }>;
  units?: Array<{
    id: number;
    code: string;
    name: string;
    fullName: string;
    unit: string;
    conversionValue: number;
    basePrice: number;
  }>;
  inventories?: Array<{
    productId: number;
    productCode: string;
    productName: string;
    branchId: number;
    branchName: string;
    cost: number;
    onHand: number;
    reserved: number;
  }>;
  priceBooks?: Array<{
    productId: number;
    priceBookId: number;
    priceBookName: string;
    price: number;
    isActive: boolean;
    startDate?: string;
    endDate?: string;
  }>;
  images?: Array<{
    image: string;
  }>;
}

@Injectable()
export class KiotVietProductService {
  private readonly logger = new Logger(KiotVietProductService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkProductSyncService: LarkProductSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  // ============================================================================
  // SYNC CONTROL & TRACKING - EXACT COPY FROM CUSTOMER
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical product sync...');
        await this.syncHistoricalProducts();
        return;
      }

      // Default: historical sync
      this.logger.log('Running default historical product sync...');
      await this.syncHistoricalProducts();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('product_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical product sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC WITH ROBUST ERROR HANDLING
  // ============================================================================

  async syncHistoricalProducts(): Promise<void> {
    const syncName = 'product_historical';

    // Declare all variables at function scope
    let currentItem = 0;
    let processedCount = 0;
    let totalProducts = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedProductIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical product sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalProducts > 0) {
          if (currentItem >= totalProducts) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalProducts} products`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalProducts) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalProducts} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const productListResponse = await this.fetchProductsListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includeInventory: true,
            includePricebook: true,
            includeSerials: true,
            includeBatchExpires: true,
            includeWarranties: true,
            includeRemoveIds: true,
            includeQuantity: true,
            includeMaterial: true,
            includeCombo: true,
          });

          // VALIDATION: Check response structure
          if (!productListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          // Reset error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // Extract data
          const { total, data: products } = productListResponse;

          if (total !== undefined && total !== null) {
            if (totalProducts === 0) {
              totalProducts = total;
              this.logger.log(`📊 Total products detected: ${totalProducts}`);
            } else if (total !== totalProducts) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalProducts} -> ${total}. Using last valid: ${lastValidTotal}`,
              );
              totalProducts = total;
            }
            lastValidTotal = total;
          }

          if (!products || !Array.isArray(products) || products.length === 0) {
            this.logger.warn('⚠️ Empty products array received');
            consecutiveEmptyPages++;

            if (totalProducts > 0 && currentItem >= totalProducts) {
              this.logger.log('✅ Reached end of data (empty page past total)');
              break;
            }

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Reached end after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          const newProducts = products.filter((product) => {
            if (processedProductIds.has(product.id)) {
              this.logger.debug(
                `⚠️ Duplicate product ID detected: ${product.id} (${product.code})`,
              );
              return false;
            }
            processedProductIds.add(product.id);
            return true;
          });

          if (newProducts.length !== products.length) {
            this.logger.warn(
              `🔄 Filtered out ${products.length - newProducts.length} duplicate products on page ${currentPage}`,
            );
          }

          if (newProducts.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all products already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newProducts.length} products from page ${currentPage}...`,
          );

          const productsWithDetails =
            await this.enrichProductsWithDetails(newProducts);
          const savedProducts =
            await this.saveProductsToDatabase(productsWithDetails);

          processedCount += savedProducts.length;
          currentItem += this.PAGE_SIZE;

          if (totalProducts > 0) {
            const completionPercentage = (processedCount / totalProducts) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalProducts} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalProducts) {
              this.logger.log('🎉 All products processed successfully!');
              break;
            }
          }

          if (savedProducts.length > 0) {
            try {
              await this.syncProductsToLarkBase(savedProducts);
              this.logger.log(
                `🚀 Synced ${savedProducts.length} products to LarkBase`,
              );
            } catch (larkError) {
              this.logger.warn(
                `⚠️ LarkBase sync failed for page ${currentPage}: ${larkError.message}`,
              );
            }
          }

          if (totalProducts > 0) {
            if (
              currentItem >= totalProducts &&
              processedCount >= totalProducts * 0.95
            ) {
              this.logger.log(
                '✅ Sync completed - reached expected data range',
              );
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ API Error (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Failed after ${MAX_CONSECUTIVE_ERROR_PAGES} consecutive API errors. Last error: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(
              `Maximum retry limit (${MAX_TOTAL_RETRIES}) exceeded`,
            );
          }

          // Exponential backoff
          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`⏳ Retrying after ${delay}ms...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      // SUCCESS: Mark sync as completed
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalProducts },
      });

      const completionRate =
        totalProducts > 0 ? (processedCount / totalProducts) * 100 : 100;

      this.logger.log(
        `✅ Historical product sync completed: ${processedCount}/${totalProducts} (${completionRate.toFixed(1)}% completion rate)`,
      );

      // this.logger.log('🚀 Starting LarkBase product sync...');
      // await this.larkProductSyncService.syncProductsToLarkBase();
    } catch (error) {
      this.logger.error(`❌ Historical product sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalProducts },
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS WITH RETRY LOGIC
  // ============================================================================

  async fetchProductsListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeInventory?: boolean;
      includePricebook?: boolean;
      includeSerials?: boolean;
      includeBatchExpires?: boolean;
      includeWarranties?: boolean;
      includeRemoveIds?: boolean;
      includeQuantity?: boolean;
      includeMaterial?: boolean;
      includeCombo?: boolean;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchProductsList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchProductsList(params: {
    currentItem?: number;
    pageSize?: number;
    orderBy?: string;
    orderDirection?: string;
    includeInventory?: boolean;
    includePricebook?: boolean;
    includeSerials?: boolean;
    includeBatchExpires?: boolean;
    includeWarranties?: boolean;
    includeRemoveIds?: boolean;
    includeQuantity?: boolean;
    includeMaterial?: boolean;
    includeCombo?: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'createdDate',
      orderDirection: params.orderDirection || 'DESC',
      includeInventory: (params.includeInventory || true).toString(),
      includePricebook: (params.includePricebook || true).toString(),
      includeSerials: (params.includeSerials || true).toString(),
      includeBatchExpires: (params.includeBatchExpires || true).toString(),
      includeWarranties: (params.includeWarranties || true).toString(),
      includeRemoveIds: (params.includeRemoveIds || true).toString(),
      includeQuantity: (params.includeQuantity || true).toString(),
      includeMaterial: (params.includeMaterial || true).toString(),
      includeCombo: (params.includeCombo || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/products?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichProductsWithDetails(
    products: KiotVietProduct[],
  ): Promise<KiotVietProduct[]> {
    this.logger.log(`🔍 Enriching ${products.length} products with details...`);

    const enrichedProducts: any[] = [];
    for (const product of products) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/products/${product.id}`, {
            headers,
          }),
        );
        if (response.data) {
          enrichedProducts.push(response.data);
        } else {
          enrichedProducts.push(product);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich product ${product.id}: ${error.message}`,
        );
        enrichedProducts.push(product);
      }
    }

    return enrichedProducts;
  }

  private async saveProductsToDatabase(
    products: KiotVietProduct[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${products.length} products to database...`);
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['product'],
        syncMode: 'historical',
        ...data,
      },
      update: data,
    });
  }
}
