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

    const savedProducts: any[] = [];

    for (const productData of products) {
      try {
        // Handle Category relationship
        let internalCategoryId: number | null = null;
        if (productData.categoryId) {
          const category = await this.prismaService.category.findFirst({
            where: { kiotVietId: productData.categoryId },
          });
          internalCategoryId = category?.id || null;
        }

        // Handle TradeMark relationship (NESTED DATA)
        let internalTradeMarkId: number | null = null;
        if (productData.tradeMarkId) {
          // Process nested trademark data if available
          await this.processNestedTradeMark(productData.tradeMarkId);

          const tradeMark = await this.prismaService.tradeMark.findFirst({
            where: { kiotVietId: productData.tradeMarkId },
          });
          internalTradeMarkId = tradeMark?.id || null;
        }

        // Handle Master Product relationship
        let internalMasterProductId: number | null = null;
        if (productData.masterProductId) {
          const masterProduct = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(productData.masterProductId) },
          });
          internalMasterProductId = masterProduct?.id || null;
        }

        // Core Product upsert
        const product = await this.prismaService.product.upsert({
          where: { kiotVietId: BigInt(productData.id) },
          update: {
            code: productData.code,
            barCode: productData.barCode || null,
            name: productData.name,
            fullName: productData.fullName,
            categoryId: internalCategoryId,
            tradeMarkId: internalTradeMarkId,
            type: productData.type || null,
            description: productData.description || null,
            allowsSale:
              productData.allowsSale !== undefined
                ? productData.allowsSale
                : true,
            hasVariants: productData.hasVariants || false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : new Prisma.Decimal(0),
            unit: productData.unit || null,
            masterProductId: internalMasterProductId,
            masterUnitId: productData.masterUnitId
              ? BigInt(productData.masterUnitId)
              : null,
            conversionValue: productData.conversionValue || null,
            weight: productData.weight || null,
            isLotSerialControl: productData.isLotSerialControl || false,
            isBatchExpireControl: productData.isBatchExpireControl || false,
            orderTemplate: productData.orderTemplate || null,
            minQuantity: productData.minQuantity || null,
            maxQuantity: productData.maxQuantity || null,
            isRewardPoint:
              productData.isRewardPoint !== undefined
                ? productData.isRewardPoint
                : true,
            isActive:
              productData.isActive !== undefined ? productData.isActive : true,
            retailerId: productData.retailerId || null,
            modifiedDate: productData.modifiedDate
              ? new Date(productData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
          create: {
            kiotVietId: BigInt(productData.id),
            code: productData.code,
            barCode: productData.barCode || null,
            name: productData.name,
            fullName: productData.fullName,
            categoryId: internalCategoryId,
            tradeMarkId: internalTradeMarkId,
            type: productData.type || null,
            description: productData.description || null,
            allowsSale:
              productData.allowsSale !== undefined
                ? productData.allowsSale
                : true,
            hasVariants: productData.hasVariants || false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : new Prisma.Decimal(0),
            unit: productData.unit || null,
            masterProductId: internalMasterProductId,
            masterUnitId: productData.masterUnitId
              ? BigInt(productData.masterUnitId)
              : null,
            conversionValue: productData.conversionValue || null,
            weight: productData.weight || null,
            isLotSerialControl: productData.isLotSerialControl || false,
            isBatchExpireControl: productData.isBatchExpireControl || false,
            orderTemplate: productData.orderTemplate || null,
            minQuantity: productData.minQuantity || null,
            maxQuantity: productData.maxQuantity || null,
            isRewardPoint:
              productData.isRewardPoint !== undefined
                ? productData.isRewardPoint
                : true,
            isActive:
              productData.isActive !== undefined ? productData.isActive : true,
            retailerId: productData.retailerId || null,
            createdDate: productData.createdDate
              ? new Date(productData.createdDate)
              : new Date(),
            modifiedDate: productData.modifiedDate
              ? new Date(productData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
        });

        // Process nested relations
        await this.processProductNestedData(product.id, productData);

        savedProducts.push(product);

        this.logger.debug(
          `✅ Saved product: ${product.code} (ID: ${product.id})`,
        );
      } catch (error) {
        this.logger.error(
          `❌ Failed to save product ${productData.id} (${productData.code}): ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Successfully saved ${savedProducts.length}/${products.length} products to database`,
    );
    return savedProducts;
  }

  private async processNestedTradeMark(tradeMarkId: number): Promise<void> {
    try {
      // Check if trademark already exists
      const existingTradeMark = await this.prismaService.tradeMark.findFirst({
        where: { kiotVietId: tradeMarkId },
      });

      if (!existingTradeMark) {
        // Fetch trademark details if not exists
        try {
          const headers = await this.authService.getRequestHeaders();
          const response = await firstValueFrom(
            this.httpService.get(`${this.baseUrl}/trademark`, {
              headers,
              timeout: 30000,
            }),
          );

          if (response.data?.data) {
            const tradeMarkData = response.data.data.find(
              (tm) => tm.id === tradeMarkId,
            );

            if (tradeMarkData) {
              await this.prismaService.tradeMark.create({
                data: {
                  kiotVietId: tradeMarkData.id,
                  name: tradeMarkData.name,
                  retailerId: tradeMarkData.retailerId || null,
                  createdDate: tradeMarkData.createdDate
                    ? new Date(tradeMarkData.createdDate)
                    : new Date(),
                  modifiedDate: tradeMarkData.modifiedDate
                    ? new Date(tradeMarkData.modifiedDate)
                    : new Date(),
                  lastSyncedAt: new Date(),
                },
              });

              this.logger.debug(
                `✅ Created trademark: ${tradeMarkData.name} (ID: ${tradeMarkData.id})`,
              );
            }
          }
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to fetch trademark ${tradeMarkId}: ${error.message}`,
          );
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Error processing trademark ${tradeMarkId}: ${error.message}`,
      );
    }
  }

  private async processProductNestedData(
    productId: number,
    productData: KiotVietProduct,
  ): Promise<void> {
    // Process Attributes
    if (productData.attributes && productData.attributes.length > 0) {
      await this.processProductAttributes(productId, productData.attributes);
    }

    // Process Inventories (from nested data)
    if (productData.inventories && productData.inventories.length > 0) {
      await this.processProductInventories(productId, productData.inventories);
    }

    // Process PriceBooks (NESTED DATA - NO SEPARATE ENDPOINT)
    if (productData.priceBooks && productData.priceBooks.length > 0) {
      await this.processProductPriceBooks(productId, productData.priceBooks);
    }

    // Process Images
    if (productData.images && productData.images.length > 0) {
      await this.processProductImages(productId, productData.images);
    }

    // Process Units
    if (productData.units && productData.units.length > 0) {
      await this.processProductUnits(productId, productData.units);
    }
  }

  private async processProductAttributes(
    productId: number,
    attributes: any[],
  ): Promise<void> {
    try {
      // Clear existing attributes
      await this.prismaService.productAttribute.deleteMany({
        where: { productId },
      });

      // Create new attributes
      for (const attr of attributes) {
        await this.prismaService.productAttribute.create({
          data: {
            productId,
            attributeName: attr.attributeName,
            attributeValue: attr.attributeValue,
            lastSyncedAt: new Date(),
          },
        });
      }

      this.logger.debug(
        `✅ Processed ${attributes.length} attributes for product ${productId}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to process attributes for product ${productId}: ${error.message}`,
      );
    }
  }

  private async processProductInventories(
    productId: number,
    inventories: any[],
  ): Promise<void> {
    try {
      // Clear existing inventories
      await this.prismaService.productInventory.deleteMany({
        where: { productId },
      });

      // Create new inventories
      for (const inventory of inventories) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: inventory.branchId },
        });

        if (branch) {
          await this.prismaService.productInventory.create({
            data: {
              productId,
              branchId: branch.id,
              onHand: inventory.onHand || 0,
              reserved: inventory.reserved || 0,
              onOrder: 0, // Default value
              cost: inventory.cost ? new Prisma.Decimal(inventory.cost) : null,
              minQuantity: null,
              maxQuantity: null,
              lastSyncedAt: new Date(),
            },
          });
        }
      }

      this.logger.debug(
        `✅ Processed ${inventories.length} inventories for product ${productId}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to process inventories for product ${productId}: ${error.message}`,
      );
    }
  }

  private async processProductPriceBooks(
    productId: number,
    priceBooks: any[],
  ): Promise<void> {
    try {
      // Clear existing price book details for this product
      await this.prismaService.priceBookDetail.deleteMany({
        where: { productId },
      });

      for (const priceBookData of priceBooks) {
        // Ensure PriceBook exists (nested data processing)
        let priceBook = await this.prismaService.priceBook.findFirst({
          where: { kiotVietId: priceBookData.priceBookId },
        });

        if (!priceBook) {
          // Create PriceBook from nested data
          priceBook = await this.prismaService.priceBook.create({
            data: {
              kiotVietId: priceBookData.priceBookId,
              name:
                priceBookData.priceBookName ||
                `Price Book ${priceBookData.priceBookId}`,
              isActive:
                priceBookData.isActive !== undefined
                  ? priceBookData.isActive
                  : true,
              isGlobal: false,
              startDate: priceBookData.startDate
                ? new Date(priceBookData.startDate)
                : null,
              endDate: priceBookData.endDate
                ? new Date(priceBookData.endDate)
                : null,
              forAllCusGroup: false,
              forAllUser: false,
              retailerId: null,
              createdDate: new Date(),
              modifiedDate: new Date(),
              lastSyncedAt: new Date(),
            },
          });

          this.logger.debug(
            `✅ Created PriceBook: ${priceBook.name} (ID: ${priceBook.kiotVietId})`,
          );
        }

        // Create PriceBookDetail
        await this.prismaService.priceBookDetail.create({
          data: {
            priceBookId: priceBook.id,
            productId,
            price: priceBookData.price
              ? new Prisma.Decimal(priceBookData.price)
              : new Prisma.Decimal(0),
          },
        });
      }

      this.logger.debug(
        `✅ Processed ${priceBooks.length} price books for product ${productId}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to process price books for product ${productId}: ${error.message}`,
      );
    }
  }

  private async processProductImages(
    productId: number,
    images: any[],
  ): Promise<void> {
    try {
      // Clear existing images
      await this.prismaService.productImage.deleteMany({
        where: { productId },
      });

      // Create new images
      for (let i = 0; i < images.length; i++) {
        const image = images[i];
        await this.prismaService.productImage.create({
          data: {
            productId,
            imageUrl: image.image,
            displayOrder: i,
            lastSyncedAt: new Date(),
          },
        });
      }

      this.logger.debug(
        `✅ Processed ${images.length} images for product ${productId}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to process images for product ${productId}: ${error.message}`,
      );
    }
  }

  private async processProductUnits(
    productId: number,
    units: any[],
  ): Promise<void> {
    try {
      // Note: ProductUnit table doesn't exist in schema, so we'll log for now
      this.logger.debug(
        `📋 Product has ${units.length} units (schema not implemented)`,
      );

      // If ProductUnit table is added to schema later, implement here:
      // await this.prismaService.productUnit.deleteMany({ where: { productId } });
      // for (const unit of units) { ... }
    } catch (error) {
      this.logger.error(
        `❌ Failed to process units for product ${productId}: ${error.message}`,
      );
    }
  }

  // ============================================================================
  // ADD syncProductsToLarkBase method - EXACT CUSTOMER PATTERN
  // ============================================================================

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${products.length} products...`,
      );

      await this.larkProductSyncService.syncProductsToLarkBase(products);

      this.logger.log('✅ LarkBase product sync completed');
    } catch (error) {
      this.logger.error(`❌ LarkBase product sync failed: ${error.message}`);

      // Mark products as FAILED for retry
      try {
        const productIds = products
          .map((p) => p.id)
          .filter((id) => id !== undefined);
        if (productIds.length > 0) {
          await this.prismaService.product.updateMany({
            where: { id: { in: productIds } },
            data: { larkSyncStatus: 'FAILED' },
          });
        }
      } catch (updateError) {
        this.logger.error(
          `Failed to update product status: ${updateError.message}`,
        );
      }

      throw error;
    }
  }

  // ============================================================================
  // ENHANCED updateSyncControl method - COMPLETE CUSTOMER PATTERN
  // ============================================================================

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['product'],
          syncMode: 'historical',
          isRunning: false,
          isEnabled: true,
          status: 'idle',
          ...data,
        },
        update: {
          ...data,
          lastRunAt:
            data.status === 'completed' || data.status === 'failed'
              ? new Date()
              : undefined,
        },
      });
    } catch (error) {
      this.logger.error(
        `Failed to update sync control '${name}': ${error.message}`,
      );
      throw error;
    }
  }
}
