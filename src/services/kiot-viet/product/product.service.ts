// src/services/kiot-viet/product/product.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkProductSyncService } from '../../lark/product/lark-product-sync.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

interface KiotVietProduct {
  id: number;
  code: string;
  barCode?: string;
  name: string;
  fullName: string;
  categoryId?: number;
  categoryName?: string;
  tradeMarkId?: number;
  tradeMarkName?: string;
  type?: number;
  description?: string;
  allowsSale?: boolean;
  hasVariants?: boolean;
  basePrice?: number;
  unit?: string;
  masterProductId?: number;
  masterCode?: string;
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

  // Detailed fields from enrichment
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
    productCode?: string;
    productName?: string;
    branchId: number;
    branchName: string;
    cost: number;
    onHand: number;
    reserved: number;
    actualReserved?: number;
    minQuantity?: number;
    maxQuantity?: number;
    isActive?: boolean;
    onOrder?: number;
  }>;

  priceBooks?: Array<{
    productId: number;
    priceBookId: number;
    priceBookName: string;
    price: number;
    isActive?: boolean;
    startDate?: string;
    endDate?: string;
  }>;

  images?: Array<{
    image: string;
  }>;

  productSerials?: Array<{
    productId: number;
    serialNumber: string;
    status: number;
    branchId: number;
    quantity?: number;
    createdDate?: string;
    modifiedDate?: string;
  }>;

  productBatchExpires?: Array<{
    productId: number;
    onHand: number;
    batchName: string;
    expireDate?: string;
    fullNameVirgule: string;
    branchId: number;
  }>;

  warranties?: Array<{
    productId: number;
    description?: string;
    numberTime: number;
    timeType: number;
    warrantyType: number;
    createdDate?: string;
    modifiedDate?: string;
  }>;

  productFormulas?: Array<{
    materialId: number;
    materialCode: string;
    materialFullName: string;
    materialName: string;
    quantity: number;
    basePrice: number;
    productId?: number;
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
  // SYNC CONTROL & TRACKING - EXACT COPY FROM INVOICE
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_historical' },
      });

      // Priority: Historical sync first
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical product sync...');
        await this.syncHistoricalProducts();
        return;
      }

      // Default: historical sync (no recent sync for products)
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
  // HISTORICAL SYNC - ENHANCED WITH ADVANCED ERROR HANDLING
  // ============================================================================

  async syncHistoricalProducts(): Promise<void> {
    const syncName = 'product_historical';

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
              `✅ Pagination complete. Processed ${processedCount}/${totalProducts} products`,
            );
            break;
          }
        }

        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})...`,
          );

          const response = await this.fetchProductsListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includeInventory: true,
            includePricebook: true,
            includeSerials: true,
            includeBatchExpires: true,
            includeWarranties: true,
            includeQuantity: true,
            includeMaterial: true,
            includeCombo: true,
          });

          consecutiveErrorPages = 0;

          const { data: products, total } = response;

          if (total !== undefined && total !== null) {
            if (totalProducts === 0) {
              this.logger.log(
                `📊 Total products detected: ${total}. Starting processing...`,
              );
              totalProducts = total;
            } else if (total !== totalProducts && total !== lastValidTotal) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalProducts} → ${total}. Using latest.`,
              );
              totalProducts = total;
            }
            lastValidTotal = total;
          }

          if (!products || products.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalProducts > 0 && currentItem >= totalProducts) {
              this.logger.log('✅ Reached end of data (empty page past total)');
              break;
            }

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
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

          // Process products
          this.logger.log(
            `🔄 Processing ${newProducts.length} products from page ${currentPage}...`,
          );

          const productsWithDetails =
            await this.enrichProductsWithDetails(newProducts);
          const savedProducts =
            await this.saveProductsToDatabase(productsWithDetails);
          await this.syncProductsToLarkBase(savedProducts);

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

          consecutiveEmptyPages = 0;
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (
            consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES ||
            totalRetries >= MAX_TOTAL_RETRIES
          ) {
            throw new Error(
              `Too many consecutive errors (${consecutiveErrorPages}) or total retries (${totalRetries}). Last error: ${error.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
        }
      }

      // Auto-disable historical sync and update final status
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
      includeRemoveIds: (params.includeRemoveIds || false).toString(),
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

  // ============================================================================
  // ENRICH PRODUCTS WITH DETAILS - CORE LOGIC
  // ============================================================================

  private async enrichProductsWithDetails(
    products: KiotVietProduct[],
  ): Promise<KiotVietProduct[]> {
    this.logger.log(`🔍 Enriching ${products.length} products with details...`);

    const enrichedProducts: KiotVietProduct[] = [];

    for (const product of products) {
      try {
        const headers = await this.authService.getRequestHeaders();

        // Fetch detailed product information
        const queryParams = new URLSearchParams({
          includeInventory: 'true',
          includePricebook: 'true',
          includeSerials: 'true',
          includeBatchExpires: 'true',
          includeWarranties: 'true',
          includeQuantity: 'true',
          includeMaterial: 'true',
          includeCombo: 'true',
        });

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/products/${product.id}?${queryParams}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedProducts.push(response.data);
        } else {
          enrichedProducts.push(product);
        }

        // Rate limiting
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich product ${product.code}: ${error.message}`,
        );
        enrichedProducts.push(product);
      }
    }

    return enrichedProducts;
  }

  // ============================================================================
  // SAVE PRODUCTS TO DATABASE - COMPLETE LOGIC
  // ============================================================================

  private async saveProductsToDatabase(
    products: KiotVietProduct[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${products.length} products to database...`);

    const savedProducts: any[] = [];

    for (const productData of products) {
      try {
        const category = productData.categoryId
          ? await this.prismaService.category.findFirst({
              where: { kiotVietId: productData.categoryId },
              select: { id: true, name: true },
            })
          : null;

        if (productData.categoryId && !category) {
          this.logger.warn(
            `⚠️ [DEPENDENCY MISS] Category ${productData.categoryId} not found for product ${productData.code} - proceeding with null categoryId`,
          );
        }

        const tradeMark = productData.tradeMarkId
          ? await this.prismaService.tradeMark.findFirst({
              where: { kiotVietId: productData.tradeMarkId },
              select: { id: true, name: true },
            })
          : null;

        if (productData.tradeMarkId && !tradeMark) {
          this.logger.warn(
            `⚠️ [DEPENDENCY MISS] TradeMark ${productData.tradeMarkId} not found for product ${productData.code} - proceeding with null tradeMarkId`,
          );
        }

        // Find master product if exists
        const masterProduct = productData.masterProductId
          ? await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(productData.masterProductId) },
              select: { kiotVietId: true },
            })
          : null;

        // ============================================================================
        // SAVE MAIN PRODUCT
        // ============================================================================

        const product = await this.prismaService.product.upsert({
          where: { kiotVietId: BigInt(productData.id) },
          update: {
            code: productData.code,
            barCode: productData.barCode || null,
            name: productData.name,
            fullName: productData.fullName,
            categoryId: category?.id ?? null,
            tradeMarkId: tradeMark?.id ?? null,
            type: productData.type ?? null,
            description: productData.description || null,
            allowsSale: productData.allowsSale ?? true,
            hasVariants: productData.hasVariants ?? false,
            basePrice: new Prisma.Decimal(productData.basePrice || 0),
            unit: productData.unit || null,
            masterProductId: masterProduct?.kiotVietId ?? null,
            masterUnitId: productData.masterUnitId
              ? BigInt(productData.masterUnitId)
              : null,
            conversionValue: productData.conversionValue ?? null,
            weight: productData.weight ?? null,
            isLotSerialControl: productData.isLotSerialControl ?? false,
            isBatchExpireControl: productData.isBatchExpireControl ?? false,
            orderTemplate: productData.orderTemplate || null,
            minQuantity: productData.minQuantity ?? null,
            maxQuantity: productData.maxQuantity ?? null,
            isRewardPoint: productData.isRewardPoint ?? true,
            isActive: productData.isActive ?? true,
            retailerId: productData.retailerId ?? null,
            modifiedDate: productData.modifiedDate
              ? new Date(productData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING' as const,
          },
          create: {
            kiotVietId: BigInt(productData.id),
            code: productData.code,
            barCode: productData.barCode || null,
            name: productData.name,
            fullName: productData.fullName,
            categoryId: category?.id ?? null,
            tradeMarkId: tradeMark?.id ?? null,
            type: productData.type ?? null,
            description: productData.description || null,
            allowsSale: productData.allowsSale ?? true,
            hasVariants: productData.hasVariants ?? false,
            basePrice: new Prisma.Decimal(productData.basePrice || 0),
            unit: productData.unit || null,
            masterProductId: masterProduct?.kiotVietId ?? null,
            masterUnitId: productData.masterUnitId
              ? BigInt(productData.masterUnitId)
              : null,
            conversionValue: productData.conversionValue ?? null,
            weight: productData.weight ?? null,
            isLotSerialControl: productData.isLotSerialControl ?? false,
            isBatchExpireControl: productData.isBatchExpireControl ?? false,
            orderTemplate: productData.orderTemplate || null,
            minQuantity: productData.minQuantity ?? null,
            maxQuantity: productData.maxQuantity ?? null,
            isRewardPoint: productData.isRewardPoint ?? true,
            isActive: productData.isActive ?? true,
            retailerId: productData.retailerId ?? null,
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

        if (productData.attributes && productData.attributes.length > 0) {
          await this.saveProductAttributes(product.id, productData.attributes);
        }

        if (productData.images && productData.images.length > 0) {
          await this.saveProductImages(product.id, productData.images);
        }

        if (productData.inventories && productData.inventories.length > 0) {
          await this.saveProductInventories(
            product.id,
            productData.inventories,
          );
        }

        if (productData.priceBooks && productData.priceBooks.length > 0) {
          await this.saveProductPriceBooks(product.id, productData.priceBooks);
        }

        if (
          productData.productSerials &&
          productData.productSerials.length > 0
        ) {
          await this.saveProductSerials(product.id, productData.productSerials);
        }

        if (
          productData.productBatchExpires &&
          productData.productBatchExpires.length > 0
        ) {
          await this.saveProductBatchExpires(
            product.id,
            productData.productBatchExpires,
          );
        }

        if (productData.warranties && productData.warranties.length > 0) {
          await this.saveProductWarranties(product.id, productData.warranties);
        }

        if (
          productData.productFormulas &&
          productData.productFormulas.length > 0
        ) {
          await this.saveProductFormulas(
            product.id,
            productData.productFormulas,
          );
        }

        savedProducts.push(product);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save product ${productData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`✅ Saved ${savedProducts.length} products successfully`);
    return savedProducts;
  }

  // ============================================================================
  // HELPER METHODS FOR SAVING RELATED DATA
  // ============================================================================

  private async saveProductAttributes(
    productId: number,
    attributes: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productAttribute.deleteMany({
        where: { productId },
      });

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
    } catch (error) {
      this.logger.error(
        `❌ Failed to save attributes for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductImages(
    productId: number,
    images: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productImage.deleteMany({
        where: { productId },
      });

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
    } catch (error) {
      this.logger.error(
        `❌ Failed to save images for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductInventories(
    productId: number,
    inventories: any[],
  ): Promise<void> {
    if (!inventories || inventories.length === 0) {
      this.logger.debug(`No inventories to save for product ${productId}`);
      return;
    }

    try {
      await this.prismaService.productInventory.deleteMany({
        where: { productId },
      });

      let processedCount = 0;
      let skippedCount = 0;

      for (const inventory of inventories) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: inventory.branchId },
          select: { id: true, name: true },
        });

        if (!branch) {
          this.logger.warn(
            `⚠️ [DEPENDENCY MISS] Branch ${inventory.branchId} (${inventory.branchName || 'unknown'}) not found - skipping inventory for product ${productId}`,
          );
          skippedCount++;
          continue;
        }

        await this.prismaService.productInventory.create({
          data: {
            productId,
            branchId: branch.id,
            onHand: inventory.onHand || 0,
            reserved: inventory.reserved || 0,
            onOrder: inventory.onOrder || 0,
            cost: inventory.cost ? new Prisma.Decimal(inventory.cost) : null,
            minQuantity: inventory.minQuantity || null,
            maxQuantity: inventory.maxQuantity || null,
            lastSyncedAt: new Date(),
          },
        });
        processedCount++;
      }

      this.logger.log(
        `✅ Product ${productId} inventories: ${processedCount} processed, ${skippedCount} skipped`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to save inventories for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductPriceBooks(
    productId: number,
    priceBooks: any[],
  ): Promise<void> {
    if (!priceBooks || priceBooks.length === 0) {
      this.logger.debug(`No pricebooks to save for product ${productId}`);
      return;
    }

    try {
      await this.prismaService.priceBookDetail.deleteMany({
        where: { productId },
      });

      let processedCount = 0;
      let skippedCount = 0;

      for (const priceBook of priceBooks) {
        const existingPriceBook = await this.prismaService.priceBook.findFirst({
          where: { kiotVietId: priceBook.priceBookId },
          select: { id: true, name: true },
        });

        if (!existingPriceBook) {
          this.logger.warn(
            `⚠️ [DEPENDENCY MISS] PriceBook ${priceBook.priceBookId} (${priceBook.priceBookName || 'unknown'}) not found - skipping price for product ${productId}`,
          );
          skippedCount++;
          continue;
        }

        await this.prismaService.priceBookDetail.create({
          data: {
            priceBookId: existingPriceBook.id,
            productId,
            price: new Prisma.Decimal(priceBook.price || 0),
          },
        });
        processedCount++;
      }

      this.logger.log(
        `✅ Product ${productId} pricebooks: ${processedCount} processed, ${skippedCount} skipped`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to save price books for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductSerials(
    productId: number,
    serials: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productSerial.deleteMany({
        where: { productId },
      });

      for (const serial of serials) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: serial.branchId },
          select: { id: true },
        });

        if (branch) {
          await this.prismaService.productSerial.create({
            data: {
              productId,
              serialNumber: serial.serialNumber,
              branchId: branch.id,
              status: serial.status,
              quantity: serial.quantity || 1,
              createdDate: serial.createdDate
                ? new Date(serial.createdDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save serials for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductBatchExpires(
    productId: number,
    batches: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productBatchExpire.deleteMany({
        where: { productId },
      });

      for (const batch of batches) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: batch.branchId },
          select: { id: true },
        });

        if (branch) {
          await this.prismaService.productBatchExpire.create({
            data: {
              productId,
              batchName: batch.batchName,
              fullNameVirgule: batch.fullNameVirgule,
              expireDate: batch.expireDate ? new Date(batch.expireDate) : null,
              onHand: batch.onHand || 0,
              branchId: branch.id,
              lastSyncedAt: new Date(),
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save batch expires for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductWarranties(
    productId: number,
    warranties: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productWarranty.deleteMany({
        where: { productId },
      });

      for (const warranty of warranties) {
        await this.prismaService.productWarranty.create({
          data: {
            productId,
            description: warranty.description || null,
            numberTime: warranty.numberTime,
            timeType: warranty.timeType,
            warrantyType: warranty.warrantyType,
            createdDate: warranty.createdDate
              ? new Date(warranty.createdDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save warranties for product ${productId}: ${error.message}`,
      );
    }
  }

  private async saveProductFormulas(
    productId: number,
    formulas: any[],
  ): Promise<void> {
    try {
      await this.prismaService.productFormula.deleteMany({
        where: { productId },
      });

      for (const formula of formulas) {
        const materialProduct = await this.prismaService.product.findFirst({
          where: { kiotVietId: BigInt(formula.materialId) },
          select: { id: true },
        });

        if (materialProduct) {
          await this.prismaService.productFormula.create({
            data: {
              productId,
              materialId: materialProduct.id,
              quantity: formula.quantity,
              basePrice: new Prisma.Decimal(formula.basePrice || 0),
              lastSyncedAt: new Date(),
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save formulas for product ${productId}: ${error.message}`,
      );
    }
  }

  // ============================================================================
  // SYNC TO LARKBASE
  // ============================================================================

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${products.length} products...`,
      );

      const productsToSync = products.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (productsToSync.length === 0) {
        this.logger.log('📋 No products need LarkBase sync');
        return;
      }

      await this.larkProductSyncService.syncProductsToLarkBase(productsToSync);
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
  // ENHANCED updateSyncControl method - COMPLETE PATTERN
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
