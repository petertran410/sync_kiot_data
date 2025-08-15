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

  inventories: Array<{
    productId: number;
    productCode?: string;
    productName?: string;
    branchId: number;
    branchName?: string;
    cost: number;
    onHand: number;
    reserved: number;
    lineNumber: number;
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
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
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

  private async enrichProductsWithDetails(
    products: KiotVietProduct[],
  ): Promise<KiotVietProduct[]> {
    this.logger.log(`🔍 Enriching ${products.length} products with details...`);

    const enrichedProducts: KiotVietProduct[] = [];

    for (const product of products) {
      try {
        const headers = await this.authService.getRequestHeaders();

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

  private async saveProductsToDatabase(
    products: KiotVietProduct[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${products.length} products to database...`);

    const savedProducts: any[] = [];

    for (const productData of products) {
      try {
        const category = await this.prismaService.category.findFirst({
          where: { kiotVietId: productData.categoryId },
          select: { id: true, name: true },
        });

        const tradeMark = await this.prismaService.tradeMark.findFirst({
          where: { kiotVietId: productData.tradeMarkId },
          select: { id: true, name: true },
        });
        // if (!productData.id || !productData.code || !productData.name) {
        //   continue;
        // }

        // let categoryId: number | null = null;
        // if (productData.categoryId && productData.categoryName) {
        //   const category = await this.prismaService.category.upsert({
        //     where: { kiotVietId: productData.categoryId },
        //     update: {
        //       name: productData.categoryName.trim(),
        //       lastSyncedAt: new Date(),
        //     },
        //     create: {
        //       kiotVietId: productData.categoryId,
        //       name: productData.categoryName.trim(),
        //       lastSyncedAt: new Date(),
        //     },
        //     select: { id: true },
        //   });
        //   categoryId = category.id;
        // }

        // let tradeMarkId: number | null = null;
        // if (productData.tradeMarkId && productData.tradeMarkName) {
        //   const tradeMark = await this.prismaService.tradeMark.upsert({
        //     where: { kiotVietId: productData.tradeMarkId },
        //     update: {
        //       name: productData.tradeMarkName.trim(),
        //       lastSyncedAt: new Date(),
        //     },
        //     create: {
        //       kiotVietId: productData.tradeMarkId,
        //       name: productData.tradeMarkName.trim(),
        //       lastSyncedAt: new Date(),
        //     },
        //     select: { id: true },
        //   });
        //   tradeMarkId = tradeMark.id;
        // }

        const product = await this.prismaService.product.upsert({
          where: { kiotVietId: BigInt(productData.id) },
          update: {
            code: productData.code.trim(),
            name: productData.name.trim(),
            fullName: productData.fullName?.trim() || productData.name.trim(),
            categoryId: category?.id,
            categoryName: category?.name,
            tradeMarkId: tradeMark?.id,
            tradeMarkName: tradeMark?.name,
            allowsSale: productData.allowsSale ?? true,
            type: productData.type ?? 1,
            hasVariants: productData.hasVariants ?? false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : null,
            weight: productData.weight ?? null,
            unit: productData.unit?.trim() || null,
            conversionValue: productData.conversionValue ?? 1,
            description: productData.description?.trim() || null,
            isLotSerialControl: productData.isLotSerialControl ?? false,
            isBatchExpireControl: productData.isBatchExpireControl ?? false,
            orderTemplate: productData.orderTemplate?.trim() || null,
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
          create: {
            kiotVietId: BigInt(productData.id),
            code: productData.code.trim(),
            name: productData.name.trim(),
            fullName: productData.fullName?.trim() || productData.name.trim(),
            categoryId: category?.id,
            categoryName: category?.name,
            tradeMarkId: tradeMark?.id,
            tradeMarkName: tradeMark?.name,
            allowsSale: productData.allowsSale ?? true,
            type: productData.type ?? 1,
            hasVariants: productData.hasVariants ?? false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : null,
            weight: productData.weight ?? null,
            unit: productData.unit?.trim() || null,
            conversionValue: productData.conversionValue ?? 1,
            description: productData.description?.trim() || null,
            isLotSerialControl: productData.isLotSerialControl ?? false,
            isBatchExpireControl: productData.isBatchExpireControl ?? false,
            orderTemplate: productData.orderTemplate?.trim() || null,
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

        if (productData.inventories && productData.inventories.length > 0) {
          for (let i = 0; i < productData.inventories.length; i++) {
            const detail = productData.inventories[i];
            await this.prismaService.productInventory.upsert({
              where: {
                productId_lineNumber: {
                  productId: product?.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                productCode: product.code,
                productName: product.name,
                branchId: detail.branchId,
                branchName: detail.branchName,
                cost: detail.cost,
                onHand: detail.onHand,
                reserved: detail.reserved,
                lineNumber: i + 1,
                actualReserved: detail.actualReserved,
                minQuantity: detail.minQuantity,
                maxQuantity: detail.maxQuantity,
                isActive: detail.isActive,
                onOrder: detail.onOrder,
                lastSyncedAt: new Date(),
              },
              create: {
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                branchId: detail.branchId,
                branchName: detail.branchName,
                cost: detail.cost,
                onHand: detail.onHand,
                reserved: detail.reserved,
                lineNumber: i + 1,
                actualReserved: detail.actualReserved,
                minQuantity: detail.minQuantity,
                maxQuantity: detail.maxQuantity,
                isActive: detail.isActive,
                onOrder: detail.onOrder,
                lastSyncedAt: new Date(),
              },
            });
          }
        }

        if (productData.attributes && productData.attributes.length > 0) {
          for (let i = 0; i < productData.attributes.length; i++) {
            const detail = productData.attributes[i];

            await this.prismaService.productAttribute.upsert({
              where: {
                productId_lineNumber: {
                  productId: product.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                productId: product.id,
                attributeName: detail.attributeName,
                attributeValue: detail.attributeValue,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
              create: {
                productId: product.id,
                attributeName: detail.attributeName,
                attributeValue: detail.attributeValue,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
        }

        if (productData.images && productData.images.length > 0) {
          for (let i = 0; i < productData.images.length; i++) {
            const detail = productData.images[i];

            await this.prismaService.productImage.upsert({
              where: {
                productId_lineNumber: {
                  productId: product.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                productId: product.id,
                imageUrl: detail.image,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
              create: {
                productId: product.id,
                imageUrl: detail.image,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
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

      const enrichedProducts = await Promise.all(
        productsToSync.map(async (product) => {
          const inventories =
            await this.prismaService.productInventory.findMany({
              where: { productId: product.id },
              select: {
                branchId: true,
                onHand: true,
                reserved: true,
                onOrder: true,
                cost: true,
              },
            });

          const priceBooks = await this.prismaService.priceBookDetail.findMany({
            where: { productId: product.id },
            select: {
              priceBookId: true,
              price: true,
            },
          });

          return {
            ...product,
            inventories: inventories || [],
            priceBooks: priceBooks || [],
          };
        }),
      );

      await this.larkProductSyncService.syncProductsToLarkBase(
        enrichedProducts,
      );
      this.logger.log('✅ LarkBase product sync completed');
    } catch (error) {
      this.logger.error(`❌ LarkBase product sync failed: ${error.message}`);

      try {
        const productIds = products
          .map((p) => p.id)
          .filter((id) => id !== undefined);

        if (productIds.length > 0) {
          await this.prismaService.product.updateMany({
            where: { id: { in: productIds } },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncedAt: new Date(),
            },
          });
        }
      } catch (updateError) {
        this.logger.error(
          `Failed to update product status: ${updateError.message}`,
        );
      }

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

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
