import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkProductSyncService } from '../../lark/product/lark-product-sync.service';
import { async, firstValueFrom } from 'rxjs';
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
    productName: string;
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
      const runningProductSyncs = await this.prismaService.syncControl.findMany(
        {
          where: {
            OR: [{ name: 'product_historical' }, { name: 'product_lark_sync' }],
            isRunning: true,
          },
        },
      );

      if (runningProductSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningProductSyncs.length} Product syncs still running: ${runningProductSyncs.map((s) => s.name).join(', ')}`,
        );
        this.logger.warn('Skipping product sync to avoid conficts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical product sync...');
        await this.syncHistoricalProducts();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log('Historical product sync is running');
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

    this.logger.log('Historical product sync enabled');
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

      this.logger.log('Starting historical product sync...');

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
              `Pagination complete. Processed ${processedCount}/${totalProducts} products`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalProducts) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalProducts} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        const dateEnd = new Date();
        dateEnd.setDate(dateEnd.getDate() + 1);
        const dateEndStr = dateEnd.toISOString().split('T')[0];

        try {
          const response = await this.fetchProductsListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id',
            orderDirection: 'ASC',
            includeInventory: true,
            includePricebook: true,
            includeSerials: true,
            includeBatchExpires: true,
            includeWarranties: true,
            includeQuantity: true,
            includeMaterial: true,
            includeCombo: true,
            lastModifiedFrom: '2024-12-1',
            toDate: dateEndStr,
          });

          if (!response) {
            this.logger.warn('Received null response from KiotViet API');

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

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { data: products, total } = response;

          if (total !== undefined && total !== null) {
            if (totalProducts === 0) {
              this.logger.log(
                `Total products detected: ${total}. Starting processing...`,
              );

              totalProducts = total;
            } else if (total !== totalProducts) {
              this.logger.warn(
                `Total count changed: ${totalProducts} → ${total}. Using latest.`,
              );
              totalProducts = total;
            }
            lastValidTotal = total;
          }

          if (!products || products.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalProducts > 0 && currentItem >= totalProducts) {
              this.logger.log('Reached end of data (empty page past total)');
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

          const existingProductIds = new Set(
            (
              await this.prismaService.product.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newProducts = products.filter((product) => {
            if (
              !existingProductIds.has(product.id) &&
              !processedProductIds.has(product.id)
            ) {
              processedProductIds.add(product.id);
              return true;
            }
            return false;
          });

          const existingProducts = products.filter((product) => {
            if (
              existingProductIds.has(product.id) &&
              !processedProductIds.has(product.id)
            ) {
              processedProductIds.add(product.id);
              return true;
            }
            return false;
          });

          if (newProducts.length === 0 && existingProducts.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all products already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedProducts: any[] = [];

          if (newProducts.length > 0) {
            this.logger.log(
              `Processing ${newProducts.length} NEW products from page ${currentPage}...`,
            );

            const savedProducts =
              await this.saveProductsToDatabase(newProducts);
            pageProcessedCount += savedProducts.length;
            allSavedProducts.push(...savedProducts);
          }

          if (existingProducts.length > 0) {
            this.logger.log(
              `Processing ${existingProducts.length} EXISTING products from page ${currentPage}...`,
            );

            const savedProducts =
              await this.saveProductsToDatabase(existingProducts);
            pageProcessedCount += savedProducts.length;
            allSavedProducts.push(...savedProducts);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (allSavedProducts.length > 0) {
            try {
              await this.syncProductsToLarkBase(allSavedProducts);
              this.logger.log(
                `Synced ${allSavedProducts.length} products to LarkBase`,
              );
            } catch (error) {
              this.logger.warn(
                `LarkBase sync failed for page${currentPage}: ${error.message}`,
              );
            }
          }

          if (totalProducts > 0) {
            const completionPercentage = (processedCount / totalProducts) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalProducts} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalProducts) {
              this.logger.log('All products processed successfully');
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
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
        `Historical product sync completed: ${processedCount}/${totalProducts} (${completionRate.toFixed(1)}% completion rate)`,
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
      lastModifiedFrom?: string;
      toDate?: string;
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
          `API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          this.logger.log(`Retrying after ${delay / 1000}s delay...`);
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
    lastModifiedFrom?: string;
    toDate?: string;
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

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }
    if (params.toDate) {
      queryParams.append('toDate', params.toDate);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/products?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async saveProductsToDatabase(
    products: KiotVietProduct[],
  ): Promise<any[]> {
    this.logger.log(`Saving ${products.length} products to database...`);

    const savedProducts: any[] = [];

    for (const productData of products) {
      try {
        const category = await this.prismaService.category.findFirst({
          where: { kiotVietId: productData.categoryId },
          select: {
            id: true,
            name: true,
            parent_name: true,
            child_name: true,
            branch_name: true,
          },
        });

        const tradeMark = await this.prismaService.tradeMark.findFirst({
          where: { kiotVietId: productData.tradeMarkId },
          select: { id: true, name: true },
        });

        const product = await this.prismaService.product.upsert({
          where: { kiotVietId: BigInt(productData.id) },
          update: {
            code: productData.code.trim(),
            name: productData.name,
            fullName: productData.fullName ?? productData.name,
            categoryId: category?.id,
            categoryName: category?.name,
            parent_name: category?.parent_name,
            child_name: category?.child_name,
            branch_name: category?.branch_name,
            tradeMarkId: tradeMark?.id,
            tradeMarkName: tradeMark?.name,
            allowsSale: productData.allowsSale ?? true,
            type: productData.type ?? 1,
            hasVariants: productData.hasVariants ?? false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : 0,
            weight: productData.weight ?? 0,
            unit: productData.unit ?? '',
            conversionValue: productData.conversionValue ?? 1,
            description: productData.description ?? '',
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
            name: productData.name,
            fullName: productData.fullName ?? productData.name,
            categoryId: category?.id,
            categoryName: category?.name,
            parent_name: category?.parent_name,
            child_name: category?.child_name,
            branch_name: category?.branch_name,
            tradeMarkId: tradeMark?.id,
            tradeMarkName: tradeMark?.name,
            allowsSale: productData.allowsSale ?? true,
            type: productData.type ?? 1,
            hasVariants: productData.hasVariants ?? false,
            basePrice: productData.basePrice
              ? new Prisma.Decimal(productData.basePrice)
              : 0,
            weight: productData.weight ?? 0,
            unit: productData.unit ?? '',
            conversionValue: productData.conversionValue ?? 1,
            description: productData.description ?? '',
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

        const inventories: any[] = [];
        if (productData.inventories && productData.inventories.length > 0) {
          for (let i = 0; i < productData.inventories.length; i++) {
            const detail = productData.inventories[i];

            const branch = await this.prismaService.branch.findFirst({
              where: { kiotVietId: detail.branchId },
              select: { id: true },
            });

            const inventory = await this.prismaService.productInventory.upsert({
              where: {
                productId_lineNumber: {
                  productId: product.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                productCode: product.code,
                productName: product.name,
                branchId: branch?.id ?? null,
                branchName: detail.branchName ?? null,
                cost: detail.cost ?? 0,
                onHand: detail.onHand ?? 0,
                reserved: detail.reserved ?? 0,
                lineNumber: i + 1,
                actualReserved: detail.actualReserved ?? 0,
                minQuantity: detail.minQuantity ?? 0,
                maxQuantity: detail.maxQuantity ?? 0,
                isActive: detail.isActive,
                onOrder: detail.onOrder ?? 0,
                lastSyncedAt: new Date(),
              },
              create: {
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                branchId: branch?.id ?? null,
                branchName: detail.branchName ?? null,
                cost: detail.cost ?? 0,
                onHand: detail.onHand ?? 0,
                reserved: detail.reserved ?? 0,
                lineNumber: i + 1,
                actualReserved: detail.actualReserved ?? 0,
                minQuantity: detail.minQuantity ?? 0,
                maxQuantity: detail.maxQuantity ?? 0,
                isActive: detail.isActive,
                onOrder: detail.onOrder ?? 0,
                lastSyncedAt: new Date(),
              },
            });
            inventories.push(inventory);
          }
        }

        const priceBooks: any[] = [];
        if (productData.priceBooks && productData.priceBooks.length > 0) {
          for (let i = 0; i < productData.priceBooks.length; i++) {
            const detail = productData.priceBooks[i];
            const pricebook = await this.prismaService.priceBook.findFirst({
              where: { kiotVietId: detail.priceBookId },
            });

            const priceBookDetail =
              await this.prismaService.priceBookDetail.upsert({
                where: {
                  productId_lineNumber: {
                    productId: product.id,
                    lineNumber: i + 1,
                  },
                },
                update: {
                  lineNumber: i + 1,
                  priceBookId: pricebook?.id ?? null,
                  priceBookName: pricebook?.name,
                  price: detail.price ?? 0,
                  lastSyncedAt: new Date(),
                  productId: product.id,
                  productName: product.name,
                  productKiotId: product.kiotVietId,
                },
                create: {
                  lineNumber: i + 1,
                  priceBookId: pricebook?.id ?? null,
                  priceBookName: pricebook?.name,
                  price: detail.price ?? 0,
                  lastSyncedAt: new Date(),
                  productId: product.id,
                  productName: product.name,
                  productKiotId: product.kiotVietId,
                },
              });
            priceBooks.push(priceBookDetail);
          }
        }

        savedProducts.push({
          ...product,
          inventories,
          priceBooks,
        });
      } catch (error) {
        this.logger.error(
          `❌ Failed to save product ${productData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`Saved ${savedProducts.length} products successfully`);
    return savedProducts;
  }

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    try {
      this.logger.log(
        `Starting LarkBase sync for ${products.length} products...`,
      );

      const productsToSync = products.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (productsToSync.length === 0) {
        this.logger.log('No products need LarkBase sync');
        return;
      }

      await this.larkProductSyncService.syncProductsToLarkBase(productsToSync);
      this.logger.log('✅ LarkBase product sync completed');
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      throw error;
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
