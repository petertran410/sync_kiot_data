// src/services/kiot-viet/product/product.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietProductService {
  private readonly logger = new Logger(KiotVietProductService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async fetchProducts(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/products`, {
          headers,
          params: {
            ...params,
            includeInventory: true,
            includePricebook: true,
            includeSerials: true,
            includeBatchExpires: true,
            includeWarranties: true,
            includeRemoveIds: true,
            includeQuantity: true,
            includeMaterial: true,
            includeSoftDeletedAttribute: false,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch products: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveProducts(products: any[]) {
    if (!products || products.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = products.map((p) => BigInt(p.id));
    const existingProducts = await this.prismaService.product.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<string, number>(
      existingProducts.map((p) => [p.kiotVietId.toString(), p.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const productData of products) {
      try {
        const kiotVietId = BigInt(productData.id);
        const existingId = existingMap.get(kiotVietId.toString());

        if (existingId) {
          await this.updateProduct(existingId, productData);
          updatedCount++;
        } else {
          await this.createProduct(productData);
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save product ${productData.code}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async createProduct(productData: any) {
    const product = await this.prismaService.product.create({
      data: await this.prepareProductCreateData(productData),
    });

    await this.handleProductRelations(product.id, productData);
  }

  private async updateProduct(productId: number, productData: any) {
    await this.prismaService.product.update({
      where: { id: productId },
      data: await this.prepareProductUpdateData(productData),
    });

    await this.handleProductRelations(productId, productData);
  }

  private async prepareProductCreateData(
    productData: any,
  ): Promise<Prisma.ProductCreateInput> {
    const data: Prisma.ProductCreateInput = {
      kiotVietId: BigInt(productData.id),
      code: productData.code,
      barCode: productData.barCode,
      name: productData.name,
      fullName: productData.fullName,
      type: productData.type,
      description: productData.description,
      allowsSale: productData.allowsSale,
      hasVariants: productData.hasVariants,
      basePrice: new Prisma.Decimal(productData.basePrice || 0),
      unit: productData.unit,
      // Fix: Use masterProduct relation instead of masterProductId
      masterUnitId: productData.masterUnitId
        ? BigInt(productData.masterUnitId)
        : null,
      conversionValue: productData.conversionValue,
      weight: productData.weight,
      isLotSerialControl: productData.isLotSerialControl || false,
      isBatchExpireControl: productData.isBatchExpireControl || false,
      orderTemplate: productData.orderTemplate,
      minQuantity: productData.minQuantity,
      maxQuantity: productData.maxQuantity,
      isRewardPoint: productData.isRewardPoint !== false,
      isActive: productData.isActive !== false,
      retailerId: productData.retailerId,
      createdDate: productData.createdDate
        ? new Date(productData.createdDate)
        : new Date(),
      modifiedDate: productData.modifiedDate
        ? new Date(productData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // Fix: Handle masterProduct relationship correctly
    if (productData.masterProductId) {
      data.masterProduct = {
        connect: { kiotVietId: BigInt(productData.masterProductId) },
      };
    }

    // Handle category relationship
    if (productData.categoryId) {
      const category = await this.prismaService.category.findFirst({
        where: { kiotVietId: productData.categoryId },
      });
      if (category) {
        data.category = { connect: { id: category.id } };
      }
    }

    // Handle trademark relationship
    if (productData.tradeMarkId) {
      const tradeMark = await this.prismaService.tradeMark.findFirst({
        where: { kiotVietId: productData.tradeMarkId },
      });
      if (tradeMark) {
        data.tradeMark = { connect: { id: tradeMark.id } };
      }
    }

    return data;
  }

  private async prepareProductUpdateData(
    productData: any,
  ): Promise<Prisma.ProductUpdateInput> {
    const data: Prisma.ProductUpdateInput = {
      code: productData.code,
      barCode: productData.barCode,
      name: productData.name,
      fullName: productData.fullName,
      type: productData.type,
      description: productData.description,
      allowsSale: productData.allowsSale,
      hasVariants: productData.hasVariants,
      basePrice: new Prisma.Decimal(productData.basePrice || 0),
      unit: productData.unit,
      // Fix: Use masterProduct relation instead of masterProductId
      masterUnitId: productData.masterUnitId
        ? BigInt(productData.masterUnitId)
        : null,
      conversionValue: productData.conversionValue,
      weight: productData.weight,
      isLotSerialControl: productData.isLotSerialControl || false,
      isBatchExpireControl: productData.isBatchExpireControl || false,
      orderTemplate: productData.orderTemplate,
      minQuantity: productData.minQuantity,
      maxQuantity: productData.maxQuantity,
      isRewardPoint: productData.isRewardPoint !== false,
      isActive: productData.isActive !== false,
      retailerId: productData.retailerId,
      modifiedDate: productData.modifiedDate
        ? new Date(productData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // Fix: Handle masterProduct relationship correctly
    if (productData.masterProductId) {
      data.masterProduct = {
        connect: { kiotVietId: BigInt(productData.masterProductId) },
      };
    }

    // Handle category relationship
    if (productData.categoryId) {
      const category = await this.prismaService.category.findFirst({
        where: { kiotVietId: productData.categoryId },
      });
      if (category) {
        data.category = { connect: { id: category.id } };
      }
    }

    // Handle trademark relationship
    if (productData.tradeMarkId) {
      const tradeMark = await this.prismaService.tradeMark.findFirst({
        where: { kiotVietId: productData.tradeMarkId },
      });
      if (tradeMark) {
        data.tradeMark = { connect: { id: tradeMark.id } };
      }
    }

    return data;
  }

  private async handleProductRelations(productId: number, productData: any) {
    // Handle attributes
    if (productData.attributes && productData.attributes.length > 0) {
      await this.prismaService.productAttribute.deleteMany({
        where: { productId },
      });

      for (const attr of productData.attributes) {
        try {
          await this.prismaService.productAttribute.create({
            data: {
              kiotVietId: attr.kiotVietId ? BigInt(attr.kiotVietId) : null,
              productId,
              attributeName: attr.attributeName,
              attributeValue: attr.attributeValue,
              lastSyncedAt: new Date(),
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save attribute: ${error.message}`);
        }
      }
    }

    // Handle images
    if (productData.images && productData.images.length > 0) {
      await this.prismaService.productImage.deleteMany({
        where: { productId },
      });

      for (let i = 0; i < productData.images.length; i++) {
        try {
          await this.prismaService.productImage.create({
            data: {
              productId,
              imageUrl: productData.images[i].Image || productData.images[i],
              displayOrder: i,
              lastSyncedAt: new Date(),
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save image: ${error.message}`);
        }
      }
    }

    // Handle inventories
    if (productData.inventories && productData.inventories.length > 0) {
      for (const inv of productData.inventories) {
        try {
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: inv.branchId },
          });

          if (branch) {
            await this.prismaService.productInventory.upsert({
              where: {
                productId_branchId: {
                  productId,
                  branchId: branch.id,
                },
              },
              create: {
                productId,
                branchId: branch.id,
                onHand: inv.onHand || 0,
                reserved: inv.reserved || 0,
                onOrder: inv.onOrder || 0,
                cost: inv.cost ? new Prisma.Decimal(inv.cost) : null,
                minQuantity: inv.minQuality,
                maxQuantity: inv.maxQuality,
                lastSyncedAt: new Date(),
              },
              update: {
                onHand: inv.onHand || 0,
                reserved: inv.reserved || 0,
                onOrder: inv.onOrder || 0,
                cost: inv.cost ? new Prisma.Decimal(inv.cost) : null,
                minQuantity: inv.minQuality,
                maxQuantity: inv.maxQuality,
                modifiedDate: new Date(),
                lastSyncedAt: new Date(),
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save inventory: ${error.message}`);
        }
      }
    }

    // Handle serials
    if (productData.productSerials && productData.productSerials.length > 0) {
      for (const serial of productData.productSerials) {
        try {
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: serial.branchId },
          });

          if (branch) {
            await this.prismaService.productSerial.upsert({
              where: {
                productId_serialNumber_branchId: {
                  productId,
                  serialNumber: serial.serialNumber,
                  branchId: branch.id,
                },
              },
              create: {
                kiotVietId: serial.kiotVietId
                  ? BigInt(serial.kiotVietId)
                  : null,
                productId,
                serialNumber: serial.serialNumber,
                branchId: branch.id,
                status: serial.status,
                quantity: serial.quantity || 1,
                createdDate: serial.createdDate
                  ? new Date(serial.createdDate)
                  : new Date(),
                modifiedDate: serial.modifiedDate
                  ? new Date(serial.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
              update: {
                status: serial.status,
                quantity: serial.quantity || 1,
                modifiedDate: serial.modifiedDate
                  ? new Date(serial.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save serial: ${error.message}`);
        }
      }
    }

    // Handle batch expires
    if (
      productData.productBatchExpires &&
      productData.productBatchExpires.length > 0
    ) {
      for (const batch of productData.productBatchExpires) {
        try {
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: batch.branchId },
          });

          if (branch) {
            await this.prismaService.productBatchExpire.upsert({
              where: {
                productId_batchName_branchId: {
                  productId,
                  batchName: batch.batchName,
                  branchId: branch.id,
                },
              },
              create: {
                kiotVietId: batch.kiotVietId ? BigInt(batch.kiotVietId) : null,
                productId,
                batchName: batch.batchName,
                fullNameVirgule: batch.fullNameVirgule,
                expireDate: batch.expireDate
                  ? new Date(batch.expireDate)
                  : null,
                onHand: batch.onHand || 0,
                branchId: branch.id,
                createdDate: batch.createdDate
                  ? new Date(batch.createdDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
              update: {
                fullNameVirgule: batch.fullNameVirgule,
                expireDate: batch.expireDate
                  ? new Date(batch.expireDate)
                  : null,
                onHand: batch.onHand || 0,
                lastSyncedAt: new Date(),
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save batch expire: ${error.message}`);
        }
      }
    }

    // Handle warranties
    if (
      productData.productWarranties &&
      productData.productWarranties.length > 0
    ) {
      await this.prismaService.productWarranty.deleteMany({
        where: { productId },
      });

      for (const warranty of productData.productWarranties) {
        try {
          await this.prismaService.productWarranty.create({
            data: {
              kiotVietId: warranty.Id ? BigInt(warranty.Id) : null,
              productId,
              description: warranty.description,
              numberTime: warranty.numberTime,
              timeType: warranty.timeType,
              warrantyType: warranty.warrantyType,
              createdDate: warranty.createdDate
                ? new Date(warranty.createdDate)
                : new Date(),
              modifiedDate: warranty.modifiedDate
                ? new Date(warranty.modifiedDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save warranty: ${error.message}`);
        }
      }
    }

    // Handle price books
    if (productData.priceBooks && productData.priceBooks.length > 0) {
      for (const priceBook of productData.priceBooks) {
        try {
          const existingPriceBook =
            await this.prismaService.priceBook.findFirst({
              where: { kiotVietId: priceBook.priceBookId },
            });

          if (existingPriceBook) {
            await this.prismaService.priceBookDetail.upsert({
              where: {
                priceBookId_productId: {
                  priceBookId: existingPriceBook.id,
                  productId,
                },
              },
              create: {
                kiotVietId: priceBook.kiotVietId
                  ? BigInt(priceBook.kiotVietId)
                  : null,
                priceBookId: existingPriceBook.id,
                productId,
                price: new Prisma.Decimal(priceBook.price || 0),
              },
              update: {
                price: new Prisma.Decimal(priceBook.price || 0),
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save price book: ${error.message}`);
        }
      }
    }

    // Handle product formulas (combo products)
    if (productData.productFormulas && productData.productFormulas.length > 0) {
      await this.prismaService.productFormula.deleteMany({
        where: { productId },
      });

      for (const formula of productData.productFormulas) {
        try {
          const materialProduct = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(formula.materialId) },
          });

          if (materialProduct) {
            await this.prismaService.productFormula.create({
              data: {
                kiotVietId: formula.kiotVietId
                  ? BigInt(formula.kiotVietId)
                  : null,
                productId,
                materialId: materialProduct.id,
                quantity: formula.quantity,
                basePrice: new Prisma.Decimal(formula.basePrice || 0),
                lastSyncedAt: new Date(),
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save product formula: ${error.message}`);
        }
      }
    }
  }

  private async handleRemovedProducts(removedIds: number[]) {
    if (!removedIds || removedIds.length === 0) return 0;

    try {
      const result = await this.prismaService.product.updateMany({
        where: { kiotVietId: { in: removedIds.map((id) => BigInt(id)) } },
        data: { isActive: false, lastSyncedAt: new Date() },
      });

      this.logger.log(`Marked ${result.count} products as inactive`);
      return result.count;
    } catch (error) {
      this.logger.error(`Failed to handle removed products: ${error.message}`);
      throw error;
    }
  }

  async syncRecentProducts(days: number = 7): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'product_recent' },
        create: {
          name: 'product_recent',
          entities: ['product'],
          syncMode: 'recent',
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
        },
      });

      const lastModifiedFrom = dayjs()
        .subtract(days, 'day')
        .format('YYYY-MM-DD');
      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchProducts({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveProducts(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Product recent sync progress: ${totalProcessed} products processed`,
          );
        }

        if (response.removeId && response.removeId.length > 0) {
          await this.handleRemovedProducts(response.removeId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'product_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Product recent sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'product_recent' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Product recent sync failed: ${error.message}`);
      throw error;
    }
  }

  async syncHistoricalProducts(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'product_historical' },
        create: {
          name: 'product_historical',
          entities: ['product'],
          syncMode: 'historical',
          isRunning: true,
          isEnabled: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
          progress: {},
        },
      });

      let currentItem = 0;
      let totalProcessed = 0;
      let batchCount = 0;
      let hasMoreData = true;
      const productBatch: any[] = [];

      this.logger.log('Starting historical product sync...');

      while (hasMoreData) {
        const response = await this.fetchProducts({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          productBatch.push(...response.data);

          if (
            productBatch.length >= this.BATCH_SIZE ||
            response.data.length < this.PAGE_SIZE
          ) {
            const { created, updated } =
              await this.batchSaveProducts(productBatch);
            totalProcessed += created + updated;
            batchCount++;

            await this.prismaService.syncControl.update({
              where: { name: 'product_historical' },
              data: {
                progress: {
                  totalProcessed,
                  batchCount,
                  lastProcessedItem: currentItem + response.data.length,
                },
              },
            });

            this.logger.log(
              `Product historical sync batch ${batchCount}: ${totalProcessed} products processed`,
            );
            productBatch.length = 0;
          }
        }

        if (response.removeId && response.removeId.length > 0) {
          await this.handleRemovedProducts(response.removeId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'product_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, batchCount },
        },
      });

      this.logger.log(
        `Product historical sync completed: ${totalProcessed} products processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'product_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Product historical sync failed: ${error.message}`);
      throw error;
    }
  }
}
