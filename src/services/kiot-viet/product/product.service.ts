// src/services/kiot-viet/product/product.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class KiotVietProductService {
  private readonly logger = new Logger(KiotVietProductService.name);
  private readonly PAGE_SIZE = 50;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncHistoricalProducts(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'product_historical' },
        create: {
          name: 'product_historical',
          entities: ['product'],
          syncMode: 'historical',
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

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchProducts({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.saveProductsToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Product sync progress: ${totalProcessed} products processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'product_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Product sync completed: ${totalProcessed} products processed`,
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

      this.logger.error(`Product sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchProducts(params: any): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const queryParams = new URLSearchParams();
      if (params.currentItem !== undefined) {
        queryParams.append('currentItem', params.currentItem.toString());
      }
      if (params.pageSize) {
        queryParams.append('pageSize', params.pageSize.toString());
      }

      const url = `${baseUrl}/products?${queryParams.toString()}`;

      const response = await this.httpService
        .get(url, {
          headers: {
            Retailer: this.configService.get<string>('KIOT_SHOP_NAME'),
            Authorization: `Bearer ${accessToken}`,
          },
        })
        .toPromise();

      return response?.data;
    } catch (error) {
      this.logger.error(`Failed to fetch products: ${error.message}`);
      throw error;
    }
  }

  private async saveProductsToDatabase(
    products: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const productData of products) {
      try {
        const existingProduct = await this.prismaService.product.findUnique({
          where: { kiotVietId: BigInt(productData.id) },
        });

        if (existingProduct) {
          const updateData = await this.prepareProductUpdateData(productData);
          await this.prismaService.product.update({
            where: { id: existingProduct.id },
            data: updateData,
          });

          await this.handleProductRelations(existingProduct.id, productData);
          updatedCount++;
        } else {
          const createData = await this.prepareProductCreateData(productData);
          if (createData) {
            const product = await this.prismaService.product.create({
              data: createData,
            });

            await this.handleProductRelations(product.id, productData);
            createdCount++;
          }
        }
      } catch (error) {
        this.logger.error(
          `Failed to save product ${productData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async prepareProductCreateData(
    productData: any,
  ): Promise<Prisma.ProductCreateInput | null> {
    try {
      const data: Prisma.ProductCreateInput = {
        kiotVietId: BigInt(productData.id),
        code: productData.code,
        name: productData.name,
        fullName: productData.fullName || productData.name,
        allowsSale:
          productData.allowsSale !== undefined ? productData.allowsSale : true,
        hasVariants: productData.hasVariants || false,
        basePrice: productData.basePrice
          ? parseFloat(productData.basePrice)
          : 0,
        weight: productData.weight || null,
        unit: productData.unit || null,
        masterUnitId: productData.masterUnitId
          ? BigInt(productData.masterUnitId)
          : null,
        conversionValue: productData.conversionValue || null,
        createdDate: productData.createdDate
          ? new Date(productData.createdDate)
          : new Date(),
        modifiedDate: productData.modifiedDate
          ? new Date(productData.modifiedDate)
          : new Date(),
        lastSyncedAt: new Date(),
      };

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

      // Handle master product relationship
      if (productData.masterProductId) {
        const masterProduct = await this.prismaService.product.findFirst({
          where: { kiotVietId: BigInt(productData.masterProductId) },
        });
        if (masterProduct) {
          data.masterProduct = { connect: { id: masterProduct.id } };
        }
      }

      return data;
    } catch (error) {
      this.logger.error(
        `Failed to prepare product create data: ${error.message}`,
      );
      return null;
    }
  }

  private async prepareProductUpdateData(
    productData: any,
  ): Promise<Prisma.ProductUpdateInput> {
    const data: Prisma.ProductUpdateInput = {
      code: productData.code,
      name: productData.name,
      fullName: productData.fullName || productData.name,
      allowsSale:
        productData.allowsSale !== undefined ? productData.allowsSale : true,
      hasVariants: productData.hasVariants || false,
      basePrice: productData.basePrice ? parseFloat(productData.basePrice) : 0,
      weight: productData.weight || null,
      unit: productData.unit || null,
      masterUnitId: productData.masterUnitId
        ? BigInt(productData.masterUnitId)
        : null,
      conversionValue: productData.conversionValue || null,
      modifiedDate: productData.modifiedDate
        ? new Date(productData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // Handle relationships (same as create)
    if (productData.categoryId) {
      const category = await this.prismaService.category.findFirst({
        where: { kiotVietId: productData.categoryId },
      });
      if (category) {
        data.category = { connect: { id: category.id } };
      }
    }

    if (productData.tradeMarkId) {
      const tradeMark = await this.prismaService.tradeMark.findFirst({
        where: { kiotVietId: productData.tradeMarkId },
      });
      if (tradeMark) {
        data.tradeMark = { connect: { id: tradeMark.id } };
      }
    }

    if (productData.masterProductId) {
      const masterProduct = await this.prismaService.product.findFirst({
        where: { kiotVietId: BigInt(productData.masterProductId) },
      });
      if (masterProduct) {
        data.masterProduct = { connect: { id: masterProduct.id } };
      }
    }

    return data;
  }

  private async handleProductRelations(
    productId: number,
    productData: any,
  ): Promise<void> {
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
            },
          });
        } catch (error) {
          this.logger.error(
            `Failed to create product attribute: ${error.message}`,
          );
        }
      }
    }

    // Handle inventories
    if (productData.inventories && productData.inventories.length > 0) {
      await this.prismaService.productInventory.deleteMany({
        where: { productId },
      });

      for (const inventory of productData.inventories) {
        try {
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: inventory.branchId },
          });

          if (branch) {
            await this.prismaService.productInventory.create({
              data: {
                productId,
                branchId: branch.id,
                cost: inventory.cost ? parseFloat(inventory.cost) : 0,
                onHand: inventory.onHand || 0,
                reserved: inventory.reserved || 0,
              },
            });
          }
        } catch (error) {
          this.logger.error(
            `Failed to create product inventory: ${error.message}`,
          );
        }
      }
    }

    // Handle images
    if (productData.images && productData.images.length > 0) {
      await this.prismaService.productImage.deleteMany({
        where: { productId },
      });

      for (const image of productData.images) {
        try {
          await this.prismaService.productImage.create({
            data: {
              productId,
              imageUrl: image.image,
            },
          });
        } catch (error) {
          this.logger.error(`Failed to create product image: ${error.message}`);
        }
      }
    }
  }
}
