// src/services/kiot-viet/category/category.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietCategoryService {
  private readonly logger = new Logger(KiotVietCategoryService.name);
  private readonly baseUrl: string;
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

  async fetchCategories(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/categories`, {
          headers,
          params: {
            ...params,
            hierachicalData: false,
            includeRemoveIds: true,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch categories: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveCategories(categories: any[]) {
    if (!categories || categories.length === 0)
      return { created: 0, updated: 0 };

    const kiotVietIds = categories.map((c) => c.categoryId);

    const existingCategories = await this.prismaService.category.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingCategories
        .filter((c) => c.kiotVietId !== null)
        .map((c) => [c.kiotVietId!, c.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const categoryData of categories) {
      try {
        const existingId = existingMap.get(categoryData.categoryId);

        const commonData = {
          name: categoryData.categoryName,
          hasChildren: categoryData.hasChild || false,
          retailerId: categoryData.retailerId,
          modifiedDate: categoryData.modifiedDate
            ? new Date(categoryData.modifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
        };

        if (existingId) {
          await this.prismaService.category.update({
            where: { id: existingId },
            data: commonData,
          });
          updatedCount++;
        } else {
          await this.prismaService.category.create({
            data: {
              kiotVietId: categoryData.categoryId,
              ...commonData,
              parentId: categoryData.parentId || null,
              createdDate: categoryData.createdDate
                ? new Date(categoryData.createdDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save category ${categoryData.categoryName}: ${error.message}`,
        );
      }
    }

    // Handle parent-child relationships in a second pass
    await this.updateCategoryHierarchy(categories);

    return { created: createdCount, updated: updatedCount };
  }

  private async updateCategoryHierarchy(categories: any[]) {
    for (const categoryData of categories) {
      if (categoryData.parentId) {
        try {
          const parentCategory = await this.prismaService.category.findFirst({
            where: { kiotVietId: categoryData.parentId },
          });

          if (parentCategory) {
            await this.prismaService.category.updateMany({
              where: { kiotVietId: categoryData.categoryId },
              data: { parentId: parentCategory.id },
            });
          }
        } catch (error) {
          this.logger.error(
            `Failed to update category hierarchy for ${categoryData.categoryName}: ${error.message}`,
          );
        }
      }
    }
  }

  private async handleRemovedCategories(removedIds: number[]) {
    if (!removedIds || removedIds.length === 0) return 0;

    try {
      const result = await this.prismaService.category.updateMany({
        where: { kiotVietId: { in: removedIds } },
        data: { lastSyncedAt: new Date() },
      });

      this.logger.log(`Updated ${result.count} removed categories`);
      return result.count;
    } catch (error) {
      this.logger.error(
        `Failed to handle removed categories: ${error.message}`,
      );
      throw error;
    }
  }

  async syncCategories(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'category_sync' },
        create: {
          name: 'category_sync',
          entities: ['category'],
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

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchCategories({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveCategories(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Category sync progress: ${totalProcessed} processed`,
          );
        }

        if (response.removedIds && response.removedIds.length > 0) {
          await this.handleRemovedCategories(response.removedIds);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'category_sync' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(`Category sync completed: ${totalProcessed} processed`);
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'category_sync' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Category sync failed: ${error.message}`);
      throw error;
    }
  }
}
