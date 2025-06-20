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

  // FIXED: New method to sort categories by dependency (parents before children)
  private sortCategoriesByDependency(categories: any[]): any[] {
    const categoryMap = new Map<number, any>();
    const parentChildMap = new Map<number, number[]>();
    const rootCategories: number[] = [];

    // Build maps
    for (const category of categories) {
      categoryMap.set(category.categoryId, category);

      if (category.parentId) {
        if (!parentChildMap.has(category.parentId)) {
          parentChildMap.set(category.parentId, []);
        }
        parentChildMap.get(category.parentId)!.push(category.categoryId);
      } else {
        rootCategories.push(category.categoryId);
      }
    }

    const sortedCategories: any[] = [];
    const visited = new Set<number>();

    // Recursive function to process categories in dependency order
    const processCategory = (categoryId: number) => {
      if (visited.has(categoryId)) return;

      const category = categoryMap.get(categoryId);
      if (!category) return;

      // If this category has a parent, process parent first
      if (category.parentId && !visited.has(category.parentId)) {
        processCategory(category.parentId);
      }

      // Process current category
      if (!visited.has(categoryId)) {
        visited.add(categoryId);
        sortedCategories.push(category);

        // Process children
        const children = parentChildMap.get(categoryId) || [];
        for (const childId of children) {
          processCategory(childId);
        }
      }
    };

    // Start with root categories
    for (const rootId of rootCategories) {
      processCategory(rootId);
    }

    // Process any remaining categories (in case of orphaned records)
    for (const category of categories) {
      if (!visited.has(category.categoryId)) {
        processCategory(category.categoryId);
      }
    }

    this.logger.log(
      `Sorted ${categories.length} categories by dependency order`,
    );
    return sortedCategories;
  }

  // FIXED: Updated method to handle categories with proper dependency ordering
  private async batchSaveCategories(categories: any[]) {
    if (!categories || categories.length === 0)
      return { created: 0, updated: 0 };

    // FIXED: Sort categories to ensure parents are processed before children
    const sortedCategories = this.sortCategoriesByDependency(categories);

    const kiotVietIds = sortedCategories.map((c) => c.categoryId);
    const existingCategories = await this.prismaService.category.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingCategories
        .filter((c) => c.kiotVietId !== null)
        .map((c) => [c.kiotVietId!, c.id]),
    );

    // Create a map to track newly created categories
    const newlyCreatedMap = new Map<number, number>();

    let createdCount = 0;
    let updatedCount = 0;

    // FIXED: Process categories in dependency order
    for (const categoryData of sortedCategories) {
      try {
        const existingId = existingMap.get(categoryData.categoryId);

        // Prepare parent reference
        let parentReference: { connect: { id: number } } | null = null;
        if (categoryData.parentId) {
          // Check existing categories first
          const existingParentId = existingMap.get(categoryData.parentId);
          if (existingParentId) {
            parentReference = { connect: { id: existingParentId } };
          } else {
            // Check newly created categories
            const newlyCreatedParentId = newlyCreatedMap.get(
              categoryData.parentId,
            );
            if (newlyCreatedParentId) {
              parentReference = { connect: { id: newlyCreatedParentId } };
            } else {
              // Parent doesn't exist, log warning and create without parent
              this.logger.warn(
                `Parent category ${categoryData.parentId} not found for category ${categoryData.categoryName}. Creating without parent.`,
              );
              parentReference = null;
            }
          }
        }

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
          const updateData: any = { ...commonData };
          if (parentReference) {
            updateData.parent = parentReference;
          }

          await this.prismaService.category.update({
            where: { id: existingId },
            data: updateData,
          });
          updatedCount++;
        } else {
          const createData: any = {
            kiotVietId: categoryData.categoryId,
            ...commonData,
            createdDate: categoryData.createdDate
              ? new Date(categoryData.createdDate)
              : new Date(),
          };

          if (parentReference) {
            createData.parent = parentReference;
          }

          const newCategory = await this.prismaService.category.create({
            data: createData,
          });

          // Track newly created category
          newlyCreatedMap.set(categoryData.categoryId, newCategory.id);
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save category ${categoryData.categoryName}: ${error.message}`,
        );
        // Continue processing other categories instead of stopping
        continue;
      }
    }

    this.logger.log(
      `Category batch processing completed: ${createdCount} created, ${updatedCount} updated`,
    );

    return { created: createdCount, updated: updatedCount };
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
      const allCategories: any[] = [];

      // FIXED: First, collect all categories
      while (hasMoreData) {
        const response = await this.fetchCategories({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          allCategories.push(...response.data);
        }

        if (response.removedIds && response.removedIds.length > 0) {
          await this.handleRemovedCategories(response.removedIds);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      // FIXED: Process all categories together to ensure proper dependency ordering
      if (allCategories.length > 0) {
        const { created, updated } =
          await this.batchSaveCategories(allCategories);
        totalProcessed = created + updated;

        this.logger.log(
          `Category sync completed: ${totalProcessed} categories processed (${created} created, ${updated} updated)`,
        );
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
