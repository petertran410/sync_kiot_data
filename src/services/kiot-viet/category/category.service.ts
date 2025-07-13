// src/services/kiot-viet/category/category.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietCategory {
  categoryId: number;
  parentId?: number;
  categoryName: string;
  retailerId?: number;
  hasChild?: boolean;
  modifiedDate?: string;
  createdDate?: string;
}

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

  // ============================================================================
  // HISTORICAL SYNC - SIMPLE VERSION
  // ============================================================================

  async syncHistoricalCategories(): Promise<void> {
    const syncName = 'category_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical category sync...');

      // Fetch all categories with hierarchical data
      const response = await this.fetchCategoriesWithRetry({
        hierachicalData: false, // Get flat list for easier processing
        orderBy: 'createdDate',
        orderDirection: 'ASC',
        pageSize: 100,
      });

      const categories = response.data || [];
      this.logger.log(`📊 Found ${categories.length} categories to sync`);

      if (categories.length > 0) {
        const saved = await this.saveCategoriesToDatabase(categories);
        this.logger.log(`✅ Saved ${saved.created + saved.updated} categories`);
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log('✅ Historical category sync completed');
    } catch (error) {
      this.logger.error(`❌ Historical category sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS
  // ============================================================================

  async fetchCategoriesWithRetry(
    params: {
      hierachicalData?: boolean;
      orderBy?: string;
      orderDirection?: string;
      pageSize?: number;
      currentItem?: number;
      lastModifiedFrom?: string;
    },
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchCategories(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
        }
      }
    }

    throw lastError;
  }

  async fetchCategories(params: {
    hierachicalData?: boolean;
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      hierachicalData: (params.hierachicalData || false).toString(),
      orderBy: params.orderBy || 'createdDate',
      orderDirection: params.orderDirection || 'ASC',
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/categories?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // DATABASE SAVE
  // ============================================================================

  private async saveCategoriesToDatabase(
    categories: KiotVietCategory[],
  ): Promise<{ created: number; updated: number }> {
    this.logger.log(`💾 Saving ${categories.length} categories to database...`);

    // Sort categories by dependency (parents first)
    const sortedCategories = this.sortCategoriesByDependency(categories);

    let created = 0;
    let updated = 0;

    for (const categoryData of sortedCategories) {
      try {
        // Find parent category if exists
        const parentCategory = categoryData.parentId
          ? await this.prismaService.category.findFirst({
              where: { kiotVietId: categoryData.parentId },
              select: { id: true },
            })
          : null;

        const result = await this.prismaService.category.upsert({
          where: { kiotVietId: categoryData.categoryId },
          update: {
            name: categoryData.categoryName,
            parentId: parentCategory?.id ?? null,
            hasChildren: categoryData.hasChild ?? false,
            retailerId: categoryData.retailerId ?? null,
            modifiedDate: categoryData.modifiedDate
              ? new Date(categoryData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: categoryData.categoryId,
            name: categoryData.categoryName,
            parentId: parentCategory?.id ?? null,
            hasChildren: categoryData.hasChild ?? false,
            retailerId: categoryData.retailerId ?? null,
            createdDate: categoryData.createdDate
              ? new Date(categoryData.createdDate)
              : new Date(),
            modifiedDate: categoryData.modifiedDate
              ? new Date(categoryData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        // Check if it was created or updated
        const existingCount = await this.prismaService.category.count({
          where: {
            kiotVietId: categoryData.categoryId,
            createdDate: {
              lt: new Date(Date.now() - 1000), // Created more than 1 second ago
            },
          },
        });

        if (existingCount > 0) {
          updated++;
        } else {
          created++;
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save category ${categoryData.categoryName}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Categories saved: ${created} created, ${updated} updated`,
    );
    return { created, updated };
  }

  // ============================================================================
  // DEPENDENCY SORTING - PARENTS FIRST
  // ============================================================================

  private sortCategoriesByDependency(
    categories: KiotVietCategory[],
  ): KiotVietCategory[] {
    const categoryMap = new Map<number, KiotVietCategory>();
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

    // Sort by dependency order
    const sortedCategories: KiotVietCategory[] = [];
    const visited = new Set<number>();

    const processCategory = (categoryId: number) => {
      const category = categoryMap.get(categoryId);
      if (!category || visited.has(categoryId)) return;

      // If has parent, process parent first
      if (category.parentId && !visited.has(category.parentId)) {
        processCategory(category.parentId);
      }

      // Process current category
      visited.add(categoryId);
      sortedCategories.push(category);

      // Process children
      const children = parentChildMap.get(categoryId) || [];
      for (const childId of children) {
        processCategory(childId);
      }
    };

    // Start with root categories
    for (const rootId of rootCategories) {
      processCategory(rootId);
    }

    // Process any remaining categories
    for (const category of categories) {
      if (!visited.has(category.categoryId)) {
        processCategory(category.categoryId);
      }
    }

    return sortedCategories;
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('category_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical category sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['category'],
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
