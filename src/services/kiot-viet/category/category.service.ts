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
  rank?: number;
  children?: KiotVietCategory[];
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
  // HISTORICAL SYNC - FIXED PAGINATION LOGIC
  // ============================================================================

  async syncHistoricalCategories(): Promise<void> {
    const syncName = 'category_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalCategories = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let processedCategoryIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical category sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 3; // Reduced from 5 for faster detection
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        // Progress logging
        if (totalCategories > 0) {
          const progressPercentage = (processedCount / totalCategories) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${processedCount}/${totalCategories} - ${progressPercentage.toFixed(1)}% completed)`,
          );

          // FIXED: Early termination check based on processed count, not currentItem
          if (processedCount >= totalCategories) {
            this.logger.log(
              `✅ All categories processed successfully! Final count: ${processedCount}/${totalCategories}`,
            );
            break;
          }
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const categoryListResponse = await this.fetchCategoriesWithRetry({
            hierachicalData: true,
            orderBy: 'createdDate',
            orderDirection: 'ASC',
            pageSize: this.PAGE_SIZE,
            currentItem,
          });

          if (!categoryListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 API returned null ${consecutiveEmptyPages} times - ending pagination`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            currentItem += this.PAGE_SIZE; // Move to next page even on null response
            continue;
          }

          // Reset error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: categories } = categoryListResponse;

          // Set total count on first successful response
          if (total !== undefined && total !== null) {
            if (totalCategories === 0) {
              totalCategories = total;
              this.logger.log(
                `📊 Total categories detected: ${totalCategories}`,
              );
            } else if (total !== totalCategories) {
              this.logger.warn(
                `⚠️ Total count updated: ${totalCategories} → ${total}`,
              );
              totalCategories = total;
            }
          }

          // FIXED: Handle empty response data properly
          if (!categories || categories.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            // FIXED: Check if we've reached expected end
            if (totalCategories > 0 && processedCount >= totalCategories) {
              this.logger.log(
                '✅ All expected categories processed - pagination complete',
              );
              break;
            }

            // FIXED: Stop after too many consecutive empty pages
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            // Continue to next page
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // FIXED: Duplicate filtering with better validation
          const newCategories = categories.filter((category) => {
            // Validate required fields
            if (!category.categoryId || !category.categoryName) {
              this.logger.warn(
                `⚠️ Skipping invalid category: id=${category.categoryId}, name='${category.categoryName}'`,
              );
              return false;
            }

            // Check for duplicates
            if (processedCategoryIds.has(category.categoryId)) {
              this.logger.debug(
                `⚠️ Duplicate category ID detected: ${category.categoryId} (${category.categoryName})`,
              );
              return false;
            }

            processedCategoryIds.add(category.categoryId);
            return true;
          });

          // Log filtering results
          if (newCategories.length !== categories.length) {
            this.logger.warn(
              `🔄 Filtered out ${categories.length - newCategories.length} invalid/duplicate categories on page ${currentPage}`,
            );
          }

          // FIXED: Skip page if all categories were filtered out
          if (newCategories.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all categories were filtered out`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newCategories.length} categories from page ${currentPage}...`,
          );

          // Process categories
          const categoriesWithDetails =
            await this.enrichCategoriesWithDetails(newCategories);
          const savedCategories = await this.saveCategoriesToDatabase(
            categoriesWithDetails,
          );

          processedCount += savedCategories.length;

          // FIXED: Progress calculation based on actual processed count
          if (totalCategories > 0) {
            const completionPercentage =
              (processedCount / totalCategories) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalCategories} (${completionPercentage.toFixed(1)}%)`,
            );
          } else {
            this.logger.log(
              `📈 Progress: ${processedCount} categories processed`,
            );
          }

          // Move to next page
          currentItem += this.PAGE_SIZE;

          // Rate limiting
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          this.logger.error(
            `❌ Error fetching page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            this.logger.error(
              `💥 Too many consecutive errors (${consecutiveErrorPages}). Stopping sync.`,
            );
            throw error;
          }

          // Exponential backoff for retries
          await new Promise((resolve) =>
            setTimeout(resolve, RETRY_DELAY_MS * consecutiveErrorPages),
          );

          // Don't increment currentItem on error - retry the same page
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        error: null,
        progress: { processedCount, expectedTotal: totalCategories },
      });

      const completionRate =
        totalCategories > 0 ? (processedCount / totalCategories) * 100 : 100;

      this.logger.log(
        `✅ Historical category sync completed: ${processedCount}/${totalCategories} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
        progress: { processedCount, expectedTotal: totalCategories },
      });

      this.logger.error(`❌ Historical category sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // API METHODS - ENHANCED ERROR HANDLING
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
          // Exponential backoff
          const delayMs = 1000 * Math.pow(2, attempt - 1);
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError;
  }

  private async fetchCategories(params: {
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
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    // ENHANCED: Conservative parameter handling for KiotViet API compatibility
    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

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
  // ENRICH WITH DETAILS
  // ============================================================================

  private async enrichCategoriesWithDetails(
    categories: KiotVietCategory[],
  ): Promise<KiotVietCategory[]> {
    this.logger.log(
      `🔍 Enriching ${categories.length} categories with details...`,
    );

    const enrichedCategories: KiotVietCategory[] = [];

    for (const category of categories) {
      try {
        // CORRECTED: Use categoryId field name from API
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/categories/${category.categoryId}`,
            {
              headers,
              timeout: 15000,
            },
          ),
        );

        if (response.data && response.data.categoryId) {
          enrichedCategories.push(response.data);
        } else {
          this.logger.warn(
            `⚠️ No detailed data for category ${category.categoryId}, using basic data`,
          );
          enrichedCategories.push(category);
        }

        // Rate limiting
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich category ${category.categoryId}: ${error.message}`,
        );
        enrichedCategories.push(category);
      }
    }

    return enrichedCategories;
  }

  // ============================================================================
  // DATABASE SAVE - ENHANCED VALIDATION
  // ============================================================================

  private async saveCategoriesToDatabase(
    categories: KiotVietCategory[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${categories.length} categories to database...`);

    const savedCategories: any[] = [];

    // ENHANCED: Process in hierarchical order - parents first, then children
    const processedCategories = this.flattenAndSortCategories(categories);

    for (const categoryData of processedCategories) {
      try {
        // CORRECTED: Enhanced validation with actual API field names
        if (
          !categoryData.categoryId ||
          !categoryData.categoryName ||
          categoryData.categoryName.trim() === ''
        ) {
          this.logger.warn(
            `⚠️ Skipping invalid category: categoryId=${categoryData.categoryId}, categoryName='${categoryData.categoryName}'`,
          );
          continue;
        }

        // ENHANCED: Resolve parent relationship if exists
        let parentDatabaseId: number | null = null;
        if (categoryData.parentId) {
          const parentCategory = await this.prismaService.category.findFirst({
            where: { kiotVietId: categoryData.parentId },
            select: { id: true, name: true },
          });

          if (parentCategory) {
            parentDatabaseId = parentCategory.id;
          } else {
            this.logger.warn(
              `⚠️ Parent category ${categoryData.parentId} not found for category ${categoryData.categoryId}`,
            );
          }
        }

        const category = await this.prismaService.category.upsert({
          where: { kiotVietId: categoryData.categoryId },
          update: {
            name: categoryData.categoryName.trim(),
            parentId: parentDatabaseId,
            hasChild: categoryData.hasChild ?? false,
            retailerId: categoryData.retailerId || null,
            rank: categoryData.rank ?? 0,
            modifiedDate: categoryData.modifiedDate
              ? new Date(categoryData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: categoryData.categoryId,
            name: categoryData.categoryName.trim(),
            parentId: parentDatabaseId,
            hasChild: categoryData.hasChild ?? false,
            retailerId: categoryData.retailerId || null,
            rank: categoryData.rank ?? 0,
            createdDate: categoryData.createdDate
              ? new Date(categoryData.createdDate)
              : new Date(),
            modifiedDate: categoryData.modifiedDate
              ? new Date(categoryData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedCategories.push(category);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save category ${categoryData.categoryName}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `💾 Saved ${savedCategories.length} categories to database`,
    );
    return savedCategories;
  }

  private flattenAndSortCategories(
    categories: KiotVietCategory[],
  ): KiotVietCategory[] {
    const flattened: KiotVietCategory[] = [];
    const visited = new Set<number>();

    const processCategory = (category: KiotVietCategory) => {
      if (visited.has(category.categoryId)) {
        return;
      }

      visited.add(category.categoryId);
      flattened.push(category);

      // Process children recursively
      if (category.children && category.children.length > 0) {
        for (const child of category.children) {
          processCategory(child);
        }
      }
    };

    // Process root categories first (those without parentId)
    const rootCategories = categories.filter((cat) => !cat.parentId);
    const childCategories = categories.filter((cat) => cat.parentId);

    // Process all root categories and their hierarchies
    for (const rootCategory of rootCategories) {
      processCategory(rootCategory);
    }

    // Process any remaining child categories that weren't part of the hierarchy
    for (const childCategory of childCategories) {
      if (!visited.has(childCategory.categoryId)) {
        processCategory(childCategory);
      }
    }

    this.logger.log(
      `📊 Flattened ${categories.length} hierarchical categories into ${flattened.length} ordered entries`,
    );

    return flattened;
  }

  private validateCategoryData(category: KiotVietCategory): boolean {
    // Required field validation
    if (!category.categoryId || typeof category.categoryId !== 'number') {
      this.logger.warn(`⚠️ Invalid categoryId: ${category.categoryId}`);
      return false;
    }

    if (
      !category.categoryName ||
      typeof category.categoryName !== 'string' ||
      category.categoryName.trim() === ''
    ) {
      this.logger.warn(
        `⚠️ Invalid categoryName for ID ${category.categoryId}: '${category.categoryName}'`,
      );
      return false;
    }

    // Parent relationship validation
    if (category.parentId && typeof category.parentId !== 'number') {
      this.logger.warn(
        `⚠️ Invalid parentId for category ${category.categoryId}: ${category.parentId}`,
      );
      return false;
    }

    // Hierarchy consistency validation
    if (
      category.hasChild &&
      (!category.children || category.children.length === 0)
    ) {
      this.logger.debug(
        `ℹ️ Category ${category.categoryId} marked as hasChild but no children provided`,
      );
    }

    return true;
  }

  /**
   * Detect circular references in category hierarchy
   */
  private detectCircularReferences(categories: KiotVietCategory[]): boolean {
    const visited = new Set<number>();
    const recursionStack = new Set<number>();

    const dfs = (
      categoryId: number,
      categoryMap: Map<number, KiotVietCategory>,
    ): boolean => {
      if (recursionStack.has(categoryId)) {
        this.logger.error(
          `🔄 Circular reference detected for category ${categoryId}`,
        );
        return true;
      }

      if (visited.has(categoryId)) {
        return false;
      }

      visited.add(categoryId);
      recursionStack.add(categoryId);

      const category = categoryMap.get(categoryId);
      if (category?.parentId) {
        if (dfs(category.parentId, categoryMap)) {
          return true;
        }
      }

      recursionStack.delete(categoryId);
      return false;
    };

    const categoryMap = new Map<number, KiotVietCategory>();
    for (const category of categories) {
      categoryMap.set(category.categoryId, category);
    }

    for (const category of categories) {
      if (dfs(category.categoryId, categoryMap)) {
        return true;
      }
    }

    return false;
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
