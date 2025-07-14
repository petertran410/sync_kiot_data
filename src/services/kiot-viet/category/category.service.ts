// src/services/kiot-viet/category/category.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, firstValueFrom } from 'rxjs';

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

    let currentItem = 0;
    let processedCount = 0;
    let totalCategories = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedCategoryIds = new Set<number>();
    // let allCategories: KiotVietCategory[] = [];

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical category sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalCategories > 0) {
          if (currentItem >= totalCategories) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalCategories} categories`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalCategories) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalCategories} - ${progressPercentage.toFixed(1)}%)`,
          );
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
                `🔚 Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: categories } = categoryListResponse;

          if (total !== undefined && total !== null) {
            if (totalCategories === 0) {
              totalCategories = total;
              this.logger.log(
                `📊 Total categories detected: ${totalCategories}`,
              );
            } else if (total !== totalCategories) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalCategories} -> ${total}. Using latest.`,
              );
              totalCategories = total;
            }
            lastValidTotal = total;
          }

          if (!categories || categories.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalCategories > 0 && currentItem >= totalCategories) {
              this.logger.log('✅ Reached end of data (empty page past total)');
              break;
            }

            // If too many consecutive empty pages, stop
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            // Move to next page
            currentItem += this.PAGE_SIZE;
            continue;
          }

          const newCategories = categories.filter((category) => {
            if (processedCategoryIds.has(category.id)) {
              this.logger.debug(
                `⚠️ Duplicate category ID detected: ${category.id} (${category.code})`,
              );
              return false;
            }
            processedCategoryIds.add(category.id);
            return true;
          });

          if (newCategories.length !== categories.length) {
            this.logger.warn(
              `🔄 Filtered out ${categories.length - newCategories.length} duplicate categories on page ${currentPage}`,
            );
          }

          if (newCategories.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all categories already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newCategories.length} categories from page ${currentPage}...`,
          );

          const categoriesWithDetails =
            await this.enrichCategoriesWithDetails(newCategories);
          const savedCategories = await this.saveCategoriesToDatabase(
            categoriesWithDetails,
          );

          processedCount += savedCategories.length;
          currentItem += this.PAGE_SIZE;

          if (totalCategories > 0) {
            const completionPercentage =
              (processedCount / totalCategories) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalCategories} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalCategories) {
              this.logger.log('🎉 All categories processed successfully!');
              break;
            }
          }

          if (totalCategories > 0) {
            if (
              currentItem >= totalCategories &&
              processedCount >= totalCategories * 0.95
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
            `❌ API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          // Exponential backoff
          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`⏳ Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalCategories },
      });

      const completionRate =
        totalCategories > 0 ? (processedCount / totalCategories) * 100 : 100;

      this.logger.log(
        `✅ Historical category sync completed: ${processedCount}/${totalCategories} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical category sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalCategories },
      });

      throw error;
    }
  }

  //   const response = await this.fetchCategoriesWithRetry({
  //     hierachicalData: true,
  //     orderBy: 'createdDate',
  //     orderDirection: 'ASC',
  //     pageSize: this.PAGE_SIZE,
  //     currentItem,
  //   });

  //   const { data: categories, total } = response;

  //   // Set total on first page
  //   if (totalCategories === 0 && total !== undefined) {
  //     totalCategories = total;
  //     this.logger.log(`📊 Total categories detected: ${totalCategories}`);
  //   }

  //   // Check if we have data
  //   if (!categories || categories.length === 0) {
  //     this.logger.log(
  //       '✅ No more categories to fetch - pagination complete',
  //     );
  //     break;
  //   }

  //   // Add to all categories
  //   allCategories.push(...categories);
  //   processedCount += categories.length;
  //   currentItem += this.PAGE_SIZE;

  //   this.logger.log(
  //     `📈 Progress: ${processedCount}/${totalCategories || 'unknown'} categories fetched`,
  //   );

  //   // Break if we've got all categories
  //   if (totalCategories > 0 && processedCount >= totalCategories) {
  //     this.logger.log('🎉 All categories fetched successfully!');
  //     break;
  //   }

  //   // Break if this page was not full (last page)
  //   if (categories.length < this.PAGE_SIZE) {
  //     this.logger.log('✅ Last page reached (partial page)');
  //     break;
  //   }

  //   // Rate limiting delay
  //   await new Promise((resolve) => setTimeout(resolve, 100));
  // }

  // this.logger.log(`📊 Total fetched: ${allCategories.length} categories`);

  // if (allCategories.length > 0) {
  //   const saved = await this.saveCategoriesToDatabase(allCategories);
  //   this.logger.log(`✅ Saved ${saved.created + saved.updated} categories`);
  // }

  // await this.updateSyncControl(syncName, {
  //   isRunning: false,
  //   isEnabled: false, // Auto-disable after completion
  //   status: 'completed',
  //   completedAt: new Date(),
  //   lastRunAt: new Date(),
  //   progress: { processedCount, expectedTotal: totalCategories },
  // });

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
      hierachicalData: (params.hierachicalData || true).toString(),
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

  private async saveCategoriesToDatabase(categories: any[]): Promise<any[]> {
    this.logger.log(`💾 Saving ${categories.length} categories to database...`);

    const saveCategories: any[] = [];

    for (const categoryData of categories) {
      try {
        const parentCategory = categoryData.parentId
          ? await this.prismaService.category.findFirst({
              where: { kiotVietId: categoryData.parentId },
              select: { id: true },
            })
          : null;

        const category = await this.prismaService.category.upsert({
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

        saveCategories.push(category);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save category ${categoryData.categoryName}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${saveCategories.length} categories to database`);
    return saveCategories;
  }

  private async enrichCategoriesWithDetails(
    categories: KiotVietCategory[],
  ): Promise<KiotVietCategory[]> {
    this.logger.log(
      `🔍 Enriching ${categories.length} categories with details...`,
    );

    const enrichedCategories: any[] = [];
    for (const category of categories) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/categories/${category.categoryId}`,
            {
              headers,
            },
          ),
        );
        if (response.data) {
          enrichedCategories.push(response.data);
        } else {
          enrichedCategories.push(category);
        }
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
  // DEPENDENCY SORTING - PARENTS FIRST
  // ============================================================================

  // private sortCategoriesByDependency(
  //   categories: KiotVietCategory[],
  // ): KiotVietCategory[] {
  //   const categoryMap = new Map<number, KiotVietCategory>();
  //   const parentChildMap = new Map<number, number[]>();
  //   const rootCategories: number[] = [];

  //   // Build maps
  //   for (const category of categories) {
  //     categoryMap.set(category.categoryId, category);

  //     if (category.parentId) {
  //       if (!parentChildMap.has(category.parentId)) {
  //         parentChildMap.set(category.parentId, []);
  //       }
  //       parentChildMap.get(category.parentId)!.push(category.categoryId);
  //     } else {
  //       rootCategories.push(category.categoryId);
  //     }
  //   }

  //   // Sort by dependency order
  //   const sortedCategories: KiotVietCategory[] = [];
  //   const visited = new Set<number>();

  //   const processCategory = (categoryId: number) => {
  //     const category = categoryMap.get(categoryId);
  //     if (!category || visited.has(categoryId)) return;

  //     // If has parent, process parent first
  //     if (category.parentId && !visited.has(category.parentId)) {
  //       processCategory(category.parentId);
  //     }

  //     // Process current category
  //     visited.add(categoryId);
  //     sortedCategories.push(category);

  //     // Process children
  //     const children = parentChildMap.get(categoryId) || [];
  //     for (const childId of children) {
  //       processCategory(childId);
  //     }
  //   };

  //   // Start with root categories
  //   for (const rootId of rootCategories) {
  //     processCategory(rootId);
  //   }

  //   // Process any remaining categories
  //   for (const category of categories) {
  //     if (!visited.has(category.categoryId)) {
  //       processCategory(category.categoryId);
  //     }
  //   }

  //   return sortedCategories;
  // }

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
