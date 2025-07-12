// src/services/kiot-viet/pricebook/pricebook.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietPriceBook {
  id: number;
  name: string;
  isActive: boolean;
  isGlobal: boolean;
  startDate?: string;
  endDate?: string;
  forAllCusGroup: boolean;
  forAllUser: boolean;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
  details?: Array<{
    productId: number;
    productCode: string;
    price: number;
  }>;
  branches?: Array<{
    branchId: number;
    branchName: string;
  }>;
  customerGroups?: Array<{
    customerGroupId: number;
    customerGroupName: string;
  }>;
  users?: Array<{
    userId: number;
    userName: string;
  }>;
}

@Injectable()
export class KiotVietPriceBookService {
  private readonly logger = new Logger(KiotVietPriceBookService.name);
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
  // SYNC CONTROL & TRACKING - EXACT COPY FROM CUSTOMER PATTERN
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'pricebook_historical' },
      });

      // Only historical sync for PriceBook
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical pricebook sync...');
        await this.syncHistoricalPriceBooks();
        return;
      }

      // Default: historical sync
      this.logger.log('Running default historical pricebook sync...');
      await this.syncHistoricalPriceBooks();
    } catch (error) {
      this.logger.error(`PriceBook sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('pricebook_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical pricebook sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC - DATABASE ONLY
  // ============================================================================

  async syncHistoricalPriceBooks(): Promise<void> {
    const syncName = 'pricebook_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalPriceBooks = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let processedPriceBookIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(
        '🚀 Starting historical pricebook sync (DATABASE ONLY)...',
      );

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalPriceBooks > 0) {
          if (currentItem >= totalPriceBooks) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalPriceBooks} pricebooks`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalPriceBooks) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalPriceBooks} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const priceBookListResponse = await this.fetchPriceBooksListWithRetry(
            {
              currentItem,
              pageSize: this.PAGE_SIZE,
              orderBy: 'id',
              orderDirection: 'ASC',
              includeTotal: true,
            },
          );

          if (!priceBookListResponse) {
            this.logger.warn(
              '⚠️ Received null response from KiotViet PriceBook API',
            );
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

          const { total, data: priceBooks } = priceBookListResponse;

          if (total !== undefined && total !== null) {
            if (totalPriceBooks === 0) {
              totalPriceBooks = total;
              this.logger.log(
                `📊 Total pricebooks detected: ${totalPriceBooks}`,
              );
            }
          }

          if (
            !priceBooks ||
            !Array.isArray(priceBooks) ||
            priceBooks.length === 0
          ) {
            this.logger.warn('⚠️ Empty pricebooks array received');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Reached end after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(`🔄 Processing ${priceBooks.length} pricebooks...`);

          for (const priceBookData of priceBooks) {
            try {
              if (!priceBookData.id) {
                this.logger.warn('⚠️ PriceBook missing ID, skipping...');
                continue;
              }

              if (processedPriceBookIds.has(priceBookData.id)) {
                this.logger.warn(
                  `⚠️ Duplicate pricebook ID ${priceBookData.id}, skipping...`,
                );
                continue;
              }

              await this.processPriceBook(priceBookData);
              processedPriceBookIds.add(priceBookData.id);
              processedCount++;

              // Memory management
              if (processedPriceBookIds.size > 10000) {
                processedPriceBookIds.clear();
                this.logger.log(
                  '🧹 Cleared processed ID cache for memory management',
                );
              }
            } catch (error) {
              this.logger.error(
                `❌ Failed to process pricebook ${priceBookData.id}: ${error.message}`,
              );
            }
          }

          // Update progress
          await this.updateSyncControl(syncName, {
            progress: { processedCount, expectedTotal: totalPriceBooks },
            lastRunAt: new Date(),
          });

          currentItem += this.PAGE_SIZE;
        } catch (error) {
          consecutiveErrorPages++;
          this.logger.error(
            `❌ PriceBook API Error (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Failed after ${MAX_CONSECUTIVE_ERROR_PAGES} consecutive API errors. Last error: ${error.message}`,
            );
          }

          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`⏳ Retrying after ${delay}ms...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      // SUCCESS: Mark sync as completed
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalPriceBooks },
      });

      const completionRate =
        totalPriceBooks > 0 ? (processedCount / totalPriceBooks) * 100 : 100;
      this.logger.log(
        `✅ Historical pricebook sync completed: ${processedCount}/${totalPriceBooks} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Historical pricebook sync failed: ${error.message}`,
      );

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalPriceBooks },
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS WITH RETRY LOGIC
  // ============================================================================

  async fetchPriceBooksListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeTotal?: boolean;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchPriceBooksList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ PriceBook API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchPriceBooksList(params: {
    currentItem?: number;
    pageSize?: number;
    orderBy?: string;
    orderDirection?: string;
    includeTotal?: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'id',
      orderDirection: params.orderDirection || 'ASC',
      includeTotal: (params.includeTotal || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  async fetchPriceBookDetails(priceBookId: number): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks/${priceBookId}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // PRICEBOOK PROCESSING - DATABASE ONLY
  // ============================================================================

  private async processPriceBook(
    priceBookData: KiotVietPriceBook,
  ): Promise<void> {
    try {
      const existingPriceBook = await this.prismaService.priceBook.findFirst({
        where: { kiotVietId: priceBookData.id },
      });

      if (existingPriceBook) {
        await this.updatePriceBook(existingPriceBook.id, priceBookData);
      } else {
        await this.createPriceBook(priceBookData);
      }

      // Fetch and process pricebook details
      await this.processPriceBookDetails(priceBookData.id);
    } catch (error) {
      this.logger.error(
        `Failed to process pricebook ${priceBookData.id}: ${error.message}`,
      );
      throw error;
    }
  }

  private async createPriceBook(
    priceBookData: KiotVietPriceBook,
  ): Promise<any> {
    const data = {
      kiotVietId: priceBookData.id,
      name: priceBookData.name,
      isActive: priceBookData.isActive,
      isGlobal: priceBookData.isGlobal || false,
      startDate: priceBookData.startDate
        ? new Date(priceBookData.startDate)
        : null,
      endDate: priceBookData.endDate ? new Date(priceBookData.endDate) : null,
      forAllCusGroup: priceBookData.forAllCusGroup || false,
      forAllUser: priceBookData.forAllUser || false,
      retailerId: priceBookData.retailerId || null,
      createdDate: priceBookData.createdDate
        ? new Date(priceBookData.createdDate)
        : new Date(),
      modifiedDate: priceBookData.modifiedDate
        ? new Date(priceBookData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    return await this.prismaService.priceBook.create({ data });
  }

  private async updatePriceBook(
    priceBookId: number,
    priceBookData: KiotVietPriceBook,
  ): Promise<void> {
    const data = {
      name: priceBookData.name,
      isActive: priceBookData.isActive,
      isGlobal: priceBookData.isGlobal || false,
      startDate: priceBookData.startDate
        ? new Date(priceBookData.startDate)
        : null,
      endDate: priceBookData.endDate ? new Date(priceBookData.endDate) : null,
      forAllCusGroup: priceBookData.forAllCusGroup || false,
      forAllUser: priceBookData.forAllUser || false,
      retailerId: priceBookData.retailerId || null,
      modifiedDate: priceBookData.modifiedDate
        ? new Date(priceBookData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    await this.prismaService.priceBook.update({
      where: { id: priceBookId },
      data,
    });
  }

  private async processPriceBookDetails(priceBookId: number): Promise<void> {
    try {
      const priceBookDetails = await this.fetchPriceBookDetails(priceBookId);

      if (!priceBookDetails || !priceBookDetails.data) {
        this.logger.warn(`No details found for pricebook ${priceBookId}`);
        return;
      }

      const priceBook = await this.prismaService.priceBook.findFirst({
        where: { kiotVietId: priceBookId },
      });

      if (!priceBook) {
        this.logger.warn(`PriceBook ${priceBookId} not found in database`);
        return;
      }

      // Clear existing details
      await this.prismaService.priceBookDetail.deleteMany({
        where: { priceBookId: priceBook.id },
      });

      // Process each detail
      for (const detail of priceBookDetails.data) {
        try {
          const product = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(detail.productId) },
          });

          if (product) {
            await this.prismaService.priceBookDetail.create({
              data: {
                priceBookId: priceBook.id,
                productId: product.id,
                price: parseFloat(detail.price.toString()),
              },
            });
          }
        } catch (error) {
          this.logger.error(
            `Failed to create pricebook detail: ${error.message}`,
          );
        }
      }

      this.logger.log(
        `✅ Processed ${priceBookDetails.data.length} details for pricebook ${priceBookId}`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to process pricebook details for ${priceBookId}: ${error.message}`,
      );
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateSyncControl(name: string, data: any): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['pricebook'],
        syncMode: 'historical',
        ...data,
      },
      update: data,
    });
  }
}
