// src/services/kiot-viet/pricebook/pricebook.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, firstValueFrom } from 'rxjs';

interface KiotVietPriceBook {
  id: number;
  name: string;
  isActive?: boolean;
  isGlobal?: boolean;
  startDate?: string;
  endDate?: string;
  forAllCusGroup?: boolean;
  forAllUser?: boolean;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
  priceBookBranches?: Array<{
    id: number;
    priceBookId: number;
    branchId: number;
    branchName?: string;
  }>;
  priceBookCustomerGroups?: Array<{
    id: number;
    priceBookId: number;
    customerGroupId: number;
    customerGroupName?: string;
  }>;
  priceBookUsers?: Array<{
    id: number;
    priceBookId: number;
    userId: number;
    userName?: string;
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
  // HISTORICAL SYNC
  // ============================================================================

  async syncHistoricalPriceBooks(): Promise<void> {
    const syncName = 'pricebook_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalPriceBooks = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedPricebookIds = new Set<number>();
    // let allPriceBooks: KiotVietPriceBook[] = [];

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical pricebook sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalPriceBooks > 0) {
          if (currentItem >= totalPriceBooks) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalPriceBooks} categories`,
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

        this.logger.log(
          `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})...`,
        );

        try {
          const pricebookListResponse = await this.fetchPriceBooksWithRetry({
            includePriceBookBranch: true,
            includePriceBookCustomerGroups: true,
            includePriceBookUsers: true,
            pageSize: this.PAGE_SIZE,
            currentItem,
          });

          if (!pricebookListResponse) {
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

          const { total, data: pricebooks } = pricebookListResponse;

          if (total !== undefined && total !== null) {
            if (totalPriceBooks === 0) {
              totalPriceBooks = total;
              this.logger.log(
                `📊 Total pricebooks detected: ${totalPriceBooks}`,
              );
            } else if (total !== totalPriceBooks) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalPriceBooks} -> ${total}. Using latest.`,
              );
              totalPriceBooks = total;
            }
            lastValidTotal = total;
          }

          if (!pricebooks || pricebooks.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalPriceBooks > 0 && currentItem >= totalPriceBooks) {
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

          const newPricebooks = pricebooks.filter((pricebook) => {
            if (processedPricebookIds.has(pricebook.id)) {
              this.logger.debug(
                `⚠️ Duplicate pricebook ID detected: ${pricebook.id} (${pricebook.code})`,
              );
              return false;
            }
            processedPricebookIds.add(pricebook.id);
            return true;
          });

          if (newPricebooks.length !== pricebooks.length) {
            this.logger.warn(
              `🔄 Filtered out ${pricebooks.length - newPricebooks.length} duplicate pricebooks on page ${currentPage}`,
            );
          }

          if (newPricebooks.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all pricebooks already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newPricebooks.length} pricebooks from page ${currentPage}...`,
          );

          const pricebooksWithDetails =
            await this.enrichPriceBooksWithDetails(newPricebooks);
          const savedPricebooks = await this.savePriceBooksToDatabase(
            pricebooksWithDetails,
          );

          processedCount += savedPricebooks.length;
          currentItem += this.PAGE_SIZE;

          if (totalPriceBooks > 0) {
            const completionPercentage =
              (processedCount / totalPriceBooks) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalPriceBooks} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalPriceBooks) {
              this.logger.log('🎉 All categories processed successfully!');
              break;
            }
          }

          if (totalPriceBooks > 0) {
            if (
              currentItem >= totalPriceBooks &&
              processedCount >= totalPriceBooks * 0.95
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

  async fetchPriceBooks(params: {
    includePriceBookBranch?: boolean;
    includePriceBookCustomerGroups?: boolean;
    includePriceBookUsers?: boolean;
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      includePriceBookBranch: (
        params.includePriceBookBranch || false
      ).toString(),
      includePriceBookCustomerGroups: (
        params.includePriceBookCustomerGroups || false
      ).toString(),
      includePriceBookUsers: (params.includePriceBookUsers || false).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    // Only add orderBy if specified (be conservative)
    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // API METHODS
  // ============================================================================

  async fetchPriceBooksWithRetry(
    params: {
      includePriceBookBranch?: boolean;
      includePriceBookCustomerGroups?: boolean;
      includePriceBookUsers?: boolean;
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
        return await this.fetchPriceBooks(params);
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

  // ============================================================================
  // DATABASE SAVE
  // ============================================================================

  private async savePriceBooksToDatabase(priceBooks: any[]): Promise<any[]> {
    this.logger.log(`💾 Saving ${priceBooks.length} pricebooks to database...`);

    const savePricebooks: any[] = [];

    for (const priceBookData of priceBooks) {
      try {
        if (!priceBookData.id || !priceBookData.name) {
          this.logger.warn(
            `⚠️ Skipping invalid pricebook: id=${priceBookData.id}, name=${priceBookData.name}`,
          );
          this.logger.debug(
            `Raw pricebook data: ${JSON.stringify(priceBookData)}`,
          );
          continue;
        }

        const pricebook = await this.prismaService.priceBook.upsert({
          where: { kiotVietId: priceBookData.id },
          update: {
            name: priceBookData.name,
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: priceBookData.id,
            name: priceBookData.name,
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            createdDate: priceBookData.createdDate
              ? new Date(priceBookData.createdDate)
              : new Date(),
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        // Save related data
        await this.savePriceBookBranches(
          pricebook.id,
          priceBookData.priceBookBranches || [],
        );
        await this.savePriceBookCustomerGroups(
          pricebook.id,
          priceBookData.priceBookCustomerGroups || [],
        );
        await this.savePriceBookUsers(
          pricebook.id,
          priceBookData.priceBookUsers || [],
        );
      } catch (error) {
        this.logger.error(
          `❌ Failed to save pricebook ${priceBookData.name}: ${error.message}`,
        );
      }
    }

    return savePricebooks;
  }

  private async enrichPriceBooksWithDetails(
    pricebooks: KiotVietPriceBook[],
  ): Promise<KiotVietPriceBook[]> {
    this.logger.log(
      `🔍 Enriching ${pricebooks.length} pricebooks with details...`,
    );

    const enrichedPricebooks: any[] = [];
    for (const pricebook of pricebooks) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/pricebooks/${pricebook.id}`, {
            headers,
          }),
        );
        if (response.data) {
          enrichedPricebooks.push(response.data);
        } else {
          enrichedPricebooks.push(pricebook);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich pricebook ${pricebook.id}: ${error.message}`,
        );
        enrichedPricebooks.push(pricebook);
      }
    }

    return enrichedPricebooks;
  }

  // ============================================================================
  // SAVE RELATED DATA
  // ============================================================================

  private async savePriceBookBranches(
    priceBookId: number,
    branches: any[],
  ): Promise<void> {
    if (branches.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookBranch.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const branchData of branches) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: branchData.branchId },
          select: { id: true },
        });

        if (branch) {
          await this.prismaService.priceBookBranch.create({
            data: {
              kiotVietId: branchData.id ? BigInt(branchData.id) : null,
              priceBookId,
              branchId: branch.id,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook branches: ${error.message}`,
      );
    }
  }

  private async savePriceBookCustomerGroups(
    priceBookId: number,
    customerGroups: any[],
  ): Promise<void> {
    if (customerGroups.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookCustomerGroup.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const cgData of customerGroups) {
        const customerGroup = await this.prismaService.customerGroup.findFirst({
          where: { kiotVietId: cgData.customerGroupId },
          select: { id: true },
        });

        if (customerGroup) {
          await this.prismaService.priceBookCustomerGroup.create({
            data: {
              kiotVietId: cgData.id ? BigInt(cgData.id) : null,
              priceBookId,
              customerGroupId: customerGroup.id,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook customer groups: ${error.message}`,
      );
    }
  }

  private async savePriceBookUsers(
    priceBookId: number,
    users: any[],
  ): Promise<void> {
    if (users.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookUser.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const userData of users) {
        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: BigInt(userData.userId) },
          select: { kiotVietId: true },
        });

        if (user) {
          await this.prismaService.priceBookUser.create({
            data: {
              kiotVietId: userData.id ? BigInt(userData.id) : null,
              priceBookId,
              userId: user.kiotVietId,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(`❌ Failed to save pricebook users: ${error.message}`);
    }
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('pricebook_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical pricebook sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['pricebook'],
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
