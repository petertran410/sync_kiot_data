import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietPriceBook {
  id: number;
  kiotVietId: number;
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
    branchName: string;
    retailerId: number;
    lastSyncedAt: string;
  }>;
  priceBookCustomerGroups?: Array<{
    id: number;
    priceBookId: number;
    customerGroupId: number;
    customerGroupName?: string;
    retailerId?: number;
    lineNumber: number;
    lastSyncedAt: string;
  }>;
  priceBookUsers?: Array<{
    id: number;
    priceBookId: number;
    userId: number;
    userName?: string;
    lastSyncedAt: string;
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

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'pricebook_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical pricebook sync...');
        await this.syncHistoricalPriceBooks();
        return;
      }

      this.logger.log('Running default historical pricebook sync...');
      await this.syncHistoricalPriceBooks();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
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

  async syncHistoricalPriceBooks(): Promise<void> {
    const syncName = 'pricebook_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalPriceBooks = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedPricebookIds = new Set<number>();

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
              `✅ Pagination complete. Processed: ${processedCount}/${totalPriceBooks} pricebooks`,
            );
            break;
          }
        }
        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchPriceBooksListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            includePriceBookBranch: true,
            includePriceBookCustomerGroups: true,
            includePriceBookUsers: true,
          });

          consecutiveErrorPages = 0;

          const { data: pricebooks, total } = response;

          if (total !== undefined && total !== null) {
            if (totalPriceBooks === 0) {
              this.logger.log(
                `📊 Total pricebooks detected: ${total}. Starting processing...`,
              );
              totalPriceBooks = total;
            } else if (total !== totalPriceBooks && total !== lastValidTotal) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalPriceBooks} → ${total}. Using latest.`,
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

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

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
              this.logger.log('🎉 All pricebooks processed successfully!');
              break;
            }
          }

          consecutiveEmptyPages = 0;
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (
            consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES ||
            totalRetries >= MAX_TOTAL_RETRIES
          ) {
            throw new Error(
              `Too many consecutive errors (${consecutiveErrorPages}) or total retries (${totalRetries}). Last error: ${error.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
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
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      this.logger.error(
        `❌ Historical pricebook sync failed: ${error.message}`,
      );
      throw error;
    }
  }

  async fetchPriceBooksListWithRetry(
    params: {
      pageSize?: number;
      currentItem?: number;
      includePriceBookBranch: boolean;
      includePriceBookCustomerGroups: boolean;
      includePriceBookUsers: boolean;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchPriceBooksList(params);
      } catch (error) {
        lastError = error as Error;

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  private async fetchPriceBooksList(params: {
    currentItem?: number;
    pageSize?: number;
    includePriceBookBranch: boolean;
    includePriceBookCustomerGroups: boolean;
    includePriceBookUsers: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      includePriceBookBranch: (
        params.includePriceBookBranch || true
      ).toString(),
      includePriceBookCustomerGroups: (
        params.includePriceBookCustomerGroups || true
      ).toString(),
      includePriceBookUsers: (params.includePriceBookUsers || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichPriceBooksWithDetails(
    pricebooks: KiotVietPriceBook[],
  ): Promise<KiotVietPriceBook[]> {
    this.logger.log(
      `🔍 Enriching ${pricebooks.length} pricebooks with details...`,
    );

    const enrichedPricebooks: KiotVietPriceBook[] = [];

    for (const pricebook of pricebooks) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const queryParams = new URLSearchParams({
          includePriceBookBranch: 'true',
          includePriceBookCustomerGroups: 'true',
          includePriceBookUsers: 'true',
        });

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/pricebooks/${pricebook.id}?${queryParams}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedPricebooks.push(response.data);
        } else {
          enrichedPricebooks.push(pricebook);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich pricebook ${pricebook.name}: ${error.message}`,
        );
        enrichedPricebooks.push(pricebook);
      }
    }

    return enrichedPricebooks;
  }

  private async savePriceBooksToDatabase(
    priceBooks: KiotVietPriceBook[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${priceBooks.length} pricebooks to database...`);

    const savedPricebooks: any[] = [];

    for (const priceBookData of priceBooks) {
      try {
        if (!priceBookData.id || !priceBookData.name) {
          continue;
        }

        const pricebook = await this.prismaService.priceBook.upsert({
          where: { kiotVietId: priceBookData.id },
          update: {
            name: priceBookData.name.trim(),
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
            retailerId: 310831,
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: priceBookData.id,
            name: priceBookData.name.trim(),
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
            retailerId: 310831,
            createdDate: priceBookData.createdDate
              ? new Date(priceBookData.createdDate)
              : new Date(),
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedPricebooks.push(pricebook);

        if (
          priceBookData.priceBookBranches &&
          priceBookData.priceBookBranches.length > 0
        ) {
          for (let i = 0; i < priceBookData.priceBookBranches.length; i++) {
            const detail = priceBookData.priceBookBranches[i];
            const branch = await this.prismaService.branch.findFirst({
              where: { kiotVietId: detail.branchId },
              select: { id: true, name: true },
            });

            await this.prismaService.priceBookBranch.upsert({
              where: {
                priceBookId_lineNumber: {
                  priceBookId: pricebook.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                branchId: branch?.id,
                branchName: branch?.name,
                retailerId: detail.retailerId,
                lastSyncedAt: new Date(),
              },
              create: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                branchId: branch?.id,
                retailerId: detail.retailerId,
                branchName: branch?.name,
                lastSyncedAt: new Date(),
              },
            });
          }
        }

        if (
          priceBookData.priceBookCustomerGroups &&
          priceBookData.priceBookCustomerGroups.length > 0
        ) {
          for (
            let i = 0;
            i < priceBookData.priceBookCustomerGroups.length;
            i++
          ) {
            const detail = priceBookData.priceBookCustomerGroups[i];
            const customerGroup =
              await this.prismaService.customerGroup.findFirst({
                where: { kiotVietId: detail.customerGroupId },
                select: { id: true, name: true },
              });

            await this.prismaService.priceBookCustomerGroup.upsert({
              where: {
                priceBookId_lineNumber: {
                  priceBookId: pricebook.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                customerGroupId: customerGroup?.id,
                customerGroupName: customerGroup?.name,
                retailerId: detail.retailerId,
                lastSyncedAt: new Date(),
              },
              create: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                customerGroupId: customerGroup?.id,
                customerGroupName: customerGroup?.name,
                retailerId: detail.retailerId,
                lastSyncedAt: new Date(),
              },
            });
          }
        }

        if (
          priceBookData.priceBookUsers &&
          priceBookData.priceBookUsers.length > 0
        ) {
          for (let i = 0; i < priceBookData.priceBookUsers.length; i++) {
            const detail = priceBookData.priceBookUsers[i];
            const user = await this.prismaService.user.findFirst({
              where: { kiotVietId: BigInt(detail.userId) },
              select: { id: true, userName: true },
            });

            await this.prismaService.priceBookUser.upsert({
              where: {
                priceBookId_lineNumber: {
                  priceBookId: pricebook.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                userId: user?.id,
                userName: user?.userName,
                lastSyncedAt: new Date(),
              },
              create: {
                lineNumber: i + 1,
                kiotVietId: detail.id,
                priceBookId: pricebook.id,
                userId: user?.id,
                userName: user?.userName,
                lastSyncedAt: new Date(),
              },
            });
          }
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save pricebook ${priceBookData.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `💾 Saved ${savedPricebooks.length} pricebooks to database`,
    );
    return savedPricebooks;
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
