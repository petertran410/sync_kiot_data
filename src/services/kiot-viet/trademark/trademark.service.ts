// src/services/kiot-viet/trademark/trademark.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, firstValueFrom } from 'rxjs';
import { response } from 'express';

interface KiotVietTradeMark {
  tradeMarkId: number;
  tradeMarkName: string;
  createdDate?: string;
  modifiedDate?: string;
}

@Injectable()
export class KiotVietTradeMarkService {
  private readonly logger = new Logger(KiotVietTradeMarkService.name);
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
        where: { name: 'trademark_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical trademark sync...');
        await this.syncHistoricalTradeMarks();
        return;
      }

      this.logger.log('Running default historical trademark sync...');
      await this.syncHistoricalTradeMarks();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('trademark_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical product sync enabled');
  }

  async syncHistoricalTradeMarks(): Promise<void> {
    const syncName = 'trademark_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalTradeMarks = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedTrademarkIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(
        '🚀 Starting historical trademark sync with pagination...',
      );

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalTradeMarks > 0) {
          if (currentItem >= totalTradeMarks) {
            this.logger.log(
              `✅ Pagination complete. Processed ${processedCount}/${totalTradeMarks} trademarks`,
            );
            break;
          }
        }

        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchTradeMarksListWithRetry({
            pageSize: this.PAGE_SIZE,
            currentItem,
          });

          consecutiveErrorPages = 0;

          const { data: trademarks, total } = response;

          if (total !== undefined && total !== null) {
            if (totalTradeMarks === 0) {
              this.logger.log(
                `📊 Total trademarks detected: ${total}. Starting processing...`,
              );
              totalTradeMarks = total;
            } else if (total !== totalTradeMarks && total !== lastValidTotal) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalTradeMarks} → ${total}. Using latest.`,
              );
              totalTradeMarks = total;
            }
            lastValidTotal = total;
          }

          if (!trademarks || trademarks.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalTradeMarks > 0 && currentItem >= totalTradeMarks) {
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

          const newTrademarks = trademarks.filter((trademark) => {
            if (processedTrademarkIds.has(trademark.tradeMarkId)) {
              this.logger.debug(
                `⚠️ Duplicate trademark ID detected: ${trademark.tradeMarkId} (${trademark.tradeMarkName})`,
              );
              return false;
            }
            processedTrademarkIds.add(trademark.tradeMarkId);
            return true;
          });

          if (newTrademarks.length !== trademarks.length) {
            this.logger.warn(
              `🔄 Filtered out ${trademarks.length - newTrademarks.length} duplicate trademarks on page ${currentPage}`,
            );
          }

          if (newTrademarks.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all trademarks already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newTrademarks.length} trademarks from page ${currentPage}...`,
          );

          const trademakrsWithDetails =
            await this.enrichTradeMarksWithDetails(newTrademarks);
          const savedTradeMarks = await this.saveTradeMarksToDatabase(
            trademakrsWithDetails,
          );

          processedCount += savedTradeMarks.length;
          currentItem += this.PAGE_SIZE;

          if (totalTradeMarks > 0) {
            const completionPercentage =
              (processedCount / totalTradeMarks) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalTradeMarks} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalTradeMarks) {
              this.logger.log('🎉 All trademarks processed successfully!');
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
        progress: { processedCount, expectedTotal: totalTradeMarks },
      });

      const completionRate =
        totalTradeMarks > 0 ? (processedCount / totalTradeMarks) * 100 : 100;

      this.logger.log(
        `✅ Historical trademark sync completed: ${processedCount}/${totalTradeMarks} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Historical trademark sync failed: ${error.message}`,
      );

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalTradeMarks },
      });

      throw error;
    }
  }

  async fetchTradeMarksListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchTradeMarksList(params);
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

  async fetchTradeMarksList(params: {
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/trademark?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichTradeMarksWithDetails(
    trademarks: KiotVietTradeMark[],
  ): Promise<KiotVietTradeMark[]> {
    this.logger.log(
      `🔍 Enriching ${trademarks.length} trademarks with details...`,
    );

    const enrichedTrademarks: KiotVietTradeMark[] = [];

    for (const trademark of trademarks) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/trademark/${trademark.tradeMarkId}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedTrademarks.push(response.data);
        } else {
          enrichedTrademarks.push(trademark);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich trademark ${trademark.tradeMarkName}: ${error.message}`,
        );
        enrichedTrademarks.push(trademark);
      }
    }

    return enrichedTrademarks;
  }

  private async saveTradeMarksToDatabase(
    tradeMarks: KiotVietTradeMark[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${tradeMarks.length} trademarks to database...`);

    const savedTrademarks: any[] = [];

    for (const tradeMarkData of tradeMarks) {
      try {
        const trademark = await this.prismaService.tradeMark.upsert({
          where: { kiotVietId: tradeMarkData.tradeMarkId },
          update: {
            name: tradeMarkData.tradeMarkName,
            modifiedDate: tradeMarkData.modifiedDate
              ? new Date(tradeMarkData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: tradeMarkData.tradeMarkId,
            name: tradeMarkData.tradeMarkName,
            createdDate: tradeMarkData.createdDate
              ? new Date(tradeMarkData.createdDate)
              : new Date(),
            modifiedDate: tradeMarkData.modifiedDate
              ? new Date(tradeMarkData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedTrademarks.push(trademark);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save trademark ${tradeMarkData.tradeMarkName}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Saved ${savedTrademarks.length} Trademarks successfully`,
    );
    return savedTrademarks;
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['trademark'],
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
