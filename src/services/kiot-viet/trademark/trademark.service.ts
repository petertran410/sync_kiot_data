// src/services/kiot-viet/trademark/trademark.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

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

  // ============================================================================
  // HISTORICAL SYNC
  // ============================================================================

  async syncHistoricalTradeMarks(): Promise<void> {
    const syncName = 'trademark_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalTradeMarks = 0;
    let allTradeMarks: KiotVietTradeMark[] = [];

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

      // Pagination loop to get ALL trademarks
      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        this.logger.log(
          `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})...`,
        );

        const response = await this.fetchTradeMarksWithRetry({
          pageSize: this.PAGE_SIZE,
          currentItem,
          // Remove orderBy to avoid 500 error
        });

        const { data: tradeMarks, total } = response;

        // Set total on first page
        if (totalTradeMarks === 0 && total !== undefined) {
          totalTradeMarks = total;
          this.logger.log(`📊 Total trademarks detected: ${totalTradeMarks}`);
        }

        // Check if we have data
        if (!tradeMarks || tradeMarks.length === 0) {
          this.logger.log(
            '✅ No more trademarks to fetch - pagination complete',
          );
          break;
        }

        // Add to all trademarks
        allTradeMarks.push(...tradeMarks);
        processedCount += tradeMarks.length;
        currentItem += this.PAGE_SIZE;

        this.logger.log(
          `📈 Progress: ${processedCount}/${totalTradeMarks || 'unknown'} trademarks fetched`,
        );

        // Break if we've got all trademarks
        if (totalTradeMarks > 0 && processedCount >= totalTradeMarks) {
          this.logger.log('🎉 All trademarks fetched successfully!');
          break;
        }

        // Break if this page was not full (last page)
        if (tradeMarks.length < this.PAGE_SIZE) {
          this.logger.log('✅ Last page reached (partial page)');
          break;
        }

        // Rate limiting delay
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      this.logger.log(`📊 Total fetched: ${allTradeMarks.length} trademarks`);

      if (allTradeMarks.length > 0) {
        const saved = await this.saveTradeMarksToDatabase(allTradeMarks);
        this.logger.log(`✅ Saved ${saved.created + saved.updated} trademarks`);
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalTradeMarks },
      });

      this.logger.log('✅ Historical trademark sync completed');
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

  async fetchTradeMarks(params: {
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    // Only add orderBy if specified (avoid 500 error)
    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/trademark?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // API METHODS
  // ============================================================================

  async fetchTradeMarksWithRetry(
    params: {
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
        return await this.fetchTradeMarks(params);
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

  private async saveTradeMarksToDatabase(
    tradeMarks: KiotVietTradeMark[],
  ): Promise<{ created: number; updated: number }> {
    this.logger.log(`💾 Saving ${tradeMarks.length} trademarks to database...`);

    let created = 0;
    let updated = 0;

    for (const tradeMarkData of tradeMarks) {
      try {
        const result = await this.prismaService.tradeMark.upsert({
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

        // Check if it was created or updated
        const existingCount = await this.prismaService.tradeMark.count({
          where: {
            kiotVietId: tradeMarkData.tradeMarkId,
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
          `❌ Failed to save trademark ${tradeMarkData.tradeMarkName}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Trademarks saved: ${created} created, ${updated} updated`,
    );
    return { created, updated };
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('trademark_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical trademark sync enabled');
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
