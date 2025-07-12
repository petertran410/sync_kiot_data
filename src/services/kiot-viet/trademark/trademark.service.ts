// src/services/kiot-viet/trademark/trademark.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietTradeMark {
  id: number;
  name: string;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
}

@Injectable()
export class KiotVietTradeMarkService {
  private readonly logger = new Logger(KiotVietTradeMarkService.name);
  private readonly baseUrl: string;

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
        where: { name: 'trademark_historical' },
      });

      // Only historical sync for TradeMark
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical trademark sync...');
        await this.syncHistoricalTradeMarks();
        return;
      }

      // Default: historical sync
      this.logger.log('Running default historical trademark sync...');
      await this.syncHistoricalTradeMarks();
    } catch (error) {
      this.logger.error(`TradeMark sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('trademark_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical trademark sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC - DATABASE ONLY (SIMPLER IMPLEMENTATION)
  // ============================================================================

  async syncHistoricalTradeMarks(): Promise<void> {
    const syncName = 'trademark_historical';
    let processedCount = 0;

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(
        '🚀 Starting historical trademark sync (DATABASE ONLY)...',
      );

      // TradeMark API is simpler - single request without pagination
      const tradeMarkListResponse = await this.fetchTradeMarksWithRetry();

      if (!tradeMarkListResponse || !tradeMarkListResponse.data) {
        this.logger.warn('⚠️ No trademarks data received from KiotViet API');

        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
          progress: { processedCount: 0, expectedTotal: 0 },
        });

        return;
      }

      const tradeMarks = tradeMarkListResponse.data;
      const totalTradeMarks = tradeMarks.length;

      this.logger.log(`📊 Total trademarks detected: ${totalTradeMarks}`);
      this.logger.log(`🔄 Processing ${totalTradeMarks} trademarks...`);

      for (const tradeMarkData of tradeMarks) {
        try {
          if (!tradeMarkData.id) {
            this.logger.warn('⚠️ TradeMark missing ID, skipping...');
            continue;
          }

          await this.processTradeMark(tradeMarkData);
          processedCount++;

          // Progress logging every 10 items
          if (processedCount % 10 === 0) {
            this.logger.log(
              `📄 Processed ${processedCount}/${totalTradeMarks} trademarks`,
            );
          }
        } catch (error) {
          this.logger.error(
            `❌ Failed to process trademark ${tradeMarkData.id}: ${error.message}`,
          );
        }
      }

      // SUCCESS: Mark sync as completed
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
        progress: { processedCount, expectedTotal: 0 },
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS WITH RETRY LOGIC
  // ============================================================================

  async fetchTradeMarksWithRetry(maxRetries: number = 5): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchTradeMarksList();
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ TradeMark API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchTradeMarksList(): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    // TradeMark API supports lastModifiedFrom parameter only
    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/trademark`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // TRADEMARK PROCESSING - DATABASE ONLY
  // ============================================================================

  private async processTradeMark(
    tradeMarkData: KiotVietTradeMark,
  ): Promise<void> {
    try {
      const existingTradeMark = await this.prismaService.tradeMark.findFirst({
        where: { kiotVietId: tradeMarkData.id },
      });

      if (existingTradeMark) {
        await this.updateTradeMark(existingTradeMark.id, tradeMarkData);
      } else {
        await this.createTradeMark(tradeMarkData);
      }
    } catch (error) {
      this.logger.error(
        `Failed to process trademark ${tradeMarkData.id}: ${error.message}`,
      );
      throw error;
    }
  }

  private async createTradeMark(
    tradeMarkData: KiotVietTradeMark,
  ): Promise<any> {
    const data = {
      kiotVietId: tradeMarkData.id,
      name: tradeMarkData.name,
      retailerId: tradeMarkData.retailerId || null,
      createdDate: tradeMarkData.createdDate
        ? new Date(tradeMarkData.createdDate)
        : new Date(),
      modifiedDate: tradeMarkData.modifiedDate
        ? new Date(tradeMarkData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    return await this.prismaService.tradeMark.create({ data });
  }

  private async updateTradeMark(
    tradeMarkId: number,
    tradeMarkData: KiotVietTradeMark,
  ): Promise<void> {
    const data = {
      name: tradeMarkData.name,
      retailerId: tradeMarkData.retailerId || null,
      modifiedDate: tradeMarkData.modifiedDate
        ? new Date(tradeMarkData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    await this.prismaService.tradeMark.update({
      where: { id: tradeMarkId },
      data,
    });
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateSyncControl(name: string, data: any): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['trademark'],
        syncMode: 'historical',
        ...data,
      },
      update: data,
    });
  }
}
