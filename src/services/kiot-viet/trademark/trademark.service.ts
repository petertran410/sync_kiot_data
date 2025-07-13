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

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical trademark sync...');

      const response = await this.fetchTradeMarksWithRetry({
        orderBy: 'tradeMarkName',
        orderDirection: 'ASC',
        pageSize: this.PAGE_SIZE,
      });

      const tradeMarks = response.data || [];
      this.logger.log(`📊 Found ${tradeMarks.length} trademarks to sync`);

      if (tradeMarks.length > 0) {
        const saved = await this.saveTradeMarksToDatabase(tradeMarks);
        this.logger.log(`✅ Saved ${saved.created + saved.updated} trademarks`);
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
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
      });

      throw error;
    }
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

  async fetchTradeMarks(params: {
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      orderBy: params.orderBy || 'tradeMarkName',
      orderDirection: params.orderDirection || 'ASC',
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

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
