// src/services/kiot-viet/trademark/trademark.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

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

  async fetchTradeMarks(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/trademark`, {
          headers,
          params: {
            ...params,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch trademarks: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveTradeMarks(tradeMarks: any[]) {
    if (!tradeMarks || tradeMarks.length === 0)
      return { created: 0, updated: 0 };

    const kiotVietIds = tradeMarks.map((t) => t.tradeMarkId);
    const existingTradeMarks = await this.prismaService.tradeMark.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingTradeMarks.map((t) => [t.kiotVietId, t.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const tradeMarkData of tradeMarks) {
      try {
        const existingId = existingMap.get(tradeMarkData.tradeMarkId);

        if (existingId) {
          await this.prismaService.tradeMark.update({
            where: { id: existingId },
            data: {
              name: tradeMarkData.tradeMarkName,
              retailerId: tradeMarkData.retailerId,
              modifiedDate: tradeMarkData.modifiedDate
                ? new Date(tradeMarkData.modifiedDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.tradeMark.create({
            data: {
              kiotVietId: tradeMarkData.tradeMarkId,
              name: tradeMarkData.tradeMarkName,
              retailerId: tradeMarkData.retailerId,
              createdDate: tradeMarkData.createdDate
                ? new Date(tradeMarkData.createdDate)
                : new Date(),
              modifiedDate: tradeMarkData.modifiedDate
                ? new Date(tradeMarkData.modifiedDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save trademark ${tradeMarkData.tradeMarkName}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  async syncTradeMarks(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'trademark_sync' },
        create: {
          name: 'trademark_sync',
          entities: ['trademark'],
          syncMode: 'recent',
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
        },
      });

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchTradeMarks({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveTradeMarks(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `TradeMark sync progress: ${totalProcessed} processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'trademark_sync' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(`TradeMark sync completed: ${totalProcessed} processed`);
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'trademark_sync' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`TradeMark sync failed: ${error.message}`);
      throw error;
    }
  }
}
