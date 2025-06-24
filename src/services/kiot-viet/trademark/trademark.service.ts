// src/services/kiot-viet/trademark/trademark.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';

@Injectable()
export class KiotVietTradeMarkService {
  private readonly logger = new Logger(KiotVietTradeMarkService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncTradeMarks(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'trademark_historical' },
        create: {
          name: 'trademark_historical',
          entities: ['trademark'],
          syncMode: 'historical',
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

      const response = await this.fetchTradeMarks();
      let totalProcessed = 0;

      if (response.data && response.data.length > 0) {
        const { created, updated } = await this.saveTradeMarksToDatabase(
          response.data,
        );
        totalProcessed = created + updated;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'trademark_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Trademark sync completed: ${totalProcessed} trademarks processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'trademark_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Trademark sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchTradeMarks(): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const url = `${baseUrl}/trademarks`;

      const response = await this.httpService
        .get(url, {
          headers: {
            Retailer: this.configService.get<string>('KIOT_SHOP_NAME'),
            Authorization: `Bearer ${accessToken}`,
          },
        })
        .toPromise();

      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch trademarks: ${error.message}`);
      throw error;
    }
  }

  private async saveTradeMarksToDatabase(
    tradeMarks: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const tradeMarkData of tradeMarks) {
      try {
        const existingTradeMark = await this.prismaService.tradeMark.findUnique(
          {
            where: { kiotVietId: tradeMarkData.id },
          },
        );

        if (existingTradeMark) {
          await this.prismaService.tradeMark.update({
            where: { id: existingTradeMark.id },
            data: {
              name: tradeMarkData.name,
              retailerId: tradeMarkData.retailerId || null,
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
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save trademark ${tradeMarkData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
}
