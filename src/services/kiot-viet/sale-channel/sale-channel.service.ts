// src/services/kiot-viet/sale-channel/sale-channel.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietSaleChannelService {
  private readonly logger = new Logger(KiotVietSaleChannelService.name);
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

  async fetchSaleChannels(params: { currentItem?: number; pageSize?: number }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/salechannel`, {
          headers,
          params,
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch sale channels: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveSaleChannels(saleChannels: any[]) {
    if (!saleChannels || saleChannels.length === 0)
      return { created: 0, updated: 0 };

    const kiotVietIds = saleChannels.map((s) => s.id);
    const existingSaleChannels = await this.prismaService.saleChannel.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingSaleChannels.map((s) => [s.kiotVietId, s.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const saleChannelData of saleChannels) {
      try {
        const existingId = existingMap.get(saleChannelData.id);

        if (existingId) {
          await this.prismaService.saleChannel.update({
            where: { id: existingId },
            data: {
              name: saleChannelData.name,
              isActive: saleChannelData.isActive,
              img: saleChannelData.img,
              isNotDelete: saleChannelData.isNotDelete,
              position: saleChannelData.position,
              retailerId: saleChannelData.retailerId,
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.saleChannel.create({
            data: {
              kiotVietId: saleChannelData.id,
              name: saleChannelData.name,
              isActive: saleChannelData.isActive,
              img: saleChannelData.img,
              isNotDelete: saleChannelData.isNotDelete,
              position: saleChannelData.position,
              retailerId: saleChannelData.retailerId,
              createdDate: new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save sale channel ${saleChannelData.name}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  async syncSaleChannels(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'salechannel_sync' },
        create: {
          name: 'salechannel_sync',
          entities: ['salechannel'],
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
        const response = await this.fetchSaleChannels({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveSaleChannels(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `SaleChannel sync progress: ${totalProcessed} processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'salechannel_sync' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `SaleChannel sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'salechannel_sync' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`SaleChannel sync failed: ${error.message}`);
      throw error;
    }
  }
}
