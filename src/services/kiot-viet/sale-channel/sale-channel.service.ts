// src/services/kiot-viet/sale-channel/sale-channel.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';

@Injectable()
export class KiotVietSaleChannelService {
  private readonly logger = new Logger(KiotVietSaleChannelService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncSaleChannels(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'salechannel_historical' },
        create: {
          name: 'salechannel_historical',
          entities: ['salechannel'],
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

      const response = await this.fetchSaleChannels();
      let totalProcessed = 0;

      if (response.data && response.data.length > 0) {
        const { created, updated } = await this.saveSaleChannelsToDatabase(
          response.data,
        );
        totalProcessed = created + updated;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'salechannel_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Sale channel sync completed: ${totalProcessed} channels processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'salechannel_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Sale channel sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchSaleChannels(): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const url = `${baseUrl}/salechannels`;

      const response = await this.httpService
        .get(url, {
          headers: {
            Retailer: this.configService.get<string>('KIOT_SHOP_NAME'),
            Authorization: `Bearer ${accessToken}`,
          },
        })
        .toPromise();

      return response?.data;
    } catch (error) {
      this.logger.error(`Failed to fetch sale channels: ${error.message}`);
      throw error;
    }
  }

  private async saveSaleChannelsToDatabase(
    saleChannels: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const saleChannelData of saleChannels) {
      try {
        const existingSaleChannel =
          await this.prismaService.saleChannel.findUnique({
            where: { kiotVietId: saleChannelData.id },
          });

        if (existingSaleChannel) {
          await this.prismaService.saleChannel.update({
            where: { id: existingSaleChannel.id },
            data: {
              name: saleChannelData.name,
              isActive:
                saleChannelData.isActivate !== undefined
                  ? saleChannelData.isActivate
                  : true,
              position: saleChannelData.position || 0,
              retailerId: saleChannelData.retailerId || null,
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.saleChannel.create({
            data: {
              kiotVietId: saleChannelData.id,
              name: saleChannelData.name,
              isActive:
                saleChannelData.isActivate !== undefined
                  ? saleChannelData.isActivate
                  : true,
              position: saleChannelData.position || 0,
              retailerId: saleChannelData.retailerId || null,
              // Remove createdBy as it doesn't exist in the model
              createdDate: saleChannelData.createdDate
                ? new Date(saleChannelData.createdDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save sale channel ${saleChannelData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
}
