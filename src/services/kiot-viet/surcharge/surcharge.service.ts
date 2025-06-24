// src/services/kiot-viet/surcharge/surcharge.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';

@Injectable()
export class KiotVietSurchargeService {
  private readonly logger = new Logger(KiotVietSurchargeService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncSurcharges(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'surcharge_historical' },
        create: {
          name: 'surcharge_historical',
          entities: ['surcharge'],
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

      const response = await this.fetchSurcharges();
      let totalProcessed = 0;

      if (response.data && response.data.length > 0) {
        const { created, updated } = await this.saveSurchargesToDatabase(
          response.data,
        );
        totalProcessed = created + updated;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'surcharge_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Surcharge sync completed: ${totalProcessed} surcharges processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'surcharge_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Surcharge sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchSurcharges(): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const url = `${baseUrl}/surcharges`;

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
      this.logger.error(`Failed to fetch surcharges: ${error.message}`);
      throw error;
    }
  }

  private async saveSurchargesToDatabase(
    surcharges: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const surchargeData of surcharges) {
      try {
        const existingSurcharge = await this.prismaService.surcharge.findUnique(
          {
            where: { kiotVietId: surchargeData.id },
          },
        );

        if (existingSurcharge) {
          await this.prismaService.surcharge.update({
            where: { id: existingSurcharge.id },
            data: {
              code: surchargeData.code || null,
              name: surchargeData.name,
              valueRatio: surchargeData.valueRatio || null,
              value: surchargeData.value
                ? parseFloat(surchargeData.value)
                : null,
              retailerId: surchargeData.retailerId || null,
              isActive:
                surchargeData.isActive !== undefined
                  ? surchargeData.isActive
                  : true,
              modifiedDate: surchargeData.modifiedDate
                ? new Date(surchargeData.modifiedDate)
                : new Date(),
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.surcharge.create({
            data: {
              kiotVietId: surchargeData.id,
              code: surchargeData.code || null,
              name: surchargeData.name,
              valueRatio: surchargeData.valueRatio || null,
              value: surchargeData.value
                ? parseFloat(surchargeData.value)
                : null,
              retailerId: surchargeData.retailerId || null,
              isActive:
                surchargeData.isActive !== undefined
                  ? surchargeData.isActive
                  : true,
              createdDate: surchargeData.createdDate
                ? new Date(surchargeData.createdDate)
                : new Date(),
              modifiedDate: surchargeData.modifiedDate
                ? new Date(surchargeData.modifiedDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save surcharge ${surchargeData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
}
