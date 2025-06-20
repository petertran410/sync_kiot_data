// src/services/kiot-viet/surcharge/surcharge.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

@Injectable()
export class KiotVietSurchargeService {
  private readonly logger = new Logger(KiotVietSurchargeService.name);
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

  async fetchSurcharges(params: { currentItem?: number; pageSize?: number }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/surchages`, {
          headers,
          params,
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch surcharges: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveSurcharges(surcharges: any[]) {
    if (!surcharges || surcharges.length === 0)
      return { created: 0, updated: 0 };

    const kiotVietIds = surcharges.map((s) => s.id);
    const existingSurcharges = await this.prismaService.surcharge.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingSurcharges.map((s) => [s.kiotVietId, s.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const surchargeData of surcharges) {
      try {
        const existingId = existingMap.get(surchargeData.id);

        const commonData = {
          code: surchargeData.surchargeCode,
          name: surchargeData.surchargeName,
          valueRatio: surchargeData.valueRatio,
          value: surchargeData.value
            ? new Prisma.Decimal(surchargeData.value)
            : null,
          retailerId: surchargeData.retailerId,
          modifiedDate: surchargeData.modifiedDate
            ? new Date(surchargeData.modifiedDate)
            : new Date(),
        };

        if (existingId) {
          await this.prismaService.surcharge.update({
            where: { id: existingId },
            data: commonData,
          });
          updatedCount++;
        } else {
          await this.prismaService.surcharge.create({
            data: {
              kiotVietId: surchargeData.id,
              ...commonData,
              createdDate: surchargeData.createDate
                ? new Date(surchargeData.createDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save surcharge ${surchargeData.surchargeName}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  async syncSurcharges(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'surcharge_sync' },
        create: {
          name: 'surcharge_sync',
          entities: ['surcharge'],
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
        const response = await this.fetchSurcharges({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveSurcharges(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Surcharge sync progress: ${totalProcessed} processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'surcharge_sync' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(`Surcharge sync completed: ${totalProcessed} processed`);
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'surcharge_sync' },
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
}
