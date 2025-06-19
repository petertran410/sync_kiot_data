import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

@Injectable()
export class KiotVietCustomerGroupService {
  private readonly logger = new Logger(KiotVietCustomerGroupService.name);
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

  async fetchCustomerGroups() {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/customers/group`, {
          headers,
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch customer groups: ${error.message}`);
      throw error;
    }
  }

  async syncCustomerGroups(): Promise<void> {
    try {
      this.logger.log('Starting customer groups sync...');
      const response = await this.fetchCustomerGroups();

      if (response.data && response.data.length > 0) {
        let processedCount = 0;

        for (const groupData of response.data) {
          try {
            await this.prismaService.customerGroup.upsert({
              where: { kiotVietId: groupData.id },
              create: {
                kiotVietId: groupData.id,
                name: groupData.name,
                description: groupData.description,
                discount: groupData.discount
                  ? new Prisma.Decimal(groupData.discount)
                  : null,
                retailerId: groupData.retailerId,
                createdDate: groupData.createdDate
                  ? new Date(groupData.createdDate)
                  : new Date(),
                modifiedDate: groupData.modifiedDate
                  ? new Date(groupData.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
              update: {
                name: groupData.name,
                description: groupData.description,
                discount: groupData.discount
                  ? new Prisma.Decimal(groupData.discount)
                  : null,
                retailerId: groupData.retailerId,
                modifiedDate: groupData.modifiedDate
                  ? new Date(groupData.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
            });
            processedCount++;
          } catch (error) {
            this.logger.error(
              `Failed to sync customer group ${groupData.name}: ${error.message}`,
            );
          }
        }

        this.logger.log(
          `Customer groups sync completed: ${processedCount} groups processed`,
        );
      } else {
        this.logger.log('No customer groups found');
      }
    } catch (error) {
      this.logger.error(`Customer groups sync failed: ${error.message}`);
      throw error;
    }
  }
}
