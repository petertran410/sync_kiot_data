// src/services/kiot-viet/user/user.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class KiotVietUserService {
  private readonly logger = new Logger(KiotVietUserService.name);
  private readonly PAGE_SIZE = 50;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncHistoricalUsers(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'user_historical' },
        create: {
          name: 'user_historical',
          entities: ['user'],
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

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchUsers({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.saveUsersToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `User sync progress: ${totalProcessed} users processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'user_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(`User sync completed: ${totalProcessed} users processed`);
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'user_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`User sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchUsers(params: any): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const queryParams = new URLSearchParams();
      if (params.currentItem !== undefined) {
        queryParams.append('currentItem', params.currentItem.toString());
      }
      if (params.pageSize) {
        queryParams.append('pageSize', params.pageSize.toString());
      }

      const url = `${baseUrl}/users?${queryParams.toString()}`;

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
      this.logger.error(`Failed to fetch users: ${error.message}`);
      throw error;
    }
  }

  private async saveUsersToDatabase(
    users: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const userData of users) {
      try {
        const existingUser = await this.prismaService.user.findUnique({
          where: { kiotVietId: BigInt(userData.id) },
        });

        if (existingUser) {
          await this.prismaService.user.update({
            where: { id: existingUser.id },
            data: {
              userName: userData.userName,
              givenName: userData.givenName,
              address: userData.address || null,
              mobilePhone: userData.mobilePhone || null,
              email: userData.email || null,
              description: userData.description || null,
              birthDate: userData.birthDate
                ? new Date(userData.birthDate)
                : null,
              retailerId: userData.retailerId || null,
              isActive:
                userData.isActive !== undefined ? userData.isActive : true,
              modifiedDate: userData.modifiedDate
                ? new Date(userData.modifiedDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.user.create({
            data: {
              kiotVietId: BigInt(userData.id),
              userName: userData.userName,
              givenName: userData.givenName,
              address: userData.address || null,
              mobilePhone: userData.mobilePhone || null,
              email: userData.email || null,
              description: userData.description || null,
              birthDate: userData.birthDate
                ? new Date(userData.birthDate)
                : null,
              retailerId: userData.retailerId || null,
              isActive:
                userData.isActive !== undefined ? userData.isActive : true,
              createdDate: userData.createdDate
                ? new Date(userData.createdDate)
                : new Date(),
              modifiedDate: userData.modifiedDate
                ? new Date(userData.modifiedDate)
                : new Date(),
              lastSyncedAt: new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save user ${userData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
}
