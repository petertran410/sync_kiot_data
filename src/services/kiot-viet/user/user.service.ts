// src/services/kiot-viet/user/user.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietUserService {
  private readonly logger = new Logger(KiotVietUserService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500;
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

  async fetchUsers(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/users`, {
          headers,
          params: {
            ...params,
            includeRemoveIds: true,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch users: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveUsers(users: any[]) {
    if (!users || users.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = users.map((u) => BigInt(u.id));
    const existingUsers = await this.prismaService.user.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<string, number>(
      existingUsers.map((u) => [u.kiotVietId.toString(), u.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const userData of users) {
      try {
        const kiotVietId = BigInt(userData.id);
        const existingId = existingMap.get(kiotVietId.toString());

        const commonData = {
          userName: userData.userName,
          givenName: userData.givenName,
          address: userData.address,
          mobilePhone: userData.mobilePhone,
          email: userData.email,
          description: userData.description,
          birthDate: userData.birthDate ? new Date(userData.birthDate) : null,
          retailerId: userData.retailerId,
          lastSyncedAt: new Date(),
        };

        if (existingId) {
          await this.prismaService.user.update({
            where: { id: existingId },
            data: commonData,
          });
          updatedCount++;
        } else {
          await this.prismaService.user.create({
            data: {
              kiotVietId: kiotVietId,
              ...commonData,
              createdDate: userData.createdDate
                ? new Date(userData.createdDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save user ${userData.userName}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async handleRemovedUsers(removeIds: number[]) {
    if (!removeIds || removeIds.length === 0) return 0;

    try {
      // Mark users as inactive instead of deleting them
      const result = await this.prismaService.user.updateMany({
        where: { kiotVietId: { in: removeIds.map((id) => BigInt(id)) } },
        data: {
          isActive: false,
          lastSyncedAt: new Date(),
        },
      });

      this.logger.log(`Marked ${result.count} users as inactive`);
      return result.count;
    } catch (error) {
      this.logger.error(`Failed to handle removed users: ${error.message}`);
      throw error;
    }
  }

  async syncRecentUsers(days: number = 7): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'user_recent' },
        create: {
          name: 'user_recent',
          entities: ['user'],
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

      const lastModifiedFrom = dayjs()
        .subtract(days, 'day')
        .format('YYYY-MM-DD');

      this.logger.log(`Starting recent user sync for last ${days} days...`);

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchUsers({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveUsers(response.data);
          totalProcessed += created + updated;

          this.logger.log(
            `User recent sync progress: ${totalProcessed} processed`,
          );
        }

        if (response.removeIds && response.removeIds.length > 0) {
          await this.handleRemovedUsers(response.removeIds);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'user_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `User recent sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'user_recent' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`User recent sync failed: ${error.message}`);
      throw error;
    }
  }

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

      this.logger.log('Starting historical user sync...');

      let currentItem = 0;
      let totalProcessed = 0;
      let batchCount = 0;
      let hasMoreData = true;
      const userBatch: any[] = [];

      while (hasMoreData) {
        const response = await this.fetchUsers({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          userBatch.push(...response.data);

          if (userBatch.length >= this.BATCH_SIZE) {
            const { created, updated } = await this.batchSaveUsers(userBatch);
            totalProcessed += created + updated;
            batchCount++;

            await this.prismaService.syncControl.update({
              where: { name: 'user_historical' },
              data: {
                progress: {
                  totalProcessed,
                  batchCount,
                  lastProcessedItem: currentItem + response.data.length,
                },
              },
            });

            this.logger.log(
              `User historical sync batch ${batchCount}: ${totalProcessed} users processed`,
            );
            userBatch.length = 0;
          }
        }

        if (response.removeIds && response.removeIds.length > 0) {
          await this.handleRemovedUsers(response.removeIds);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      // Process remaining users in batch
      if (userBatch.length > 0) {
        const { created, updated } = await this.batchSaveUsers(userBatch);
        totalProcessed += created + updated;
        batchCount++;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'user_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, batchCount },
        },
      });

      this.logger.log(
        `User historical sync completed: ${totalProcessed} users processed`,
      );
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

      this.logger.error(`User historical sync failed: ${error.message}`);
      throw error;
    }
  }
}
