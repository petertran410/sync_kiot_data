// src/services/kiot-viet/user/user.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietUserService {
  private readonly logger = new Logger(KiotVietUserService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const runningUserSyncs = await this.prismaService.syncControl.findMany({
        where: {
          OR: [{ name: 'user_historical' }],
          isRunning: true,
        },
      });

      if (runningUserSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningUserSyncs.length} User syncs still running: ${runningUserSyncs.map((s) => s.name).join(',')}`,
        );
        this.logger.warn('Skipping user sync to avoid conficts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'user_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical user sync...');
        await this.syncHistoricalUsers();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log('Historical user sync is running');
        return;
      }

      this.logger.log('Running defaut historical transfer sync...');
      await this.syncHistoricalUsers();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('user_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('Historical user sync enabled');
  }

  async syncHistoricalUsers(): Promise<void> {
    const syncName = 'user_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalUsers = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedUserIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('Starting historical transfer sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalUsers > 0) {
          if (currentItem >= totalUsers) {
            this.logger.log(
              `Pagination complete. Processed ${processedCount}/${totalUsers} transfers`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalUsers) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalUsers} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const response = await this.fetchUsersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
          });

          if (!response) {
            this.logger.warn('Received null response from KiotViet API');

            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { data: users, total } = response;

          if (total != undefined && total !== null) {
            if (totalUsers === 0) {
              this.logger.log(
                `Total users detected: ${total}. Starting processing...`,
              );

              totalUsers = total;
            } else if (total !== totalUsers) {
              this.logger.warn(
                `Total count changed: ${totalUsers} -> ${total}. Using latest.`,
              );
              totalUsers = total;
            }
            lastValidTotal = total;
          }

          if (!users || users.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalUsers > 0 && currentItem >= totalUsers) {
              this.logger.log('Reached end of data (empty page past total');
              break;
            }

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          const existingUserIds = new Set(
            (
              await this.prismaService.user.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newUsers = users.filter((user) => {
            if (
              !existingUserIds.has(user.id) &&
              !processedUserIds.has(user.id)
            ) {
              processedUserIds.add(user.id);
              return true;
            }
            return false;
          });

          const existingUsers = users.filter((user) => {
            if (
              existingUserIds.has(user.id) &&
              !processedUserIds.has(user.id)
            ) {
              processedUserIds.add(user.id);
              return true;
            }
            return false;
          });

          if (newUsers.length === 0 && existingUsers.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all users already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedUsers: any[] = [];

          if (newUsers.length > 0) {
            this.logger.log(
              `Processing ${newUsers.length} NEW users from page ${currentPage}...`,
            );

            const savedUsers = await this.saveUsersToDatabase(newUsers);
            pageProcessedCount += savedUsers.length;
            allSavedUsers.push(...savedUsers);
          }

          if (existingUsers.length > 0) {
            this.logger.log(
              `Processing ${existingUsers.length} EXISTING users from page ${currentPage}...`,
            );

            const savedUsers = await this.saveUsersToDatabase(existingUsers);
            pageProcessedCount += savedUsers.length;
            allSavedUsers.push(...savedUsers);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (totalUsers > 0) {
            const completionPercentage = (processedCount / totalUsers) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalUsers} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalUsers) {
              this.logger.log('All transfers processed successfully');
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalUsers },
      });

      const completionRate =
        totalUsers > 0 ? (processedCount / totalUsers) * 100 : 100;

      this.logger.log(
        `Historical transfer sync completed: ${processedCount}/${totalUsers} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.log(`Historical transfer sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalUsers },
      });

      throw error;
    }
  }

  private async fetchUsersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchUsersList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          this.logger.log(`Retrying after ${delay / 1000}s delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchUsersList(params: {
    currentItem?: number;
    pageSize?: number;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/users?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async saveUsersToDatabase(users: any[]): Promise<any[]> {
    this.logger.log(`Saving ${users.length} users to database...`);
    const savedUsers: any[] = [];

    for (const userData of users) {
      try {
        const user = await this.prismaService.user.upsert({
          where: { kiotVietId: BigInt(userData.id) },
          update: {
            kiotVietId: BigInt(userData.id),
            userName: userData.userName,
            givenName: userData.givenName,
            mobilePhone: userData.mobilePhone || '',
            email: userData.email || '',
            retailerId: userData.retailerId || null,
            createdDate: userData.createdDate
              ? new Date(userData.createdDate)
              : new Date(),
          },
          create: {
            kiotVietId: BigInt(userData.id),
            userName: userData.userName,
            givenName: userData.givenName,
            mobilePhone: userData.mobilePhone || '',
            email: userData.email || '',
            retailerId: userData.retailerId || null,
            createdDate: userData.createdDate
              ? new Date(userData.createdDate)
              : new Date(),
          },
        });

        savedUsers.push(user);
      } catch (error) {
        this.logger.error(
          `Failed to save user ${userData.id}: ${error.message}`,
        );
      }
    }

    this.logger.log(`Saved ${savedUsers.length} users successfully`);
    return savedUsers;
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['user'],
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
