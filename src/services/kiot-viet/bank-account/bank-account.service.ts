// src/services/kiot-viet/bank-account/bank-account.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietBankAccountService {
  private readonly logger = new Logger(KiotVietBankAccountService.name);
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

  async fetchBankAccounts(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/BankAccounts`, {
          headers,
          params: {
            ...params,
            includeRemoveIds: true,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch bank accounts: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveBankAccounts(bankAccounts: any[]) {
    if (!bankAccounts || bankAccounts.length === 0)
      return { created: 0, updated: 0 };

    const kiotVietIds = bankAccounts.map((b) => b.id);
    const existingBankAccounts = await this.prismaService.bankAccount.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<number, number>(
      existingBankAccounts.map((b) => [b.kiotVietId, b.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const bankAccountData of bankAccounts) {
      try {
        const existingId = existingMap.get(bankAccountData.id);

        const commonData = {
          bankName: bankAccountData.bankName,
          accountNumber: bankAccountData.accountNumber,
          description: bankAccountData.description,
          retailerId: bankAccountData.retailerId,
          modifiedDate: bankAccountData.modifiedDate
            ? new Date(bankAccountData.modifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
        };

        if (existingId) {
          await this.prismaService.bankAccount.update({
            where: { id: existingId },
            data: commonData,
          });
          updatedCount++;
        } else {
          await this.prismaService.bankAccount.create({
            data: {
              kiotVietId: bankAccountData.id,
              ...commonData,
              createdDate: bankAccountData.createdDate
                ? new Date(bankAccountData.createdDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save bank account ${bankAccountData.bankName}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async handleRemovedBankAccounts(removedIds: number[]) {
    if (!removedIds || removedIds.length === 0) return 0;

    try {
      const result = await this.prismaService.bankAccount.updateMany({
        where: { kiotVietId: { in: removedIds } },
        data: { lastSyncedAt: new Date() },
      });

      this.logger.log(`Updated ${result.count} removed bank accounts`);
      return result.count;
    } catch (error) {
      this.logger.error(
        `Failed to handle removed bank accounts: ${error.message}`,
      );
      throw error;
    }
  }

  async syncBankAccounts(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'bankaccount_sync' },
        create: {
          name: 'bankaccount_sync',
          entities: ['bankaccount'],
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
        const response = await this.fetchBankAccounts({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveBankAccounts(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `BankAccount sync progress: ${totalProcessed} processed`,
          );
        }

        if (response.removeIds && response.removeIds.length > 0) {
          await this.handleRemovedBankAccounts(response.removeIds);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'bankaccount_sync' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `BankAccount sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'bankaccount_sync' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`BankAccount sync failed: ${error.message}`);
      throw error;
    }
  }
}
