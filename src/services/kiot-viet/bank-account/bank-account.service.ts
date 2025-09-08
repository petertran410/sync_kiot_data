// src/services/kiot-viet/bank-account/bank-account.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';

@Injectable()
export class KiotVietBankAccountService {
  private readonly logger = new Logger(KiotVietBankAccountService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
  ) {}

  async syncBankAccounts(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'bankaccount_historical' },
        create: {
          name: 'bankaccount_historical',
          entities: ['bankaccount'],
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

      const response = await this.fetchBankAccounts();
      let totalProcessed = 0;

      if (response.data && response.data.length > 0) {
        const { created, updated } = await this.saveBankAccountsToDatabase(
          response.data,
        );
        totalProcessed = created + updated;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'bankaccount_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Bank account sync completed: ${totalProcessed} accounts processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'bankaccount_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Bank account sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchBankAccounts(): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const url = `${baseUrl}/accounts`;

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
      this.logger.error(`Failed to fetch bank accounts: ${error.message}`);
      throw error;
    }
  }

  private async saveBankAccountsToDatabase(
    bankAccounts: any[],
  ): Promise<{ created: number; updated: number }> {
    let createdCount = 0;
    let updatedCount = 0;

    for (const bankAccountData of bankAccounts) {
      try {
        const existingBankAccount =
          await this.prismaService.bankAccount.findUnique({
            where: { kiotVietId: bankAccountData.id },
          });

        if (existingBankAccount) {
          await this.prismaService.bankAccount.update({
            where: { id: existingBankAccount.id },
            data: {
              bankName: bankAccountData.bankName,
              accountNumber: bankAccountData.accountNumber || null,
              retailerId: bankAccountData.retailerId || null,
            },
          });
          updatedCount++;
        } else {
          await this.prismaService.bankAccount.create({
            data: {
              kiotVietId: bankAccountData.id,
              bankName: bankAccountData.bankName,
              accountNumber: bankAccountData.accountNumber || null,
              retailerId: bankAccountData.retailerId || null,
              createdDate: bankAccountData.createdDate
                ? new Date(bankAccountData.createdDate)
                : new Date(),
            },
          });
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save bank account ${bankAccountData.id}: ${error.message}`,
        );
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
}
