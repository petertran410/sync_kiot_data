import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietBranchService {
  private readonly logger = new Logger(KiotVietBranchService.name);
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

  async fetchBranches() {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/branches`, {
          headers,
          params: {
            includeRemoveIds: true,
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch branches: ${error.message}`);
      throw error;
    }
  }

  async syncBranches(): Promise<void> {
    try {
      this.logger.log('Starting branch sync...');
      const response = await this.fetchBranches();

      if (response.data && response.data.length > 0) {
        let processedCount = 0;

        for (const branchData of response.data) {
          try {
            await this.prismaService.branch.upsert({
              where: { kiotVietId: branchData.id },
              create: {
                kiotVietId: branchData.id,
                name: branchData.branchName,
                code: branchData.branchCode,
                contactNumber: branchData.contactNumber,
                email: branchData.email,
                address: branchData.address,
                retailerId: branchData.retailerId,
                isActive: true,
                createdDate: branchData.createdDate
                  ? new Date(branchData.createdDate)
                  : new Date(),
                modifiedDate: branchData.modifiedDate
                  ? new Date(branchData.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
              update: {
                name: branchData.branchName,
                code: branchData.branchCode,
                contactNumber: branchData.contactNumber,
                email: branchData.email,
                address: branchData.address,
                retailerId: branchData.retailerId,
                modifiedDate: branchData.modifiedDate
                  ? new Date(branchData.modifiedDate)
                  : new Date(),
                lastSyncedAt: new Date(),
              },
            });
            processedCount++;
          } catch (error) {
            this.logger.error(
              `Failed to sync branch ${branchData.branchName}: ${error.message}`,
            );
          }
        }

        this.logger.log(
          `Branch sync completed: ${processedCount} branches processed`,
        );
      }

      if (response.removedIds && response.removedIds.length > 0) {
        await this.prismaService.branch.updateMany({
          where: { kiotVietId: { in: response.removedIds } },
          data: { isActive: false, lastSyncedAt: new Date() },
        });
        this.logger.log(
          `Marked ${response.removedIds.length} branches as inactive`,
        );
      }
    } catch (error) {
      this.logger.error(`Branch sync failed: ${error.message}`);
      throw error;
    }
  }
}
