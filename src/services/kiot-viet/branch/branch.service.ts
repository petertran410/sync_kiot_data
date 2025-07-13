// src/services/kiot-viet/branch/branch.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietBranch {
  id: number;
  branchName: string;
  branchCode?: string;
  contactNumber?: string;
  subContactNumber?: string;
  email?: string;
  address?: string;
  location?: string;
  wardName?: string;
  isActive?: boolean;
  isLock?: boolean;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
}

@Injectable()
export class KiotVietBranchService {
  private readonly logger = new Logger(KiotVietBranchService.name);
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

  // ============================================================================
  // HISTORICAL SYNC
  // ============================================================================

  async syncHistoricalBranches(): Promise<void> {
    const syncName = 'branch_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical branch sync...');

      const response = await this.fetchBranchesWithRetry({
        orderBy: 'branchName',
        orderDirection: 'ASC',
        pageSize: this.PAGE_SIZE,
      });

      const branches = response.data || [];
      this.logger.log(`📊 Found ${branches.length} branches to sync`);

      if (branches.length > 0) {
        const saved = await this.saveBranchesToDatabase(branches);
        this.logger.log(`✅ Saved ${saved.created + saved.updated} branches`);
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log('✅ Historical branch sync completed');
    } catch (error) {
      this.logger.error(`❌ Historical branch sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS
  // ============================================================================

  async fetchBranchesWithRetry(
    params: {
      orderBy?: string;
      orderDirection?: string;
      pageSize?: number;
      currentItem?: number;
      lastModifiedFrom?: string;
      includeRemoveIds?: boolean;
    },
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchBranches(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
        }
      }
    }

    throw lastError;
  }

  async fetchBranches(params: {
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
    includeRemoveIds?: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      orderBy: params.orderBy || 'branchName',
      orderDirection: params.orderDirection || 'ASC',
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
      includeRemoveIds: (params.includeRemoveIds || false).toString(),
    });

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/branches?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // DATABASE SAVE
  // ============================================================================

  private async saveBranchesToDatabase(
    branches: KiotVietBranch[],
  ): Promise<{ created: number; updated: number }> {
    this.logger.log(`💾 Saving ${branches.length} branches to database...`);

    let created = 0;
    let updated = 0;

    for (const branchData of branches) {
      try {
        const result = await this.prismaService.branch.upsert({
          where: { kiotVietId: branchData.id },
          update: {
            name: branchData.branchName,
            contactNumber: branchData.contactNumber || null,
            subContactNumber: branchData.subContactNumber || null,
            email: branchData.email || null,
            address: branchData.address || null,
            location: branchData.location || null,
            wardName: branchData.wardName || null,
            isActive: branchData.isActive ?? true,
            isLock: branchData.isLock ?? false,
            retailerId: branchData.retailerId ?? null,
            modifiedDate: branchData.modifiedDate
              ? new Date(branchData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: branchData.id,
            name: branchData.branchName,
            contactNumber: branchData.contactNumber || null,
            subContactNumber: branchData.subContactNumber || null,
            email: branchData.email || null,
            address: branchData.address || null,
            location: branchData.location || null,
            wardName: branchData.wardName || null,
            isActive: branchData.isActive ?? true,
            isLock: branchData.isLock ?? false,
            retailerId: branchData.retailerId ?? null,
            createdDate: branchData.createdDate
              ? new Date(branchData.createdDate)
              : new Date(),
            modifiedDate: branchData.modifiedDate
              ? new Date(branchData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        // Check if it was created or updated
        const existingCount = await this.prismaService.branch.count({
          where: {
            kiotVietId: branchData.id,
            createdDate: {
              lt: new Date(Date.now() - 1000), // Created more than 1 second ago
            },
          },
        });

        if (existingCount > 0) {
          updated++;
        } else {
          created++;
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save branch ${branchData.branchName}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Branches saved: ${created} created, ${updated} updated`,
    );
    return { created, updated };
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('branch_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical branch sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['branch'],
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
