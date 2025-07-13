// src/services/kiot-viet/pricebook/pricebook.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietPriceBook {
  id: number;
  name: string;
  isActive?: boolean;
  isGlobal?: boolean;
  startDate?: string;
  endDate?: string;
  forAllCusGroup?: boolean;
  forAllUser?: boolean;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
  priceBookBranches?: Array<{
    id: number;
    priceBookId: number;
    branchId: number;
    branchName?: string;
  }>;
  priceBookCustomerGroups?: Array<{
    id: number;
    priceBookId: number;
    customerGroupId: number;
    customerGroupName?: string;
  }>;
  priceBookUsers?: Array<{
    id: number;
    priceBookId: number;
    userId: number;
    userName?: string;
  }>;
}

@Injectable()
export class KiotVietPriceBookService {
  private readonly logger = new Logger(KiotVietPriceBookService.name);
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

  async syncHistoricalPriceBooks(): Promise<void> {
    const syncName = 'pricebook_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical pricebook sync...');

      const response = await this.fetchPriceBooksWithRetry({
        includePriceBookBranch: true,
        includePriceBookCustomerGroups: true,
        includePriceBookUsers: true,
        orderBy: 'name',
        orderDirection: 'ASC',
        pageSize: this.PAGE_SIZE,
      });

      const priceBooks = response.data || [];
      this.logger.log(`📊 Found ${priceBooks.length} pricebooks to sync`);

      if (priceBooks.length > 0) {
        const saved = await this.savePriceBooksToDatabase(priceBooks);
        this.logger.log(`✅ Saved ${saved.created + saved.updated} pricebooks`);
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log('✅ Historical pricebook sync completed');
    } catch (error) {
      this.logger.error(
        `❌ Historical pricebook sync failed: ${error.message}`,
      );

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

  async fetchPriceBooksWithRetry(
    params: {
      includePriceBookBranch?: boolean;
      includePriceBookCustomerGroups?: boolean;
      includePriceBookUsers?: boolean;
      orderBy?: string;
      orderDirection?: string;
      pageSize?: number;
      currentItem?: number;
      lastModifiedFrom?: string;
    },
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchPriceBooks(params);
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

  async fetchPriceBooks(params: {
    includePriceBookBranch?: boolean;
    includePriceBookCustomerGroups?: boolean;
    includePriceBookUsers?: boolean;
    orderBy?: string;
    orderDirection?: string;
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      includePriceBookBranch: (
        params.includePriceBookBranch || false
      ).toString(),
      includePriceBookCustomerGroups: (
        params.includePriceBookCustomerGroups || false
      ).toString(),
      includePriceBookUsers: (params.includePriceBookUsers || false).toString(),
      orderBy: params.orderBy || 'name',
      orderDirection: params.orderDirection || 'ASC',
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // DATABASE SAVE
  // ============================================================================

  private async savePriceBooksToDatabase(
    priceBooks: KiotVietPriceBook[],
  ): Promise<{ created: number; updated: number }> {
    this.logger.log(`💾 Saving ${priceBooks.length} pricebooks to database...`);

    let created = 0;
    let updated = 0;

    for (const priceBookData of priceBooks) {
      try {
        // Save main pricebook
        const result = await this.prismaService.priceBook.upsert({
          where: { kiotVietId: priceBookData.id },
          update: {
            name: priceBookData.name,
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: priceBookData.id,
            name: priceBookData.name,
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            createdDate: priceBookData.createdDate
              ? new Date(priceBookData.createdDate)
              : new Date(),
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        // Check if it was created or updated
        const existingCount = await this.prismaService.priceBook.count({
          where: {
            kiotVietId: priceBookData.id,
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

        // Save related data
        await this.savePriceBookBranches(
          result.id,
          priceBookData.priceBookBranches || [],
        );
        await this.savePriceBookCustomerGroups(
          result.id,
          priceBookData.priceBookCustomerGroups || [],
        );
        await this.savePriceBookUsers(
          result.id,
          priceBookData.priceBookUsers || [],
        );
      } catch (error) {
        this.logger.error(
          `❌ Failed to save pricebook ${priceBookData.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ PriceBooks saved: ${created} created, ${updated} updated`,
    );
    return { created, updated };
  }

  // ============================================================================
  // SAVE RELATED DATA
  // ============================================================================

  private async savePriceBookBranches(
    priceBookId: number,
    branches: any[],
  ): Promise<void> {
    if (branches.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookBranch.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const branchData of branches) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: branchData.branchId },
          select: { id: true },
        });

        if (branch) {
          await this.prismaService.priceBookBranch.create({
            data: {
              kiotVietId: branchData.id ? BigInt(branchData.id) : null,
              priceBookId,
              branchId: branch.id,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook branches: ${error.message}`,
      );
    }
  }

  private async savePriceBookCustomerGroups(
    priceBookId: number,
    customerGroups: any[],
  ): Promise<void> {
    if (customerGroups.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookCustomerGroup.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const cgData of customerGroups) {
        const customerGroup = await this.prismaService.customerGroup.findFirst({
          where: { kiotVietId: cgData.customerGroupId },
          select: { id: true },
        });

        if (customerGroup) {
          await this.prismaService.priceBookCustomerGroup.create({
            data: {
              kiotVietId: cgData.id ? BigInt(cgData.id) : null,
              priceBookId,
              customerGroupId: customerGroup.id,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook customer groups: ${error.message}`,
      );
    }
  }

  private async savePriceBookUsers(
    priceBookId: number,
    users: any[],
  ): Promise<void> {
    if (users.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookUser.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const userData of users) {
        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: BigInt(userData.userId) },
          select: { kiotVietId: true },
        });

        if (user) {
          await this.prismaService.priceBookUser.create({
            data: {
              kiotVietId: userData.id ? BigInt(userData.id) : null,
              priceBookId,
              userId: user.kiotVietId,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(`❌ Failed to save pricebook users: ${error.message}`);
    }
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('pricebook_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical pricebook sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['pricebook'],
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
