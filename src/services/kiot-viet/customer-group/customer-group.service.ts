// src/services/kiot-viet/customer-group/customer-group.service.ts - Replace existing file

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

// ============================================================================
// INTERFACES
// ============================================================================
interface KiotVietCustomerGroup {
  id: number;
  name: string;
  description?: string;
  discount?: number;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
  createdBy?: number;
  customerGroupDetails?: Array<{
    id: number;
    customerId: number;
    groupId: number;
  }>;
}

@Injectable()
export class KiotVietCustomerGroupService {
  private readonly logger = new Logger(KiotVietCustomerGroupService.name);
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
  // MAIN HISTORICAL SYNC METHOD
  // ============================================================================

  async syncHistoricalCustomerGroups(): Promise<void> {
    const syncName = 'customer_group_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        progress: { processedCount: 0 },
      });

      this.logger.log('🚀 Starting historical customer group sync...');

      // Fetch all customer groups from KiotViet
      const response = await this.fetchCustomerGroupsWithRetry();

      if (!response?.data || response.data.length === 0) {
        this.logger.log('📋 No customer groups found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { processedCount: 0, expectedTotal: 0 },
        });
        return;
      }

      const customerGroups = response.data;
      const totalGroups = customerGroups.length;

      this.logger.log(`📊 Found ${totalGroups} customer groups to sync`);

      // Validate and save to database
      const validGroups = this.validateCustomerGroups(customerGroups);
      const savedGroups = await this.saveCustomerGroupsToDatabase(validGroups);

      const processedCount = savedGroups.length;
      const completionRate =
        totalGroups > 0
          ? Math.round((processedCount / totalGroups) * 100)
          : 100;

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        progress: { processedCount, expectedTotal: totalGroups },
      });

      this.logger.log(
        `✅ Historical customer group sync completed: ${processedCount}/${totalGroups} (${completionRate}% completion rate)`,
      );
    } catch (error) {
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      this.logger.error(
        `❌ Historical customer group sync failed: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // API METHODS WITH RETRY
  // ============================================================================

  async fetchCustomerGroupsWithRetry(maxRetries: number = 3): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchCustomerGroupsFromAPI(); // ✅ RENAMED
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delayMs = 1000 * Math.pow(2, attempt - 1);
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError;
  }

  private async fetchCustomerGroupsFromAPI(): Promise<any> {
    // ✅ RENAMED to avoid conflict
    const headers = await this.authService.getRequestHeaders();

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/customers/group`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // VALIDATION
  // ============================================================================

  private validateCustomerGroups(
    customerGroups: KiotVietCustomerGroup[],
  ): KiotVietCustomerGroup[] {
    const validGroups: KiotVietCustomerGroup[] = [];

    for (const group of customerGroups) {
      if (this.validateCustomerGroupData(group)) {
        validGroups.push(group);
      }
    }

    this.logger.log(
      `✅ Validation complete: ${validGroups.length}/${customerGroups.length} customer groups valid`,
    );

    return validGroups;
  }

  private validateCustomerGroupData(group: KiotVietCustomerGroup): boolean {
    // Required field validation
    if (!group.id || typeof group.id !== 'number') {
      this.logger.warn(`⚠️ Invalid group ID: ${group.id}`);
      return false;
    }

    if (
      !group.name ||
      typeof group.name !== 'string' ||
      group.name.trim() === ''
    ) {
      this.logger.warn(
        `⚠️ Invalid group name for ID ${group.id}: '${group.name}'`,
      );
      return false;
    }

    // Discount validation
    if (
      group.discount &&
      (typeof group.discount !== 'number' || group.discount < 0)
    ) {
      this.logger.warn(
        `⚠️ Invalid discount for group ${group.id}: ${group.discount}`,
      );
      return false;
    }

    return true;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  private async saveCustomerGroupsToDatabase(
    customerGroups: KiotVietCustomerGroup[],
  ): Promise<any[]> {
    this.logger.log(
      `💾 Saving ${customerGroups.length} customer groups to database...`,
    );

    const savedGroups: any[] = [];

    for (const groupData of customerGroups) {
      try {
        const group = await this.prismaService.customerGroup.upsert({
          where: { kiotVietId: groupData.id },
          update: {
            name: groupData.name.trim(),
            description: groupData.description?.trim() || null,
            discount: groupData.discount
              ? new Prisma.Decimal(groupData.discount)
              : null,
            retailerId: groupData.retailerId || null,
            modifiedDate: groupData.modifiedDate
              ? new Date(groupData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: groupData.id,
            name: groupData.name.trim(),
            description: groupData.description?.trim() || null,
            discount: groupData.discount
              ? new Prisma.Decimal(groupData.discount)
              : null,
            retailerId: groupData.retailerId || null,
            createdDate: groupData.createdDate
              ? new Date(groupData.createdDate)
              : new Date(),
            modifiedDate: groupData.modifiedDate
              ? new Date(groupData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedGroups.push(group);

        // Process customer group relationships if available
        if (
          groupData.customerGroupDetails &&
          groupData.customerGroupDetails.length > 0
        ) {
          await this.syncCustomerGroupRelations(
            group.id,
            groupData.customerGroupDetails,
          );
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save customer group ${groupData.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `💾 Saved ${savedGroups.length} customer groups to database`,
    );
    return savedGroups;
  }

  private async syncCustomerGroupRelations(
    customerGroupId: number,
    relations: Array<{ id: number; customerId: number; groupId: number }>,
  ): Promise<void> {
    try {
      for (const relation of relations) {
        // Find customer in database
        const customer = await this.prismaService.customer.findFirst({
          where: { kiotVietId: BigInt(relation.customerId) },
          select: { id: true },
        });

        if (customer) {
          await this.prismaService.customerGroupRelation.upsert({
            where: {
              customerId_customerGroupId: {
                customerId: customer.id,
                customerGroupId: customerGroupId,
              },
            },
            update: {
              kiotVietId: BigInt(relation.id),
            },
            create: {
              kiotVietId: BigInt(relation.id),
              customerId: customer.id,
              customerGroupId: customerGroupId,
            },
          });
        }
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to sync customer group relations for group ${customerGroupId}: ${error.message}`,
      );
    }
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('customer_group_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical customer group sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['customer_group'],
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

  // ============================================================================
  // LEGACY METHODS (for backward compatibility)
  // ============================================================================

  async fetchCustomerGroups() {
    return this.fetchCustomerGroupsWithRetry();
  }

  async syncCustomerGroups(): Promise<void> {
    this.logger.warn(
      '⚠️ Using legacy syncCustomerGroups method. Consider using syncHistoricalCustomerGroups instead.',
    );
    return this.syncHistoricalCustomerGroups();
  }
}
