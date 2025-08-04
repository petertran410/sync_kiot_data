import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import { response } from 'express';

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

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('customer_group_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical customer group sync enabled');
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_group_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical customer_group sync...');
        await this.syncHistoricalCustomerGroups();
        return;
      }

      this.logger.log('Running default historical customer_group sync...');
      await this.syncHistoricalCustomerGroups();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async syncHistoricalCustomerGroups(): Promise<void> {
    const syncName = 'customer_group_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalCustomerGroups = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedCustomerGroupsIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical customer group sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalCustomerGroups > 0) {
          if (currentItem >= totalCustomerGroups) {
            this.logger.log(
              `✅ Pagination complete. Processed ${processedCount}/${totalCustomerGroups} customer_group`,
            );
            break;
          }
        }

        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchCustomerGroupsWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
          });

          consecutiveErrorPages = 0;

          const { data: customer_groups, total } = response;

          if (total !== undefined && total !== null) {
            if (totalCustomerGroups === 0) {
              this.logger.log(
                `📊 Total customer_group detected: ${total}. Starting processing...`,
              );

              totalCustomerGroups = total;
            } else if (
              total !== totalCustomerGroups &&
              total !== lastValidTotal
            ) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalCustomerGroups} → ${total}. Using latest.`,
              );

              totalCustomerGroups = total;
            }
            lastValidTotal = total;
          }

          if (!customer_groups || customer_groups.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );

            consecutiveEmptyPages++;

            if (totalCustomerGroups > 0 && currentItem >= totalCustomerGroups) {
              this.logger.log('✅ Reached end of data (empty page past total)');
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

          const newCustomerGroups = customer_groups.filter((customer_group) => {
            if (processedCustomerGroupsIds.has(customer_group.id)) {
              this.logger.debug(
                `⚠️ Duplicate customer_group ID detected: ${customer_group.id} (${customer_group.code})`,
              );
              return false;
            }
            processedCustomerGroupsIds.add(customer_group.id);
            return true;
          });

          if (newCustomerGroups.length !== customer_groups.length) {
            this.logger.warn(
              `🔄 Filtered out ${customer_groups.length - newCustomerGroups.length} duplicate customer_groups on page ${currentPage}`,
            );
          }

          if (newCustomerGroups.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all customer_groups already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newCustomerGroups.length} customer_groups from page ${currentPage}...`,
          );

          const customerGroupsWithDetails =
            await this.enrichCustomerGroupsWithDetails(newCustomerGroups);
          const savedCustomerGroups = await this.saveCustomerGroupsToDatabase(
            customerGroupsWithDetails,
          );

          processedCount += savedCustomerGroups.length;
          currentItem += this.PAGE_SIZE;

          if (totalCustomerGroups > 0) {
            const completionPercentage =
              (processedCount / totalCustomerGroups) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalCustomerGroups} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalCustomerGroups) {
              this.logger.log('🎉 All customer_groups processed successfully!');
              break;
            }
          }

          consecutiveEmptyPages = 0;
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (
            consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES ||
            totalRetries >= MAX_TOTAL_RETRIES
          ) {
            throw new Error(
              `Too many consecutive errors (${consecutiveErrorPages}) or total retries (${totalRetries}). Last error: ${error.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalCustomerGroups },
      });

      const completionRate =
        totalCustomerGroups > 0
          ? (processedCount / totalCustomerGroups) * 100
          : 100;

      this.logger.log(
        `✅ Historical customer_group sync completed: ${processedCount}/${totalCustomerGroups} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Historical customer_group sync failed: ${error.message}`,
      );

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalCustomerGroups },
      });

      throw error;
    }
  }

  async fetchCustomerGroupsWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchCustomerGroupsList(params);
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

  private async fetchCustomerGroupsList(params: {
    currentItem?: number;
    pageSize?: number;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/customers/group?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichCustomerGroupsWithDetails(
    customer_groups: KiotVietCustomerGroup[],
  ): Promise<KiotVietCustomerGroup[]> {
    this.logger.log(
      `🔍 Enriching ${customer_groups.length} customer_groups with details...`,
    );

    const enrichedCustomerGroups: KiotVietCustomerGroup[] = [];

    for (const customer_group of customer_groups) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/customers/group`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data) {
          enrichedCustomerGroups.push(response.data);
        } else {
          enrichedCustomerGroups.push(customer_group);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich supplier ${customer_group.name}: ${error.message}`,
        );

        enrichedCustomerGroups.push(customer_group);
      }
    }
    return enrichedCustomerGroups;
  }

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

  // async fetchCustomerGroups() {
  //   return this.fetchCustomerGroupsWithRetry();
  // }

  async syncCustomerGroups(): Promise<void> {
    this.logger.warn(
      '⚠️ Using legacy syncCustomerGroups method. Consider using syncHistoricalCustomerGroups instead.',
    );
    return this.syncHistoricalCustomerGroups();
  }
}
