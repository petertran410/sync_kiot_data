import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietBranchService } from '../branch/branch.service';
import { KiotVietAuthService } from '../auth.service';
import { KiotVietCustomerGroupService } from '../customer-group/customer-group.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly branchService: KiotVietBranchService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;

    this.logger.debug(`HttpService injected: ${!!this.httpService}`);
    this.logger.debug(`ConfigService injected: ${!!this.configService}`);
    this.logger.debug(`PrismaService injected: ${!!this.prismaService}`);
    this.logger.debug(`AuthService injected: ${!!this.authService}`);
    this.logger.debug(`Base URL: ${this.baseUrl}`);
  }

  async fetchCustomers(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      if (!this.httpService) {
        throw new Error('HttpService is not available');
      }

      if (!this.authService) {
        throw new Error('AuthService is not available');
      }

      if (!this.baseUrl) {
        throw new Error('Base URL is not configured');
      }

      this.logger.debug('Getting request headers...');
      const headers = await this.authService.getRequestHeaders();

      this.logger.debug('Making HTTP request to KiotViet API...');
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/customers`, {
          headers,
          params: {
            ...params,
            includeRemoveIds: true,
            includeTotal: true,
            includeCustomerGroup: true,
            includeCustomerSocial: true,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch customers: ${error.message}`);
      this.logger.error(`Error stack: ${error.stack}`);
      throw error;
    }
  }

  private async handleCustomerGroups(
    customerId: number,
    groupsString: string | null,
  ): Promise<void> {
    if (!groupsString) return;

    try {
      const groupNames = groupsString.split('|').filter((name) => name.trim());

      if (groupNames.length === 0) return;

      const existingGroups = await this.prismaService.customerGroup.findMany({
        where: { name: { in: groupNames } },
      });

      for (const group of existingGroups) {
        try {
          await this.prismaService.customerGroupRelation.upsert({
            where: {
              customerId_customerGroupId: {
                customerId: customerId,
                customerGroupId: group.id,
              },
            },
            create: {
              customerId: customerId,
              customerGroupId: group.id,
            },
            update: {},
          });
        } catch (error) {
          this.logger.debug(
            `Customer group relation already exists: Customer ${customerId}, Group ${group.id}`,
          );
        }
      }

      const foundGroupNames = existingGroups.map((g) => g.name);
      const missingGroups = groupNames.filter(
        (name) => !foundGroupNames.includes(name),
      );
      if (missingGroups.length > 0) {
        this.logger.warn(
          `Customer groups not found: ${missingGroups.join(', ')}`,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to handle customer groups for customer ${customerId}: ${error.message}`,
      );
    }
  }

  async batchSaveCustomers(customers: any[]) {
    if (!customers || customers.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = customers.map((c) => BigInt(c.id));

    const existingCustomers = await this.prismaService.customer.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<string, number>(
      existingCustomers.map((c) => [c.kiotVietId.toString(), c.id]),
    );

    const customersToCreate: Array<{
      createData: Prisma.CustomerCreateInput;
      groups: string | null;
    }> = [];
    const customersToUpdate: Array<{
      id: number;
      data: Prisma.CustomerUpdateInput;
      groups: string | null;
    }> = [];

    for (const customerData of customers) {
      const kiotVietId = BigInt(customerData.id);
      const existingId = existingMap.get(kiotVietId.toString());

      if (existingId) {
        customersToUpdate.push({
          id: existingId,
          data: await this.prepareCustomerUpdateData(customerData),
          groups: customerData.groups || null,
        });
      } else {
        const createData = await this.prepareCustomerCreateData(customerData);
        if (createData) {
          customersToCreate.push({
            createData: createData,
            groups: customerData.groups || null,
          });
        }
      }
    }

    let createdCount = 0;
    let updatedCount = 0;

    if (customersToCreate.length > 0) {
      for (const { createData, groups } of customersToCreate) {
        try {
          const createdCustomer = await this.prismaService.customer.create({
            data: createData,
          });

          if (groups) {
            await this.handleCustomerGroups(createdCustomer.id, groups);
          }

          createdCount++;
        } catch (error) {
          this.logger.error(
            `Failed to create customer ${createData.code}: ${error.message}`,
          );
        }
      }
    }

    if (customersToUpdate.length > 0) {
      for (const { id, data, groups } of customersToUpdate) {
        try {
          await this.prismaService.customer.update({
            where: { id },
            data,
          });

          if (groups) {
            await this.prismaService.customerGroupRelation.deleteMany({
              where: { customerId: id },
            });

            await this.handleCustomerGroups(id, groups);
          }

          updatedCount++;
        } catch (error) {
          this.logger.error(
            `Failed to update customer ${id}: ${error.message}`,
          );
        }
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private convertToLocalTime(dateString: string): Date {
    if (!dateString) return new Date();

    const date = new Date(dateString);
    return date;
  }

  private async prepareCustomerCreateData(
    customerData: any,
  ): Promise<Prisma.CustomerCreateInput | null> {
    const data: Prisma.CustomerCreateInput = {
      kiotVietId: BigInt(customerData.id),
      code: customerData.code,
      name: customerData.name,
      type: customerData.type || 0,
      gender: customerData.gender,
      birthDate: customerData.birthDate
        ? this.convertToLocalTime(customerData.birthDate)
        : null,
      contactNumber: customerData.contactNumber,
      address: customerData.address,
      locationName: customerData.locationName,
      wardName: customerData.wardName,
      email: customerData.email,
      organization: customerData.organization,
      taxCode: customerData.taxCode,
      comments: customerData.comments,
      debt: customerData.debt ? new Prisma.Decimal(customerData.debt) : null,
      totalInvoiced: customerData.totalInvoiced
        ? new Prisma.Decimal(customerData.totalInvoiced)
        : null,
      totalPoint: customerData.totalPoint || null,
      totalRevenue: customerData.totalRevenue
        ? new Prisma.Decimal(customerData.totalRevenue)
        : null,
      rewardPoint: customerData.rewardPoint
        ? BigInt(customerData.rewardPoint)
        : null,
      psidFacebook: customerData.psidFacebook
        ? BigInt(customerData.psidFacebook)
        : null,
      retailerId: customerData.retailerId,
      isActive: true,
      createdDate: customerData.createdDate
        ? this.convertToLocalTime(customerData.createdDate)
        : new Date(),
      modifiedDate: customerData.modifiedDate
        ? this.convertToLocalTime(customerData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    if (customerData.branchId) {
      try {
        const branchExists = await this.prismaService.branch.findFirst({
          where: { kiotVietId: customerData.branchId },
        });

        if (branchExists) {
          data.branch = {
            connect: { kiotVietId: customerData.branchId },
          };
        } else {
          this.logger.warn(
            `Branch with kiotVietId ${customerData.branchId} not found for customer ${customerData.code}`,
          );
        }
      } catch (error) {
        this.logger.error(
          `Error checking branch for customer ${customerData.code}: ${error.message}`,
        );
      }
    }

    return data;
  }

  private async prepareCustomerUpdateData(
    customerData: any,
  ): Promise<Prisma.CustomerUpdateInput> {
    const data: Prisma.CustomerUpdateInput = {
      code: customerData.code,
      name: customerData.name,
      type: customerData.type,
      gender: customerData.gender,
      birthDate: customerData.birthDate
        ? this.convertToLocalTime(customerData.birthDate)
        : null,
      contactNumber: customerData.contactNumber,
      address: customerData.address,
      locationName: customerData.locationName,
      wardName: customerData.wardName,
      email: customerData.email,
      organization: customerData.organization,
      taxCode: customerData.taxCode,
      comments: customerData.comments,
      debt: customerData.debt ? new Prisma.Decimal(customerData.debt) : null,
      totalInvoiced: customerData.totalInvoiced
        ? new Prisma.Decimal(customerData.totalInvoiced)
        : null,
      totalPoint: customerData.totalPoint,
      totalRevenue: customerData.totalRevenue
        ? new Prisma.Decimal(customerData.totalRevenue)
        : null,
      rewardPoint: customerData.rewardPoint
        ? BigInt(customerData.rewardPoint)
        : null,
      psidFacebook: customerData.psidFacebook
        ? BigInt(customerData.psidFacebook)
        : null,
      retailerId: customerData.retailerId,
      isActive: true,
      modifiedDate: customerData.modifiedDate
        ? this.convertToLocalTime(customerData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    if (customerData.branchId) {
      try {
        const branchExists = await this.prismaService.branch.findFirst({
          where: { kiotVietId: customerData.branchId },
        });

        if (branchExists) {
          data.branch = {
            connect: { kiotVietId: customerData.branchId },
          };
        } else {
          this.logger.debug(
            `Branch ${customerData.branchId} not found for customer ${customerData.code} - skipping branch relationship`,
          );
        }
      } catch (error) {
        this.logger.error(
          `Error checking branch for customer ${customerData.code}: ${error.message}`,
        );
      }
    }

    return data;
  }

  async handleRemovedCustomers(removedIds: number[]) {
    if (!removedIds || removedIds.length === 0) return 0;

    try {
      const result = await this.prismaService.customer.updateMany({
        where: { kiotVietId: { in: removedIds.map((id) => BigInt(id)) } },
        data: { isActive: false, lastSyncedAt: new Date() },
      });

      this.logger.log(`Marked ${result.count} customers as inactive`);
      return result.count;
    } catch (error) {
      this.logger.error(`Failed to handle removed customers: ${error.message}`);
      throw error;
    }
  }

  async removeDuplicateCustomers(): Promise<number> {
    try {
      const duplicates = await this.prismaService.$queryRaw<
        Array<{ kiotVietId: bigint; count: number }>
      >`
        SELECT "kiotVietId", COUNT(*) as count 
        FROM "Customer" 
        GROUP BY "kiotVietId" 
        HAVING COUNT(*) > 1
      `;

      let totalRemoved = 0;

      for (const dup of duplicates) {
        const records = await this.prismaService.customer.findMany({
          where: { kiotVietId: dup.kiotVietId },
          orderBy: { lastSyncedAt: 'desc' },
        });

        if (records.length > 1) {
          const idsToRemove = records.slice(1).map((r) => r.id);
          await this.prismaService.customer.deleteMany({
            where: { id: { in: idsToRemove } },
          });
          totalRemoved += idsToRemove.length;
        }
      }

      this.logger.log(`Removed ${totalRemoved} duplicate customer records`);
      return totalRemoved;
    } catch (error) {
      this.logger.error(`Failed to remove duplicates: ${error.message}`);
      throw error;
    }
  }

  async syncRecentCustomers(days: number = 7): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_historical', isRunning: true },
      });

      if (historicalSync) {
        this.logger.log('Historical sync is running. Skipping recent sync.');
        return;
      }

      await this.prismaService.syncControl.upsert({
        where: { name: 'customer_recent' },
        create: {
          name: 'customer_recent',
          entities: ['customer'],
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
      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchCustomers({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveCustomers(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Recent sync progress: ${totalProcessed} customers processed`,
          );
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedCustomers(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      const duplicatesRemoved = await this.removeDuplicateCustomers();

      await this.prismaService.syncControl.update({
        where: { name: 'customer_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved },
        },
      });

      this.logger.log(
        `Recent sync completed: ${totalProcessed} customers processed, ${duplicatesRemoved} duplicates removed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'customer_recent' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Recent sync failed: ${error.message}`);
      throw error;
    }
  }

  async syncHistoricalCustomers(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'customer_historical' },
        create: {
          name: 'customer_historical',
          entities: ['customer'],
          syncMode: 'historical',
          isRunning: true,
          isEnabled: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
          progress: {},
        },
      });

      let currentItem = 0;
      let totalProcessed = 0;
      let batchCount = 0;
      let hasMoreData = true;
      const customerBatch: any[] = [];

      this.logger.log('Starting historical customer sync...');

      while (hasMoreData) {
        const response = await this.fetchCustomers({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          customerBatch.push(...response.data);

          if (
            customerBatch.length >= this.BATCH_SIZE ||
            response.data.length < this.PAGE_SIZE
          ) {
            const { created, updated } =
              await this.batchSaveCustomers(customerBatch);
            totalProcessed += created + updated;
            batchCount++;

            await this.prismaService.syncControl.update({
              where: { name: 'customer_historical' },
              data: {
                progress: {
                  totalProcessed,
                  batchCount,
                  lastProcessedItem: currentItem + response.data.length,
                },
              },
            });

            this.logger.log(
              `Historical sync batch ${batchCount}: ${totalProcessed} customers processed`,
            );
            customerBatch.length = 0;
          }
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedCustomers(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      this.logger.log('Removing duplicates...');
      const duplicatesRemoved = await this.removeDuplicateCustomers();

      await this.prismaService.syncControl.update({
        where: { name: 'customer_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved, batchCount },
        },
      });

      this.logger.log(
        `Historical sync completed: ${totalProcessed} customers processed, ${duplicatesRemoved} duplicates removed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'customer_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Historical sync failed: ${error.message}`);
      throw error;
    }
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    // First sync branches
    this.logger.log('Syncing branches first...');
    try {
      await this.branchService.syncBranches();
    } catch (error) {
      this.logger.warn(`Branch sync failed, continuing: ${error.message}`);
    }

    this.logger.log('Syncing customer groups...');
    try {
      await this.customerGroupService.syncCustomerGroups();
    } catch (error) {
      this.logger.warn(
        `Customer group sync failed, continuing: ${error.message}`,
      );
    }

    const historicalSync = await this.prismaService.syncControl.findFirst({
      where: { name: 'customer_historical' },
    });

    if (!historicalSync) {
      this.logger.log(
        'No historical sync record found. Starting full historical sync...',
      );
      await this.syncHistoricalCustomers();
      return;
    }

    if (historicalSync?.isEnabled && historicalSync?.isRunning) {
      this.logger.log(
        'System restart detected: Historical sync was running, resuming...',
      );
      await this.syncHistoricalCustomers();
    } else if (historicalSync?.isEnabled && !historicalSync?.isRunning) {
      this.logger.log(
        'System restart detected: Historical sync enabled, starting...',
      );
      await this.syncHistoricalCustomers();
    } else if (historicalSync?.status === 'completed') {
      this.logger.log('Historical sync completed. Running recent sync...');
      await this.syncRecentCustomers();
    } else {
      this.logger.log('System restart detected: Running recent sync...');
      await this.syncRecentCustomers();
    }
  }
}
