import { CustomerGroupRelation } from './../../../../node_modules/.prisma/client/index.d';
// src/services/kiot-viet/customer/customer.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { LarkBaseService } from '../../lark/lark-base.service';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);
  private readonly PAGE_SIZE = 50;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
    private readonly larkBaseService: LarkBaseService,
  ) {}

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
      let hasMoreData = true;
      let batchCount = 0;

      while (hasMoreData) {
        const response = await this.fetchCustomers({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.saveCustomersToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          await this.markCustomersForLarkBaseSync(response.data);

          this.logger.log(
            `Historical sync batch ${++batchCount}: ${totalProcessed} customers processed`,
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

      await this.syncPendingToLarkBase();
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

  async syncRecentCustomers(days: number = 4): Promise<void> {
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
          const { created, updated } = await this.saveCustomersToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          await this.markCustomersForLarkBaseSync(response.data);

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

      await this.syncPendingToLarkBase();
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

  // ===== DATABASE → LARKBASE SYNC METHODS =====
  async syncPendingToLarkBase(): Promise<{ success: number; failed: number }> {
    try {
      const pendingCustomers = await this.prismaService.customer.findMany({
        where: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: { lt: 3 },
        },
        include: {
          branch: true,
        },
        take: 100,
      });

      if (pendingCustomers.length === 0) {
        this.logger.log('No pending customers to sync to LarkBase');
        return { success: 0, failed: 0 };
      }

      this.logger.log(
        `Syncing ${pendingCustomers.length} pending customers to LarkBase`,
      );

      const recordsToCreate: any[] = [];
      const recordsToUpdate: any[] = [];

      for (const customer of pendingCustomers) {
        if (customer.larkRecordId) {
          recordsToUpdate.push(customer);
        } else {
          recordsToCreate.push(customer);
        }
      }

      let totalSuccess = 0;
      let totalFailed = 0;

      if (recordsToCreate.length > 0) {
        const createResult = await this.larkBaseCreateBatch(recordsToCreate);
        totalSuccess += createResult.success;
        totalFailed += createResult.failed;
      }

      if (recordsToUpdate.length > 0) {
        const updateResult = await this.larkBaseUpdateBatch(recordsToUpdate);
        totalSuccess += updateResult.success;
        totalFailed += updateResult.failed;
      }

      this.logger.log(
        `LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: 0 };
    }
  }

  private async larkBaseCreateBatch(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response =
        await this.larkBaseService.directCreateCustomers(customers);

      if (response.success > 0 && response.records) {
        for (const [index, customer] of customers.entries()) {
          if (response.records[index]) {
            await this.prismaService.customer.update({
              where: { id: customer.id },
              data: {
                larkRecordId: response.records[index].record_id,
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
                larkSyncRetries: 0,
              },
            });
          }
        }
      }

      if (response.failed > 0) {
        const failedCustomers = customers.slice(response.success);
        for (const customer of failedCustomers) {
          await this.prismaService.customer.update({
            where: { id: customer.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase create batch failed: ${error.message}`);

      for (const customer of customers) {
        await this.prismaService.customer.update({
          where: { id: customer.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: customers.length };
    }
  }

  private async larkBaseUpdateBatch(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response =
        await this.larkBaseService.directUpdateCustomers(customers);

      if (response.success > 0) {
        const successfulCustomers = customers.slice(0, response.success);
        for (const customer of successfulCustomers) {
          await this.prismaService.customer.update({
            where: { id: customer.id },
            data: {
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });
        }
      }

      if (response.failed > 0) {
        const failedCustomers = customers.slice(response.success);
        for (const customer of failedCustomers) {
          await this.prismaService.customer.update({
            where: { id: customer.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase update batch failed: ${error.message}`);

      for (const customer of customers) {
        await this.prismaService.customer.update({
          where: { id: customer.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: customers.length };
    }
  }

  private async markCustomersForLarkBaseSync(customers: any[]): Promise<void> {
    try {
      const kiotVietIds = customers.map((c: any) => BigInt(c.id));

      await this.prismaService.customer.updateMany({
        where: { kiotVietId: { in: kiotVietIds } },
        data: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: 0,
        },
      });

      this.logger.debug(
        `Marked ${customers.length} customers for LarkBase sync`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to mark customers for LarkBase sync: ${error.message}`,
      );
    }
  }

  // ===== EXISTING METHODS (COMPLETE IMPLEMENTATION) =====
  private async fetchCustomers(params: any): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const queryParams = new URLSearchParams();
      if (params.currentItem !== undefined) {
        queryParams.append('currentItem', params.currentItem.toString());
      }
      if (params.pageSize) {
        queryParams.append('pageSize', params.pageSize.toString());
      }
      if (params.lastModifiedFrom) {
        queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
      }
      queryParams.append('includeRemove', 'true');

      const url = `${baseUrl}/customers?${queryParams.toString()}`;

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
      this.logger.error(`Failed to fetch customers: ${error.message}`);
      throw error;
    }
  }

  private async saveCustomersToDatabase(
    customers: any[],
  ): Promise<{ created: number; updated: number }> {
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
      const existingCustomer = await this.prismaService.customer.findUnique({
        where: { kiotVietId: BigInt(customerData.id) },
      });

      if (existingCustomer) {
        const updateData = await this.prepareCustomerUpdateData(customerData);
        customersToUpdate.push({
          id: existingCustomer.id,
          data: updateData,
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

    return await this.processDatabaseOperations(
      customersToCreate,
      customersToUpdate,
    );
  }

  private async processDatabaseOperations(
    customersToCreate: Array<{
      createData: Prisma.CustomerCreateInput;
      groups: string | null;
    }>,
    customersToUpdate: Array<{
      id: number;
      data: Prisma.CustomerUpdateInput;
      groups: string | null;
    }>,
  ) {
    let createdCount = 0;
    let updatedCount = 0;

    // Create customers
    for (const customerToCreate of customersToCreate) {
      try {
        const customer = await this.prismaService.customer.create({
          data: customerToCreate.createData,
        });

        if (customerToCreate.groups) {
          await this.handleCustomerGroups(customer.id, customerToCreate.groups);
        }

        createdCount++;
      } catch (error) {
        this.logger.error(`Failed to create customer: ${error.message}`);
      }
    }

    // Update customers
    for (const customerToUpdate of customersToUpdate) {
      try {
        await this.prismaService.customer.update({
          where: { id: customerToUpdate.id },
          data: customerToUpdate.data,
        });

        if (customerToUpdate.groups) {
          await this.handleCustomerGroups(
            customerToUpdate.id,
            customerToUpdate.groups,
          );
        }

        updatedCount++;
      } catch (error) {
        this.logger.error(`Failed to update customer: ${error.message}`);
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async prepareCustomerCreateData(
    customerData: any,
  ): Promise<Prisma.CustomerCreateInput | null> {
    try {
      const data: Prisma.CustomerCreateInput = {
        kiotVietId: BigInt(customerData.id),
        code: customerData.code,
        name: customerData.name,
        gender: customerData.gender,
        birthDate: customerData.birthDate
          ? new Date(customerData.birthDate)
          : null,
        contactNumber: customerData.contactNumber || null,
        address: customerData.address || null,
        locationName: customerData.locationName || null,
        wardName: customerData.wardName || null,
        email: customerData.email || null,
        organization: customerData.organization || null,
        taxCode: customerData.taxCode || null,
        comments: customerData.comments || null,
        debt: customerData.debt ? parseFloat(customerData.debt) : 0,
        totalInvoiced: customerData.totalInvoiced
          ? parseFloat(customerData.totalInvoiced)
          : 0,
        totalInvoicedWithoutReturn: customerData.totalInvoicedWithoutReturn
          ? parseFloat(customerData.totalInvoicedWithoutReturn)
          : 0,
        totalRevenue: customerData.totalRevenue
          ? parseFloat(customerData.totalRevenue)
          : 0,
        totalPoint: customerData.totalPoint || 0,
        rewardPoint: customerData.rewardPoint || 0,
        retailerId: customerData.retailerId || null,
        branchId: customerData.branchId || null,
        type: customerData.type || null,
        isActive:
          customerData.isActive !== undefined ? customerData.isActive : true,
        isLock: customerData.isLock !== undefined ? customerData.isLock : false,
        psidFacebook: customerData.psidFacebook
          ? BigInt(customerData.psidFacebook)
          : null,
        createdDate: customerData.createdDate
          ? new Date(customerData.createdDate)
          : new Date(),
        modifiedDate: customerData.modifiedDate
          ? new Date(customerData.modifiedDate)
          : new Date(),
        lastSyncedAt: new Date(),
        larkSyncStatus: 'PENDING',
      };

      // Handle branch relationship
      if (customerData.branchId) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: customerData.branchId },
        });
        if (branch) {
          data.branch = { connect: { id: branch.id } };
        }
      }

      return data;
    } catch (error) {
      this.logger.error(
        `Failed to prepare customer create data: ${error.message}`,
      );
      return null;
    }
  }

  private async prepareCustomerUpdateData(
    customerData: any,
  ): Promise<Prisma.CustomerUpdateInput> {
    const data: Prisma.CustomerUpdateInput = {
      code: customerData.code,
      name: customerData.name,
      gender: customerData.gender,
      birthDate: customerData.birthDate
        ? new Date(customerData.birthDate)
        : null,
      contactNumber: customerData.contactNumber || null,
      address: customerData.address || null,
      locationName: customerData.locationName || null,
      wardName: customerData.wardName || null,
      email: customerData.email || null,
      organization: customerData.organization || null,
      taxCode: customerData.taxCode || null,
      comments: customerData.comments || null,
      debt: customerData.debt ? parseFloat(customerData.debt) : 0,
      totalInvoiced: customerData.totalInvoiced
        ? parseFloat(customerData.totalInvoiced)
        : 0,
      totalInvoicedWithoutReturn: customerData.totalInvoicedWithoutReturn
        ? parseFloat(customerData.totalInvoicedWithoutReturn)
        : 0,
      totalRevenue: customerData.totalRevenue
        ? parseFloat(customerData.totalRevenue)
        : 0,
      totalPoint: customerData.totalPoint || 0,
      rewardPoint: customerData.rewardPoint || 0,
      retailerId: customerData.retailerId || null,
      branchId: customerData.branchId || null,
      type: customerData.type || null,
      isActive:
        customerData.isActive !== undefined ? customerData.isActive : true,
      isLock: customerData.isLock !== undefined ? customerData.isLock : false,
      psidFacebook: customerData.psidFacebook
        ? BigInt(customerData.psidFacebook)
        : null,
      modifiedDate: customerData.modifiedDate
        ? new Date(customerData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
      larkSyncStatus: 'PENDING',
    };

    // Handle branch relationship
    if (customerData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: customerData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
      }
    }

    return data;
  }

  private async handleCustomerGroups(
    customerId: number,
    groupsString: string,
  ): Promise<void> {
    try {
      // Remove existing group associations
      await this.prismaService.customerGroupRelation.deleteMany({
        where: { customerId },
      });

      if (!groupsString) return;

      const groupIds = groupsString.split(',').map((id) => parseInt(id.trim()));

      for (const groupId of groupIds) {
        const group = await this.prismaService.customerGroup.findFirst({
          where: { kiotVietId: groupId },
        });

        if (group) {
          await this.prismaService.customerGroupRelation.create({
            data: {
              customerId,
              customerGroupId: group.id,
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(`Failed to handle customer groups: ${error.message}`);
    }
  }

  private async handleRemovedCustomers(removedIds: number[]): Promise<void> {
    try {
      for (const removedId of removedIds) {
        await this.prismaService.customer.updateMany({
          where: { kiotVietId: BigInt(removedId) },
          data: { isActive: false },
        });
      }
      this.logger.log(`Marked ${removedIds.length} customers as inactive`);
    } catch (error) {
      this.logger.error(`Failed to handle removed customers: ${error.message}`);
    }
  }

  private async removeDuplicateCustomers(): Promise<number> {
    try {
      const duplicates = await this.prismaService.$queryRaw`
        SELECT kiot_viet_id, MIN(id) as keep_id
        FROM "Customer"
        GROUP BY kiot_viet_id
        HAVING COUNT(*) > 1
      `;

      let removedCount = 0;

      for (const duplicate of duplicates as any[]) {
        const duplicateCustomers = await this.prismaService.customer.findMany({
          where: { kiotVietId: duplicate.kiot_viet_id },
          orderBy: { id: 'asc' },
        });

        // Keep the first one, delete the rest
        for (let i = 1; i < duplicateCustomers.length; i++) {
          await this.prismaService.customer.delete({
            where: { id: duplicateCustomers[i].id },
          });
          removedCount++;
        }
      }

      if (removedCount > 0) {
        this.logger.log(`Removed ${removedCount} duplicate customers`);
      }

      return removedCount;
    } catch (error) {
      this.logger.error(
        `Failed to remove duplicate customers: ${error.message}`,
      );
      return 0;
    }
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    // Sync branches and customer groups are handled by the bus scheduler now
    // This method can focus on customer-specific logic

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
