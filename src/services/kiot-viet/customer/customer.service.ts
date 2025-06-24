import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { LarkBaseService } from '../../lark/lark-base.service';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';
import { async } from 'rxjs';

interface CustomerToCreate {
  createData: Prisma.CustomerCreateInput;
  groups: string | null;
}

interface CustomerToUpdate {
  id: number;
  data: Prisma.CustomerUpdateInput;
  groups: string | null;
}

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

          // IMMEDIATE LarkBase sync - no more delayed batch sync
          await this.syncCustomersToLarkBaseImmediate(response.data);

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

      // Mark historical sync as COMPLETED and DISABLED
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

          await this.syncCustomersToLarkBaseImmediate(response.data);

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
    // Explicitly type the arrays
    const customersToCreate: CustomerToCreate[] = [];
    const customersToUpdate: CustomerToUpdate[] = [];

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
    customersToCreate: CustomerToCreate[],
    customersToUpdate: CustomerToUpdate[],
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
        type: customerData.type || null,
        isActive:
          customerData.isActive !== undefined ? customerData.isActive : true,
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

      // Handle branch relationship (remove branchId)
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
      type: customerData.type || null,
      isActive:
        customerData.isActive !== undefined ? customerData.isActive : true,
      psidFacebook: customerData.psidFacebook
        ? BigInt(customerData.psidFacebook)
        : null,
      modifiedDate: customerData.modifiedDate
        ? new Date(customerData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
      larkSyncStatus: 'PENDING',
    };

    // Handle branch relationship (remove branchId)
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
        SELECT kiotVietId, MIN(id) as keep_id
        FROM "Customer"
        GROUP BY kiotVietId
        HAVING COUNT(*) > 1
      `;

      let removedCount = 0;

      for (const duplicate of duplicates as any[]) {
        const duplicateCustomers = await this.prismaService.customer.findMany({
          where: { kiotVietId: duplicate.kiotVietId },
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

    // If historical sync is completed and disabled, run recent sync
    if (historicalSync.status === 'completed' && !historicalSync.isEnabled) {
      this.logger.log('Historical sync completed. Running recent sync...');
      await this.syncRecentCustomers();
      return;
    }

    // If historical sync is enabled but not running, start it
    if (historicalSync.isEnabled && !historicalSync.isRunning) {
      this.logger.log('Starting historical sync...');
      await this.syncHistoricalCustomers();
      return;
    }

    // If historical sync is running, skip
    if (historicalSync.isRunning) {
      this.logger.log('Historical sync is already running. Skipping...');
      return;
    }

    // Default to recent sync
    this.logger.log('Running recent sync...');
    await this.syncRecentCustomers();
  }

  // NEW: Immediate LarkBase sync method
  private async syncCustomersToLarkBaseImmediate(
    customers: any[],
  ): Promise<void> {
    if (!customers || customers.length === 0) return;

    try {
      // Direct sync to LarkBase immediately
      const result =
        await this.larkBaseService.directCreateCustomers(customers);

      this.logger.debug(
        `LarkBase immediate sync: ${result.success} success, ${result.failed} failed`,
      );
    } catch (error) {
      this.logger.error(`LarkBase immediate sync failed: ${error.message}`);

      return error;
    }
  }
}
