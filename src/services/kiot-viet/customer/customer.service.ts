import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500; // Process 500 customers at once for performance
  private readonly PAGE_SIZE = 100; // KiotViet API limit

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
  ) {
    this.baseUrl = this.configService.get<string>('KIOT_BASE_URL');
  }

  /**
   * Fetches customers from KiotViet with pagination
   */
  async fetchCustomers(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/customers`, {
          headers,
          params: {
            ...params,
            includeRemoveIds: true,
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch customers: ${error.message}`);
      throw error;
    }
  }

  /**
   * Batch save customers with duplicate prevention
   */
  async batchSaveCustomers(customers: any[]) {
    if (!customers || customers.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = customers.map((c) => BigInt(c.id));

    // Get existing customers to determine create vs update
    const existingCustomers = await this.prismaService.customer.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map(
      existingCustomers.map((c) => [c.kiotVietId.toString(), c.id]),
    );

    const customersToCreate = [];
    const customersToUpdate = [];

    for (const customerData of customers) {
      const kiotVietId = BigInt(customerData.id);
      const customerRecord = this.prepareCustomerData(customerData);

      if (existingMap.has(kiotVietId.toString())) {
        customersToUpdate.push({
          id: existingMap.get(kiotVietId.toString()),
          data: customerRecord,
        });
      } else {
        customersToCreate.push(customerRecord);
      }
    }

    let createdCount = 0;
    let updatedCount = 0;

    // Batch create new customers
    if (customersToCreate.length > 0) {
      await this.prismaService.customer.createMany({
        data: customersToCreate,
        skipDuplicates: true, // Extra safety
      });
      createdCount = customersToCreate.length;
    }

    // Batch update existing customers
    if (customersToUpdate.length > 0) {
      await this.prismaService.$transaction(
        customersToUpdate.map(({ id, data }) =>
          this.prismaService.customer.update({
            where: { id },
            data,
          }),
        ),
      );
      updatedCount = customersToUpdate.length;
    }

    return { created: createdCount, updated: updatedCount };
  }

  /**
   * Prepare customer data for database
   */
  private prepareCustomerData(customerData: any): Prisma.CustomerCreateInput {
    return {
      kiotVietId: BigInt(customerData.id),
      code: customerData.code,
      name: customerData.name,
      type: customerData.type,
      gender: customerData.gender,
      birthDate: customerData.birthDate
        ? new Date(customerData.birthDate)
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
      branchId: customerData.branchId,
      isActive: true,
      createdDate: customerData.createdDate
        ? new Date(customerData.createdDate)
        : new Date(),
      modifiedDate: customerData.modifiedDate
        ? new Date(customerData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };
  }

  /**
   * Handle removed customers
   */
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

  /**
   * Remove duplicate customers (keep latest lastSyncedAt)
   */
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

  /**
   * Sync recent customers (last X days)
   */
  async syncRecentCustomers(days: number = 7): Promise<void> {
    try {
      // Check if historical sync is running
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_historical', isRunning: true },
      });

      if (historicalSync) {
        this.logger.log('Historical sync is running. Skipping recent sync.');
        return;
      }

      // Update sync control
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

      // Remove duplicates
      const duplicatesRemoved = await this.removeDuplicateCustomers();

      // Update sync control
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

  /**
   * Sync all historical customers
   */
  async syncHistoricalCustomers(): Promise<void> {
    try {
      // Update sync control
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
          progress: null,
        },
      });

      let currentItem = 0;
      let totalProcessed = 0;
      let batchCount = 0;
      let hasMoreData = true;
      const customerBatch = [];

      this.logger.log('Starting historical customer sync...');

      while (hasMoreData) {
        const response = await this.fetchCustomers({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          customerBatch.push(...response.data);

          // Process in batches for performance
          if (
            customerBatch.length >= this.BATCH_SIZE ||
            response.data.length < this.PAGE_SIZE
          ) {
            const { created, updated } =
              await this.batchSaveCustomers(customerBatch);
            totalProcessed += created + updated;
            batchCount++;

            // Update progress
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
            customerBatch.length = 0; // Clear batch
          }
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedCustomers(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      // Remove duplicates
      this.logger.log('Removing duplicates...');
      const duplicatesRemoved = await this.removeDuplicateCustomers();

      // Complete historical sync and disable it
      await this.prismaService.syncControl.update({
        where: { name: 'customer_historical' },
        data: {
          isRunning: false,
          isEnabled: false, // Disable after completion
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

  /**
   * Check sync status and determine what to run on startup
   */
  async checkAndRunAppropriateSync(): Promise<void> {
    const historicalSync = await this.prismaService.syncControl.findFirst({
      where: { name: 'customer_historical' },
    });

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
    } else {
      this.logger.log('System restart detected: Running recent sync...');
      await this.syncRecentCustomers();
    }
  }
}
