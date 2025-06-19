// src/services/kiot-viet/customer.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);
  private readonly baseUrl: string;

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
   * Saves or updates a customer in the database
   */
  async saveCustomer(customerData: any) {
    try {
      // Check if customer already exists
      const existingCustomer = await this.prismaService.customer.findUnique({
        where: { kiotVietId: BigInt(customerData.id) },
      });

      // Prepare the customer data
      const customerToSave: Prisma.CustomerCreateInput = {
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

      if (existingCustomer) {
        // Update existing customer
        await this.prismaService.customer.update({
          where: { id: existingCustomer.id },
          data: customerToSave,
        });
        return { action: 'updated', id: existingCustomer.id };
      } else {
        // Create new customer
        const newCustomer = await this.prismaService.customer.create({
          data: customerToSave,
        });
        return { action: 'created', id: newCustomer.id };
      }
    } catch (error) {
      this.logger.error(
        `Failed to save customer ${customerData.id}: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Handles removed customers (based on removeIds from API)
   */
  async handleRemovedCustomers(removedIds: number[]) {
    if (!removedIds || removedIds.length === 0) return;

    try {
      // Mark customers as inactive instead of deleting them
      await this.prismaService.customer.updateMany({
        where: {
          kiotVietId: {
            in: removedIds.map((id) => BigInt(id)),
          },
        },
        data: {
          isActive: false,
          lastSyncedAt: new Date(),
        },
      });

      this.logger.log(`Marked ${removedIds.length} customers as inactive`);
    } catch (error) {
      this.logger.error(`Failed to handle removed customers: ${error.message}`);
      throw error;
    }
  }

  /**
   * Syncs recent customers (modified within the last X days)
   */
  async syncRecentCustomers(days: number = 7) {
    try {
      // Check if historical sync is running
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: {
          syncType: 'customer_historical',
          isRunning: true,
        },
      });

      if (historicalSync) {
        this.logger.log('Historical sync is running. Skipping recent sync.');
        return;
      }

      // Update sync control
      await this.prismaService.syncControl.upsert({
        where: {
          id: 1,
        },
        create: {
          id: 1,
          syncType: 'customer_current',
          isRunning: true,
          status: 'in_progress',
        },
        update: {
          isRunning: true,
          lastRunAt: new Date(),
          status: 'in_progress',
        },
      });

      const lastModifiedFrom = dayjs()
        .subtract(days, 'day')
        .format('YYYY-MM-DD');
      let currentItem = 0;
      const pageSize = 100;
      let hasMoreData = true;
      let totalProcessed = 0;

      while (hasMoreData) {
        const response = await this.fetchCustomers({
          lastModifiedFrom,
          currentItem,
          pageSize,
        });

        // Process customers
        if (response.data && response.data.length > 0) {
          for (const customer of response.data) {
            await this.saveCustomer(customer);
            totalProcessed++;
          }
        }

        // Handle removed customers
        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedCustomers(response.removedId);
        }

        // Check if we have more data to fetch
        if (!response.data || response.data.length < pageSize) {
          hasMoreData = false;
        } else {
          currentItem += pageSize;
        }
      }

      // Update sync control
      await this.prismaService.syncControl.update({
        where: { id: 1 },
        data: {
          isRunning: false,
          lastRunAt: new Date(),
          status: 'success',
          message: `Successfully synced ${totalProcessed} recent customers`,
        },
      });

      this.logger.log(`Successfully synced ${totalProcessed} recent customers`);
    } catch (error) {
      // Update sync control with error
      await this.prismaService.syncControl.update({
        where: { id: 1 },
        data: {
          isRunning: false,
          lastRunAt: new Date(),
          status: 'failed',
          message: `Error syncing recent customers: ${error.message}`,
        },
      });

      this.logger.error(`Failed to sync recent customers: ${error.message}`);
      throw error;
    }
  }

  /**
   * Syncs all historical customers from KiotViet
   */
  async syncHistoricalCustomers() {
    try {
      // Update sync control
      await this.prismaService.syncControl.upsert({
        where: {
          id: 2,
        },
        create: {
          id: 2,
          syncType: 'customer_historical',
          isRunning: true,
          status: 'in_progress',
        },
        update: {
          isRunning: true,
          lastRunAt: new Date(),
          status: 'in_progress',
          lastSyncedId: null, // Reset progress
        },
      });

      let currentItem = 0;
      const pageSize = 100;
      let hasMoreData = true;
      let totalProcessed = 0;
      let batchCount = 0;

      while (hasMoreData) {
        this.logger.log(
          `Fetching historical customers batch ${batchCount++} (items ${currentItem} to ${currentItem + pageSize - 1})`,
        );

        const response = await this.fetchCustomers({
          currentItem,
          pageSize,
        });

        // Process customers
        if (response.data && response.data.length > 0) {
          for (const customer of response.data) {
            await this.saveCustomer(customer);
            totalProcessed++;
          }

          // Update progress every batch
          await this.prismaService.syncControl.update({
            where: { id: 2 },
            data: {
              lastSyncedId: BigInt(response.data[response.data.length - 1].id),
              message: `Processing batch ${batchCount}. ${totalProcessed} customers synced so far.`,
            },
          });
        }

        // Handle removed customers
        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedCustomers(response.removedId);
        }

        // Check if we have more data to fetch
        if (!response.data || response.data.length < pageSize) {
          hasMoreData = false;
        } else {
          currentItem += pageSize;
        }
      }

      // Update sync control
      await this.prismaService.syncControl.update({
        where: { id: 2 },
        data: {
          isRunning: false,
          lastRunAt: new Date(),
          status: 'success',
          message: `Successfully synced ${totalProcessed} historical customers`,
        },
      });

      this.logger.log(
        `Successfully synced ${totalProcessed} historical customers`,
      );
    } catch (error) {
      // Update sync control with error
      await this.prismaService.syncControl.update({
        where: { id: 2 },
        data: {
          isRunning: false,
          lastRunAt: new Date(),
          status: 'failed',
          message: `Error syncing historical customers: ${error.message}`,
        },
      });

      this.logger.error(
        `Failed to sync historical customers: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Removes duplicate customers, keeping only one record
   */
  async removeDuplicateCustomers() {
    try {
      // Find all KiotViet IDs that have more than one entry
      const duplicates = await this.prismaService.$queryRaw`
        SELECT "kiotVietId", COUNT(*) as count 
        FROM "Customer" 
        GROUP BY "kiotVietId" 
        HAVING COUNT(*) > 1
      `;

      let totalRemoved = 0;

      for (const dup of duplicates as any[]) {
        // Get all records for this kiotVietId
        const records = await this.prismaService.customer.findMany({
          where: { kiotVietId: dup.kiotVietId },
          orderBy: { lastSyncedAt: 'desc' }, // Keep the most recently synced
        });

        // Keep the first one (most recent), delete the rest
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
      this.logger.error(
        `Failed to remove duplicate customers: ${error.message}`,
      );
      throw error;
    }
  }
}
