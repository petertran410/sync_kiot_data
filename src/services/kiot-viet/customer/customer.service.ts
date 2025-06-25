// src/services/kiot-viet/customer/customer.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import { LarkCustomerSyncService } from '../../lark/customer/lark-customer-sync.service';

interface KiotVietCustomer {
  id: number;
  code: string;
  name: string;
  type?: number;
  gender?: boolean;
  birthDate?: string;
  contactNumber?: string;
  address?: string;
  locationName?: string;
  wardName?: string;
  email?: string;
  organization?: string;
  comments?: string;
  taxCode?: string;
  debt?: number;
  totalInvoiced?: number;
  totalPoint?: number;
  totalRevenue?: number;
  rewardPoint?: number;
  psidFacebook?: number;
  retailerId?: number;
  branchId?: number;
  modifiedDate?: string;
  createdDate?: string;
  groups?: string;
}

@Injectable()
export class KiotVietCustomerService {
  private readonly logger = new Logger(KiotVietCustomerService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  // ============================================================================
  // SYNC CONTROL & TRACKING SYSTEM
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      // Check tracking system
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_historical' },
      });

      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_recent' },
      });

      // Priority: Historical sync first
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical customer sync...');
        await this.syncHistoricalCustomers();
        return;
      }

      // Then recent sync
      if (recentSync?.isEnabled && !recentSync.isRunning) {
        this.logger.log('Starting recent customer sync...');
        await this.syncRecentCustomers(4);
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent customer sync...');
      await this.syncRecentCustomers(4);
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // HISTORICAL SYNC (Complete dataset)
  // ============================================================================

  async syncHistoricalCustomers(): Promise<void> {
    const syncName = 'customer_historical';

    try {
      // Update tracking: start sync
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical customer sync...');

      let currentItem = 0;
      let processedCount = 0;
      let totalCustomers = 0;

      while (true) {
        this.logger.log(
          `📄 Fetching customers page: ${Math.floor(currentItem / this.PAGE_SIZE) + 1}`,
        );

        // Step 1: Get basic customer list (Hybrid approach)
        const customerListResponse = await this.fetchCustomersList({
          currentItem,
          pageSize: this.PAGE_SIZE,
          orderBy: 'createdDate',
          orderDirection: 'DESC',
          includeTotal: true,
          includeCustomerGroup: true,
          includeCustomerSocial: true,
        });

        if (
          !customerListResponse.data ||
          customerListResponse.data.length === 0
        ) {
          this.logger.log('📋 No more customers to process');
          break;
        }

        totalCustomers = customerListResponse.total || totalCustomers;
        this.logger.log(
          `📊 Processing ${customerListResponse.data.length} customers (Total: ${totalCustomers})`,
        );

        // Step 2: Get full details for each customer (Hybrid approach)
        const customersWithDetails = await this.enrichCustomersWithDetails(
          customerListResponse.data,
        );

        // Step 3: Save to database
        const savedCustomers =
          await this.saveCustomersToDatabase(customersWithDetails);

        // Step 4: Sync to LarkBase
        await this.syncCustomersToLarkBase(savedCustomers);

        processedCount += customersWithDetails.length;
        currentItem += this.PAGE_SIZE;

        this.logger.log(
          `✅ Processed ${processedCount}/${totalCustomers} customers`,
        );
      }

      // Update tracking: complete
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Disable after completion
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      // Enable recent sync
      await this.updateSyncControl('customer_recent', {
        isEnabled: true,
        status: 'idle',
      });

      this.logger.log(
        `🎉 Historical customer sync completed: ${processedCount} customers processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical customer sync failed: ${error.message}`);

      // Update tracking: failed
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
      });

      throw error;
    }
  }

  // ============================================================================
  // RECENT SYNC (Incremental updates)
  // ============================================================================

  async syncRecentCustomers(days: number = 4): Promise<void> {
    const syncName = 'customer_recent';

    try {
      // Update tracking: start sync
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(`🔄 Starting recent customer sync (${days} days)...`);

      // Calculate date range
      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      // Get recent customers
      const recentCustomers = await this.fetchRecentCustomers(fromDate);

      if (recentCustomers.length === 0) {
        this.logger.log('📋 No recent customer updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(
        `📊 Processing ${recentCustomers.length} recent customers`,
      );

      // Get full details for recent customers
      const customersWithDetails =
        await this.enrichCustomersWithDetails(recentCustomers);

      // Save to database
      const savedCustomers =
        await this.saveCustomersToDatabase(customersWithDetails);

      // Sync to LarkBase
      await this.syncCustomersToLarkBase(savedCustomers);

      // Update tracking: complete
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log(
        `✅ Recent customer sync completed: ${customersWithDetails.length} customers processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Recent customer sync failed: ${error.message}`);

      // Update tracking: failed
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

  async fetchCustomersList(params: {
    currentItem?: number;
    pageSize?: number;
    orderBy?: string;
    orderDirection?: string;
    includeTotal?: boolean;
    includeCustomerGroup?: boolean;
    includeCustomerSocial?: boolean;
    lastModifiedFrom?: string;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();

      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/customers`, {
          headers,
          params: {
            currentItem: params.currentItem || 0,
            pageSize: params.pageSize || this.PAGE_SIZE,
            orderBy: params.orderBy || 'createdDate',
            orderDirection: params.orderDirection || 'DESC',
            includeTotal: params.includeTotal || true,
            includeCustomerGroup: params.includeCustomerGroup || true,
            includeCustomerSocial: params.includeCustomerSocial || true,
            ...(params.lastModifiedFrom && {
              lastModifiedFrom: params.lastModifiedFrom,
            }),
          },
        }),
      );

      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch customers list: ${error.message}`);
      throw error;
    }
  }

  async fetchCustomerDetails(customerId: number): Promise<KiotVietCustomer> {
    try {
      const headers = await this.authService.getRequestHeaders();

      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/customers/${customerId}`, {
          headers,
        }),
      );

      return data;
    } catch (error) {
      this.logger.error(
        `Failed to fetch customer details for ID ${customerId}: ${error.message}`,
      );
      throw error;
    }
  }

  async fetchRecentCustomers(fromDate: Date): Promise<KiotVietCustomer[]> {
    try {
      const response = await this.fetchCustomersList({
        lastModifiedFrom: fromDate.toISOString(),
        pageSize: 100,
        orderBy: 'modifiedDate',
        orderDirection: 'DESC',
      });

      return response.data || [];
    } catch (error) {
      this.logger.error(`Failed to fetch recent customers: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // HYBRID APPROACH: Enrich customers with missing details
  // ============================================================================

  async enrichCustomersWithDetails(
    customers: KiotVietCustomer[],
  ): Promise<KiotVietCustomer[]> {
    const enrichedCustomers: KiotVietCustomer[] = [];

    for (const customer of customers) {
      try {
        // Check if critical fields are missing
        const needsDetails =
          !customer.contactNumber || !customer.email || !customer.gender;

        if (needsDetails) {
          this.logger.debug(
            `🔍 Getting full details for customer: ${customer.code}`,
          );
          const fullDetails = await this.fetchCustomerDetails(customer.id);
          enrichedCustomers.push({ ...customer, ...fullDetails });
        } else {
          enrichedCustomers.push(customer);
        }

        // Add small delay to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich customer ${customer.code}: ${error.message}`,
        );
        // Use basic data if details fetch fails
        enrichedCustomers.push(customer);
      }
    }

    return enrichedCustomers;
  }

  // ============================================================================
  // DATABASE OPERATIONS (Duplicate Prevention)
  // ============================================================================

  async saveCustomersToDatabase(customers: KiotVietCustomer[]): Promise<any[]> {
    const savedCustomers: any[] = [];

    // ⭐ OPTIMIZATION: Pre-load all branch mappings
    const branchMappings = new Map<number, number>(); // KiotViet ID → Internal ID

    const allBranches = await this.prismaService.branch.findMany({
      select: { id: true, kiotVietId: true },
    });

    for (const branch of allBranches) {
      branchMappings.set(branch.kiotVietId, branch.id);
    }

    this.logger.debug(`📋 Loaded ${branchMappings.size} branch mappings`);

    for (const customerData of customers) {
      try {
        // ⭐ FAST LOOKUP: Use cached mapping
        let internalBranchId: any = null;

        if (
          customerData.branchId &&
          branchMappings.has(customerData.branchId)
        ) {
          internalBranchId = branchMappings.get(customerData.branchId);
          this.logger.debug(
            `✅ Mapped branchId: KiotViet ${customerData.branchId} → DB ${internalBranchId}`,
          );
        } else if (customerData.branchId) {
          this.logger.warn(
            `⚠️ Branch ${customerData.branchId} not found for customer ${customerData.code}`,
          );
        }

        const customer = await this.prismaService.customer.upsert({
          where: { kiotVietId: customerData.id },
          create: {
            kiotVietId: customerData.id,
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
            comments: customerData.comments,
            taxCode: customerData.taxCode,
            debt: customerData.debt
              ? new Prisma.Decimal(customerData.debt)
              : null,
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
            branchId: internalBranchId, // ⭐ Use mapped internal ID
            createdDate: customerData.createdDate
              ? new Date(customerData.createdDate)
              : new Date(),
            modifiedDate: customerData.modifiedDate
              ? new Date(customerData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
          update: {
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
            comments: customerData.comments,
            taxCode: customerData.taxCode,
            debt: customerData.debt
              ? new Prisma.Decimal(customerData.debt)
              : null,
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
            branchId: internalBranchId, // ⭐ Use mapped internal ID
            modifiedDate: customerData.modifiedDate
              ? new Date(customerData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
        });

        savedCustomers.push(customer);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save customer ${customerData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${savedCustomers.length} customers to database`);
    return savedCustomers;
  }

  // ============================================================================
  // LARKBASE SYNC (Error handling with STOP on failure)
  // ============================================================================

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers...`,
      );

      // Filter customers that need LarkBase sync
      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      );

      if (customersToSync.length === 0) {
        this.logger.log('📋 No customers need LarkBase sync');
        return;
      }

      // Sync to LarkBase
      await this.larkCustomerSyncService.syncCustomersToLarkBase(
        customersToSync,
      );

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`🛑 STOPPING sync to prevent data duplication`);

      // Update all failed customers
      const customerIds = customers.map((c) => c.id);
      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncedAt: new Date(),
        },
      });

      // Do NOT throw error to continue database sync, but stop LarkBase sync
      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateSyncControl(name: string, updates: any) {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['customer'],
        syncMode: name.includes('historical') ? 'historical' : 'recent',
        ...updates,
      },
      update: updates,
    });
  }

  async getSyncStatus(): Promise<any> {
    const statuses = await this.prismaService.syncControl.findMany({
      where: {
        name: { in: ['customer_historical', 'customer_recent'] },
      },
    });

    return {
      historical: statuses.find((s) => s.name === 'customer_historical'),
      recent: statuses.find((s) => s.name === 'customer_recent'),
      timestamp: new Date(),
    };
  }

  // Manual triggers
  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('customer_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('🔧 Historical sync enabled');
  }

  async enableRecentSync(): Promise<void> {
    await this.updateSyncControl('customer_recent', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('🔧 Recent sync enabled');
  }
}
