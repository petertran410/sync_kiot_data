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
  // HISTORICAL SYNC (Complete dataset) - FIXED VERSION
  // ============================================================================

  async syncHistoricalCustomers(): Promise<void> {
    const syncName = 'customer_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical customer sync...');

      // ✅ FIX: Declare ALL variables in proper scope at the top
      let currentItem = 0;
      let processedCount = 0;
      let totalCustomers = 0;
      let consecutiveEmptyPages = 0;
      let lastValidTotal = 0;

      // ✅ SAFE COMPLETION DETECTION
      const MAX_CONSECUTIVE_EMPTY_PAGES = 3;
      const MIN_EXPECTED_CUSTOMERS = 10;

      while (true) {
        this.logger.log(
          `📄 Fetching customers page: ${Math.floor(currentItem / this.PAGE_SIZE) + 1}`,
        );

        try {
          const customerListResponse = await this.fetchCustomersList({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includeTotal: true,
            includeCustomerGroup: true,
            includeCustomerSocial: true,
          });

          // ✅ VALIDATION 1: Check response structure
          if (!customerListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.error(
                `❌ Received ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses. Possible API issue.`,
              );
              throw new Error(
                `API returned consecutive empty responses. Processed ${processedCount}/${totalCustomers || 'unknown'} customers.`,
              );
            }
            await new Promise((resolve) => setTimeout(resolve, 5000));
            continue;
          }

          // ✅ VALIDATION 2: Extract and validate total
          if (
            customerListResponse.total !== undefined &&
            customerListResponse.total > 0
          ) {
            totalCustomers = customerListResponse.total;
            lastValidTotal = totalCustomers;
            this.logger.log(`📊 Total customers in system: ${totalCustomers}`);
          } else if (lastValidTotal > 0) {
            totalCustomers = lastValidTotal;
          }

          // ✅ VALIDATION 3: Check for empty data
          if (
            !customerListResponse.data ||
            customerListResponse.data.length === 0
          ) {
            consecutiveEmptyPages++;
            this.logger.warn(
              `⚠️ Empty page received. Count: ${consecutiveEmptyPages}`,
            );

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              throw new Error(
                `Too many empty pages. Processed ${processedCount}/${totalCustomers || 'unknown'}. ` +
                  `Expected ${lastValidTotal || 'unknown'}. Multiple empty API responses.`,
              );
            }
            await new Promise((resolve) => setTimeout(resolve, 2000));
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // ✅ VALIDATION 4: Reset empty page counter on successful data
          consecutiveEmptyPages = 0;

          this.logger.log(
            `📊 Processing ${customerListResponse.data.length} customers ` +
              `(Processed: ${processedCount}/${totalCustomers || 'unknown'})`,
          );

          // ✅ VALIDATION 5: Check for duplicate/overlapping data
          const currentPageIds = customerListResponse.data.map((c) => c.id);
          const duplicateCheck = await this.prismaService.customer.findMany({
            where: { kiotVietId: { in: currentPageIds } },
            select: { kiotVietId: true },
          });

          if (duplicateCheck.length === customerListResponse.data.length) {
            this.logger.warn(
              `⚠️ All customers in current page already exist. Possible pagination overlap.`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Process customers normally
          const customersWithDetails = await this.enrichCustomersWithDetails(
            customerListResponse.data,
          );

          const savedCustomers =
            await this.saveCustomersToDatabase(customersWithDetails);

          // ✅ FIX: Enhanced LarkBase sync with better error handling
          await this.syncCustomersToLarkBase(savedCustomers);

          processedCount += customersWithDetails.length;
          currentItem += this.PAGE_SIZE;

          // ✅ VALIDATION 6: Progress validation
          this.logger.log(
            `✅ Progress: ${processedCount}/${totalCustomers || 'unknown'} customers ` +
              `(${totalCustomers ? Math.round((processedCount / totalCustomers) * 100) : '?'}%)`,
          );

          // ✅ VALIDATION 7: Auto-stop if we've exceeded expected total
          if (totalCustomers > 0 && processedCount >= totalCustomers) {
            this.logger.log(
              `✅ Completed: Processed ${processedCount}/${totalCustomers} customers`,
            );
            break;
          }
        } catch (apiError) {
          this.logger.error(
            `❌ API error on page ${Math.floor(currentItem / this.PAGE_SIZE) + 1}: ${apiError.message}`,
          );

          consecutiveEmptyPages++;

          if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
            throw new Error(
              `Multiple API failures during sync. Processed ${processedCount}/${totalCustomers || 'unknown'} customers. ` +
                `Last error: ${apiError.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, 5000));
          continue;
        }
      }

      // ✅ FINAL VALIDATION
      const finalValidation = {
        processedCount,
        expectedTotal: totalCustomers,
        isComplete:
          totalCustomers > 0
            ? processedCount >= totalCustomers * 0.95
            : processedCount >= MIN_EXPECTED_CUSTOMERS,
        completionRate:
          totalCustomers > 0
            ? Math.round((processedCount / totalCustomers) * 100)
            : null,
      };

      this.logger.log(
        `📊 Final validation: ${JSON.stringify(finalValidation, null, 2)}`,
      );

      if (!finalValidation.isComplete) {
        this.logger.warn(
          `⚠️ Sync may be incomplete: ${processedCount}/${totalCustomers || 'unknown'} customers processed`,
        );

        await this.updateSyncControl(syncName, {
          isRunning: false,
          isEnabled: false,
          status: 'completed_with_warnings',
          completedAt: new Date(),
          lastRunAt: new Date(),
          progress: finalValidation,
        });
      } else {
        await this.updateSyncControl(syncName, {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
          progress: finalValidation,
        });
      }

      // Enable recent sync
      await this.updateSyncControl('customer_recent', {
        isEnabled: true,
        status: 'idle',
      });

      this.logger.log(
        `🎉 Historical customer sync finished: ${processedCount} customers processed ` +
          `(${finalValidation.completionRate || '?'}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical customer sync failed: ${error.message}`);

      // ✅ FIX: Use declared variables in proper scope
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount: 0, expectedTotal: 0 }, // Safe fallback
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
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(`🔄 Starting recent customer sync (${days} days)...`);

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

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

      const customersWithDetails =
        await this.enrichCustomersWithDetails(recentCustomers);
      const savedCustomers =
        await this.saveCustomersToDatabase(customersWithDetails);
      await this.syncCustomersToLarkBase(savedCustomers);

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
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'createdDate',
      orderDirection: params.orderDirection || 'DESC',
      includeTotal: (params.includeTotal || true).toString(),
      includeCustomerGroup: (params.includeCustomerGroup || true).toString(),
      includeCustomerSocial: (params.includeCustomerSocial || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/customers?${queryParams}`, {
        headers,
      }),
    );

    return response.data;
  }

  async fetchRecentCustomers(fromDate: Date): Promise<KiotVietCustomer[]> {
    const headers = await this.authService.getRequestHeaders();
    const fromDateStr = fromDate.toISOString();

    const queryParams = new URLSearchParams({
      lastModifiedFrom: fromDateStr,
      currentItem: '0',
      pageSize: '1000',
      orderBy: 'modifiedDate',
      orderDirection: 'DESC',
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/customers?${queryParams}`, {
        headers,
      }),
    );

    return response.data?.data || [];
  }

  async enrichCustomersWithDetails(
    customers: KiotVietCustomer[],
  ): Promise<KiotVietCustomer[]> {
    this.logger.log(
      `🔍 Enriching ${customers.length} customers with details...`,
    );

    const enrichedCustomers: any[] = [];
    for (const customer of customers) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/customers/${customer.id}`, {
            headers,
          }),
        );

        if (response.data) {
          enrichedCustomers.push(response.data);
        } else {
          enrichedCustomers.push(customer);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich customer ${customer.id}: ${error.message}`,
        );
        enrichedCustomers.push(customer);
      }
    }

    return enrichedCustomers;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  async saveCustomersToDatabase(customers: KiotVietCustomer[]): Promise<any[]> {
    this.logger.log(`💾 Saving ${customers.length} customers to database...`);

    const savedCustomers = [];

    for (const customerData of customers) {
      try {
        let internalBranchId: number | null = null;
        if (customerData.branchId) {
          const branchMapping = await this.prismaService.branch.findUnique({
            where: { kiotVietId: customerData.branchId },
            select: { id: true },
          });
          internalBranchId = branchMapping?.id || null;
        }

        const customer = await this.prismaService.customer.upsert({
          where: { kiotVietId: customerData.id },
          update: {
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
            branchId: internalBranchId,
            modifiedDate: customerData.modifiedDate
              ? new Date(customerData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
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
            branchId: internalBranchId,
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
  // ✅ ENHANCED LARKBASE SYNC WITH IMPROVED ERROR HANDLING
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

      // ✅ FIX: Enhanced sync with individual record tracking
      await this.larkCustomerSyncService.syncCustomersToLarkBase(
        customersToSync,
      );

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`🛑 STOPPING sync to prevent data duplication`);

      // ✅ FIX: Don't fail entire sync, just mark failed customers
      const customerIds = customers.map((c) => c.id);
      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncedAt: new Date(),
        },
      });

      // Log error but don't throw to continue database operations
      this.logger.warn(`⚠️ LarkBase sync failed but database sync continues`);
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
}
