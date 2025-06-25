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
      let consecutiveEmptyPages = 0;
      let lastValidTotal = 0;

      // ✅ SAFE COMPLETION DETECTION
      const MAX_CONSECUTIVE_EMPTY_PAGES = 3; // Allow 3 empty pages before stopping
      const MIN_EXPECTED_CUSTOMERS = 10; // Minimum customers expected in system

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
                `API returned consecutive empty responses. Sync may be incomplete.`,
              );
            }

            // Wait and retry
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
          }

          // ✅ VALIDATION 2: Update total customer count if valid
          if (customerListResponse.total && customerListResponse.total > 0) {
            lastValidTotal = customerListResponse.total;
            totalCustomers = lastValidTotal;
          }

          // ✅ VALIDATION 3: Check for empty data
          if (
            !customerListResponse.data ||
            customerListResponse.data.length === 0
          ) {
            consecutiveEmptyPages++;

            this.logger.warn(
              `⚠️ Empty page ${Math.floor(currentItem / this.PAGE_SIZE) + 1}. ` +
                `Consecutive empty pages: ${consecutiveEmptyPages}/${MAX_CONSECUTIVE_EMPTY_PAGES}`,
            );

            // ✅ SAFE STOPPING CONDITIONS
            const conditions = {
              hasValidTotal: lastValidTotal > 0,
              reachedOrExceededTotal: processedCount >= lastValidTotal,
              hasMinimumData: processedCount >= MIN_EXPECTED_CUSTOMERS,
              maxEmptyPages:
                consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES,
            };

            this.logger.log(
              `📊 Completion check: ${JSON.stringify(conditions, null, 2)}`,
            );

            // Stop if we have valid total and reached it
            if (conditions.hasValidTotal && conditions.reachedOrExceededTotal) {
              this.logger.log(
                `✅ Reached expected total: ${processedCount}/${lastValidTotal} customers processed`,
              );
              break;
            }

            // Stop if we hit max empty pages and have reasonable data
            if (conditions.maxEmptyPages && conditions.hasMinimumData) {
              if (conditions.hasValidTotal) {
                this.logger.warn(
                  `⚠️ Stopping with potential incomplete data: ${processedCount}/${lastValidTotal} customers processed`,
                );
              } else {
                this.logger.log(
                  `✅ Completed sync: ${processedCount} customers processed (no total available)`,
                );
              }
              break;
            }

            // Continue if we haven't met stopping conditions
            if (consecutiveEmptyPages < MAX_CONSECUTIVE_EMPTY_PAGES) {
              currentItem += this.PAGE_SIZE;
              await new Promise((resolve) => setTimeout(resolve, 1000));
              continue;
            }

            // Fatal: too many empty pages without reasonable data
            throw new Error(
              `Sync incomplete: Only ${processedCount} customers processed. ` +
                `Expected ${lastValidTotal || 'unknown'}. Multiple empty API responses.`,
            );
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

            // Skip this page but continue (might be pagination issue)
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Process customers normally
          const customersWithDetails = await this.enrichCustomersWithDetails(
            customerListResponse.data,
          );

          const savedCustomers =
            await this.saveCustomersToDatabase(customersWithDetails);
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

          // Wait before retry
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

        // Don't throw error, but mark as potentially incomplete
        await this.updateSyncControl(syncName, {
          isRunning: false,
          isEnabled: false,
          status: 'completed_with_warnings',
          completedAt: new Date(),
          lastRunAt: new Date(),
          progress: finalValidation,
        });
      } else {
        // Mark as successfully completed
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

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalCustomers },
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
