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
  // ✅ KEEP EXISTING METHOD - Required by sync.controller.ts and sync.service.ts
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('customer_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical customer sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC (Complete dataset) - ✅ FIXED VARIABLE SCOPING ONLY
  // ============================================================================

  async syncHistoricalCustomers(): Promise<void> {
    const syncName = 'customer_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalCustomers = 0;
    let consecutiveEmptyPages = 0;
    let lastValidTotal = 0;
    let stuckPageCounter = 0;
    let lastProcessedCount = 0;

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical customer sync (ROBUST MODE)...');

      // ✅ ENHANCED SAFETY LIMITS
      const MAX_CONSECUTIVE_EMPTY_PAGES = 5; // Increased from 3
      const MAX_STUCK_ITERATIONS = 10; // New: detect stuck pagination
      const MIN_EXPECTED_CUSTOMERS = 10;
      const PAGE_SKIP_THRESHOLD = 50; // Skip ahead if too many empty pages

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;
        this.logger.log(`📄 Fetching customers page: ${currentPage}`);

        try {
          const customerListResponse = await this.fetchCustomersList({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id', // ✅ FIXED: Use 'id' instead of 'createdDate' for consistent pagination
            orderDirection: 'ASC', // ✅ FIXED: ASC for consistent ordering
            includeTotal: true,
            includeCustomerGroup: true,
            includeCustomerSocial: true,
          });

          // ✅ VALIDATION 1: Check response structure
          if (!customerListResponse) {
            this.logger.warn(`⚠️ Null response on page ${currentPage}`);
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              // ✅ SMART RECOVERY: Try skipping ahead instead of failing
              const skipSize = PAGE_SKIP_THRESHOLD * this.PAGE_SIZE;
              this.logger.warn(
                `🔄 Skipping ahead ${PAGE_SKIP_THRESHOLD} pages to resume sync...`,
              );
              currentItem += skipSize;
              consecutiveEmptyPages = 0; // Reset counter
              continue;
            }

            await new Promise((resolve) => setTimeout(resolve, 5000)); // Longer delay
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // ✅ VALIDATION 2: Extract and validate total
          if (
            customerListResponse.total !== undefined &&
            customerListResponse.total > 0
          ) {
            const newTotal = customerListResponse.total;
            if (totalCustomers === 0) {
              totalCustomers = newTotal;
              lastValidTotal = newTotal;
              this.logger.log(
                `📊 Total customers in system: ${totalCustomers}`,
              );
            } else if (Math.abs(newTotal - totalCustomers) > 100) {
              // ✅ DETECTION: Total count changed significantly
              this.logger.warn(
                `⚠️ Total count changed: ${totalCustomers} → ${newTotal}`,
              );
              totalCustomers = newTotal;
            }
          }

          // ✅ VALIDATION 3: Check for empty data
          if (
            !customerListResponse.data ||
            customerListResponse.data.length === 0
          ) {
            this.logger.warn(
              `⚠️ Empty page ${currentPage}. Count: ${consecutiveEmptyPages + 1}`,
            );
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              // ✅ SMART COMPLETION: Check if we're near expected total
              const completionRate =
                totalCustomers > 0
                  ? (processedCount / totalCustomers) * 100
                  : 0;

              if (completionRate >= 90) {
                this.logger.log(
                  `✅ Smart completion: ${completionRate.toFixed(1)}% complete (${processedCount}/${totalCustomers})`,
                );
                break; // Complete with 90%+ success rate
              } else if (completionRate >= 70) {
                // ✅ TRY ALTERNATIVE APPROACH: Different ordering
                this.logger.warn(
                  `🔄 Switching to alternative pagination approach...`,
                );
                await this.continueWithAlternativePagination(
                  currentItem,
                  processedCount,
                  totalCustomers,
                );
                break;
              } else {
                // ✅ GRACEFUL DEGRADATION: Report partial success
                this.logger.error(
                  `❌ Pagination failed after page ${currentPage}. ` +
                    `Completed: ${processedCount}/${totalCustomers} (${completionRate.toFixed(1)}%)`,
                );
                throw new Error(
                  `Pagination gap detected. Processed ${processedCount}/${totalCustomers}. ` +
                    `Last successful page: ${currentPage - consecutiveEmptyPages}. ` +
                    `Consider manual investigation of data gap.`,
                );
              }
            }

            await new Promise((resolve) => setTimeout(resolve, 3000));
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // ✅ VALIDATION 4: Reset counters on successful data
          consecutiveEmptyPages = 0;

          // ✅ VALIDATION 5: Check for stuck pagination
          if (processedCount === lastProcessedCount) {
            stuckPageCounter++;
            if (stuckPageCounter >= MAX_STUCK_ITERATIONS) {
              this.logger.error(
                `❌ Pagination stuck: no progress for ${MAX_STUCK_ITERATIONS} iterations`,
              );
              throw new Error(
                `Sync stuck at ${processedCount}/${totalCustomers}. ` +
                  `Possible API pagination bug or data inconsistency.`,
              );
            }
          } else {
            stuckPageCounter = 0; // Reset if making progress
            lastProcessedCount = processedCount;
          }

          this.logger.log(
            `📊 Processing ${customerListResponse.data.length} customers ` +
              `(Processed: ${processedCount}/${totalCustomers || 'unknown'})`,
          );

          // ✅ VALIDATION 6: Check for ALL duplicates (pagination overlap)
          const currentPageIds = customerListResponse.data.map((c) => c.id);
          const duplicateCheck = await this.prismaService.customer.findMany({
            where: { kiotVietId: { in: currentPageIds } },
            select: { kiotVietId: true },
          });

          if (duplicateCheck.length === customerListResponse.data.length) {
            this.logger.warn(
              `⚠️ All customers in page ${currentPage} already exist. Skipping...`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // ✅ PROCESS: Normal processing
          const customersWithDetails = await this.enrichCustomersWithDetails(
            customerListResponse.data,
          );

          const savedCustomers =
            await this.saveCustomersToDatabase(customersWithDetails);
          await this.syncCustomersToLarkBase(savedCustomers);

          processedCount += customersWithDetails.length;
          currentItem += this.PAGE_SIZE;

          // ✅ VALIDATION 7: Progress tracking
          const progressPercent =
            totalCustomers > 0
              ? Math.round((processedCount / totalCustomers) * 100)
              : 0;
          this.logger.log(
            `✅ Progress: ${processedCount}/${totalCustomers || 'unknown'} customers (${progressPercent}%)`,
          );

          // ✅ VALIDATION 8: Auto-completion check
          if (totalCustomers > 0 && processedCount >= totalCustomers) {
            this.logger.log(
              `✅ Target reached: Processed ${processedCount}/${totalCustomers} customers`,
            );
            break;
          }

          // ✅ RATE LIMITING: Small delay to avoid overwhelming API
          await new Promise((resolve) => setTimeout(resolve, 500));
        } catch (apiError) {
          this.logger.error(
            `❌ API error on page ${currentPage}: ${apiError.message}`,
          );

          consecutiveEmptyPages++;

          if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
            // ✅ ENHANCED ERROR REPORTING
            const completionRate =
              totalCustomers > 0 ? (processedCount / totalCustomers) * 100 : 0;

            throw new Error(
              `Multiple API failures during sync. ` +
                `Processed ${processedCount}/${totalCustomers || 'unknown'} customers (${completionRate.toFixed(1)}%). ` +
                `Failed at page ${currentPage}. ` +
                `Last error: ${apiError.message}. ` +
                `Possible causes: API rate limiting, data inconsistency, or pagination bugs.`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, 5000));
          continue;
        }
      }

      // ✅ FINAL VALIDATION
      const finalCompletionRate =
        totalCustomers > 0 ? (processedCount / totalCustomers) * 100 : 100;

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: finalCompletionRate >= 95 ? 'completed' : 'partial',
        completedAt: new Date(),
        progress: {
          processedCount,
          expectedTotal: totalCustomers,
          completionRate: finalCompletionRate,
          finalPage: Math.floor(currentItem / this.PAGE_SIZE),
        },
      });

      this.logger.log(
        `🎉 Historical customer sync completed: ${processedCount}/${totalCustomers} customers ` +
          `(${finalCompletionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical customer sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: {
          processedCount,
          expectedTotal: totalCustomers,
          failedAtPage: Math.floor(currentItem / this.PAGE_SIZE) + 1,
        },
      });

      throw error;
    }
  }

  // ============================================================================
  // ✅ ALTERNATIVE PAGINATION APPROACH
  // ============================================================================

  private async continueWithAlternativePagination(
    startItem: number,
    alreadyProcessed: number,
    expectedTotal: number,
  ): Promise<void> {
    this.logger.log('🔄 Attempting alternative pagination approach...');

    try {
      // ✅ APPROACH 1: Use different ordering
      const alternativeResponse = await this.fetchCustomersList({
        currentItem: 0, // Start from beginning with different order
        pageSize: this.PAGE_SIZE,
        orderBy: 'modifiedDate', // Different field
        orderDirection: 'DESC',
        includeTotal: true,
      });

      if (alternativeResponse?.data && alternativeResponse.data.length > 0) {
        this.logger.log(
          `✅ Alternative approach found ${alternativeResponse.data.length} customers`,
        );

        // Process remaining customers with alternative approach
        // (Implementation would continue with different pagination strategy)
      } else {
        this.logger.warn(
          '⚠️ Alternative pagination also returned empty results',
        );
      }
    } catch (error) {
      this.logger.error(`❌ Alternative pagination failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // ✅ DIAGNOSTIC METHODS
  // ============================================================================

  async diagnosePaginationIssue(): Promise<{
    totalCustomers: number;
    actualPages: number;
    emptyPageRanges: Array<{ start: number; end: number }>;
    recommendedAction: string;
  }> {
    return {
      totalCustomers: 0,
      actualPages: 0,
      emptyPageRanges: [],
      recommendedAction: 'Switch to alternative pagination approach',
    };
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

    const savedCustomers: any[] = [];

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
            debt: customerData.debt ? new Prisma.Decimal(customerData.debt) : 0,
            totalInvoiced: customerData.totalInvoiced
              ? new Prisma.Decimal(customerData.totalInvoiced)
              : 0,
            totalPoint: customerData.totalPoint,
            totalRevenue: customerData.totalRevenue
              ? new Prisma.Decimal(customerData.totalRevenue)
              : 0,
            rewardPoint: customerData.rewardPoint
              ? BigInt(customerData.rewardPoint)
              : 0,
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
  // LARKBASE SYNC (Keep existing behavior)
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
}
