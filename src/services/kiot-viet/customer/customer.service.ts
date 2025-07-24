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
        await this.syncRecentCustomers(3);
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent customer sync...');
      await this.syncRecentCustomers(3);
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('customer_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical customer sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC with Robust Error Handling
  // ============================================================================

  async syncHistoricalCustomers(): Promise<void> {
    const syncName = 'customer_historical';

    // Declare all variables at function scope
    let currentItem = 0;
    let processedCount = 0;
    let totalCustomers = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedCustomerIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical customer sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalCustomers > 0) {
          if (currentItem >= totalCustomers) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalCustomers} customers`,
            );
            break;
          }

          // Progress logging
          const progressPercentage = (currentItem / totalCustomers) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalCustomers} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const customerListResponse = await this.fetchCustomersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includeTotal: true,
            includeCustomerGroup: true,
            includeCustomerSocial: true,
          });

          // VALIDATION: Check response structure
          if (!customerListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          // Reset error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // Extract data
          const { total, data: customers } = customerListResponse;

          if (total !== undefined && total !== null) {
            if (totalCustomers === 0) {
              totalCustomers = total;
              this.logger.log(`📊 Total customers detected: ${totalCustomers}`);
            } else if (total !== totalCustomers) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalCustomers} -> ${total}. Using latest.`,
              );
              totalCustomers = total;
            }
            lastValidTotal = total;
          }

          // Handle empty data array
          if (!customers || customers.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            // If we know the total and we're past it, stop
            if (totalCustomers > 0 && currentItem >= totalCustomers) {
              this.logger.log('✅ Reached end of data (empty page past total)');
              break;
            }

            // If too many consecutive empty pages, stop
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            // Move to next page
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // 🆕 DUPLICATE DETECTION
          const newCustomers = customers.filter((customer) => {
            if (processedCustomerIds.has(customer.id)) {
              this.logger.debug(
                `⚠️ Duplicate customer ID detected: ${customer.id} (${customer.code})`,
              );
              return false;
            }
            processedCustomerIds.add(customer.id);
            return true;
          });

          if (newCustomers.length !== customers.length) {
            this.logger.warn(
              `🔄 Filtered out ${customers.length - newCustomers.length} duplicate customers on page ${currentPage}`,
            );
          }

          // Skip if no new customers after deduplication
          if (newCustomers.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all customers already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Process customers
          this.logger.log(
            `🔄 Processing ${newCustomers.length} customers from page ${currentPage}...`,
          );

          const customersWithDetails =
            await this.enrichCustomersWithDetails(newCustomers);
          const savedCustomers =
            await this.saveCustomersToDatabase(customersWithDetails);

          // Track progress
          processedCount += savedCustomers.length;
          currentItem += this.PAGE_SIZE;

          // Progress update
          if (totalCustomers > 0) {
            const completionPercentage =
              (processedCount / totalCustomers) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalCustomers} (${completionPercentage.toFixed(1)}%)`,
            );

            // Early completion check
            if (processedCount >= totalCustomers) {
              this.logger.log('🎉 All customers processed successfully!');
              break;
            }
          }

          // Sync to LarkBase (optional - can be done in batches)
          if (savedCustomers.length > 0) {
            try {
              await this.syncCustomersToLarkBase(savedCustomers);
              this.logger.log(
                `🚀 Synced ${savedCustomers.length} customers to LarkBase`,
              );
            } catch (larkError) {
              this.logger.warn(
                `⚠️ LarkBase sync failed for page ${currentPage}: ${larkError.message}`,
              );
              // Continue with next page even if LarkBase sync fails
            }
          }

          // 🆕 SMART BREAK CONDITIONS
          if (totalCustomers > 0) {
            // If we've processed enough and are past the expected range
            if (
              currentItem >= totalCustomers &&
              processedCount >= totalCustomers * 0.95
            ) {
              this.logger.log(
                '✅ Sync completed - reached expected data range',
              );
              break;
            }
          }

          // Respect rate limits
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          // Exponential backoff
          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`⏳ Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      // Final completion logging
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalCustomers },
      });

      await this.updateSyncControl('customer_recent', {
        isEnabled: true,
        isRunning: false,
        status: 'idle',
      });

      const completionRate =
        totalCustomers > 0 ? (processedCount / totalCustomers) * 100 : 100;
      this.logger.log(
        `✅ Historical customer sync completed: ${processedCount}/${totalCustomers} (${completionRate.toFixed(1)}% completion rate)`,
      );
      this.logger.log(
        `🔄 AUTO-TRANSITION: Historical sync disabled, Recent sync enabled for future cycles`,
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
  // API METHODS with Retry Logic
  // ============================================================================

  async fetchCustomersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeTotal?: boolean;
      includeCustomerGroup?: boolean;
      includeCustomerSocial?: boolean;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined; // FIXED: Initialize as undefined

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchCustomersList(params);
      } catch (error) {
        lastError = error as Error; // FIXED: Cast to Error type
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError; // FIXED: Now guaranteed to be defined
  }

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
        timeout: 45000,
      }),
    );

    return response.data;
  }

  // ============================================================================
  // RECENT SYNC (Incremental updates)
  // ============================================================================

  async syncRecentCustomers(days: number = 3): Promise<void> {
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
  // EXISTING METHODS (Keep unchanged)
  // ============================================================================

  async fetchRecentCustomers(fromDate: Date): Promise<KiotVietCustomer[]> {
    const headers = await this.authService.getRequestHeaders();
    const fromDateStr = fromDate.toISOString();

    const queryParams = new URLSearchParams({
      lastModifiedFrom: fromDateStr,
      currentItem: '0',
      pageSize: '100',
      orderBy: 'modifiedDate',
      orderDirection: 'DESC',
      includeCustomerGroup: 'true',
      includeTotal: 'true',
      includeCustomerSocial: 'true',
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/customers?${queryParams}`, {
        headers,
        timeout: 60000,
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

  async saveCustomersToDatabase(customers: KiotVietCustomer[]): Promise<any[]> {
    this.logger.log(`💾 Saving ${customers.length} customers to database...`);

    const savedCustomers: any[] = [];

    for (const customerData of customers) {
      try {
        let internalBranchId: number | null = null;

        if (customerData.branchId) {
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: customerData.branchId },
          });
          internalBranchId = branch?.id || null;
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
            groups: customerData.groups || null,
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
            createdDate: customerData.createdDate
              ? new Date(customerData.createdDate)
              : new Date(),
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
            groups: customerData.groups || null,
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
            createdDate: customerData.createdDate
              ? new Date(customerData.createdDate)
              : new Date(),
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

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${customers.length} customers...`,
      );

      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (customersToSync.length === 0) {
        this.logger.log('📋 No customers need LarkBase sync');
        return;
      }

      await this.larkCustomerSyncService.syncCustomersToLarkBase(
        customersToSync,
      );

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`🛑 STOPPING sync to prevent data duplication`);

      const customerIds = customers.map((c) => c.id);
      await this.prismaService.customer.updateMany({
        where: { id: { in: customerIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncedAt: new Date(),
        },
      });

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

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
