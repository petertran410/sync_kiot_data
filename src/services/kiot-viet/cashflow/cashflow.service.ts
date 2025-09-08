import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { async, first, firstValueFrom } from 'rxjs';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { response } from 'express';

interface KiotVietCashflow {
  id: number;
  userId: number;
  address?: string;
  locationName?: string;
  wardName?: string;
  contactNumber?: string;
  status?: number;
  createdBy?: number;
  usedForFinancialReporting?: number;
  account?: string;
  branch?: string;
  user?: string;
  accountId?: number;
  origin?: string;
  cashFlowGroupId?: number;
  cashGroup?: string;
  statusValue?: string;
  method?: string;
  partnerType?: string;
  partnerId?: number;
  branchId: number;
  retailerId: number;
  transDate?: string;
  amount?: number;
  code: string;
  partnerName: string;
  description?: string;
}

@Injectable()
export class KiotVietCashflowService {
  private readonly logger = new Logger(KiotVietCashflowService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'cashflow_historical' },
      });

      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'cashflow_recent' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical cashflow sync...');
        await this.syncHistoricalCashflows();
        return;
      }

      if (recentSync?.isEnabled && !recentSync.isRunning) {
        this.logger.log('Starting recent cashflows sync...');
        await this.syncRecentCashflows(4);
        return;
      }

      this.logger.log('Running default recent cashflows sync...');
      await this.syncRecentCashflows(4);
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('cashflow_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('Historical cashflow sync enabled');
  }

  async syncHistoricalCashflows(): Promise<void> {
    const syncName = 'cashflow_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalCashflows = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedCashflowIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('Starting historical cashflow sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalCashflows > 0) {
          if (currentItem >= totalCashflows) {
            this.logger.log(
              `Pagination complete. Processed ${processedCount}/${totalCashflows} cashflows`,
            );
            break;
          }
          const progressPercentage = (currentItem / totalCashflows) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalCashflows} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        const dateEnd = new Date();
        dateEnd.setDate(dateEnd.getDate() + 1);
        const dateEndStr = dateEnd.toISOString().split('T')[0];

        try {
          const response = await this.fetchCashflowsListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            includeAccount: true,
            includeBranch: true,
            includeUser: true,
            startDate: '2024-12-1',
            endDate: dateEndStr,
          });

          if (!response) {
            this.logger.warn('Received null response from KiotViet API');
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

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { data: cashflows, total } = response;

          if (total !== undefined && total !== null) {
            if (totalCashflows === 0) {
              this.logger.log(
                `Total cashflows detected: ${total}. Starting processing...`,
              );

              totalCashflows = total;
            } else if (total !== totalCashflows) {
              this.logger.warn(
                `Total count changed: ${totalCashflows} -> ${total}. Using latest.`,
              );
              totalCashflows = total;
            }
            lastValidTotal = total;
          }

          if (!cashflows || cashflows.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalCashflows > 0 && currentItem >= totalCashflows) {
              this.logger.log('Reached end of data (empty page past total');
              break;
            }

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          const existingCashflowIds = new Set(
            (
              await this.prismaService.cashflow.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newCashflows = cashflows.filter((cashflow) => {
            if (existingCashflowIds.has(cashflow.id)) {
              return false;
            }

            if (processedCashflowIds.has(cashflow.id)) {
              this.logger.debug(
                `Duplicate cashflow ID detected: ${cashflow.id} (${cashflow.code})`,
              );
              return false;
            }

            processedCashflowIds.add(cashflow.id);
            return true;
          });

          if (newCashflows.length !== cashflows.length) {
            this.logger.warn(
              `Filtered out ${cashflows.length - newCashflows.length} duplicate cashflows on page ${currentPage}`,
            );
          }

          if (newCashflows.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all cashflows already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `Processing ${newCashflows.length} cashflows from page ${currentPage}`,
          );

          const savedCashflows =
            await this.saveCashflowsToDatabase(newCashflows);

          processedCount += savedCashflows.length;
          currentItem += this.PAGE_SIZE;

          if (totalCashflows > 0) {
            const completionPercentage =
              (processedCount / totalCashflows) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalCashflows} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalCashflows) {
              this.logger.log('All cashflows processed successfully');
              break;
            }
          }

          // if (totalCashflows > 0) {
          //   if (
          //     currentItem >= totalCashflows &&
          //     processedCount >= totalCashflows
          //   ) {
          //     this.logger.log('Sync completed - reached expected data range');
          //   }
          //   break;
          // }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (
            consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES ||
            totalRetries >= MAX_TOTAL_RETRIES
          ) {
            throw new Error(
              `Too many consecutive errors (${consecutiveErrorPages}) or total retries (${totalRetries}). Last error: ${error.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalCashflows },
      });

      const completionRate =
        totalCashflows > 0 ? (processedCount / totalCashflows) * 100 : 100;

      this.logger.log(
        `Historical cashflow sync completed: ${processedCount}/${totalCashflows} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(`Historical cashflow sync failed" ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalCashflows },
      });

      throw error;
    }
  }

  async syncRecentCashflows(days: number = 4): Promise<void> {
    const syncName = 'cashflow_recent';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(`Starting recent cashflow sync (${days} days)...`);

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      const recentCashflows = await this.fetchRecentCashflows(fromDate);

      if (recentCashflows.length === 0) {
        this.logger.log('No recent cashflows updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(`Processing ${recentCashflows.length} recent cashflows`);

      const cashflowsWithDetails = await this.enrichCashflowsWithDetails();
      await this.saveCashflowsToDatabase(cashflowsWithDetails);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log(
        `Recent cashflow sync completed: ${cashflowsWithDetails.length} cashflows progressed`,
      );
    } catch (error) {
      this.logger.error(`Recent sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { errorDetails: error.message },
      });

      throw error;
    }
  }

  async fetchCashflowsListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      includeAccount?: boolean;
      includeBranch?: boolean;
      includeUser?: boolean;
      startDate?: string;
      endDate?: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchCashflowsList(params);
      } catch (error) {
        lastError = error as Error;

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchCashflowsList(params: {
    currentItem?: number;
    pageSize?: number;
    includeAccount?: boolean;
    includeBranch?: boolean;
    includeUser?: boolean;
    startDate?: string;
    endDate?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      includeAccount: (params.includeAccount || true).toString(),
      includeBranch: (params.includeBranch || true).toString(),
      includeUser: (params.includeUser || true).toString(),
    });

    if (params.startDate) {
      queryParams.append('startDate', params.startDate);
    }
    if (params.endDate) {
      queryParams.append('endDate', params.endDate);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/cashflow?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  async fetchRecentCashflows(fromDate: Date): Promise<any[]> {
    const headers = await this.authService.getRequestHeaders();

    const fromDateStr = fromDate.toISOString().split('T')[0];
    const today = new Date();
    const todayDateStr = today.toISOString().split('T')[0];

    const queryParams = new URLSearchParams({
      currentItem: '0',
      pageSize: '100',
      includeAccount: 'true',
      includeBranch: 'true',
      includeUser: 'true',
      startDate: fromDateStr,
      endDate: todayDateStr,
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/cashflow?${queryParams}`, {
        headers,
        timeout: 60000,
      }),
    );

    return response.data?.data;
  }

  private async enrichCashflowsWithDetails(): Promise<KiotVietCashflow[]> {
    this.logger.log(`Enrich cashflows with details`);

    const enrichedCashflows: any[] = [];
    try {
      const headers = await this.authService.getRequestHeaders();
      const response = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/cashflow`, { headers }),
      );
      if (response.data) {
        enrichedCashflows.push(response.data);
      } else {
        console.log('No cashflow');
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    } catch (error) {
      this.logger.warn(`Failed to enrich cashflow: ${error.message}`);
    }

    return enrichedCashflows;
  }

  private async saveCashflowsToDatabase(
    cashflows: KiotVietCashflow[],
  ): Promise<any[]> {
    this.logger.log(`Saving ${cashflows.length} cashflows to database...`);

    const savedCashflows: any[] = [];

    for (const cashflowData of cashflows) {
      try {
        // const user = await this.prismaService.user.findFirst({
        //   where: { kiotVietId: cashflowData.userId },
        //   select: {
        //     id: true,
        //     userName: true,
        //   },
        // });

        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: cashflowData.branchId },
          select: {
            id: true,
            name: true,
          },
        });

        // const bank = await this.prismaService.bankAccount.findFirst({
        //   where: { kiotVietId: cashflowData.accountId },
        //   select: { id: true, bankName: true },
        // });

        const cashflow = await this.prismaService.cashflow.upsert({
          where: { kiotVietId: BigInt(cashflowData.id) },
          update: {
            code: cashflowData.code.trim(),
            userId: cashflowData.userId,
            address: cashflowData.address || '',
            locationName: cashflowData.locationName || '',
            wardName: cashflowData.wardName || '',
            contactNumber: cashflowData.contactNumber || '',
            status: cashflowData.status || null,
            createdBy: cashflowData.createdBy,
            usedForFinancialReporting:
              cashflowData.usedForFinancialReporting || null,
            branchName: branch?.name,
            partnerName: cashflowData.partnerName,
            userName: cashflowData.user,
            accountId: cashflowData.accountId,
            origin: cashflowData.origin || '',
            cashFlowGroupId: cashflowData.cashFlowGroupId || null,
            cashGroup: cashflowData.cashGroup || '',
            statusValue: cashflowData.statusValue || '',
            method: cashflowData.method || '',
            partnerType: cashflowData.partnerType || '',
            partnerId: cashflowData.partnerId || null,
            branchId: branch?.id,
            retailerId: cashflowData.retailerId,
            transDate: cashflowData.transDate
              ? new Date(cashflowData.transDate)
              : new Date(),
            amount: Number(cashflowData.amount) || 0,
            description: cashflowData.description || '',
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: BigInt(cashflowData.id),
            code: cashflowData.code.trim(),
            userId: cashflowData.userId,
            address: cashflowData.address || '',
            locationName: cashflowData.locationName || '',
            wardName: cashflowData.wardName || '',
            contactNumber: cashflowData.contactNumber || '',
            status: cashflowData.status || null,
            createdBy: cashflowData.createdBy,
            usedForFinancialReporting:
              cashflowData.usedForFinancialReporting || null,
            branchName: branch?.name,
            partnerName: cashflowData.partnerName,
            userName: cashflowData.user,
            accountId: cashflowData.accountId,
            origin: cashflowData.origin || '',
            cashFlowGroupId: cashflowData.cashFlowGroupId || null,
            cashGroup: cashflowData.cashGroup || '',
            statusValue: cashflowData.statusValue || '',
            method: cashflowData.method || '',
            partnerType: cashflowData.partnerType || '',
            partnerId: cashflowData.partnerId || null,
            branchId: branch?.id,
            retailerId: cashflowData.retailerId,
            transDate: cashflowData.transDate
              ? new Date(cashflowData.transDate)
              : new Date(),
            amount: Number(cashflowData.amount) || 0,
            description: cashflowData.description || '',
            lastSyncedAt: new Date(),
          },
        });

        savedCashflows.push(cashflow);
      } catch (error) {
        this.logger.log(
          `Failed to save cashflow ${cashflowData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`Saved ${savedCashflows.length} cashflows successfully`);
    return savedCashflows;
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['cashflow'],
          syncMode: 'historical',
          isRunning: false,
          isEnabled: true,
          status: 'idle',
          ...data,
        },
        update: {
          ...data,
          lastRunAt:
            data.status === 'completed' || data.status === 'failed'
              ? new Date()
              : undefined,
        },
      });
    } catch (error) {
      this.logger.error(
        `Failed to update sync control '${name}': ${error.message}`,
      );
      throw error;
    }
  }
}
