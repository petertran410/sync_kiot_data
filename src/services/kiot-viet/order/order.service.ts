import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkOrderSyncService } from '../../lark/order/lark-order-sync.service';
import { LarkSyncStatus } from '@prisma/client';

interface KiotVietOrder {
  id: number;
  code: string;
  orderDate: string;
  branchId: number;
  branchName?: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  soldById?: number;
  soldByName?: string;
  saleChannelId?: number;
  status?: number;
  statusValue?: string;
  total: number;
  totalPayment?: number;
  description?: string;
  usingCod?: boolean;
  discount?: number;
  discountRatio?: number;
  createdDate: string;
  modifiedDate?: string;
  modifiedBy?: string;
  orderDetails?: Array<{
    id?: number;
    productId?: number;
    productCode?: string;
    productName?: string;
    quantity?: number;
    price?: number;
    subTotal?: number;
    discount?: number;
    discountRatio?: number;
  }>;
  orderDelivery?: {
    receiver?: string;
    contactNumber?: string;
    address?: string;
    locationId?: number;
    locationName?: string;
    wardName?: string;
    weight?: number;
    length?: number;
    width?: number;
    height?: number;
    partnerDeliveryId?: number;
  };
  payments?: Array<{
    id?: number;
    code?: string;
    amount: number;
    method: string;
    status?: number;
    transDate: string;
    accountId?: number;
    description?: string;
  }>;
  invoiceOrderSurcharges?: Array<{
    id?: number;
    surchargeId?: number;
    surchargeName?: string;
    surValue?: number;
    price?: number;
  }>;
}

@Injectable()
export class KiotVietOrderService {
  private readonly logger = new Logger(KiotVietOrderService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  // ============================================================================
  // SYNC CONTROL & TRACKING - EXACT COPY FROM INVOICE
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_historical' },
      });

      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_recent' },
      });

      // Priority: Historical sync first
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical order sync...');
        await this.syncHistoricalOrders();
        return;
      }

      // Then recent sync
      if (recentSync?.isEnabled && !recentSync.isRunning) {
        this.logger.log('Starting recent order sync...');
        await this.syncRecentOrders(7); // Last 7 days
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent order sync...');
      await this.syncRecentOrders(7);
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('order_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical order sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC - ENHANCED WITH RATE LIMITING
  // ============================================================================

  async syncHistoricalOrders(): Promise<void> {
    const syncName = 'order_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalOrders = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedOrderIds = new Set<number>(); // Track processed IDs to avoid duplicates

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical order sync...');

      // COMPLETION DETECTION with more flexible thresholds
      const MAX_CONSECUTIVE_EMPTY_PAGES = 5; // Increased from 3
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const MIN_EXPECTED_ORDERS = 10;
      const RETRY_DELAY_MS = 2000; // 2 seconds delay between retries
      const MAX_TOTAL_RETRIES = 10; // Total retries allowed across the entire sync

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;
        this.logger.log(
          `📄 Fetching orders page: ${currentPage} (currentItem: ${currentItem})`,
        );

        try {
          const orderListResponse = await this.fetchOrdersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id',
            orderDirection: 'ASC',
            includeOrderDelivery: true,
            includePayment: true,
          });

          // VALIDATION: Check response structure
          if (!orderListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;
            consecutiveErrorPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.error(
                `❌ Received ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses. Trying final validation...`,
              );

              // Try to validate with current data before failing
              if (processedCount > 0) {
                this.logger.log(
                  `✅ Partial sync completed with ${processedCount} orders processed`,
                );
                break;
              } else {
                throw new Error(
                  `API returned ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses with no data processed`,
                );
              }
            }

            // Wait before retrying
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          // Reset error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // VALIDATION: Check data structure
          const { total, data: orders } = orderListResponse;

          if (total !== undefined && total !== null) {
            totalOrders = total;
            lastValidTotal = total;
          } else if (lastValidTotal > 0) {
            totalOrders = lastValidTotal;
          }

          this.logger.log(`📊 Total orders in system: ${totalOrders}`);

          // Handle empty data array
          if (!orders || orders.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received. Consecutive empty pages: ${consecutiveEmptyPages + 1}`,
            );
            consecutiveEmptyPages++;

            // GRACEFUL COMPLETION CHECK
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              const progressPercentage =
                totalOrders > 0 ? (processedCount / totalOrders) * 100 : 0;

              if (progressPercentage >= 95) {
                this.logger.log(
                  `✅ Sync nearly complete (${progressPercentage.toFixed(1)}%). Ending gracefully.`,
                );
                break;
              } else if (processedCount > 0) {
                this.logger.log(
                  `⚠️ Partial completion (${progressPercentage.toFixed(1)}%). Ending with partial data.`,
                );
                break;
              } else {
                throw new Error(
                  `Too many empty pages with minimal progress: ${processedCount}/${totalOrders}`,
                );
              }
            }

            // Smart pagination increment on empty pages
            if (currentItem < totalOrders * 0.9) {
              // Only skip if we're not near the end
              currentItem += this.PAGE_SIZE;
            }
            continue;
          }

          // Handle duplicate detection at page level - EXACT COPY FROM INVOICE
          const newOrders = orders.filter(
            (order) => !processedOrderIds.has(order.id),
          );
          const duplicateCount = orders.length - newOrders.length;

          if (duplicateCount > 0) {
            this.logger.warn(
              `⚠️ Found ${duplicateCount} duplicate orders in page ${currentPage}. Processing ${newOrders.length} new orders.`,
            );
          }

          if (newOrders.length === 0) {
            this.logger.warn(
              `⚠️ All orders in current page already processed. Skipping...`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Add to processed set - EXACT COPY FROM INVOICE
          newOrders.forEach((order) => processedOrderIds.add(order.id));

          this.logger.log(
            `📊 Processing ${newOrders.length} orders (Page: ${currentPage}, Processed: ${processedCount}/${totalOrders})`,
          );

          // Enrich with details
          this.logger.log(
            `🔍 Enriching ${newOrders.length} orders with details...`,
          );
          const enrichedOrders = await this.enrichOrdersWithDetails(newOrders);

          // Save to database
          this.logger.log(
            `💾 Saving ${enrichedOrders.length} orders to database...`,
          );
          const savedOrders = await this.saveOrdersToDatabase(enrichedOrders);

          // Sync to LarkBase
          await this.syncOrdersToLarkBase(savedOrders);

          processedCount += newOrders.length;
          currentItem += this.PAGE_SIZE;

          // Progress tracking
          const progressPercentage =
            totalOrders > 0 ? (processedCount / totalOrders) * 100 : 0;

          this.logger.log(
            `📈 Progress: ${processedCount}/${totalOrders} (${progressPercentage.toFixed(1)}%)`,
          );

          // ✅ CRITICAL FIX: Rate limiting delay like Invoice service
          this.logger.log('⏳ Rate limiting delay...');
          await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 second delay between pages

          // EARLY COMPLETION CHECK
          if (
            totalOrders > 0 &&
            processedCount >= totalOrders &&
            consecutiveEmptyPages === 0
          ) {
            this.logger.log('✅ All orders processed successfully');
            break;
          }

          // Reset consecutive errors on successful processing
          totalRetries = 0;
        } catch (error) {
          this.logger.error(
            `❌ Page ${currentPage} processing failed: ${error.message}`,
          );

          consecutiveErrorPages++;
          totalRetries++;

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Maximum consecutive error pages (${MAX_CONSECUTIVE_ERROR_PAGES}) reached. Aborting historical sync.`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(
              `Maximum total retries (${MAX_TOTAL_RETRIES}) reached. Aborting historical sync.`,
            );
          }

          // ✅ ENHANCED: Exponential backoff with longer delays for rate limiting
          const baseDelay =
            error.message.includes('420') ||
            error.message.includes('Too Many Requests')
              ? 10000 // 10 seconds for rate limit errors
              : RETRY_DELAY_MS;

          const backoffDelay =
            baseDelay * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(
            `⏳ Retrying after ${backoffDelay}ms delay (attempt ${totalRetries}/${MAX_TOTAL_RETRIES})...`,
          );
          await new Promise((resolve) => setTimeout(resolve, backoffDelay));
        }
      }

      // Final logging and cleanup
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false, // Auto-disable after completion
        status: 'completed',
        completedAt: new Date(),
        progress: { processedCount, expectedTotal: totalOrders },
      });

      // Auto-enable recent sync for future cycles
      await this.updateSyncControl('order_recent', {
        isEnabled: true,
        isRunning: false,
        status: 'idle',
      });

      const completionRate =
        totalOrders > 0 ? (processedCount / totalOrders) * 100 : 100;
      this.logger.log(
        `✅ Historical order sync completed: ${processedCount}/${totalOrders} (${completionRate.toFixed(1)}% completion rate)`,
      );
      this.logger.log(
        `🔄 AUTO-TRANSITION: Historical sync disabled, Recent sync enabled for future cycles`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical order sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalOrders },
      });

      throw error;
    }
  }

  // ============================================================================
  // RECENT SYNC - ENHANCED WITH RATE LIMITING
  // ============================================================================

  async syncRecentOrders(days: number = 7): Promise<void> {
    const syncName = 'order_recent';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(`🔄 Starting recent order sync (${days} days)...`);

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      const recentOrders = await this.fetchRecentOrders(fromDate);

      if (recentOrders.length === 0) {
        this.logger.log('📋 No recent order updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(`🔄 Processing ${recentOrders.length} recent orders...`);

      // Enrich with details
      const enrichedOrders = await this.enrichOrdersWithDetails(recentOrders);

      // Save to database
      const savedOrders = await this.saveOrdersToDatabase(enrichedOrders);

      // Sync to LarkBase
      await this.syncOrdersToLarkBase(savedOrders);

      const totalProcessed = savedOrders.length;
      const duplicatesRemoved = recentOrders.length - totalProcessed;

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { totalProcessed, duplicatesRemoved },
      });

      this.logger.log(
        `✅ Recent sync completed: ${recentOrders.length} orders processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Recent sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { errorDetails: error.message },
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS with ENHANCED Rate Limiting & Retry Logic
  // ============================================================================

  async fetchOrdersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeOrderDelivery?: boolean;
      includePayment?: boolean;
    },
    maxRetries: number = 5, // ✅ INCREASED: More retries for rate limiting
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchOrdersList(params);
      } catch (error) {
        lastError = error as Error;

        // ✅ ENHANCED: Special handling for rate limiting
        const isRateLimit =
          error.response?.status === 420 ||
          error.message?.includes('420') ||
          error.message?.includes('Too Many Requests');

        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}${isRateLimit ? ' (RATE LIMITED)' : ''}`,
        );

        if (attempt < maxRetries) {
          // ✅ ENHANCED: Longer delays for rate limiting
          const baseDelay = isRateLimit ? 10000 : 1000; // 10s for rate limit, 1s for others
          const delay = baseDelay * attempt; // Progressive delay

          this.logger.log(`⏳ Waiting ${delay}ms before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchOrdersList(params: {
    currentItem?: number;
    pageSize?: number;
    orderBy?: string;
    orderDirection?: string;
    includeOrderDelivery?: boolean;
    includePayment?: boolean;
  }): Promise<any> {
    const accessToken = await this.authService.getAccessToken();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'id',
      orderDirection: params.orderDirection || 'ASC',
      includeOrderDelivery: (params.includeOrderDelivery || false).toString(),
      includePayment: (params.includePayment || false).toString(),
    });

    const url = `${this.baseUrl}/orders?${queryParams}`;

    const response = await firstValueFrom(
      this.httpService.get(url, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 30000,
      }),
    );

    return response.data;
  }

  async fetchRecentOrders(fromDate: Date): Promise<KiotVietOrder[]> {
    this.logger.log(
      `🔍 Fetching orders modified since: ${fromDate.toISOString()}`,
    );

    const accessToken = await this.authService.getAccessToken();
    const orders: KiotVietOrder[] = [];
    let currentItem = 0;
    let hasMore = true;

    while (hasMore) {
      const queryParams = new URLSearchParams({
        currentItem: currentItem.toString(),
        pageSize: this.PAGE_SIZE.toString(),
        fromModifiedDate: fromDate.toISOString(),
        orderBy: 'modifiedDate',
        orderDirection: 'DESC',
        includeOrderDelivery: 'true',
        includePayment: 'true',
      });

      const url = `${this.baseUrl}/orders?${queryParams}`;

      try {
        const response = await firstValueFrom(
          this.httpService.get(url, {
            headers: {
              Authorization: `Bearer ${accessToken}`,
              'Content-Type': 'application/json',
            },
            timeout: 30000,
          }),
        );

        const { data: pageOrders, total } = response.data;

        if (pageOrders && pageOrders.length > 0) {
          orders.push(...pageOrders);
          currentItem += pageOrders.length;

          this.logger.log(
            `📥 Fetched ${pageOrders.length} recent orders (Total: ${orders.length}/${total})`,
          );

          // Check if we have more data
          hasMore =
            orders.length < total && pageOrders.length === this.PAGE_SIZE;

          // ✅ ADDED: Rate limiting delay for recent orders too
          if (hasMore) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        } else {
          hasMore = false;
        }
      } catch (error) {
        this.logger.error(`Failed to fetch recent orders: ${error.message}`);
        throw error;
      }
    }

    return orders;
  }

  // ============================================================================
  // ORDER ENRICHMENT & DATABASE OPERATIONS - ADAPTED FROM INVOICE
  // ============================================================================

  private async enrichOrdersWithDetails(
    orders: KiotVietOrder[],
  ): Promise<any[]> {
    const enrichedOrders: any[] = [];

    for (const order of orders) {
      try {
        // Fetch detailed order information if needed
        let detailedOrder = order;

        if (!order.orderDetails || order.orderDetails.length === 0) {
          detailedOrder = await this.fetchOrderDetails(order.id);
        }

        enrichedOrders.push(detailedOrder);

        // ✅ ADDED: Small delay between detail fetches to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 200));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich order ${order.id}: ${error.message}`,
        );
        // Use original order data if enrichment fails
        enrichedOrders.push(order);
      }
    }

    return enrichedOrders;
  }

  private async fetchOrderDetails(orderId: number): Promise<KiotVietOrder> {
    const accessToken = await this.authService.getAccessToken();
    const url = `${this.baseUrl}/orders/${orderId}`;

    const response = await firstValueFrom(
      this.httpService.get(url, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 15000,
      }),
    );

    return response.data;
  }

  private async saveOrdersToDatabase(orders: any[]): Promise<any[]> {
    const savedOrders: any[] = [];

    for (const order of orders) {
      try {
        const orderData = {
          kiotVietId: BigInt(order.id),
          code: order.code,
          purchaseDate: new Date(order.orderDate || order.purchaseDate),
          branchId: order.branchId,
          branchName: order.branchName || null,
          customerId: order.customerId || null,
          customerCode: order.customerCode || null,
          customerName: order.customerName || null,
          soldById: order.soldById || null,
          soldByName: order.soldByName || null,
          saleChannelId: order.saleChannelId || null,
          status: order.status || null,
          statusValue: order.statusValue || null,
          total: order.total || 0,
          totalPayment: order.totalPayment || 0,
          description: order.description || null,
          usingCod: order.usingCod || false,
          discount: order.discount || 0,
          discountRatio: order.discountRatio || 0,
          createdDate: new Date(order.createdDate),
          modifiedDate: order.modifiedDate
            ? new Date(order.modifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
          larkRecordId: null,
          larkSyncStatus: LarkSyncStatus.PENDING,
          larkSyncedAt: null,
          larkSyncRetries: 0,
        };

        const savedOrder = await this.prismaService.order.upsert({
          where: { kiotVietId: orderData.kiotVietId },
          update: {
            ...orderData,
            modifiedDate: new Date(),
          },
          create: {
            ...orderData,
          },
        });

        savedOrders.push(savedOrder);
      } catch (error) {
        this.logger.error(
          `💥 Failed to save order ${order.id}: ${error.message}`,
        );
        // Continue with other orders
      }
    }

    this.logger.log(
      `💾 Saved ${savedOrders.length}/${orders.length} orders to database`,
    );
    return savedOrders;
  }

  private async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    try {
      await this.larkOrderSyncService.syncOrdersToLarkBase(orders);
    } catch (error) {
      this.logger.error(`Failed to sync orders to LarkBase: ${error.message}`);
      // Update orders with failed status
      await this.updateOrdersSyncStatus(orders, LarkSyncStatus.FAILED);
      throw error;
    }
  }

  private async updateOrdersSyncStatus(
    orders: any[],
    status: LarkSyncStatus,
  ): Promise<void> {
    const orderIds = orders.map((order) => order.kiotVietId);

    await this.prismaService.order.updateMany({
      where: { kiotVietId: { in: orderIds } },
      data: { larkSyncStatus: status },
    });
  }

  // ============================================================================
  // SYNC CONTROL UTILITIES - EXACT COPY FROM INVOICE
  // ============================================================================

  private async updateSyncControl(
    name: string,
    data: Partial<{
      isRunning: boolean;
      isEnabled: boolean;
      status: string;
      error: string | null;
      startedAt: Date;
      completedAt: Date;
      lastRunAt: Date;
      progress: any; // ✅ FIX: Use progress instead of metadata
    }>,
  ): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['order'],
        syncMode: name.includes('historical') ? 'historical' : 'recent',
        status: 'idle',
        ...data,
      },
      update: data,
    });
  }
}
