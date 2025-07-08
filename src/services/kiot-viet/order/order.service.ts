import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { async, firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkOrderSyncService } from '../../lark/order/lark-order-sync.service';
import { LarkSyncStatus, Prisma } from '@prisma/client';
import { response } from 'express';
import { url } from 'inspector';

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

          await this.updateSyncControl(syncName, {
            progress: {
              current: processedCount,
              total: totalOrders,
              percentage: Math.round(progressPercentage),
            },
          });

          // EARLY COMPLETION CHECK
          if (
            totalOrders > 0 &&
            processedCount >= totalOrders &&
            consecutiveEmptyPages === 0
          ) {
            this.logger.log('✅ All orders processed successfully');
            break;
          }

          // Safety limit to prevent infinite loops
          if (currentItem > totalOrders * 1.5) {
            this.logger.warn(
              `⚠️ Safety limit reached. Processed: ${processedCount}/${totalOrders}`,
            );
            break;
          }

          // Rate limiting
          await new Promise((resolve) => setTimeout(resolve, 1500));
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

      // Final logging and cleanup
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalOrders },
      });

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

        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 1000 * attempt; // Progressive delay
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
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'id',
      orderDirection: params.orderDirection || 'DESC',
      includeOrderDelivery: (params.includeOrderDelivery || true).toString(),
      includePayment: (params.includePayment || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/orders?${queryParams}`, {
        headers,
        timeout: 30000, // Increased timeout
      }),
    );

    return response.data;
  }

  async fetchRecentOrders(fromDate: Date): Promise<any[]> {
    try {
      const allOrders: any[] = [];
      let currentItem = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchRecentOrdersPage(
          fromDate,
          currentItem,
        );

        if (!response.data || response.data.length === 0) {
          hasMoreData = false;
          break;
        }

        allOrders.push(...response.data);
        currentItem += response.data.length;

        hasMoreData = response.data.length === this.PAGE_SIZE;
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      return allOrders;
    } catch (error) {
      this.logger.error(`Failed to fetch recent orders: ${error.message}`);
      throw error;
    }
  }

  private async fetchRecentOrdersPage(
    fromDate: Date,
    currentItem: number,
  ): Promise<any> {
    try {
      const headers = await this.authService.getRequestHeaders();

      const response = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/orders`, {
          headers,
          params: {
            pageSize: this.PAGE_SIZE,
            currentItem,
            includeOrderDelivery: 'true',
            includePayment: 'true',
            lastModifiedFrom: fromDate.toISOString(),
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
          timeout: 60000,
        }),
      );
      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch recent orders: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // ORDER ENRICHMENT & DATABASE OPERATIONS - ADAPTED FROM INVOICE
  // ============================================================================

  private async enrichOrdersWithDetails(orders: any[]): Promise<any[]> {
    const enrichedOrders: any[] = [];

    for (const order of orders) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/orders/${order.id}`, {
            headers,
            timeout: 15000,
          }),
        );

        enrichedOrders.push(response.data);
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich order ${order.id}: ${error.message}`,
        );
        enrichedOrders.push(order);
      }
    }

    return enrichedOrders;
  }

  private async saveOrdersToDatabase(orders: any[]): Promise<any[]> {
    const savedOrders: any[] = [];

    for (const orderData of orders) {
      try {
        const customer = orderData.customerId
          ? await this.prismaService.customer.findFirst({
              where: { kiotVietId: BigInt(orderData.customerId) },
              select: { id: true },
            })
          : null;

        // Branch lookup - SAFE
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: orderData.branchId },
          select: { id: true, name: true },
        });

        const soldBy = orderData.soldById
          ? await this.prismaService.user.findFirst({
              where: { kiotVietId: BigInt(orderData.soldById) },
              select: { kiotVietId: true },
            })
          : null;

        // SaleChannel lookup - SAFE
        const saleChannel = orderData.saleChannelId
          ? await this.prismaService.saleChannel.findFirst({
              where: { kiotVietId: orderData.saleChannelId },
              select: { id: true },
            })
          : null;

        const order = await this.prismaService.order.upsert({
          where: { kiotVietId: BigInt(orderData.id) },
          update: {
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId || null,
            customerId: customer?.id ?? null,
            customerCode: orderData.customerCode || null,
            customerName: orderData.customerName || null,
            saleChannelId: orderData.saleChannelId || null,
            status: orderData.status,
            statusValue: orderData.statusValue || null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            description: orderData.description || null,
            usingCod: orderData.usingCod || false,
            discount: orderData.discoun || null,
            discountRatio: orderData.discountRatio || null,
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkRecordId: null,
            larkSyncStatus: 'PENDING' as const,
          },
          create: {
            kiotVietId: BigInt(orderData.id),
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId || null,
            customerId: customer?.id ?? null,
            customerCode: orderData.customerCode || null,
            customerName: orderData.customerName || null,
            saleChannelId: orderData.saleChannelId || null,
            status: orderData.status,
            statusValue: orderData.statusValue || null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            description: orderData.description || null,
            usingCod: orderData.usingCod || false,
            discount: orderData.discoun || null,
            discountRatio: orderData.discountRatio || null,
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkRecordId: null,
            larkSyncStatus: 'PENDING' as const,
          } satisfies Prisma.OrderUncheckedCreateInput,
        });

        if (orderData.orderDetails && orderData.orderDetails.length > 0) {
          for (const detail of orderData.orderDetails) {
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(detail.productId) },
              select: { id: true },
            });

            if (product) {
              await this.prismaService.orderDetail.upsert({
                where: {
                  kiotVietId: detail.id ? BigInt(detail.id) : BigInt(0),
                },
                update: {
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : null,
                  discountRatio: detail.discountRatio,
                  note: detail.note,
                  isMaster: detail.isMaster ?? true,
                },
                create: {
                  kiotVietId: detail.id ? BigInt(detail.id) : null,
                  orderId: order.id,
                  productId: product.id,
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : null,
                  discountRatio: detail.discountRatio,
                  note: detail.note,
                  isMaster: detail.isMaster ?? true,
                },
              });
            }
          }
        }

        // ============================================================================
        // SAVE ORDER DELIVERY (OPTIONAL)
        // ============================================================================
        if (orderData.orderDelivery) {
          await this.prismaService.orderDelivery.upsert({
            where: { orderId: order.id },
            update: {
              deliveryCode: orderData.orderDelivery.deliveryCode,
              type: orderData.orderDelivery.type,
              price: orderData.orderDelivery.price
                ? new Prisma.Decimal(orderData.orderDelivery.price)
                : null,
              receiver: orderData.orderDelivery.receiver,
              contactNumber: orderData.orderDelivery.contactNumber,
              address: orderData.orderDelivery.address,
              locationId: orderData.orderDelivery.locationId,
              locationName: orderData.orderDelivery.locationName,
              wardName: orderData.orderDelivery.wardName,
              weight: orderData.orderDelivery.weight,
              length: orderData.orderDelivery.length,
              width: orderData.orderDelivery.width,
              height: orderData.orderDelivery.height,
              partnerDeliveryId: orderData.orderDelivery.partnerDeliveryId
                ? BigInt(orderData.orderDelivery.partnerDeliveryId)
                : null,
            },
            create: {
              orderId: order.id,
              deliveryCode: orderData.orderDelivery.deliveryCode,
              type: orderData.orderDelivery.type,
              price: orderData.orderDelivery.price
                ? new Prisma.Decimal(orderData.orderDelivery.price)
                : null,
              receiver: orderData.orderDelivery.receiver,
              contactNumber: orderData.orderDelivery.contactNumber,
              address: orderData.orderDelivery.address,
              locationId: orderData.orderDelivery.locationId,
              locationName: orderData.orderDelivery.locationName,
              wardName: orderData.orderDelivery.wardName,
              weight: orderData.orderDelivery.weight,
              length: orderData.orderDelivery.length,
              width: orderData.orderDelivery.width,
              height: orderData.orderDelivery.height,
              partnerDeliveryId: orderData.orderDelivery.partnerDeliveryId
                ? BigInt(orderData.orderDelivery.partnerDeliveryId)
                : null,
            },
          });
        }

        // ============================================================================
        // SAVE PAYMENTS (OPTIONAL)
        // ============================================================================
        if (orderData.payments && orderData.payments.length > 0) {
          for (const payment of orderData.payments) {
            // Lookup BankAccount by kiotVietId
            const bankAccount = payment.accountId
              ? await this.prismaService.bankAccount.findFirst({
                  where: { kiotVietId: payment.accountId },
                  select: { id: true },
                })
              : null;

            await this.prismaService.payment.upsert({
              where: {
                kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0),
              },
              update: {
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount),
                method: payment.method,
                status: payment.status,
                transDate: new Date(payment.transDate),
                accountId: bankAccount?.id ?? null, // ✅ FIXED: Use internal BankAccount.id
                description: payment.description,
                orderId: order.id,
              },
              create: {
                kiotVietId: payment.id ? BigInt(payment.id) : null,
                orderId: order.id,
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount),
                method: payment.method,
                status: payment.status,
                transDate: new Date(payment.transDate),
                accountId: bankAccount?.id ?? null, // ✅ FIXED: Use internal BankAccount.id
                description: payment.description,
              },
            });
          }
        }

        // ============================================================================
        // SAVE ORDER SURCHARGES (OPTIONAL)
        // ============================================================================
        if (
          orderData.invoiceOrderSurcharges &&
          orderData.invoiceOrderSurcharges.length > 0
        ) {
          for (const surcharge of orderData.invoiceOrderSurcharges) {
            // Lookup surcharge by ID
            const surchargeRecord = surcharge.surchargeId
              ? await this.prismaService.surcharge.findFirst({
                  where: { kiotVietId: surcharge.surchargeId },
                  select: { id: true },
                })
              : null;

            await this.prismaService.orderSurcharge.upsert({
              where: {
                kiotVietId: surcharge.id ? BigInt(surcharge.id) : BigInt(0),
              },
              update: {
                surchargeName: surcharge.surchargeName,
                surValue: surcharge.surValue
                  ? new Prisma.Decimal(surcharge.surValue)
                  : null,
                price: surcharge.price
                  ? new Prisma.Decimal(surcharge.price)
                  : null,
              },
              create: {
                kiotVietId: surcharge.id ? BigInt(surcharge.id) : null,
                orderId: order.id,
                surchargeId: surchargeRecord?.id ?? null,
                surchargeName: surcharge.surchargeName,
                surValue: surcharge.surValue
                  ? new Prisma.Decimal(surcharge.surValue)
                  : null,
                price: surcharge.price
                  ? new Prisma.Decimal(surcharge.price)
                  : null,
                createdDate: new Date(),
              },
            });
          }
        }

        savedOrders.push(order);
      } catch (error) {
        this.logger.error(
          `Failed to save order ${orderData.id}: ${error.message}`,
        );
        // Log detailed error for debugging
        this.logger.debug(
          `Order data: ${JSON.stringify({
            id: orderData.id,
            soldById: orderData.soldById,
            customerId: orderData.customerId,
            branchId: orderData.branchId,
            payments: orderData.payments?.map((p) => ({
              id: p.id,
              accountId: p.accountId,
            })),
          })}`,
        );
      }
    }
    return savedOrders;
  }

  private async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    try {
      await this.larkOrderSyncService.syncOrdersToLarkBase(orders);
    } catch (error) {
      this.logger.error(`Failed to sync orders to LarkBase: ${error.message}`);
    }
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
