// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import { LarkOrderSyncService } from '../../lark/order/lark-order-sync.service';

interface KiotVietOrder {
  id: number;
  code: string;
  purchaseDate: string;
  branchId: number;
  branchName?: string;
  soldById?: number;
  soldByName?: string;
  cashierId?: number;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  total: number;
  totalPayment: number;
  discount?: number;
  discountRatio?: number;
  status: number;
  statusValue?: string;
  description?: string;
  usingCod?: boolean;
  modifiedDate?: string;
  createdDate?: string;
  saleChannelId?: number;
  expectedDelivery?: string;
  makeInvoice?: boolean;
  retailerId?: number;
  orderDetails?: Array<{
    id?: number;
    productId: number;
    productCode: string;
    productName: string;
    quantity: number;
    price: number;
    discount?: number;
    discountRatio?: number;
    note?: string;
    isMaster?: boolean;
  }>;
  orderDelivery?: {
    deliveryCode?: string;
    type?: number;
    price?: number;
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
  // SYNC CONTROL & TRACKING
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
  // HISTORICAL SYNC - ENHANCED WITH ADVANCED ERROR HANDLING
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
            orderDirection: 'Asc',
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
                `❌ Received ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses. Sync may be complete.`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          const { data: orders, total: apiTotal } = orderListResponse;
          totalOrders = apiTotal || lastValidTotal;

          if (apiTotal && apiTotal > 0) {
            lastValidTotal = apiTotal;
          }

          this.logger.log(
            `📦 Received ${orders?.length || 0} orders. Total from API: ${apiTotal}`,
          );

          // EMPTY PAGE DETECTION
          if (!orders || orders.length === 0) {
            consecutiveEmptyPages++;
            this.logger.warn(
              `⚠️ Empty page ${currentPage} (${consecutiveEmptyPages}/${MAX_CONSECUTIVE_EMPTY_PAGES})`,
            );

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

          // Handle duplicate detection at page level
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

          // Add to processed set
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
            totalOrders > 0 ? (processedCount / totalOrders) * 100 : 100;

          if (processedCount % 50 === 0) {
            this.logger.log(
              `⏳ Progress: ${processedCount}/${totalOrders} orders (${progressPercentage.toFixed(1)}%)`,
            );

            await this.updateSyncControl(syncName, {
              progress: {
                processedCount,
                totalExpected: totalOrders,
                progressPercentage,
              },
            });
          }

          // COMPLETION CONDITION: Multiple criteria for safety
          if (
            processedCount >= totalOrders &&
            totalOrders > MIN_EXPECTED_ORDERS
          ) {
            this.logger.log(
              `✅ Reached expected total: ${processedCount}/${totalOrders}`,
            );
            break;
          }

          // Add rate limiting
          await new Promise((resolve) => setTimeout(resolve, 500));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;
          this.logger.error(
            `❌ Page ${currentPage} failed: ${error.message} (Error ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES})`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Too many consecutive errors (${MAX_CONSECUTIVE_ERROR_PAGES}). Aborting historical sync.`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(
              `Maximum total retries (${MAX_TOTAL_RETRIES}) reached. Aborting historical sync.`,
            );
          }

          // Exponential backoff
          const backoffDelay =
            RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
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
  // RECENT SYNC
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
  // API METHODS with Retry Logic - NEW ENHANCED FUNCTIONS
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
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined; // Initialize as undefined

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchOrdersList(params);
      } catch (error) {
        lastError = error as Error; // Cast to Error type
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 1000 * attempt; // Progressive delay
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError; // Now guaranteed to be defined
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
      orderDirection: params.orderDirection || 'Asc',
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

  // ============================================================================
  // KIOTVIET API METHODS - EXISTING FUNCTIONS ENHANCED
  // ============================================================================

  private async fetchRecentOrders(fromDate: Date): Promise<any[]> {
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

        // Check if more data
        hasMoreData = response.data.length === this.PAGE_SIZE;

        // Rate limiting
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
            includeOrderDelivery: true,
            includePayment: true,
            lastModifiedFrom: fromDate.toISOString(),
            orderBy: 'modifiedDate',
            orderDirection: 'Desc',
          },
          timeout: 30000,
        }),
      );

      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch recent orders: ${error.message}`);
      throw error;
    }
  }

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
          `Failed to enrich order ${order.code}: ${error.message}`,
        );
        enrichedOrders.push(order);
      }
    }

    return enrichedOrders;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  // ============================================================================
  // FILE: src/services/kiot-viet/order/order.service.ts
  // ✅ EXACT Invoice Pattern Implementation for Order
  // ============================================================================

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
          select: { id: true },
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

        // ✅ EXACT Invoice Pattern: Complete upsert with satisfies constraint
        const order = await this.prismaService.order.upsert({
          where: { kiotVietId: BigInt(orderData.id) },
          update: {
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
            customerId: customer?.id ?? null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            discount: orderData.discount
              ? new Prisma.Decimal(orderData.discount)
              : null,
            discountRatio: orderData.discountRatio || null,
            status: orderData.status,
            statusValue: orderData.statusValue || null,
            description: orderData.description || null,
            usingCod: orderData.usingCod || false,
            saleChannelId: saleChannel?.id ?? null,
            expectedDelivery: orderData.expectedDelivery
              ? new Date(orderData.expectedDelivery)
              : null,
            retailerId: orderData.retailerId || null,
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING' as const,
          },
          create: {
            kiotVietId: BigInt(orderData.id),
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
            customerId: customer?.id ?? null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            discount: orderData.discount
              ? new Prisma.Decimal(orderData.discount)
              : null,
            discountRatio: orderData.discountRatio || null,
            status: orderData.status,
            statusValue: orderData.statusValue || null,
            description: orderData.description || null,
            usingCod: orderData.usingCod || false,
            saleChannelId: saleChannel?.id ?? null,
            expectedDelivery: orderData.expectedDelivery
              ? new Date(orderData.expectedDelivery)
              : null,
            retailerId: orderData.retailerId || null,
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            createdDate: orderData.createdDate
              ? new Date(orderData.createdDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING' as const,
          } satisfies Prisma.OrderUncheckedCreateInput,
        });

        // ============================================================================
        // SAVE ORDER DETAILS
        // ============================================================================
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

        if (orderData.payments && orderData.payments.length > 0) {
          for (const payment of orderData.payments) {
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

  // ============================================================================
  // LARKBASE INTEGRATION
  // ============================================================================

  private async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    if (orders.length === 0) return;

    try {
      await this.larkOrderSyncService.syncOrdersToLarkBase(orders);
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      // Don't throw error to prevent blocking database sync
    }
  }

  // ============================================================================
  // SYNC CONTROL UTILITIES
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
