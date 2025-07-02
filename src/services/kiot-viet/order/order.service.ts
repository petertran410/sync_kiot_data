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

    // Declare all variables at function scope
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

          const { total, data: orders } = orderListResponse;

          if (total !== undefined && total !== null) {
            totalOrders = total;
            lastValidTotal = total;
          } else if (lastValidTotal > 0) {
            totalOrders = lastValidTotal;
          }

          this.logger.log(`📊 Total orders in system: ${totalOrders}`);

          // EMPTY PAGE DETECTION
          if (!orders || orders.length === 0) {
            consecutiveEmptyPages++;
            this.logger.warn(
              `⚠️ Empty page received. Count: ${consecutiveEmptyPages + 1}`,
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

            if (currentItem < totalOrders * 0.9) {
              currentItem += this.PAGE_SIZE;
            }
            continue;
          }

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

          // Add new order IDs to processed set
          newOrders.forEach((invoice) => processedOrderIds.add(invoice.id));

          this.logger.log(
            `📊 Processing ${newOrders.length} orders (Page: ${currentPage}, Processed: ${processedCount}/${totalOrders})`,
          );

          // Enrich with details
          this.logger.log(
            `🔍 Enriching ${newOrders.length} orders with details...`,
          );

          const enrichedOrders = await this.enrichOrdersWithDetails(newOrders);

          this.logger.log(
            `💾 Saving ${enrichedOrders.length} orders to database...`,
          );

          const savedOrders = await this.saveOrdersToDatabase(enrichedOrders);

          // PROCESS ORDERS
          for (const orderData of orders) {
            try {
              await this.processAndSaveOrder(orderData);
              processedCount++;

              if (processedCount % 50 === 0) {
                this.logger.log(
                  `✅ Processed ${processedCount} orders so far...`,
                );
                await this.updateSyncControl(syncName, {
                  progress: Math.min((processedCount / totalOrders) * 100, 100),
                  processedItems: processedCount,
                  currentItem,
                });
              }
            } catch (error) {
              this.logger.error(
                `❌ Failed to process order ${orderData.id}: ${error.message}`,
              );
            }
          }

          // COMPLETION CHECK: Multiple criteria
          if (
            processedCount >= totalOrders &&
            totalOrders > MIN_EXPECTED_ORDERS
          ) {
            this.logger.log(
              `✅ Processed all expected orders: ${processedCount}/${totalOrders}`,
            );
            break;
          }

          currentItem += this.PAGE_SIZE;

          // Add delay to respect rate limits
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

      this.logger.log(
        `🎉 Historical order sync completed! Processed: ${processedCount} orders`,
      );

      // Mark historical sync as completed
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        progress: 100,
        processedItems: processedCount,
        totalItems: totalOrders,
      });

      // Trigger Lark sync
      this.logger.log('🔄 Starting Lark order sync...');
      const pendingOrders = await this.prismaService.order.findMany({
        where: { larkSyncStatus: 'PENDING' },
        take: 1000,
      });
      await this.larkOrderSyncService.syncOrdersToLarkBase(pendingOrders);
    } catch (error) {
      this.logger.error(`💥 Historical order sync failed: ${error.message}`);
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'error',
        error: error.message,
        lastErrorAt: new Date(),
      });
      throw error;
    }
  }

  // ============================================================================
  // RECENT SYNC - OPTIMIZED FOR DAILY OPERATIONS
  // ============================================================================

  async syncRecentOrders(days: number = 7): Promise<void> {
    const syncName = 'order_recent';
    let recentOrders: any[] = [];
    let savedOrders: any[] = [];

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      this.logger.log(
        `🔄 Starting recent order sync (${days} days from ${fromDate.toISOString()})...`,
      );

      // Fetch recent orders with proper date handling
      recentOrders = await this.fetchRecentOrders(fromDate);

      this.logger.log(
        `📦 Found ${recentOrders.length} recent orders to process`,
      );

      // Process each order
      for (const orderData of recentOrders) {
        try {
          await this.processAndSaveOrder(orderData);
          savedOrders.push(orderData);
        } catch (error) {
          this.logger.error(
            `❌ Failed to save order ${orderData.id}: ${error.message}`,
          );
        }
      }

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

  private async fetchOrderPage(currentItem: number): Promise<any> {
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
            orderBy: 'id',
            orderDirection: 'Asc',
          },
          timeout: 30000,
        }),
      );

      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch order page: ${error.message}`);
      throw error;
    }
  }

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
      this.logger.error(`Failed to fetch recent orders page: ${error.message}`);
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
          `Failed to enrich invoice ${order.code}: ${error.message}`,
        );
        enrichedOrders.push(order);
      }
    }

    return enrichedOrders;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  private async saveOrdersToDatabase(orders: any[]): Promise<any[]> {
    const savedOrders: any[] = [];

    for (const orderData of orders) {
      try {
      } catch (error) {}
    }
  }

  // ============================================================================
  // DATA PROCESSING METHODS
  // ============================================================================

  private async processAndSaveOrder(orderData: KiotVietOrder): Promise<void> {
    try {
      // Resolve foreign key relationships
      const branch = orderData.branchId
        ? await this.prismaService.branch.findFirst({
            where: { kiotVietId: orderData.branchId },
          })
        : null;

      const soldBy = orderData.soldById
        ? await this.prismaService.user.findFirst({
            where: { kiotVietId: BigInt(orderData.soldById) },
          })
        : null;

      const customer = orderData.customerId
        ? await this.prismaService.customer.findFirst({
            where: { kiotVietId: BigInt(orderData.customerId) },
          })
        : null;

      const saleChannel = orderData.saleChannelId
        ? await this.prismaService.saleChannel.findFirst({
            where: { kiotVietId: orderData.saleChannelId },
          })
        : null;

      // Save main order
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
          makeInvoice: orderData.makeInvoice || false,
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
          makeInvoice: orderData.makeInvoice || false,
          retailerId: orderData.retailerId || null,
          modifiedDate: orderData.modifiedDate
            ? new Date(orderData.modifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING' as const,
        },
      });

      // Save order details
      if (orderData.orderDetails && orderData.orderDetails.length > 0) {
        await this.saveOrderDetails(order.id, orderData.orderDetails);
      }

      // Save order delivery
      if (orderData.orderDelivery) {
        await this.saveOrderDelivery(order.id, orderData.orderDelivery);
      }

      // Save payments
      if (orderData.payments && orderData.payments.length > 0) {
        await this.saveOrderPayments(order.id, orderData.payments);
      }

      // Save surcharges
      if (
        orderData.invoiceOrderSurcharges &&
        orderData.invoiceOrderSurcharges.length > 0
      ) {
        await this.saveOrderSurcharges(
          order.id,
          orderData.invoiceOrderSurcharges,
        );
      }

      this.logger.debug(
        `✅ Processed order ${orderData.code} (ID: ${orderData.id})`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to process order ${orderData.id}: ${error.message}`,
      );
      throw error;
    }
  }

  private async saveOrderDetails(
    orderId: number,
    details: any[],
  ): Promise<void> {
    for (const detail of details) {
      const product = await this.prismaService.product.findFirst({
        where: { kiotVietId: BigInt(detail.productId) },
      });

      if (!product) {
        this.logger.warn(`Product not found for ID: ${detail.productId}`);
        continue;
      }

      await this.prismaService.orderDetail.upsert({
        where: {
          kiotVietId: detail.id ? BigInt(detail.id) : BigInt(-1),
        },
        update: {
          orderId,
          productId: product.id,
          quantity: detail.quantity,
          price: new Prisma.Decimal(detail.price || 0),
          discount: detail.discount
            ? new Prisma.Decimal(detail.discount)
            : null,
          discountRatio: detail.discountRatio || null,
          note: detail.note || null,
          isMaster: detail.isMaster ?? true,
        },
        create: {
          kiotVietId: detail.id ? BigInt(detail.id) : null,
          orderId,
          productId: product.id,
          quantity: detail.quantity,
          price: new Prisma.Decimal(detail.price || 0),
          discount: detail.discount
            ? new Prisma.Decimal(detail.discount)
            : null,
          discountRatio: detail.discountRatio || null,
          note: detail.note || null,
          isMaster: detail.isMaster ?? true,
        },
      });
    }
  }

  private async saveOrderDelivery(
    orderId: number,
    delivery: any,
  ): Promise<void> {
    await this.prismaService.orderDelivery.upsert({
      where: { orderId },
      update: {
        deliveryCode: delivery.deliveryCode || null,
        type: delivery.type || null,
        price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
        receiver: delivery.receiver || null,
        contactNumber: delivery.contactNumber || null,
        address: delivery.address || null,
        locationId: delivery.locationId || null,
        locationName: delivery.locationName || null,
        wardName: delivery.wardName || null,
        weight: delivery.weight || null,
        length: delivery.length || null,
        width: delivery.width || null,
        height: delivery.height || null,
        partnerDeliveryId: delivery.partnerDeliveryId
          ? BigInt(delivery.partnerDeliveryId)
          : null,
      },
      create: {
        orderId,
        deliveryCode: delivery.deliveryCode || null,
        type: delivery.type || null,
        price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
        receiver: delivery.receiver || null,
        contactNumber: delivery.contactNumber || null,
        address: delivery.address || null,
        locationId: delivery.locationId || null,
        locationName: delivery.locationName || null,
        wardName: delivery.wardName || null,
        weight: delivery.weight || null,
        length: delivery.length || null,
        width: delivery.width || null,
        height: delivery.height || null,
        partnerDeliveryId: delivery.partnerDeliveryId
          ? BigInt(delivery.partnerDeliveryId)
          : null,
      },
    });
  }

  private async saveOrderPayments(
    orderId: number,
    payments: any[],
  ): Promise<void> {
    for (const payment of payments) {
      const bankAccount = payment.accountId
        ? await this.prismaService.bankAccount.findFirst({
            where: { kiotVietId: payment.accountId },
          })
        : null;

      await this.prismaService.payment.upsert({
        where: {
          kiotVietId: payment.id ? BigInt(payment.id) : BigInt(-1),
        },
        update: {
          code: payment.code || null,
          amount: new Prisma.Decimal(payment.amount || 0),
          method: payment.method,
          status: payment.status || null,
          transDate: new Date(payment.transDate),
          accountId: bankAccount?.id ?? null,
          orderId,
          description: payment.description || null,
        },
        create: {
          kiotVietId: payment.id ? BigInt(payment.id) : null,
          code: payment.code || null,
          amount: new Prisma.Decimal(payment.amount || 0),
          method: payment.method,
          status: payment.status || null,
          transDate: new Date(payment.transDate),
          accountId: bankAccount?.id ?? null,
          orderId,
          description: payment.description || null,
        },
      });
    }
  }

  private async saveOrderSurcharges(
    orderId: number,
    surcharges: any[],
  ): Promise<void> {
    for (const surcharge of surcharges) {
      const surchargeEntity = surcharge.surchargeId
        ? await this.prismaService.surcharge.findFirst({
            where: { kiotVietId: surcharge.surchargeId },
          })
        : null;

      await this.prismaService.orderSurcharge.upsert({
        where: {
          kiotVietId: surcharge.id ? BigInt(surcharge.id) : BigInt(-1),
        },
        update: {
          orderId,
          surchargeId: surchargeEntity?.id ?? null,
          surchargeName: surcharge.surchargeName || null,
          surValue: surcharge.surValue
            ? new Prisma.Decimal(surcharge.surValue)
            : null,
          price: surcharge.price ? new Prisma.Decimal(surcharge.price) : null,
        },
        create: {
          kiotVietId: surcharge.id ? BigInt(surcharge.id) : null,
          orderId,
          surchargeId: surchargeEntity?.id ?? null,
          surchargeName: surcharge.surchargeName || null,
          surValue: surcharge.surValue
            ? new Prisma.Decimal(surcharge.surValue)
            : null,
          price: surcharge.price ? new Prisma.Decimal(surcharge.price) : null,
        },
      });
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateSyncControl(name: string, data: any): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      update: data,
      create: { name, ...data },
    });
  }
}
