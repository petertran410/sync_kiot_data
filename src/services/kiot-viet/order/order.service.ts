// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkOrderSyncService } from '../../lark/order/lark-order-sync.service';
import { Prisma } from '@prisma/client';

interface KiotVietOrder {
  id: number;
  code: string;
  purchaseDate: string;
  branchId?: number;
  branchName?: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  soldById?: number;
  soldByName?: string;
  description?: string;
  total?: number;
  totalPayment?: number;
  discountRatio?: number;
  discount?: number;
  statusValue: string;
  usingCod?: boolean;
  status?: number;
  retailerId: number;
  modifiedDate?: string;
  createdDate?: string;
  saleChannelId?: number;
  orderDetails: {
    productId: number;
    productCode?: string;
    productName?: string;
    isMaster?: boolean;
    quantity: number;
    price: number;
    discount?: number;
    discountRatio?: number;
    note?: string;
  };
  orderDelivery?: {
    deliveryCode?: string;
    type?: number;
    price?: number;
    receiver?: string;
    contactNumber?: string;
    address?: string;
    locationId?: number;
    locationName?: string;
    weight?: number;
    length?: number;
    width?: number;
    height?: number;
    partnerDeliveryId?: number;
    partnerDelivery?: {
      code?: string;
      name?: string;
      address?: string;
      contactNumber?: string;
      email?: string;
    };
  };
  payments?: Array<{
    id?: number;
    code?: string;
    amount: number;
    method: string;
    status?: number;
    statusValue?: string;
    transDate: string;
    accountId?: number;
    bankAcount?: string;
  }>;
  invoiceOrderSurcharges?: Array<{
    id?: number;
    invoiceId?: number;
    surchargeId?: number;
    surchargeName?: string;
    surValue?: number;
    price?: number;
    createdDate?: string;
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
        await this.syncRecentOrders(3);
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent order sync...');
      await this.syncRecentOrders(3);
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
    let processedOrderIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical order sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalOrders > 0) {
          if (currentItem >= totalOrders) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalOrders} orders`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalOrders) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalOrders} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const orderListResponse = await this.fetchOrdersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includePayment: true,
            includeOrderDelivery: true,
          });

          if (!orderListResponse) {
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

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: orders } = orderListResponse;

          if (total !== undefined && total !== null) {
            if (totalOrders === 0) {
              totalOrders = total;
              this.logger.log(`📊 Total orders detected: ${totalOrders}`);
            } else if (total !== totalOrders) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalOrders} -> ${total}. Using latest.`,
              );
              totalOrders = total;
            }
            lastValidTotal = total;
          }

          if (!orders || orders.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalOrders > 0 && currentItem >= totalOrders) {
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

          const newOrders = orders.filter((order) => {
            if (processedOrderIds.has(order.id)) {
              this.logger.debug(
                `⚠️ Duplicate order ID detected: ${order.id} (${order.code})`,
              );
              return false;
            }
            processedOrderIds.add(order.id);
            return true;
          });

          if (newOrders.length !== orders.length) {
            this.logger.warn(
              `🔄 Filtered out ${orders.length - newOrders.length} duplicate orders on page ${currentPage}`,
            );
          }

          if (newOrders.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all orders already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newOrders.length} orders from page ${currentPage}...`,
          );

          const ordersWithDetails =
            await this.enrichOrdersWithDetails(newOrders);
          const savedOrders =
            await this.saveOrdersToDatabase(ordersWithDetails);

          processedCount += savedOrders.length;
          currentItem += this.PAGE_SIZE;

          if (totalOrders > 0) {
            const completionPercentage = (processedCount / totalOrders) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalOrders} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalOrders) {
              this.logger.log('🎉 All orders processed successfully!');
              break;
            }
          }

          if (savedOrders.length > 0) {
            try {
              await this.syncOrdersToLarkBase(savedOrders);
              this.logger.log(
                `🚀 Synced ${savedOrders.length} orders to LarkBase`,
              );
            } catch (larkError) {
              this.logger.warn(
                `⚠️ LarkBase sync failed for page ${currentPage}: ${larkError.message}`,
              );
            }
          }

          if (totalOrders > 0) {
            if (
              currentItem >= totalOrders &&
              processedCount >= totalOrders * 0.95
            ) {
              this.logger.log(
                '✅ Sync completed - reached expected data range',
              );
              break;
            }
          }

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

  async syncRecentOrders(days: number = 3): Promise<void> {
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
        this.logger.log('📋 No recent orders updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(`📊 Processing ${recentOrders.length} recent orders`);

      const ordersWithDetails =
        await this.enrichOrdersWithDetails(recentOrders);
      const savedOrders = await this.saveOrdersToDatabase(ordersWithDetails);
      await this.syncOrdersToLarkBase(savedOrders);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log(
        `✅ Recent order sync completed: ${ordersWithDetails.length} orders processed`,
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
    maxRetries: number = 5, // Keep existing retry count
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
          const delay = 2000 * attempt;
          this.logger.log(`⏳ Retrying after ${delay / 1000}s delay...`);
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
        timeout: 45000,
      }),
    );

    return response.data;
  }

  async fetchRecentOrders(fromDate: Date): Promise<any[]> {
    const headers = await this.authService.getRequestHeaders();
    const fromDateStr = fromDate.toISOString();

    const queryParams = new URLSearchParams({
      lastModifiedFrom: fromDateStr,
      currentItem: '0',
      pageSize: '100',
      orderBy: 'purchaseDate',
      orderDirection: 'DESC',
      includeOrderDelivery: 'true',
      includePayment: 'true',
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/orders?${queryParams}`, {
        headers,
        timeout: 60000,
      }),
    );

    return response.data?.data;
  }

  private async enrichOrdersWithDetails(
    orders: KiotVietOrder[],
  ): Promise<KiotVietOrder[]> {
    this.logger.log(`🔍 Enriching ${orders.length} orders with details...`);

    const enrichedOrders: any[] = [];
    for (const order of orders) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/orders/${order.id}`, {
            headers,
          }),
        );
        if (response.data) {
          enrichedOrders.push(response.data);
        } else {
          enrichedOrders.push(order);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
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
    this.logger.log(`💾 Saving ${orders.length} orders to database...`);

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
            saleChannelId: saleChannel?.id ?? null,
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
            saleChannelId: saleChannel?.id ?? null,
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
              select: { id: true, name: true, code: true },
            });

            if (product) {
              await this.prismaService.orderDetail.upsert({
                where: {
                  orderId: order.id,
                },
                update: {
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
                create: {
                  orderId: order.id,
                  productId: product.id,
                  productName: product.name,
                  productCode: product.code,
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

        if (orderData.orderDelivery) {
          const detail = orderData.orderDelivery;
          await this.prismaService.orderDelivery.upsert({
            where: { orderId: order.id },
            update: {
              deliveryCode: detail.deliveryCode,
              type: detail.type,
              price: detail.price ? new Prisma.Decimal(detail.price) : null,
              receiver: detail.receiver,
              contactNumber: detail.contactNumber,
              address: detail.address,
              locationId: detail.locationId,
              locationName: detail.locationName,
              wardName: detail.wardName,
              weight: detail.weight,
              length: detail.length,
              width: detail.width,
              height: detail.height,
            },
            create: {
              orderId: order.id,
              deliveryCode: detail.deliveryCode,
              type: detail.type,
              price: detail.price ? new Prisma.Decimal(detail.price) : null,
              receiver: detail.receiver,
              contactNumber: detail.contactNumber,
              address: detail.address,
              locationId: detail.locationId,
              locationName: detail.locationName,
              wardName: detail.wardName,
              weight: detail.weight,
              length: detail.length,
              width: detail.width,
              height: detail.height,
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
                accountId: bankAccount?.id ?? null,
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
                accountId: bankAccount?.id ?? null,
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
          `❌ Failed to save order ${orderData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${savedOrders.length} orders to database`);
    return savedOrders;
  }

  private async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${orders.length} orders...`,
      );

      const ordersToSync = orders.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (ordersToSync.length === 0) {
        this.logger.log('No orders need LarkBase sync');
        return;
      }

      await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`🛑 STOPPING sync to prevent data duplication`);

      const invoiceIds = orders.map((c) => c.id);
      await this.prismaService.invoice.updateMany({
        where: { id: { in: invoiceIds } },
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
        entities: ['order'],
        syncMode: name.includes('historical') ? 'historical' : 'recent',
        ...updates,
      },
      update: updates,
    });
  }
}
