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
  saleChannelName?: string;
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

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const runningOrderSyncs = await this.prismaService.syncControl.findMany({
        where: {
          OR: [
            { name: 'order_historical' },
            // { name: 'order_recent' },
            { name: 'order_lark_sync' },
          ],
          isRunning: true,
        },
      });

      if (runningOrderSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningOrderSyncs.length} Order syncs still running: ${runningOrderSyncs.map((s) => s.name).join(', ')}`,
        );
        this.logger.warn('Skipping order sync to avoid conflicts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_historical' },
      });

      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_recent' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical order sync...');
        await this.syncHistoricalOrders();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log(
          'Historical order sync is running, skipping recent sync',
        );
        return;
      }
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

    this.logger.log('Historical order sync enabled');
  }

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

      this.logger.log('Starting historical order sync...');

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
              `Pagination complete. Processed: ${processedCount}/${totalOrders} orders`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalOrders) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalOrders} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        const dateStart = new Date();
        dateStart.setDate(dateStart.getDate() - 70);
        const dateStartStr = dateStart.toISOString().split('T')[0];

        const dateEnd = new Date();
        dateEnd.setDate(dateEnd.getDate() + 1);
        const dateEndStr = dateEnd.toISOString().split('T')[0];

        try {
          const orderListResponse = await this.fetchOrdersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id',
            orderDirection: 'DESC',
            includePayment: true,
            includeOrderDelivery: true,
            lastModifiedFrom: dateStartStr,
            toDate: dateEndStr,
          });

          if (!orderListResponse) {
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

          const { total, data: orders } = orderListResponse;

          if (total !== undefined && total !== null) {
            if (totalOrders === 0) {
              this.logger.log(
                `Total orders detected: ${total}. Starting processing...`,
              );

              totalOrders = total;
            } else if (total !== totalOrders) {
              this.logger.warn(
                `Total count changed: ${totalOrders} -> ${total}. Using latest.`,
              );
              totalOrders = total;
            }
            lastValidTotal = total;
          }

          if (!orders || orders.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalOrders > 0 && currentItem >= totalOrders) {
              this.logger.log('Reached end of data (empty page past total)');
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

          const pageOrderIds = orders.map((order) => BigInt(order.id));
          const existingOrderIds = new Set(
            (
              await this.prismaService.order.findMany({
                where: { kiotVietId: { in: pageOrderIds } },
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newOrders = orders.filter((order) => {
            if (
              !existingOrderIds.has(order.id) &&
              !processedOrderIds.has(order.id)
            ) {
              processedOrderIds.add(order.id);
              return true;
            }
            return false;
          });

          const existingOrders = orders.filter((order) => {
            if (
              existingOrderIds.has(order.id) &&
              !processedOrderIds.has(order.id)
            ) {
              processedOrderIds.add(order.id);
              return true;
            }
            return false;
          });

          if (newOrders.length === 0 && existingOrders.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all orders already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedOrders: any[] = [];

          if (newOrders.length > 0) {
            this.logger.log(
              `Processing ${newOrders.length} NEW orders from page ${currentPage}...`,
            );

            const savedOrders = await this.saveOrdersToDatabase(newOrders);
            pageProcessedCount += savedOrders.length;
            allSavedOrders.push(...savedOrders);
          }

          if (existingOrders.length > 0) {
            this.logger.log(
              `Processing ${existingOrders.length} EXISTING orders from page ${currentPage}`,
            );

            const savedOrders = await this.saveOrdersToDatabase(existingOrders);
            pageProcessedCount += savedOrders.length;
            allSavedOrders.push(...savedOrders);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (allSavedOrders.length > 0) {
            try {
              await this.syncOrdersToLarkBase(allSavedOrders);
              this.logger.log(
                `Synced ${allSavedOrders.length} orders to LarkBase`,
              );
            } catch (error) {
              this.logger.warn(
                `LarkBase sync failed for page ${currentPage}: ${error.message}`,
              );
            }
          }

          if (totalOrders > 0) {
            const completionPercentage = (processedCount / totalOrders) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalOrders} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalOrders) {
              this.logger.log('All orders procesed successfully!');
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

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
        `Historical order sync completed: ${processedCount}/${totalOrders} (${completionRate.toFixed(1)}% completion rate)`,
      );
      this.logger.log(
        `AUTO-TRANSITION: Historical sync disabled, Recent sync enabled for future cycles`,
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

  async fetchOrdersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeOrderDelivery?: boolean;
      includePayment?: boolean;
      lastModifiedFrom?: string;
      toDate?: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchOrdersList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          this.logger.log(`Retrying after ${delay / 1000}s delay...`);
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
    lastModifiedFrom?: string;
    toDate?: string;
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

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }
    if (params.toDate) {
      queryParams.append('toDate', params.toDate);
    }

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
    const fromDateStr = fromDate.toISOString().split('T')[0];
    const today = new Date();
    const todayDateStr = today.toISOString().split('T')[0];

    const queryParams = new URLSearchParams({
      lastModifiedFrom: fromDateStr,
      toDate: todayDateStr,
      currentItem: '0',
      pageSize: '100',
      orderBy: 'createdDate',
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

  private async saveOrdersToDatabase(orders: any[]): Promise<any[]> {
    this.logger.log(`Saving ${orders.length} orders to database...`);

    const savedOrders: any[] = [];

    const customerIds = [
      ...new Set(
        orders.filter((o) => o.customerId).map((o) => BigInt(o.customerId)),
      ),
    ];
    const branchIds = [
      ...new Set(
        orders.filter((o) => o.branchId != null).map((o) => o.branchId),
      ),
    ];
    const userIds = [
      ...new Set(
        orders.filter((o) => o.soldById).map((o) => BigInt(o.soldById)),
      ),
    ];
    const saleChannelIds = [
      ...new Set(
        orders.filter((o) => o.SaleChannelId).map((o) => o.SaleChannelId),
      ),
    ];
    const productIds = [
      ...new Set(
        orders.flatMap((o) =>
          (o.orderDetails ?? []).map((d) => BigInt(d.productId)),
        ),
      ),
    ];
    const bankAccountIds = [
      ...new Set(
        orders.flatMap((o) =>
          (o.payments ?? []).filter((p) => p.accountId).map((p) => p.accountId),
        ),
      ),
    ];
    const surchargeIds = [
      ...new Set(
        orders.flatMap((o) =>
          (o.invoiceOrderSurcharges ?? [])
            .filter((s) => s.surchargeId)
            .map((s) => s.surchargeId),
        ),
      ),
    ];

    const [
      customers,
      branches,
      users,
      saleChannels,
      defaultSaleChannel,
      products,
      bankAccounts,
      surcharges,
    ] = await Promise.all([
      customerIds.length
        ? this.prismaService.customer.findMany({
            where: { kiotVietId: { in: customerIds } },
            select: { id: true, kiotVietId: true },
          })
        : [],
      branchIds.length
        ? this.prismaService.branch.findMany({
            where: { kiotVietId: { in: branchIds } },
            select: { id: true, name: true, kiotVietId: true },
          })
        : [],
      userIds.length
        ? this.prismaService.user.findMany({
            where: { kiotVietId: { in: userIds } },
            select: { kiotVietId: true },
          })
        : [],
      saleChannelIds.length
        ? this.prismaService.saleChannel.findMany({
            where: { kiotVietId: { in: saleChannelIds } },
            select: { id: true, name: true, kiotVietId: true },
          })
        : [],
      this.prismaService.saleChannel.findFirst({
        where: { id: 1 },
        select: { id: true, name: true },
      }),
      productIds.length
        ? this.prismaService.product.findMany({
            where: { kiotVietId: { in: productIds } },
            select: { id: true, name: true, code: true, kiotVietId: true },
          })
        : [],
      bankAccountIds.length
        ? this.prismaService.bankAccount.findMany({
            where: { kiotVietId: { in: bankAccountIds } },
            select: { id: true, kiotVietId: true },
          })
        : [],
      surchargeIds.length
        ? this.prismaService.surcharge.findMany({
            where: { kiotVietId: { in: surchargeIds } },
            select: { id: true, kiotVietId: true },
          })
        : [],
    ]);

    const customerMap = new Map(
      customers.map((c): [number, any] => [Number(c.kiotVietId), c]),
    );
    const branchMap = new Map(
      branches.map((b): [number, any] => [Number(b.kiotVietId), b]),
    );
    const userMap = new Map(
      users.map((u): [number, any] => [Number(u.kiotVietId), u]),
    );
    const saleChannelMap = new Map(
      saleChannels.map((s): [number, any] => [Number(s.kiotVietId), s]),
    );
    const productMap = new Map(
      products.map((p): [number, any] => [Number(p.kiotVietId), p]),
    );
    const bankAccountMap = new Map(
      bankAccounts.map((b): [number, any] => [Number(b.kiotVietId), b]),
    );
    const surchargeMap = new Map(
      surcharges.map((s): [number, any] => [Number(s.kiotVietId), s]),
    );

    const processOrder = async (orderData: any) => {
      try {
        const customer = orderData.customerId
          ? (customerMap.get(Number(orderData.customerId)) ?? null)
          : null;

        const branch = branchMap.get(Number(orderData.branchId)) ?? null;

        const soldBy = orderData.soldById
          ? (userMap.get(Number(orderData.soldById)) ?? null)
          : null;

        let saleChannel = orderData.SaleChannelId
          ? (saleChannelMap.get(Number(orderData.SaleChannelId)) ?? null)
          : null;

        if (!saleChannel) {
          saleChannel = defaultSaleChannel;
        }

        const orderCode = orderData.code;
        const shouldSyncToLark = orderCode && orderCode.includes('DH0');

        const order = await this.prismaService.order.upsert({
          where: { kiotVietId: BigInt(orderData.id) },
          update: {
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            cashierId: soldBy?.kiotVietId ?? null,
            customerId: customer?.id ?? null,
            customerCode: orderData.customerCode ?? null,
            customerName: orderData.customerName ?? null,
            saleChannelId: saleChannel?.id ?? 1,
            saleChannelName: saleChannel?.name,
            status: orderData.status,
            statusValue: orderData.statusValue ?? null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            retailerId: 310831,
            description: orderData.description ?? '',
            usingCod: orderData.usingCod ?? false,
            discount: orderData.discount ?? 0,
            discountRatio: orderData.discountRatio ?? 0,
            createdDate: orderData.createdDate
              ? new Date(orderData.createdDate)
              : new Date(),
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
          },
          create: {
            kiotVietId: BigInt(orderData.id),
            code: orderData.code,
            purchaseDate: new Date(orderData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            cashierId: soldBy?.kiotVietId ?? null,
            customerId: customer?.id ?? null,
            customerCode: orderData.customerCode ?? null,
            customerName: orderData.customerName ?? null,
            saleChannelId: saleChannel?.id ?? 1,
            saleChannelName: saleChannel?.name,
            status: orderData.status,
            statusValue: orderData.statusValue ?? null,
            total: new Prisma.Decimal(orderData.total || 0),
            totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
            retailerId: 310831,
            description: orderData.description ?? '',
            usingCod: orderData.usingCod ?? false,
            discount: orderData.discount ?? 0,
            discountRatio: orderData.discountRatio ?? 0,
            createdDate: orderData.createdDate
              ? new Date(orderData.createdDate)
              : new Date(),
            modifiedDate: orderData.modifiedDate
              ? new Date(orderData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
          },
        });

        if (orderData.orderDetails && orderData.orderDetails.length > 0) {
          for (let i = 0; i < orderData.orderDetails.length; i++) {
            const detail = orderData.orderDetails[i];
            const product = productMap.get(Number(detail.productId)) ?? null;

            if (product) {
              await this.prismaService.orderDetail.upsert({
                where: {
                  orderId_lineNumber: {
                    orderId: order.id,
                    lineNumber: i + 1,
                  },
                },
                update: {
                  productId: product.id,
                  productName: product.name,
                  productCode: product.code,
                  quantity: detail.quantity ?? 0,
                  lineNumber: i + 1,
                  price: detail.price ? new Prisma.Decimal(detail.price) : 0,
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : 0,
                  discountRatio: detail.discountRatio ?? 0,
                  note: detail.note ?? '',
                  isMaster: detail.isMaster ?? true,
                },
                create: {
                  orderId: order.id,
                  productId: product.id,
                  productName: product.name,
                  productCode: product.code,
                  quantity: detail.quantity ?? 0,
                  lineNumber: i + 1,
                  price: detail.price ? new Prisma.Decimal(detail.price) : 0,
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : 0,
                  discountRatio: detail.discountRatio ?? 0,
                  note: detail.note ?? '',
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
              price: detail.price ? new Prisma.Decimal(detail.price) : 0,
              receiver: detail.receiver ?? '',
              contactNumber: detail.contactNumber ?? '',
              address: detail.address ?? '',
              locationId: detail.locationId ?? null,
              locationName: detail.locationName ?? '',
              wardName: detail.wardName ?? '',
              weight: detail.weight ?? 0,
              length: detail.length ?? 0,
              width: detail.width ?? 0,
              height: detail.height ?? 0,
            },
            create: {
              orderId: order.id,
              deliveryCode: detail.deliveryCode,
              type: detail.type,
              price: detail.price ? new Prisma.Decimal(detail.price) : 0,
              receiver: detail.receiver ?? '',
              contactNumber: detail.contactNumber ?? '',
              address: detail.address ?? '',
              locationId: detail.locationId ?? null,
              locationName: detail.locationName ?? '',
              wardName: detail.wardName ?? '',
              weight: detail.weight ?? 0,
              length: detail.length ?? 0,
              width: detail.width ?? 0,
              height: detail.height ?? 0,
            },
          });
        }

        if (orderData.payments && orderData.payments.length > 0) {
          for (const payment of orderData.payments) {
            const bankAccount = payment.accountId
              ? (bankAccountMap.get(Number(payment.accountId)) ?? null)
              : null;

            await this.prismaService.payment.upsert({
              where: {
                kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0),
              },
              update: {
                code: payment.code,
                amount: payment.amount ? new Prisma.Decimal(payment.amount) : 0,
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

        if (
          orderData.invoiceOrderSurcharges &&
          orderData.invoiceOrderSurcharges.length > 0
        ) {
          for (const surcharge of orderData.invoiceOrderSurcharges) {
            const surchargeRecord = surcharge.surchargeId
              ? (surchargeMap.get(Number(surcharge.surchargeId)) ?? null)
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

        return order;
      } catch (error) {
        this.logger.error(
          `❌ Failed to save order ${orderData.code}: ${error.message}`,
        );
        return null;
      }
    };

    const CONCURRENCY = 8;
    for (let i = 0; i < orders.length; i += CONCURRENCY) {
      const chunk = orders.slice(i, i + CONCURRENCY);
      const saved = await Promise.all(chunk.map(processOrder));
      savedOrders.push(...saved.filter((o) => o));
    }

    this.logger.log(`💾 Saved ${savedOrders.length} orders to database`);
    return savedOrders;
  }

  private async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    try {
      this.logger.log(`Starting LarkBase sync for ${orders.length} orders...`);

      const ordersToSync = orders.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (ordersToSync.length === 0) {
        this.logger.log('No orders need LarkBase sync');
        return;
      }

      await this.larkOrderSyncService.syncPendingAndFailed();
      this.logger.log(`LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`STOPPING sync to prevent data duplication`);

      const orderIds = orders.map((c) => c.id);
      await this.prismaService.order.updateMany({
        where: { id: { in: orderIds } },
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
