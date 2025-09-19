import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { async, firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';
import { LarkPurchaseOrderSyncService } from 'src/services/lark/purchase-order/lark-purchase-order-sync.service';

interface KiotVietPurchaseOrder {
  id: number;
  retailerId: number;
  code: string;
  description?: string;
  branchId?: number;
  branchName?: string;
  purchaseDate?: string;
  discount?: number;
  discountRatio?: number;
  total: number;
  totalPayment: number;
  status: number;
  createdDate?: string;
  supplierId: number;
  supplierName: string;
  supplierCode: string;
  purchaseById: number;
  purchaseName: string;
  exReturnSuppliers: number;
  exReturnThirdParty: number;
  purchaseOrderDetails: Array<{
    purchaseOrderId: number;
    productId: number;
    lineNumber: number;
    productCode: string;
    quantity: number;
    price: number;
    uniqueKey?: string;
    disount: number;
  }>;
  payments: Array<{
    id: number;
    purchaseOrderId: number;
    code: string;
    amount: number;
    method: string;
    status: number;
    statusValue: string;
    transDate: string;
  }>;
}

@Injectable()
export class KiotVietPurchaseOrderService {
  private readonly logger = new Logger(KiotVietPurchaseOrderService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const runningPurchaseOrderSyncs =
        await this.prismaService.syncControl.findMany({
          where: {
            OR: [
              { name: 'purchase_order_historical' },
              { name: 'purchase_order_lark_sync' },
            ],
            isRunning: true,
          },
        });

      if (runningPurchaseOrderSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningPurchaseOrderSyncs.length} PurchaseOrders sync still running: ${runningPurchaseOrderSyncs.map((s) => s.name).join(', ')}`,
        );
        this.logger.warn('Skipping purchase order sync to avoid conflicts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical purchase order sync...');
        await this.syncHistoricalPurchaseOrder();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log('Historical purchase_order sync is running');
        return;
      }

      this.logger.log('Running default historical purchase_order sync...');
      await this.syncHistoricalPurchaseOrder();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('purchase_order_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('Historical purchase_order sync enabled');
  }

  async syncHistoricalPurchaseOrder(): Promise<void> {
    const syncName = 'purchase_order_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalPurchaseOrder = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedPurchaseOrderIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('Starting historical purchase_order sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalPurchaseOrder > 0) {
          if (currentItem >= totalPurchaseOrder) {
            this.logger.log(
              `Pagination complete. Processed ${processedCount}/${totalPurchaseOrder} purchase_orders`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalPurchaseOrder) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalPurchaseOrder} - ${progressPercentage.toFixed(1)}%)`,
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
          const response = await this.fetchPurchaseOrdersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            includePayment: true,
            includeOrderDelivery: true,
            fromPurchaseDate: '2024-12-1',
            toPurchaseDate: dateEndStr,
          });

          if (!response) {
            this.logger.warn('Received null response from KiotViet API');

            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { data: purchase_orders, total } = response;

          if (total !== undefined && total !== null) {
            if (totalPurchaseOrder === 0) {
              this.logger.log(
                `Total purchase_orders detected: ${total}. Starting processing...`,
              );

              totalPurchaseOrder = total;
            } else if (total !== totalPurchaseOrder) {
              this.logger.warn(
                `Total count changed: ${totalPurchaseOrder} → ${total}. Using latest.`,
              );
              totalPurchaseOrder = total;
            }
            lastValidTotal = total;
          }

          if (!purchase_orders || purchase_orders.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalPurchaseOrder > 0 && currentItem >= totalPurchaseOrder) {
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

          const existingPurchaseOrderIds = new Set(
            (
              await this.prismaService.purchaseOrder.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newPurchaseOrders = purchase_orders.filter((purchase_order) => {
            if (
              !existingPurchaseOrderIds.has(purchase_order.id) &&
              !processedPurchaseOrderIds.has(purchase_order.id)
            ) {
              processedPurchaseOrderIds.add(purchase_order.id);
              return true;
            }
            return false;
          });

          const existingPurchaseOrders = purchase_orders.filter(
            (purchase_order) => {
              if (
                existingPurchaseOrderIds.has(purchase_order.id) &&
                !processedPurchaseOrderIds.has(purchase_order.id)
              ) {
                processedPurchaseOrderIds.add(purchase_order.id);
                return true;
              }
              return false;
            },
          );

          if (
            newPurchaseOrders.length === 0 &&
            existingPurchaseOrders.length === 0
          ) {
            this.logger.log(
              `Skipping page ${currentPage} - all purchase_orders already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedPurchaseOrders: any[] = [];

          if (newPurchaseOrders.length > 0) {
            this.logger.log(
              `Processing ${newPurchaseOrders.length} NEW purchase_order from page ${currentPage}`,
            );

            const savedPurchaseOrders =
              await this.savePurchaseOrderToDatabase(newPurchaseOrders);
            pageProcessedCount += savedPurchaseOrders.length;
            allSavedPurchaseOrders.push(...savedPurchaseOrders);
          }

          if (existingPurchaseOrders.length > 0) {
            this.logger.log(
              `Processing ${existingPurchaseOrders.length} EXISTING purchase_order from page ${currentPage}`,
            );

            const savedPurchaseOrders = await this.savePurchaseOrderToDatabase(
              existingPurchaseOrders,
            );
            pageProcessedCount += savedPurchaseOrders.length;
            allSavedPurchaseOrders.push(...savedPurchaseOrders);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (totalPurchaseOrder > 0) {
            const completionPercentage =
              (processedCount / totalPurchaseOrder) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalPurchaseOrder} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalPurchaseOrder) {
              this.logger.log('All purchase_orders processed successfully!');
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
        progress: { processedCount, expectedTotal: totalPurchaseOrder },
      });

      const completionRate =
        totalPurchaseOrder > 0
          ? (processedCount / totalPurchaseOrder) * 100
          : 100;

      this.logger.log(
        `Historical purchase_order sync completed: ${processedCount}/${totalPurchaseOrder} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(
        `Historical purchase_order sync failed: ${error.message}`,
      );

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalPurchaseOrder },
      });

      throw error;
    }
  }

  async fetchPurchaseOrdersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      includePayment?: boolean;
      includeOrderDelivery?: boolean;
      fromPurchaseDate?: string;
      toPurchaseDate?: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchPurchaseOrdersList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchPurchaseOrdersList(params: {
    currentItem?: number;
    pageSize?: number;
    includePayment?: boolean;
    includeOrderDelivery?: boolean;
    fromPurchaseDate?: string;
    toPurchaseDate?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      includePayment: (params.includePayment || true).toString(),
      includeOrderDelivery: (params.includeOrderDelivery || true).toString(),
    });

    if (params.fromPurchaseDate) {
      queryParams.append('fromPurchaseDate', params.fromPurchaseDate);
    }

    if (params.toPurchaseDate) {
      queryParams.append('toPurchaseDate', params.toPurchaseDate);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/purchaseorders?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichPurchaseOrdersWithDetails(
    purchase_orders: KiotVietPurchaseOrder[],
  ): Promise<KiotVietPurchaseOrder[]> {
    this.logger.log(
      `🔍 Enriching ${purchase_orders.length} purchase_orders with details...`,
    );

    const enrichedPurchaseOrders: KiotVietPurchaseOrder[] = [];

    for (const purchase_order of purchase_orders) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const queryParams = new URLSearchParams({
          includePayment: 'true',
          includeOrderDelivery: 'true',
        });

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/purchaseorders/${purchase_order.id}?${queryParams}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedPurchaseOrders.push(response.data);
        } else {
          enrichedPurchaseOrders.push(purchase_order);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich purchase_order ${purchase_order.code}: ${error.message}`,
        );

        enrichedPurchaseOrders.push(purchase_order);
      }
    }
    return enrichedPurchaseOrders;
  }

  private async savePurchaseOrderToDatabase(
    purchase_orders: KiotVietPurchaseOrder[],
  ): Promise<any[]> {
    this.logger.log(
      `Saving ${purchase_orders.length} purchase_orders to database...`,
    );

    const savedPurchaseOrders: any[] = [];

    for (const purchaseOrderData of purchase_orders) {
      try {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: purchaseOrderData.branchId },
          select: {
            id: true,
            name: true,
          },
        });

        const supplier = await this.prismaService.supplier.findFirst({
          where: { kiotVietId: purchaseOrderData.supplierId },
          select: {
            id: true,
            name: true,
            code: true,
          },
        });

        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: purchaseOrderData.purchaseById },
          select: {
            id: true,
            userName: true,
          },
        });

        const purchase_order = await this.prismaService.purchaseOrder.upsert({
          where: { kiotVietId: BigInt(purchaseOrderData.id) },
          update: {
            code: purchaseOrderData.code.trim(),
            retailerId: purchaseOrderData.retailerId ?? null,
            description: purchaseOrderData.description || '',
            branchId: branch?.id,
            branchName: branch?.name,
            purchaseDate: purchaseOrderData.purchaseDate
              ? new Date(purchaseOrderData.purchaseDate)
              : new Date(),
            discount: purchaseOrderData.discount ?? null,
            discountRatio: purchaseOrderData.discountRatio ?? null,
            total: new Prisma.Decimal(purchaseOrderData.total || 0),
            totalPayment: new Prisma.Decimal(
              purchaseOrderData.totalPayment || 0,
            ),
            status: purchaseOrderData.status ?? null,
            createdDate: purchaseOrderData.createdDate
              ? new Date(purchaseOrderData.createdDate)
              : new Date(),
            supplierId: supplier?.id,
            supplierName: supplier?.name,
            supplierCode: supplier?.code,
            purchaseById: user?.id,
            purchaseName: user?.userName,
            exReturnSuppliers: Number(purchaseOrderData.exReturnSuppliers || 0),
            exReturnThirdParty: Number(
              purchaseOrderData.exReturnThirdParty || 0,
            ),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
          create: {
            kiotVietId: BigInt(purchaseOrderData.id),
            code: purchaseOrderData.code.trim(),
            retailerId: purchaseOrderData.retailerId ?? null,
            description: purchaseOrderData.description || '',
            branchId: branch?.id ?? null,
            branchName: branch?.name,
            purchaseDate: purchaseOrderData.purchaseDate
              ? new Date(purchaseOrderData.purchaseDate)
              : new Date(),
            discount: purchaseOrderData.discount ?? null,
            discountRatio: purchaseOrderData.discountRatio ?? null,
            total: new Prisma.Decimal(purchaseOrderData.total || 0),
            totalPayment: new Prisma.Decimal(
              purchaseOrderData.totalPayment || 0,
            ),
            status: purchaseOrderData.status ?? null,
            createdDate: purchaseOrderData.createdDate
              ? new Date(purchaseOrderData.createdDate)
              : new Date(),
            supplierId: supplier?.id,
            supplierName: supplier?.name,
            supplierCode: supplier?.code,
            purchaseById: user?.id,
            purchaseName: user?.userName,
            exReturnSuppliers: Number(purchaseOrderData.exReturnSuppliers || 0),
            exReturnThirdParty: Number(
              purchaseOrderData.exReturnThirdParty || 0,
            ),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
        });

        if (
          purchaseOrderData.purchaseOrderDetails &&
          purchaseOrderData.purchaseOrderDetails.length > 0
        ) {
          for (
            let i = 0;
            i < purchaseOrderData.purchaseOrderDetails.length;
            i++
          ) {
            const detail = purchaseOrderData.purchaseOrderDetails[i];
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(detail.productId) },
              select: { id: true, code: true, name: true },
            });

            const acsNumber: number = i + 1;

            if (product) {
              await this.prismaService.purchaseOrderDetail.upsert({
                where: {
                  purchaseOrderId_lineNumber: {
                    purchaseOrderId: purchase_order.id,
                    lineNumber: i + 1,
                  },
                },
                update: {
                  purchaseOrderCode: purchase_order.code,
                  productId: product.id,
                  productCode: product.code,
                  lineNumber: i + 1,
                  productName: product.name,
                  quantity: detail.quantity,
                  uniqueKey: purchase_order.id + '.' + acsNumber,
                  price: detail.price,
                  discount: detail.disount,
                  larkSyncStatus: 'PENDING',
                  larkSyncedAt: new Date(),
                },
                create: {
                  purchaseOrderId: purchase_order.id,
                  purchaseOrderCode: purchase_order.code,
                  lineNumber: i + 1,
                  productId: product.id,
                  productCode: product.code,
                  productName: product.name,
                  quantity: detail.quantity,
                  uniqueKey: purchase_order.id + '.' + acsNumber,
                  price: detail.price,
                  discount: detail.disount,
                  larkSyncStatus: 'PENDING',
                  larkSyncedAt: new Date(),
                },
              });
            }
          }
        }

        if (
          purchaseOrderData.payments &&
          purchaseOrderData.payments.length > 0
        ) {
          for (const payment of purchaseOrderData.payments) {
            await this.prismaService.payment.upsert({
              where: {
                kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0),
              },
              update: {
                code: payment.code,
                amount: payment.amount,
                method: payment.method,
                status: payment.status,
                statusValue: payment.statusValue,
                transDate: payment.transDate
                  ? new Date(payment.transDate)
                  : new Date(),
              },
              create: {
                kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0),
                purchaseOrderId: purchase_order.id,
                code: payment.code,
                amount: payment.amount,
                method: payment.method,
                status: payment.status,
                statusValue: payment.statusValue,
                transDate: payment.transDate
                  ? new Date(payment.transDate)
                  : new Date(),
              },
            });
          }
        }

        savedPurchaseOrders.push(purchase_order);
      } catch (error) {
        this.logger.error(
          `Failed to save order_supplier ${purchaseOrderData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `Saved ${savedPurchaseOrders.length} purchase_orders successfully`,
    );
    return savedPurchaseOrders;
  }

  async syncPurchaseOrdersToLarkBase(purchase_orders: any[]): Promise<void> {
    try {
      this.logger.log(
        `Starting LarkBase sync for ${purchase_orders.length} purchase_orders...`,
      );

      const purchaseOrdersToSync = purchase_orders.filter(
        (s) => s.larkSyncStatus === 'PENDING' || s.larkSyncStatus === 'FAILED',
      );

      if (purchaseOrdersToSync.length === 0) {
        this.logger.log('No purchase_orders need LarkBase sync');
        return;
      }

      await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
        purchaseOrdersToSync,
      );
      this.logger.log(`LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(
        `LarkBase order_supplier sync failed: ${error.message}`,
      );

      try {
        const purchaseOrderIds = purchase_orders
          .map((o) => o.id)
          .filter((id) => id !== undefined);

        if (purchaseOrderIds.length > 0) {
          await this.prismaService.purchaseOrder.updateMany({
            where: { id: { in: purchaseOrderIds } },
            data: {
              larkSyncedAt: new Date(),
              larkSyncStatus: 'FAILED',
            },
          });
        }
      } catch (error) {
        this.logger.error(
          `Failed to update purchase_order status: ${error.message}`,
        );
      }

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

  // async syncPurchaseOrderDetailsToLarkBase(
  //   purchase_orders_details: any[],
  // ): Promise<void> {
  //   try {
  //     this.logger.log(
  //       `🚀 Starting LarkBase sync for ${purchase_orders_details.length} purchase_orders_details...`,
  //     );

  //     const purchaseOrderDetailsToSync = purchase_orders_details.filter(
  //       (s) => s.larkSyncStatus === 'PENDING' || s.larkSyncStatus === 'FAILED',
  //     );

  //     if (purchaseOrderDetailsToSync.length === 0) {
  //       this.logger.log('📋 No purchase_orders_details need LarkBase sync');
  //       return;
  //     }

  //     await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase(
  //       purchaseOrderDetailsToSync,
  //     );

  //     this.logger.log(`✅ LarkBase sync completed successfully`);
  //   } catch (error) {
  //     this.logger.error(
  //       `❌ LarkBase purchase_order_details sync failed: ${error.message}`,
  //     );

  //     try {
  //       const purchaseOrderDetailsIds = purchase_orders_details
  //         .map((p) => p.id)
  //         .filter((id) => id !== undefined);

  //       if (purchaseOrderDetailsIds.length > 0) {
  //         await this.prismaService.purchaseOrderDetail.updateMany({
  //           where: { id: { in: purchaseOrderDetailsIds } },
  //           data: {
  //             larkSyncedAt: new Date(),
  //             larkSyncStatus: 'FAILED',
  //           },
  //         });
  //       }
  //     } catch (updateError) {
  //       this.logger.error(
  //         `Failed to update purchase_order_details status: ${updateError.message}`,
  //       );
  //     }

  //     throw new Error(`LarkBase sync failed: ${error.message}`);
  //   }
  // }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['purchase_order'],
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
