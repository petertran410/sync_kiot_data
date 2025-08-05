import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';
import { LarkOrderSupplierSyncService } from 'src/services/lark/order-supplier/lark-order-supplier-sync.service';

interface KiotVietOrderSupplier {
  id: number;
  code: string;
  orderDate: string;
  branchId?: number;
  retailerId: number;
  userId?: number;
  description?: string;
  status?: number;
  discountRatio?: number;
  productQty?: number;
  discount?: number;
  createdDate?: string;
  createdBy?: number;
  orderSupplierDetails: Array<{
    id: number;
    orderSupplierId: number;
    orderSupplierCode: string;
    productId: number;
    productCode: string;
    productName: string;
    quantity: number;
    price: number;
    discount: number;
    allocation: number;
    createdDate?: string;
    description?: string;
    orderByNumber?: number;
    allocationSuppliers?: number;
    allocationThirdParty?: number;
    orderQuantity?: number;
    subTotal?: number;
  }>;
  total?: number;
  exReturnSuppliers?: number;
  exReturnThirdParty?: number;
  totalAmt?: number;
  totalQty?: number;
  totalQuantity?: number;
  totalProductType?: number;
  subTotal?: number;
  paidAmount?: number;
  toComplete?: boolean;
  statusValue?: string;
  viewPrice?: boolean;
  supplierDebt?: number;
  supplierOldDebt?: number;
  purchaseOrderCodes?: string;
}

@Injectable()
export class KiotVietOrderSupplierService {
  private readonly logger = new Logger(KiotVietOrderSupplierService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkOrderSupplierSyncService: LarkOrderSupplierSyncService,
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
        where: { name: 'order_supplier_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical order_supplier sync...');
        await this.syncHistoricalOrderSuppliers();
        return;
      }

      this.logger.log('Running default historical order_supplier sync...');
      await this.syncHistoricalOrderSuppliers();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('order_supplier_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical order_supplier sync enabled');
  }

  async syncHistoricalOrderSuppliers(): Promise<void> {
    const syncName = 'order_supplier_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalOrderSuppliers = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedOrderSupplierIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical order_supplier sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalOrderSuppliers > 0) {
          if (currentItem >= totalOrderSuppliers) {
            this.logger.log(
              `✅ Pagination complete. Processed ${processedCount}/${totalOrderSuppliers} suppliers`,
            );
            break;
          }
        }

        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchOrderSuppliersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
          });

          consecutiveErrorPages = 0;

          const { data: order_suppliers, total } = response;

          if (total !== undefined && total !== null) {
            if (totalOrderSuppliers === 0) {
              this.logger.log(
                `📊 Total order_suppliers detected: ${total}. Starting processing...`,
              );

              totalOrderSuppliers = total;
            } else if (
              total !== totalOrderSuppliers &&
              total !== lastValidTotal
            ) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalOrderSuppliers} → ${total}. Using latest.`,
              );

              totalOrderSuppliers = total;
            }
            lastValidTotal = total;
          }

          if (!order_suppliers || order_suppliers.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );

            consecutiveEmptyPages++;

            if (totalOrderSuppliers > 0 && currentItem >= totalOrderSuppliers) {
              this.logger.log('✅ Reached end of data (empty page past total)');
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

          const newOrderSuppliers = order_suppliers.filter((order_supplier) => {
            if (processedOrderSupplierIds.has(order_supplier.id)) {
              this.logger.debug(
                `⚠️ Duplicate order_supplier ID detected: ${order_supplier.id} (${order_supplier.code})`,
              );
              return false;
            }
            processedOrderSupplierIds.add(order_supplier.id);
            return true;
          });

          if (newOrderSuppliers.length !== order_suppliers.length) {
            this.logger.warn(
              `🔄 Filtered out ${order_suppliers.length - newOrderSuppliers.length} duplicate order_suppliers on page ${currentPage}`,
            );
          }

          if (newOrderSuppliers.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all order_suppliers already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newOrderSuppliers.length} order_suppliers from page ${currentPage}...`,
          );

          const orderSuppliersWithDetails =
            await this.enrichOrderSuppliersWithDetails(newOrderSuppliers);
          const savedOrderSuppliers = await this.saveOrderSuppliersToDatabase(
            orderSuppliersWithDetails,
          );
          await this.syncOrderSuppliersToLarkBase(savedOrderSuppliers);

          processedCount += savedOrderSuppliers.length;
          currentItem += this.PAGE_SIZE;

          if (totalOrderSuppliers > 0) {
            const completionPercentage =
              (processedCount / totalOrderSuppliers) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalOrderSuppliers} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalOrderSuppliers) {
              this.logger.log('🎉 All suppliers processed successfully!');
              break;
            }
          }

          consecutiveEmptyPages = 0;
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
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
        progress: { processedCount, expectedTotal: totalOrderSuppliers },
      });

      const completionRate =
        totalOrderSuppliers > 0
          ? (processedCount / totalOrderSuppliers) * 100
          : 100;

      this.logger.log(
        `✅ Historical order_supplier sync completed: ${processedCount}/${totalOrderSuppliers} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Historical order_supplier sync failed: ${error.message}`,
      );

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalOrderSuppliers },
      });

      throw error;
    }
  }

  async fetchOrderSuppliersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchOrderSuppliersList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchOrderSuppliersList(params: {
    currentItem?: number;
    pageSize?: number;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/ordersuppliers?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichOrderSuppliersWithDetails(
    order_suppliers: KiotVietOrderSupplier[],
  ): Promise<KiotVietOrderSupplier[]> {
    this.logger.log(
      `🔍 Enriching ${order_suppliers.length} order_suppliers with details...`,
    );

    const enrichedOrderSuppliers: KiotVietOrderSupplier[] = [];

    for (const order_supplier of order_suppliers) {
      try {
        const headers = await this.authService.getRequestHeaders();

        // const queryParams = new URLSearchParams({
        //   includeTotal: 'true',
        //   includeSupplierGroup: 'true',
        // });

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/ordersuppliers/${order_supplier.id}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedOrderSuppliers.push(response.data);
        } else {
          enrichedOrderSuppliers.push(order_supplier);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich supplier ${order_supplier.code}: ${error.message}`,
        );

        enrichedOrderSuppliers.push(order_supplier);
      }
    }
    return enrichedOrderSuppliers;
  }

  private async saveOrderSuppliersToDatabase(
    order_suppliers: KiotVietOrderSupplier[],
  ): Promise<any[]> {
    this.logger.log(
      `💾 Saving ${order_suppliers.length} order_suppliers to database...`,
    );

    const savedOrderSuppliers: any[] = [];

    for (const orderSupplierData of order_suppliers) {
      try {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: orderSupplierData.branchId },
          select: { id: true, name: true },
        });

        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: orderSupplierData.userId },
          select: { id: true, userName: true },
        });

        const order_supplier = await this.prismaService.orderSupplier.upsert({
          where: { kiotVietId: BigInt(orderSupplierData.id) },
          update: {
            code: orderSupplierData.code,
            orderDate: orderSupplierData.orderDate
              ? new Date(orderSupplierData.orderDate)
              : new Date(),
            branchId: branch?.id ?? null,
            retailerId: orderSupplierData.retailerId ?? null,
            userId: user?.id ?? null,
            description: orderSupplierData.description || '',
            status: orderSupplierData.status || null,
            discountRatio: orderSupplierData.discountRatio || 0,
            productQty: orderSupplierData.productQty || 0,
            discount: new Prisma.Decimal(orderSupplierData.discount || 0),
            createdDate: orderSupplierData.createdDate
              ? new Date(orderSupplierData.createdDate)
              : new Date(),
            createdBy: user?.id ?? null,
            total: new Prisma.Decimal(orderSupplierData.total || 0),
            exReturnSuppliers: new Prisma.Decimal(
              orderSupplierData.exReturnSuppliers || 0,
            ),
            exReturnThirdParty: new Prisma.Decimal(
              orderSupplierData.exReturnThirdParty || 0,
            ),
            totalAmt: new Prisma.Decimal(orderSupplierData.totalAmt || 0),
            totalQty: orderSupplierData.totalQty || 0,
            totalQuantity: orderSupplierData.totalQuantity || 0,
            totalProductType: orderSupplierData.totalProductType || 0,
            subTotal: new Prisma.Decimal(orderSupplierData.subTotal || 0),
            paidAmount: orderSupplierData.paidAmount || 0,
            toComplete: orderSupplierData.toComplete || false,
            statusValue: orderSupplierData.statusValue || '',
            viewPrice: orderSupplierData.viewPrice || false,
            supplierDebt: orderSupplierData.supplierDebt || 0,
            supplierOldDebt: orderSupplierData.supplierOldDebt || 0,
            purchaseOrderCodes: orderSupplierData.purchaseOrderCodes || '',
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
          create: {
            kiotVietId: BigInt(orderSupplierData.id),
            code: orderSupplierData.code,
            orderDate: orderSupplierData.orderDate
              ? new Date(orderSupplierData.orderDate)
              : new Date(),
            branchId: branch?.id ?? null,
            retailerId: orderSupplierData.retailerId ?? null,
            userId: user?.id ?? null,
            description: orderSupplierData.description || '',
            status: orderSupplierData.status || null,
            discountRatio: orderSupplierData.discountRatio || 0,
            productQty: orderSupplierData.productQty || 0,
            discount: new Prisma.Decimal(orderSupplierData.discount || 0),
            createdDate: orderSupplierData.createdDate
              ? new Date(orderSupplierData.createdDate)
              : new Date(),
            createdBy: user?.id ?? null,
            total: new Prisma.Decimal(orderSupplierData.total || 0),
            exReturnSuppliers: new Prisma.Decimal(
              orderSupplierData.exReturnSuppliers || 0,
            ),
            exReturnThirdParty: new Prisma.Decimal(
              orderSupplierData.exReturnThirdParty || 0,
            ),
            totalAmt: new Prisma.Decimal(orderSupplierData.totalAmt || 0),
            totalQty: orderSupplierData.totalQty || 0,
            totalQuantity: orderSupplierData.totalQuantity || 0,
            totalProductType: orderSupplierData.totalProductType || 0,
            subTotal: new Prisma.Decimal(orderSupplierData.subTotal || 0),
            paidAmount: orderSupplierData.paidAmount || 0,
            toComplete: orderSupplierData.toComplete || false,
            statusValue: orderSupplierData.statusValue || '',
            viewPrice: orderSupplierData.viewPrice || false,
            supplierDebt: orderSupplierData.supplierDebt || 0,
            supplierOldDebt: orderSupplierData.supplierOldDebt || 0,
            purchaseOrderCodes: orderSupplierData.purchaseOrderCodes || '',
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
        });

        if (
          orderSupplierData.orderSupplierDetails &&
          orderSupplierData.orderSupplierDetails.length > 0
        ) {
          for (const detail of orderSupplierData.orderSupplierDetails) {
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(detail.productId) },
              select: { id: true, name: true, code: true },
            });

            if (product) {
              await this.prismaService.orderSupplierDetail.upsert({
                where: {
                  kiotVietId: detail.id ? BigInt(detail.id) : BigInt(0),
                },
                update: {
                  orderSupplierId: order_supplier.id,
                  orderSupplierCode: order_supplier.code,
                  productId: product.id,
                  productCode: product.code,
                  productName: product.name,
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price || 0),
                  discount: new Prisma.Decimal(detail.discount || 0),
                  allocation: new Prisma.Decimal(detail.allocation || 0),
                  createdDate: detail.createdDate
                    ? new Date(detail.createdDate)
                    : new Date(),
                  description: detail.description || '',
                  orderByNumber: detail.orderByNumber || 0,
                  allocationSuppliers: detail.allocationSuppliers,
                  allocationThirdParty: detail.allocationThirdParty,
                  orderQuantity: detail.orderQuantity,
                  subTotal: new Prisma.Decimal(detail.subTotal || 0),
                },
                create: {
                  kiotVietId: BigInt(detail.id),
                  orderSupplierId: order_supplier.id,
                  orderSupplierCode: order_supplier.code,
                  productId: product.id,
                  productCode: product.code,
                  productName: product.name,
                  price: new Prisma.Decimal(detail.price || 0),
                  quantity: detail.quantity,
                  discount: new Prisma.Decimal(detail.discount || 0),
                  allocation: new Prisma.Decimal(detail.allocation || 0),
                  createdDate: detail.createdDate
                    ? new Date(detail.createdDate)
                    : new Date(),
                  description: detail.description || '',
                  orderByNumber: detail.orderByNumber || 0,
                  allocationSuppliers: detail.allocationSuppliers,
                  allocationThirdParty: detail.allocationThirdParty,
                  orderQuantity: detail.orderQuantity,
                  subTotal: new Prisma.Decimal(detail.subTotal || 0),
                },
              });
            }
          }
        }

        savedOrderSuppliers.push(order_supplier);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save order_supplier ${orderSupplierData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `✅ Saved ${savedOrderSuppliers.length} suppliers successfully`,
    );
    return savedOrderSuppliers;
  }

  async syncOrderSuppliersToLarkBase(order_suppliers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${order_suppliers.length} order_suppliers...`,
      );

      const orderSuppliersToSync = order_suppliers.filter(
        (s) => s.larkSyncStatus === 'PENDING' || s.larkSyncStatus === 'FAILED',
      );

      if (orderSuppliersToSync.length === 0) {
        this.logger.log('📋 No order_suppliers need LarkBase sync');
        return;
      }

      await this.larkOrderSupplierSyncService.syncOrderSuppliersToLarkBase(
        orderSuppliersToSync,
      );

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(
        `❌ LarkBase order_supplier sync failed: ${error.message}`,
      );

      try {
        const orderSupplierIds = order_suppliers
          .map((o) => o.id)
          .filter((id) => id !== undefined);

        if (orderSupplierIds.length > 0) {
          await this.prismaService.orderSupplier.updateMany({
            where: { id: { in: orderSupplierIds } },
            data: {
              larkSyncedAt: new Date(),
              larkSyncStatus: 'FAILED',
            },
          });
        }
      } catch (updateError) {
        this.logger.error(
          `Failed to update order_supplier status: ${updateError.message}`,
        );
      }

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['order_supplier'],
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
