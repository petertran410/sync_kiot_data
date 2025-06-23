import { LarkBaseService } from './../../lark/lark-base.service';
// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

interface EnrichedOrderData {
  orderData: any;
  branchName: string | null;
  customerName: string | null;
  userName: string | null;
}

@Injectable()
export class KiotVietOrderService {
  private readonly logger = new Logger(KiotVietOrderService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkBaseService: LarkBaseService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async fetchOrders(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/orders`, {
          headers,
          params: {
            ...params,
            includePayment: true,
            includeOrderDelivery: true,
            includeRemoveIds: true,
            SaleChannel: true,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch orders: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveOrders(orders: any[]) {
    if (!orders || orders.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = orders.map((o) => BigInt(o.id));
    const existingOrders = await this.prismaService.order.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<string, number>(
      existingOrders.map((o) => [o.kiotVietId.toString(), o.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const orderData of orders) {
      try {
        const kiotVietId = BigInt(orderData.id);
        const existingId = existingMap.get(kiotVietId.toString());

        if (existingId) {
          await this.updateOrder(existingId, orderData);
          updatedCount++;
        } else {
          await this.createOrder(orderData);
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save order ${orderData.code}: ${error.message}`,
        );
        continue;
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async createOrder(orderData: any) {
    const order = await this.prismaService.order.create({
      data: await this.prepareOrderCreateData(orderData),
    });

    await this.handleOrderRelations(order.id, orderData);
  }

  private async updateOrder(orderId: number, orderData: any) {
    await this.prismaService.order.update({
      where: { id: orderId },
      data: await this.prepareOrderUpdateData(orderData),
    });

    await this.handleOrderRelations(orderId, orderData);
  }
  private missingBranches = new Set<number>();

  private async handleMissingBranch(branchId: number, orderCode: string) {
    if (!this.missingBranches.has(branchId)) {
      this.missingBranches.add(branchId);
      this.logger.warn(
        `New missing branch detected: ${branchId} (first seen in invoice ${orderCode})`,
      );
    }
  }

  private async prepareOrderCreateData(
    orderData: any,
  ): Promise<Prisma.OrderCreateInput> {
    const total = orderData.total || 0; // Khách cần trả
    const discount = orderData.discounnt || 0;
    let thuKhac = 0;
    if (
      orderData.invoiceOrderSurcharges &&
      orderData.invoiceOrderSurcharges.length > 0
    ) {
      thuKhac = orderData.invoiceOrderSurcharges.reduce(
        (sum: any, surcharge: any) => {
          return sum + Number(surcharge.price || 0);
        },
        0,
      );
    }
    const tongThuKhac = Number(thuKhac || 0);
    const totalCostOfGoods = total + discount - tongThuKhac;
    const otherRevenue = tongThuKhac;
    const totalAfterDiscount = totalCostOfGoods - tongThuKhac;

    const data: Prisma.OrderCreateInput = {
      kiotVietId: BigInt(orderData.id),
      code: orderData.code,
      purchaseDate: new Date(orderData.purchaseDate),
      cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
      totalCostOfGoods: new Prisma.Decimal(totalCostOfGoods || 0),
      total: new Prisma.Decimal(orderData.total || 0),
      totalAfterDiscount: new Prisma.Decimal(totalAfterDiscount || 0),
      totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
      otherRevenue: new Prisma.Decimal(otherRevenue || 0),
      discount: orderData.discount
        ? new Prisma.Decimal(orderData.discount)
        : null,
      discountRatio: orderData.discountRatio,
      status: orderData.status,
      description: orderData.description,
      usingCod: orderData.usingCod || false,
      expectedDelivery: orderData.expectedDelivery
        ? new Date(orderData.expectedDelivery)
        : null,
      makeInvoice: orderData.makeInvoice || false,
      retailerId: orderData.retailerId,
      createdDate: orderData.createdDate
        ? new Date(orderData.createdDate)
        : new Date(),
      modifiedDate: orderData.modifiedDate
        ? new Date(orderData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    if (orderData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: orderData.branchId },
      });

      if (branch) {
        data.branch = { connect: { id: branch.id } };
      } else {
        await this.handleMissingBranch(orderData.branchId, orderData.code);
        this.logger.warn(
          `Branch ${orderData.branchId} not found for orders ${orderData.code}. Creating order without branch reference.`,
        );
      }
    }

    if (orderData.soldById) {
      const soldByUser = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(orderData.soldById) },
      });
      if (soldByUser) {
        data.soldBy = { connect: { kiotVietId: BigInt(orderData.soldById) } };
      } else {
        this.logger.warn(
          `User with kiotVietId ${orderData.soldById} not found for order ${orderData.code}. Creating order without soldBy reference.`,
        );
      }
    }

    if (orderData.customerId) {
      const customer = await this.prismaService.customer.findFirst({
        where: { kiotVietId: BigInt(orderData.customerId) },
      });
      if (customer) {
        data.customer = { connect: { id: customer.id } };
      } else {
        this.logger.warn(
          `Customer with kiotVietId ${orderData.customerId} not found for order ${orderData.code}`,
        );
      }
    }

    if (orderData.saleChannelId) {
      const saleChannel = await this.prismaService.saleChannel.findFirst({
        where: { kiotVietId: orderData.saleChannelId },
      });
      if (saleChannel) {
        data.saleChannel = { connect: { id: saleChannel.id } };
      } else {
        this.logger.warn(
          `Sale channel with kiotVietId ${orderData.saleChannelId} not found for order ${orderData.code}`,
        );
      }
    }

    return data;
  }

  private async prepareOrderUpdateData(
    orderData: any,
  ): Promise<Prisma.OrderUpdateInput> {
    const total = orderData.total || 0; // Khách cần trả
    const discount = orderData.discounnt || 0;
    let thuKhac = 0;
    if (
      orderData.invoiceOrderSurcharges &&
      orderData.invoiceOrderSurcharges.length > 0
    ) {
      thuKhac = orderData.invoiceOrderSurcharges.reduce(
        (sum: any, surcharge: any) => {
          return sum + Number(surcharge.price || 0);
        },
        0,
      );
    }
    const tongThuKhac = Number(thuKhac || 0);
    const totalCostOfGoods = total + discount - tongThuKhac;
    const otherRevenue = tongThuKhac;
    const totalAfterDiscount = totalCostOfGoods - tongThuKhac;

    const data: Prisma.OrderUpdateInput = {
      code: orderData.code,
      purchaseDate: new Date(orderData.purchaseDate),
      cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
      totalCostOfGoods: new Prisma.Decimal(totalCostOfGoods || 0),
      total: new Prisma.Decimal(orderData.total || 0),
      totalAfterDiscount: new Prisma.Decimal(totalAfterDiscount || 0),
      totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
      otherRevenue: new Prisma.Decimal(otherRevenue || 0),
      discount: orderData.discount
        ? new Prisma.Decimal(orderData.discount)
        : null,
      discountRatio: orderData.discountRatio,
      status: orderData.status,
      description: orderData.description,
      usingCod: orderData.usingCod || false,
      expectedDelivery: orderData.expectedDelivery
        ? new Date(orderData.expectedDelivery)
        : null,
      makeInvoice: orderData.makeInvoice || false,
      retailerId: orderData.retailerId,
      modifiedDate: orderData.modifiedDate
        ? new Date(orderData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    if (orderData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: orderData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
      } else {
        await this.handleMissingBranch(orderData.branchId, orderData.code);
        this.logger.warn(
          `Branch ${orderData.branchId} not found for orders ${orderData.code}. Updating order without branch reference.`,
        );
        data.branch = { disconnect: true };
      }
    }

    if (orderData.soldById) {
      const soldByUser = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(orderData.soldById) },
      });
      if (soldByUser) {
        data.soldBy = { connect: { kiotVietId: BigInt(orderData.soldById) } };
      } else {
        this.logger.warn(
          `User with kiotVietId ${orderData.soldById} not found for order ${orderData.code}. Updating order without soldBy reference.`,
        );
        data.soldBy = { disconnect: true };
      }
    }

    if (orderData.customerId) {
      const customer = await this.prismaService.customer.findFirst({
        where: { kiotVietId: BigInt(orderData.customerId) },
      });
      if (customer) {
        data.customer = { connect: { id: customer.id } };
      }
    }

    // Handle sale channel relationship (optional)
    if (orderData.saleChannelId) {
      const saleChannel = await this.prismaService.saleChannel.findFirst({
        where: { kiotVietId: orderData.saleChannelId },
      });
      if (saleChannel) {
        data.saleChannel = { connect: { id: saleChannel.id } };
      }
    }

    return data;
  }

  async reportMissingBranches() {
    if (this.missingBranches.size > 0) {
      this.logger.warn(
        `Total missing branches: ${this.missingBranches.size}`,
        Array.from(this.missingBranches),
      );
    }
  }

  private async handleOrderRelations(orderId: number, orderData: any) {
    if (orderData.orderDetails && orderData.orderDetails.length > 0) {
      for (const detail of orderData.orderDetails) {
        try {
          const product = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(detail.productId) },
          });

          if (product) {
            await this.prismaService.orderDetail.create({
              data: {
                kiotVietId: detail.kiotVietId
                  ? BigInt(detail.kiotVietId)
                  : null,
                orderId,
                productId: product.id,
                quantity: detail.quantity,
                price: new Prisma.Decimal(detail.price || 0),
                discount: detail.discount
                  ? new Prisma.Decimal(detail.discount)
                  : null,
                discountRatio: detail.discountRatio,
                note: detail.note,
                isMaster: detail.isMaster !== false,
              },
            });
          } else {
            this.logger.warn(
              `Product with kiotVietId ${detail.productId} not found for order ${orderData.code}`,
            );
          }
        } catch (error) {
          this.logger.error(`Failed to save order detail: ${error.message}`);
        }
      }
    }

    // Handle order delivery
    if (orderData.orderDelivery) {
      const delivery = orderData.orderDelivery;
      try {
        await this.prismaService.orderDelivery.upsert({
          where: { orderId },
          create: {
            kiotVietId: delivery.kiotVietId
              ? BigInt(delivery.kiotVietId)
              : null,
            orderId,
            deliveryCode: delivery.deliveryCode,
            type: delivery.type,
            price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
            receiver: delivery.receiver,
            contactNumber: delivery.contactNumber,
            address: delivery.address,
            locationId: delivery.locationId,
            locationName: delivery.locationName,
            wardName: delivery.wardName,
            weight: delivery.weight,
            length: delivery.length,
            width: delivery.width,
            height: delivery.height,
            partnerDeliveryId: delivery.partnerDeliveryId
              ? BigInt(delivery.partnerDeliveryId)
              : null,
          },
          update: {
            deliveryCode: delivery.deliveryCode,
            type: delivery.type,
            price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
            receiver: delivery.receiver,
            contactNumber: delivery.contactNumber,
            address: delivery.address,
            locationId: delivery.locationId,
            locationName: delivery.locationName,
            wardName: delivery.wardName,
            weight: delivery.weight,
            length: delivery.length,
            width: delivery.width,
            height: delivery.height,
            partnerDeliveryId: delivery.partnerDeliveryId
              ? BigInt(delivery.partnerDeliveryId)
              : null,
          },
        });
      } catch (error) {
        this.logger.error(`Failed to save order delivery: ${error.message}`);
      }
    }

    // Handle order surcharges
    if (
      orderData.invoiceOrderSurcharges &&
      orderData.invoiceOrderSurcharges.length > 0
    ) {
      await this.prismaService.orderSurcharge.deleteMany({
        where: { orderId },
      });

      for (const surcharge of orderData.invoiceOrderSurcharges) {
        try {
          const surchargeEntity = surcharge.surchargeId
            ? await this.prismaService.surcharge.findFirst({
                where: { kiotVietId: surcharge.surchargeId },
              })
            : null;

          await this.prismaService.orderSurcharge.create({
            data: {
              kiotVietId: surcharge.kiotVietId
                ? BigInt(surcharge.kiotVietId)
                : null,
              orderId,
              surchargeId: surchargeEntity?.id || null,
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save order surcharge: ${error.message}`);
        }
      }
    }

    // FIXED: Handle payments with proper relationships
    if (orderData.payments && orderData.payments.length > 0) {
      await this.prismaService.payment.deleteMany({
        where: { orderId },
      });

      for (const payment of orderData.payments) {
        try {
          const paymentData: any = {
            kiotVietId: payment.id ? BigInt(payment.id) : null,
            order: { connect: { id: orderId } },
            code: payment.code,
            amount: new Prisma.Decimal(payment.amount || 0),
            method: payment.method,
            status: payment.status,
            transDate: payment.transDate
              ? new Date(payment.transDate)
              : new Date(),
            bankAccountInfo: payment.bankAccount,
            description: payment.description || null,
          };

          if (payment.accountId) {
            const bankAccount = await this.prismaService.bankAccount.findFirst({
              where: { kiotVietId: payment.accountId },
            });
            if (bankAccount) {
              paymentData.bankAccount = { connect: { id: bankAccount.id } };
            } else {
              this.logger.warn(
                `Bank account with kiotVietId ${payment.accountId} not found for payment ${payment.code}`,
              );
            }
          }

          if (payment.id) {
            await this.prismaService.payment.upsert({
              where: { kiotVietId: BigInt(payment.id) },
              create: paymentData,
              update: {
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount || 0),
                method: payment.method,
                status: payment.status,
                transDate: payment.transDate
                  ? new Date(payment.transDate)
                  : new Date(),
                bankAccountInfo: payment.bankAccount,
                description: payment.description || null,
                // Update order relationship
                order: { connect: { id: orderId } },
              },
            });
          } else {
            await this.prismaService.payment.create({
              data: paymentData,
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save payment: ${error.message}`);
        }
      }
    }
  }

  private async batchSaveOrdersWithLarkTracking(orders: any[]): Promise<{
    created: number;
    updated: number;
    larkResult: { success: number; failed: number };
  }> {
    if (!orders || orders.length === 0) {
      return { created: 0, updated: 0, larkResult: { success: 0, failed: 0 } };
    }

    const { created, updated } = await this.batchSaveOrders(orders);

    const ordersWithEnrichedData = await this.enrichOrdersForLarkBase(orders);

    const larkResult = await this.larkBaseService.syncOrdersToLarkBase(
      ordersWithEnrichedData,
    );

    return { created, updated, larkResult };
  }

  private async enrichOrdersForLarkBase(
    orders: any[],
  ): Promise<EnrichedOrderData[]> {
    const enrichedOrders: EnrichedOrderData[] = [];

    for (const orderData of orders) {
      let branchName: string | null = null;
      let customerName: string | null = null;
      let userName: string | null = null;

      if (orderData.branchId) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: orderData.branchId },
          select: { name: true },
        });
        branchName = branch?.name || null;
      }

      if (orderData.customerId) {
        const customer = await this.prismaService.customer.findFirst({
          where: { kiotVietId: BigInt(orderData.customerId) },
          select: { name: true },
        });
        customerName = customer?.name || null;
      }

      if (orderData.soldById) {
        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: BigInt(orderData.soldById) },
          select: { givenName: true },
        });
        userName = user?.givenName || null;
      }

      enrichedOrders.push({
        orderData,
        branchName,
        customerName,
        userName,
      });
    }

    return enrichedOrders;
  }

  async syncRecentOrders(days: number = 7): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_historical', isRunning: true },
      });

      if (historicalSync) {
        this.logger.log('Historical sync is running. Skipping recent sync.');
        return;
      }

      await this.prismaService.syncControl.upsert({
        where: { name: 'order_recent' },
        create: {
          name: 'order_recent',
          entities: ['order'],
          syncMode: 'recent',
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
        },
      });

      await this.prismaService.syncControl.upsert({
        where: { name: 'order_recent_lark' },
        create: {
          name: 'order_recent_lark',
          entities: ['order_lark'],
          syncMode: 'recent',
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
        },
      });

      const lastModifiedFrom = dayjs()
        .subtract(days, 'day')
        .format('YYYY-MM-DD');

      this.logger.log(`Starting recent order sync for last ${days} days`);

      let currentItem = 0;
      let totalProcessed = 0;
      let totalLarkSuccess = 0;
      let totalLarkFailed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchOrders({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated, larkResult } =
            await this.batchSaveOrdersWithLarkTracking(response.data);
          totalProcessed += created + updated;

          if (larkResult) {
            totalLarkSuccess += larkResult.success;
            totalLarkFailed += larkResult.failed;
          }

          this.logger.log(
            `Recent sync progress: ${totalProcessed} orders processed, LarkBase: ${totalLarkSuccess} success, ${totalLarkFailed} failed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'order_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      await this.prismaService.syncControl.update({
        where: { name: 'order_recent_lark' },
        data: {
          isRunning: false,
          status: totalLarkFailed > 0 ? 'completed_with_errors' : 'completed',
          completedAt: new Date(),
          progress: { totalLarkSuccess, totalLarkFailed },
          error:
            totalLarkFailed > 0
              ? `${totalLarkFailed} records failed to sync to LarkBase`
              : null,
        },
      });

      this.logger.log(
        `Recent sync completed: ${totalProcessed} orders processed, LarkBase: ${totalLarkSuccess} success, ${totalLarkFailed} failed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.updateMany({
        where: { name: { in: ['order_recent', 'order_recent_lark'] } },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Recent sync failed: ${error.message}`);
      throw error;
    }
  }

  async syncHistoricalOrders(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'order_historical' },
        create: {
          name: 'order_historical',
          entities: ['order'],
          syncMode: 'historical',
          isRunning: true,
          isEnabled: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
          progress: {},
        },
      });

      await this.prismaService.syncControl.upsert({
        where: { name: 'order_historical_lark' },
        create: {
          name: 'order_historical_lark',
          entities: ['order_lark'],
          syncMode: 'historical',
          isRunning: true,
          isEnabled: true,
          status: 'in_progress',
          startedAt: new Date(),
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          error: null,
          progress: {},
        },
      });

      let currentItem = 0;
      let totalProcessed = 0;
      let totalLarkSuccess = 0;
      let totalLarkFailed = 0;
      let batchCount = 0;
      let hasMoreData = true;
      const orderBatch: any[] = [];

      this.logger.log('Starting historical order sync...');

      while (hasMoreData) {
        const response = await this.fetchOrders({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          orderBatch.push(...response.data);

          if (
            orderBatch.length >= this.BATCH_SIZE ||
            response.data.length < this.PAGE_SIZE
          ) {
            const { created, updated, larkResult } =
              await this.batchSaveOrdersWithLarkTracking(orderBatch);
            totalProcessed += created + updated;
            batchCount++;

            if (larkResult) {
              totalLarkSuccess += larkResult.success;
              totalLarkFailed += larkResult.failed;
            }

            await this.prismaService.syncControl.update({
              where: { name: 'order_historical' },
              data: {
                progress: {
                  totalProcessed,
                  batchCount,
                  lastProcessedItem: currentItem + response.data.length,
                },
              },
            });

            await this.prismaService.syncControl.update({
              where: { name: 'order_historical_lark' },
              data: {
                progress: {
                  totalLarkSuccess,
                  totalLarkFailed,
                  batchCount,
                },
              },
            });

            this.logger.log(
              `Historical sync batch ${batchCount}: ${totalProcessed} orders processed, LarkBase: ${totalLarkSuccess} success, ${totalLarkFailed} failed`,
            );
            orderBatch.length = 0;
          }
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'order_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, batchCount },
        },
      });

      await this.prismaService.syncControl.update({
        where: { name: 'order_historical_lark' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: totalLarkFailed > 0 ? 'completed_with_errors' : 'completed',
          completedAt: new Date(),
          progress: { totalLarkSuccess, totalLarkFailed, batchCount },
          error:
            totalLarkFailed > 0
              ? `${totalLarkFailed} records failed to sync to LarkBase`
              : null,
        },
      });

      this.logger.log(
        `Historical sync completed: ${totalProcessed} orders processed, LarkBase: ${totalLarkSuccess} success, ${totalLarkFailed} failed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.updateMany({
        where: {
          name: { in: ['order_historical', 'order_historical_lark'] },
        },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Historical sync failed: ${error.message}`);
      throw error;
    }
  }
}
