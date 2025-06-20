// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

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

  private async prepareOrderCreateData(
    orderData: any,
  ): Promise<Prisma.OrderCreateInput> {
    const data: Prisma.OrderCreateInput = {
      kiotVietId: BigInt(orderData.id),
      code: orderData.code,
      purchaseDate: new Date(orderData.purchaseDate),
      // Fix: Use soldBy relation instead of soldById
      cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
      total: new Prisma.Decimal(orderData.total || 0),
      totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
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
      branch: {
        create: undefined,
        connectOrCreate: undefined,
        connect: undefined,
      },
    };

    // Fix: Handle soldBy relationship correctly
    if (orderData.soldById) {
      data.soldBy = { connect: { kiotVietId: BigInt(orderData.soldById) } };
    }

    // Handle branch relationship
    if (orderData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: orderData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
      }
    }

    // Handle customer relationship
    if (orderData.customerId) {
      const customer = await this.prismaService.customer.findFirst({
        where: { kiotVietId: BigInt(orderData.customerId) },
      });
      if (customer) {
        data.customer = { connect: { id: customer.id } };
      }
    }

    // Handle sale channel relationship
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

  private async prepareOrderUpdateData(
    orderData: any,
  ): Promise<Prisma.OrderUpdateInput> {
    const data: Prisma.OrderUpdateInput = {
      code: orderData.code,
      purchaseDate: new Date(orderData.purchaseDate),
      // Fix: Use soldBy relation instead of soldById
      cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
      total: new Prisma.Decimal(orderData.total || 0),
      totalPayment: new Prisma.Decimal(orderData.totalPayment || 0),
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

    // Fix: Handle soldBy relationship correctly
    if (orderData.soldById) {
      data.soldBy = { connect: { kiotVietId: BigInt(orderData.soldById) } };
    }

    // Handle relationships similar to create
    if (orderData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: orderData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
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

  private async handleOrderRelations(orderId: number, orderData: any) {
    // Handle order details
    if (orderData.orderDetails && orderData.orderDetails.length > 0) {
      await this.prismaService.orderDetail.deleteMany({
        where: { orderId },
      });

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
              surchargeId: surchargeEntity ? surchargeEntity.id : null,
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
              createdDate: surcharge.createdDate
                ? new Date(surcharge.createdDate)
                : new Date(),
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save order surcharge: ${error.message}`);
        }
      }
    }

    // Handle payments
    if (orderData.payments && orderData.payments.length > 0) {
      await this.prismaService.payment.deleteMany({
        where: { orderId },
      });

      for (const payment of orderData.payments) {
        try {
          const bankAccount = payment.accountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.accountId },
              })
            : null;

          await this.prismaService.payment.create({
            data: {
              kiotVietId: payment.kiotVietId
                ? BigInt(payment.kiotVietId)
                : null,
              code: payment.code,
              amount: new Prisma.Decimal(payment.amount || 0),
              method: payment.method,
              status: payment.status,
              transDate: payment.transDate
                ? new Date(payment.transDate)
                : new Date(),
              accountId: bankAccount ? bankAccount.id : null,
              bankAccountInfo: payment.bankAccount,
              orderId,
              description: payment.description,
            },
          });
        } catch (error) {
          this.logger.error(`Failed to save payment: ${error.message}`);
        }
      }
    }
  }

  async syncRecentOrders(days: number = 7): Promise<void> {
    try {
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

      const lastModifiedFrom = dayjs()
        .subtract(days, 'day')
        .format('YYYY-MM-DD');
      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchOrders({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveOrders(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Order recent sync progress: ${totalProcessed} orders processed`,
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

      this.logger.log(
        `Order recent sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'order_recent' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Order recent sync failed: ${error.message}`);
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

      let currentItem = 0;
      let totalProcessed = 0;
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
            const { created, updated } = await this.batchSaveOrders(orderBatch);
            totalProcessed += created + updated;
            batchCount++;

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

            this.logger.log(
              `Order historical sync batch ${batchCount}: ${totalProcessed} orders processed`,
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

      this.logger.log(
        `Order historical sync completed: ${totalProcessed} orders processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'order_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Order historical sync failed: ${error.message}`);
      throw error;
    }
  }
}
