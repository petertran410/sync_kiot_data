// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { LarkBaseService } from '../../lark/lark-base.service';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

interface OrderToCreate {
  createData: Prisma.OrderCreateInput;
  orderData: any;
}

interface OrderToUpdate {
  id: number;
  data: Prisma.OrderUpdateInput;
  orderData: any;
}

@Injectable()
export class KiotVietOrderService {
  private readonly logger = new Logger(KiotVietOrderService.name);
  private readonly PAGE_SIZE = 50;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
    private readonly larkBaseService: LarkBaseService,
  ) {}

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
      let hasMoreData = true;
      let batchCount = 0;

      while (hasMoreData) {
        const response = await this.fetchOrders({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.saveOrdersToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          // IMMEDIATE LarkBase sync
          await this.syncOrdersToLarkBaseImmediate(response.data);

          this.logger.log(
            `Historical sync batch ${++batchCount}: ${totalProcessed} orders processed`,
          );
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedOrders(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      const duplicatesRemoved = await this.removeDuplicateOrders();

      // Mark historical sync as COMPLETED and DISABLED
      await this.prismaService.syncControl.update({
        where: { name: 'order_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved, batchCount },
        },
      });

      this.logger.log(
        `Historical sync completed: ${totalProcessed} orders processed, ${duplicatesRemoved} duplicates removed`,
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

      this.logger.error(`Historical sync failed: ${error.message}`);
      throw error;
    }
  }

  private async syncOrdersToLarkBaseImmediate(orders: any[]): Promise<void> {
    if (!orders || orders.length === 0) return;

    try {
      // Fetch related data for proper mapping
      const ordersWithRelations = await Promise.all(
        orders.map(async (order) => {
          const dbOrder = await this.prismaService.order.findFirst({
            where: { kiotVietId: BigInt(order.id) },
            include: {
              branch: true,
              customer: true,
              soldBy: true,
              invoices: true,
              orderDelivery: true,
              orderSurcharges: true,
            },
          });
          return dbOrder || order;
        }),
      );

      const result =
        await this.larkBaseService.directCreateOrders(ordersWithRelations);

      this.logger.debug(
        `LarkBase immediate sync: ${result.success} success, ${result.failed} failed`,
      );
    } catch (error) {
      this.logger.error(`LarkBase immediate sync failed: ${error.message}`);

      return error;
    }
  }

  async syncRecentOrders(days: number = 4): Promise<void> {
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
          const { created, updated } = await this.saveOrdersToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          await this.syncOrdersToLarkBaseImmediate(response.data);

          this.logger.log(
            `Recent sync progress: ${totalProcessed} orders processed`,
          );
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedOrders(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      const duplicatesRemoved = await this.removeDuplicateOrders();

      await this.prismaService.syncControl.update({
        where: { name: 'order_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved },
        },
      });

      this.logger.log(
        `Recent sync completed: ${totalProcessed} orders processed, ${duplicatesRemoved} duplicates removed`,
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

      this.logger.error(`Recent sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchOrders(params: any): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const queryParams = new URLSearchParams();
      if (params.currentItem !== undefined) {
        queryParams.append('currentItem', params.currentItem.toString());
      }
      if (params.pageSize) {
        queryParams.append('pageSize', params.pageSize.toString());
      }
      if (params.lastModifiedFrom) {
        queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
      }
      queryParams.append('includeRemove', 'true');
      queryParams.append('includeOrderSurcharge', 'true');

      const url = `${baseUrl}/orders?${queryParams.toString()}`;

      const response = await this.httpService
        .get(url, {
          headers: {
            Retailer: this.configService.get<string>('KIOT_SHOP_NAME'),
            Authorization: `Bearer ${accessToken}`,
          },
        })
        .toPromise();

      return response?.data;
    } catch (error) {
      this.logger.error(`Failed to fetch orders: ${error.message}`);
      throw error;
    }
  }

  private async saveOrdersToDatabase(
    orders: any[],
  ): Promise<{ created: number; updated: number }> {
    // Explicitly type the arrays
    const ordersToCreate: OrderToCreate[] = [];
    const ordersToUpdate: OrderToUpdate[] = [];

    for (const orderData of orders) {
      const existingOrder = await this.prismaService.order.findUnique({
        where: { kiotVietId: BigInt(orderData.id) },
      });

      if (existingOrder) {
        const updateData = await this.prepareOrderUpdateData(orderData);
        ordersToUpdate.push({
          id: existingOrder.id,
          data: updateData,
          orderData: orderData,
        });
      } else {
        const createData = await this.prepareOrderCreateData(orderData);
        if (createData) {
          ordersToCreate.push({
            createData: createData,
            orderData: orderData,
          });
        }
      }
    }

    return await this.processDatabaseOperations(ordersToCreate, ordersToUpdate);
  }

  private async processDatabaseOperations(
    ordersToCreate: OrderToCreate[],
    ordersToUpdate: OrderToUpdate[],
  ) {
    let createdCount = 0;
    let updatedCount = 0;

    // Create orders
    for (const orderToCreate of ordersToCreate) {
      try {
        const order = await this.prismaService.order.create({
          data: orderToCreate.createData,
        });

        await this.handleOrderRelations(order.id, orderToCreate.orderData);
        createdCount++;
      } catch (error) {
        this.logger.error(`Failed to create order: ${error.message}`);
      }
    }

    // Update orders
    for (const orderToUpdate of ordersToUpdate) {
      try {
        await this.prismaService.order.update({
          where: { id: orderToUpdate.id },
          data: orderToUpdate.data,
        });

        await this.handleOrderRelations(
          orderToUpdate.id,
          orderToUpdate.orderData,
        );
        updatedCount++;
      } catch (error) {
        this.logger.error(`Failed to update order: ${error.message}`);
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async removeDuplicateOrders(): Promise<number> {
    try {
      const duplicates = await this.prismaService.$queryRaw`
        SELECT kiotVietId, MIN(id) as keep_id
        FROM "Order"
        GROUP BY kiotVietId
        HAVING COUNT(*) > 1
      `;

      let removedCount = 0;

      for (const duplicate of duplicates as any[]) {
        const duplicateOrders = await this.prismaService.order.findMany({
          where: { kiotVietId: duplicate.kiotVietId },
          orderBy: { id: 'asc' },
        });

        // Keep the first one, delete the rest
        for (let i = 1; i < duplicateOrders.length; i++) {
          await this.prismaService.order.delete({
            where: { id: duplicateOrders[i].id },
          });
          removedCount++;
        }
      }

      if (removedCount > 0) {
        this.logger.log(`Removed ${removedCount} duplicate orders`);
      }

      return removedCount;
    } catch (error) {
      this.logger.error(`Failed to remove duplicate orders: ${error.message}`);
      return 0;
    }
  }

  private async prepareOrderCreateData(
    orderData: any,
  ): Promise<Prisma.OrderCreateInput | null> {
    try {
      const data: Prisma.OrderCreateInput = {
        kiotVietId: BigInt(orderData.id),
        code: orderData.code,
        purchaseDate: new Date(orderData.purchaseDate),
        cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
        totalCostOfGoods: orderData.totalCostOfGoods
          ? parseFloat(orderData.totalCostOfGoods)
          : 0,
        otherRevenue: orderData.otherRevenue
          ? parseFloat(orderData.otherRevenue)
          : 0,
        total: parseFloat(orderData.total),
        totalAfterDiscount: orderData.totalAfterDiscount
          ? parseFloat(orderData.totalAfterDiscount)
          : 0,
        totalPayment: parseFloat(orderData.totalPayment),
        discount: orderData.discount ? parseFloat(orderData.discount) : null,
        discountRatio: orderData.discountRatio || null,
        status: orderData.status,
        description: orderData.description || null,
        usingCod: orderData.usingCod || false,
        expectedDelivery: orderData.expectedDelivery
          ? new Date(orderData.expectedDelivery)
          : null,
        makeInvoice: orderData.makeInvoice || false,
        retailerId: orderData.retailerId || null,
        createdDate: orderData.createdDate
          ? new Date(orderData.createdDate)
          : new Date(),
        modifiedDate: orderData.modifiedDate
          ? new Date(orderData.modifiedDate)
          : new Date(),
        lastSyncedAt: new Date(),
        larkSyncStatus: 'PENDING',
      };

      // Handle soldBy relationship (not soldById)
      if (orderData.soldById) {
        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: BigInt(orderData.soldById) },
        });
        if (user) {
          data.soldBy = { connect: { id: user.id } };
        }
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
    } catch (error) {
      this.logger.error(
        `Failed to prepare order create data: ${error.message}`,
      );
      return null;
    }
  }

  private async prepareOrderUpdateData(
    orderData: any,
  ): Promise<Prisma.OrderUpdateInput> {
    const data: Prisma.OrderUpdateInput = {
      code: orderData.code,
      purchaseDate: new Date(orderData.purchaseDate),
      cashierId: orderData.cashierId ? BigInt(orderData.cashierId) : null,
      totalCostOfGoods: orderData.totalCostOfGoods
        ? parseFloat(orderData.totalCostOfGoods)
        : 0,
      otherRevenue: orderData.otherRevenue
        ? parseFloat(orderData.otherRevenue)
        : 0,
      total: parseFloat(orderData.total),
      totalAfterDiscount: orderData.totalAfterDiscount
        ? parseFloat(orderData.totalAfterDiscount)
        : 0,
      totalPayment: parseFloat(orderData.totalPayment),
      discount: orderData.discount ? parseFloat(orderData.discount) : null,
      discountRatio: orderData.discountRatio || null,
      status: orderData.status,
      description: orderData.description || null,
      usingCod: orderData.usingCod || false,
      expectedDelivery: orderData.expectedDelivery
        ? new Date(orderData.expectedDelivery)
        : null,
      makeInvoice: orderData.makeInvoice || false,
      retailerId: orderData.retailerId || null,
      modifiedDate: orderData.modifiedDate
        ? new Date(orderData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
      larkSyncStatus: 'PENDING',
    };

    // Handle soldBy relationship (not soldById)
    if (orderData.soldById) {
      const user = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(orderData.soldById) },
      });
      if (user) {
        data.soldBy = { connect: { id: user.id } };
      }
    }

    // Handle relationships (same as create)
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

  private async handleOrderRelations(
    orderId: number,
    orderData: any,
  ): Promise<void> {
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
                quantity: parseFloat(detail.quantity),
                price: parseFloat(detail.price),
                discount: detail.discount ? parseFloat(detail.discount) : null,
                discountRatio: detail.discountRatio || null,
                note: detail.note || null,
                isMaster:
                  detail.isMaster !== undefined ? detail.isMaster : true,
              },
            });
          }
        } catch (error) {
          this.logger.error(`Failed to create order detail: ${error.message}`);
        }
      }
    }

    // Handle order delivery
    if (orderData.orderDelivery) {
      await this.prismaService.orderDelivery.deleteMany({
        where: { orderId },
      });

      try {
        await this.prismaService.orderDelivery.create({
          data: {
            kiotVietId: orderData.orderDelivery.kiotVietId
              ? BigInt(orderData.orderDelivery.kiotVietId)
              : null,
            orderId,
            deliveryCode: orderData.orderDelivery.deliveryCode || null,
            type: orderData.orderDelivery.type || null,
            price: orderData.orderDelivery.price
              ? parseFloat(orderData.orderDelivery.price)
              : null,
            receiver: orderData.orderDelivery.receiver || null,
            contactNumber: orderData.orderDelivery.contactNumber || null,
            address: orderData.orderDelivery.address || null,
            locationId: orderData.orderDelivery.locationId || null,
            locationName: orderData.orderDelivery.locationName || null,
            wardName: orderData.orderDelivery.wardName || null,
            weight: orderData.orderDelivery.weight || null,
            length: orderData.orderDelivery.length || null,
            width: orderData.orderDelivery.width || null,
            height: orderData.orderDelivery.height || null,
            partnerDeliveryId: orderData.orderDelivery.partnerDeliveryId
              ? BigInt(orderData.orderDelivery.partnerDeliveryId)
              : null,
          },
        });
      } catch (error) {
        this.logger.error(`Failed to create order delivery: ${error.message}`);
      }
    }

    // Handle order surcharges
    if (orderData.orderSurcharges && orderData.orderSurcharges.length > 0) {
      await this.prismaService.orderSurcharge.deleteMany({
        where: { orderId },
      });

      for (const surcharge of orderData.orderSurcharges) {
        try {
          const surchargeRecord = await this.prismaService.surcharge.findFirst({
            where: { kiotVietId: surcharge.surchargeId },
          });

          await this.prismaService.orderSurcharge.create({
            data: {
              kiotVietId: surcharge.kiotVietId
                ? BigInt(surcharge.kiotVietId)
                : null,
              orderId,
              surchargeId: surchargeRecord?.id || null,
              surchargeName: surcharge.surchargeName || null,
              surValue: surcharge.surValue
                ? parseFloat(surcharge.surValue)
                : null,
              price: surcharge.price ? parseFloat(surcharge.price) : null,
              createdDate: surcharge.createdDate
                ? new Date(surcharge.createdDate)
                : new Date(),
            },
          });
        } catch (error) {
          this.logger.error(
            `Failed to create order surcharge: ${error.message}`,
          );
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
              code: payment.code || null,
              amount: parseFloat(payment.amount),
              method: payment.method,
              status: payment.status || null,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id || null,
              bankAccountInfo: payment.bankAccount || null,
              orderId,
              description: payment.description || null,
            },
          });
        } catch (error) {
          this.logger.error(`Failed to create payment: ${error.message}`);
        }
      }
    }
  }

  private async handleRemovedOrders(removedIds: number[]): Promise<void> {
    try {
      for (const removedId of removedIds) {
        await this.prismaService.order.updateMany({
          where: { kiotVietId: BigInt(removedId) },
          data: { status: 2 },
        });
      }
      this.logger.log(`Marked ${removedIds.length} orders as cancelled`);
    } catch (error) {
      this.logger.error(`Failed to handle removed orders: ${error.message}`);
    }
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    const historicalSync = await this.prismaService.syncControl.findFirst({
      where: { name: 'order_historical' },
    });

    if (!historicalSync) {
      this.logger.log(
        'No historical sync record found. Starting full historical sync...',
      );
      await this.syncHistoricalOrders();
      return;
    }

    // If historical sync is completed and disabled, run recent sync
    if (historicalSync.status === 'completed' && !historicalSync.isEnabled) {
      this.logger.log('Historical sync completed. Running recent sync...');
      await this.syncRecentOrders();
      return;
    }

    // If historical sync is enabled but not running, start it
    if (historicalSync.isEnabled && !historicalSync.isRunning) {
      this.logger.log('Starting historical sync...');
      await this.syncHistoricalOrders();
      return;
    }

    // If historical sync is running, skip
    if (historicalSync.isRunning) {
      this.logger.log('Historical sync is already running. Skipping...');
      return;
    }

    // Default to recent sync
    this.logger.log('Running recent sync...');
    await this.syncRecentOrders();
  }
}
