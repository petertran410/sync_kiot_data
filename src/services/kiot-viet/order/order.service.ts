// src/services/kiot-viet/order/order.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { LarkBaseService } from '../../lark/lark-base.service';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

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

          await this.markOrdersForLarkBaseSync(response.data);

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
        `Historical sync completed: ${totalProcessed} orders processed`,
      );

      await this.syncPendingToLarkBase();
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

          await this.markOrdersForLarkBaseSync(response.data);

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
        `Recent sync completed: ${totalProcessed} orders processed`,
      );

      await this.syncPendingToLarkBase();
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

  // ===== DATABASE → LARKBASE SYNC METHODS =====
  async syncPendingToLarkBase(): Promise<{ success: number; failed: number }> {
    try {
      const pendingOrders = await this.prismaService.order.findMany({
        where: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: { lt: 3 },
        },
        include: {
          branch: true,
          customer: true,
          soldBy: true,
          invoices: true,
          orderDelivery: true,
          orderSurcharges: true,
        },
        take: 100,
      });

      if (pendingOrders.length === 0) {
        this.logger.log('No pending orders to sync to LarkBase');
        return { success: 0, failed: 0 };
      }

      this.logger.log(
        `Syncing ${pendingOrders.length} pending orders to LarkBase`,
      );

      const recordsToCreate = [];
      const recordsToUpdate = [];

      for (const order of pendingOrders) {
        if (order.larkRecordId) {
          recordsToUpdate.push(order);
        } else {
          recordsToCreate.push(order);
        }
      }

      let totalSuccess = 0;
      let totalFailed = 0;

      if (recordsToCreate.length > 0) {
        const createResult = await this.larkBaseCreateBatch(recordsToCreate);
        totalSuccess += createResult.success;
        totalFailed += createResult.failed;
      }

      if (recordsToUpdate.length > 0) {
        const updateResult = await this.larkBaseUpdateBatch(recordsToUpdate);
        totalSuccess += updateResult.success;
        totalFailed += updateResult.failed;
      }

      this.logger.log(
        `LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: 0 };
    }
  }

  private async larkBaseCreateBatch(
    orders: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response = await this.larkBaseService.directCreateOrders(orders);

      if (response.success > 0 && response.records) {
        for (const [index, order] of orders.entries()) {
          if (response.records[index]) {
            await this.prismaService.order.update({
              where: { id: order.id },
              data: {
                larkRecordId: response.records[index].record_id,
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
                larkSyncRetries: 0,
              },
            });
          }
        }
      }

      if (response.failed > 0) {
        const failedOrders = orders.slice(response.success);
        for (const order of failedOrders) {
          await this.prismaService.order.update({
            where: { id: order.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase create batch failed: ${error.message}`);

      for (const order of orders) {
        await this.prismaService.order.update({
          where: { id: order.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: orders.length };
    }
  }

  private async larkBaseUpdateBatch(
    orders: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response = await this.larkBaseService.directUpdateOrders(orders);

      if (response.success > 0) {
        const successfulOrders = orders.slice(0, response.success);
        for (const order of successfulOrders) {
          await this.prismaService.order.update({
            where: { id: order.id },
            data: {
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });
        }
      }

      if (response.failed > 0) {
        const failedOrders = orders.slice(response.success);
        for (const order of failedOrders) {
          await this.prismaService.order.update({
            where: { id: order.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase update batch failed: ${error.message}`);

      for (const order of orders) {
        await this.prismaService.order.update({
          where: { id: order.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: orders.length };
    }
  }

  private async markOrdersForLarkBaseSync(orders: any[]): Promise<void> {
    try {
      const kiotVietIds = orders.map((o) => BigInt(o.id));

      await this.prismaService.order.updateMany({
        where: { kiotVietId: { in: kiotVietIds } },
        data: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: 0,
        },
      });

      this.logger.debug(`Marked ${orders.length} orders for LarkBase sync`);
    } catch (error) {
      this.logger.error(
        `Failed to mark orders for LarkBase sync: ${error.message}`,
      );
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
    const ordersToCreate: Array<{
      createData: Prisma.OrderCreateInput;
      orderData: any;
    }> = [];
    const ordersToUpdate: Array<{
      id: number;
      data: Prisma.OrderUpdateInput;
      orderData: any;
    }> = [];

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
    ordersToCreate: Array<{
      createData: Prisma.OrderCreateInput;
      orderData: any;
    }>,
    ordersToUpdate: Array<{
      id: number;
      data: Prisma.OrderUpdateInput;
      orderData: any;
    }>,
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

  private async prepareOrderCreateData(
    orderData: any,
  ): Promise<Prisma.OrderCreateInput | null> {
    try {
      const data: Prisma.OrderCreateInput = {
        kiotVietId: BigInt(orderData.id),
        code: orderData.code,
        purchaseDate: new Date(orderData.purchaseDate),
        soldById: orderData.soldById ? BigInt(orderData.soldById) : null,
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
      soldById: orderData.soldById ? BigInt(orderData.soldById) : null,
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
          data: { status: 2 }, // Assuming 2 means cancelled
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

    if (historicalSync?.isEnabled && historicalSync?.isRunning) {
      this.logger.log(
        'System restart detected: Historical sync was running, resuming...',
      );
      await this.syncHistoricalOrders();
    } else if (historicalSync?.isEnabled && !historicalSync?.isRunning) {
      this.logger.log(
        'System restart detected: Historical sync enabled, starting...',
      );
      await this.syncHistoricalOrders();
    } else if (historicalSync?.status === 'completed') {
      this.logger.log('Historical sync completed. Running recent sync...');
      await this.syncRecentOrders();
    } else {
      this.logger.log('System restart detected: Running recent sync...');
      await this.syncRecentOrders();
    }
  }
}
