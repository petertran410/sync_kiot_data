import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
  ) {}

  async processOrderWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const orderData of data) {
          const kiotVietId = BigInt(orderData.Id);

          const savedOrder = await this.upsertOrder(orderData);

          if (savedOrder) {
            await this.larkOrderSyncService.syncOrdersToLarkBase([savedOrder]);
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process webhook failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertOrder(orderData: any) {
    try {
      const kiotVietId = BigInt(orderData.Id);
      const branchId = await this.findBranchId(orderData.BranchId);
      const customerId = await this.findCustomerId(orderData.CustomerId);
      const soldById = orderData.SoldById ? BigInt(orderData.SoldById) : null;
      const saleChannelId = await this.findSaleChannelId(
        orderData.SaleChannelId,
      );

      const order = await this.prismaService.order.upsert({
        where: { kiotVietId },
        update: {
          status: orderData.Status,
          statusValue: orderData.StatusValue,
          total: new Prisma.Decimal(orderData.Total || 0),
          totalPayment: new Prisma.Decimal(orderData.TotalPayment || 0),
          discount: orderData.Discount
            ? new Prisma.Decimal(orderData.Discount)
            : null,
          discountRatio: orderData.DiscountRatio,
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
        create: {
          kiotVietId,
          code: orderData.Code,
          purchaseDate: new Date(orderData.PurchaseDate),
          branchId,
          soldById,
          customerId,
          total: new Prisma.Decimal(orderData.Total || 0),
          totalPayment: new Prisma.Decimal(orderData.TotalPayment || 0),
          discount: orderData.Discount
            ? new Prisma.Decimal(orderData.Discount)
            : null,
          discountRatio: orderData.DiscountRatio,
          status: orderData.Status,
          statusValue: orderData.StatusValue,
          description: orderData.Description,
          usingCod: orderData.UsingCod || false,
          saleChannelId,
          retailerId: orderData.RetailerId,
          createdDate: orderData.CreatedDate
            ? new Date(orderData.CreatedDate)
            : new Date(),
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          customerCode: orderData.CustomerCode,
          customerName: orderData.CustomerName,
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      await this.upsertOrderDetails(order.id, orderData.OrderDetails);
      await this.upsertOrderDelivery(order.id, orderData.OrderDelivery);

      this.logger.log(`✅ Upserted order ${order.code}`);
      return order;
    } catch (error) {
      this.logger.error(`❌ Upsert order failed: ${error.message}`);
      return null;
    }
  }

  private async upsertOrderDetails(orderId: number, details: any[]) {
    if (!details || details.length === 0) return;

    for (const detail of details) {
      const productId = await this.findProductId(detail.ProductId);
      if (!productId) continue;

      await this.prismaService.orderDetail.upsert({
        where: {
          orderId_lineNumber: {
            orderId,
            lineNumber: details.indexOf(detail),
          },
        },
        update: {
          quantity: detail.Quantity,
          price: new Prisma.Decimal(detail.Price || 0),
          discount: detail.Discount
            ? new Prisma.Decimal(detail.Discount)
            : null,
          discountRatio: detail.DiscountRatio,
        },
        create: {
          orderId,
          productId,
          quantity: detail.Quantity,
          price: new Prisma.Decimal(detail.Price || 0),
          discount: detail.Discount
            ? new Prisma.Decimal(detail.Discount)
            : null,
          discountRatio: detail.DiscountRatio,
          note: detail.Note,
          isMaster: detail.IsMaster ?? true,
          productCode: detail.ProductCode,
          productName: detail.ProductName,
          lineNumber: details.indexOf(detail),
        },
      });
    }
  }

  private async upsertOrderDelivery(orderId: number, delivery: any) {
    if (!delivery) return;

    await this.prismaService.orderDelivery.upsert({
      where: { orderId },
      update: {
        receiver: delivery.Receiver,
        contactNumber: delivery.ContactNumber,
        address: delivery.Address,
        locationName: delivery.LocationName,
        wardName: delivery.WardName,
      },
      create: {
        orderId,
        deliveryCode: delivery.DeliveryCode,
        type: delivery.Type,
        price: delivery.Price ? new Prisma.Decimal(delivery.Price) : null,
        receiver: delivery.Receiver,
        contactNumber: delivery.ContactNumber,
        address: delivery.Address,
        locationId: delivery.LocationId,
        locationName: delivery.LocationName,
        wardName: delivery.WardName,
        weight: delivery.Weight,
        length: delivery.Length,
        width: delivery.Width,
        height: delivery.Height,
      },
    });
  }

  private async findBranchId(kiotVietId: number) {
    const branch = await this.prismaService.branch.findUnique({
      where: { kiotVietId },
    });
    return branch?.id;
  }

  private async findCustomerId(kiotVietId: number) {
    const customer = await this.prismaService.customer.findUnique({
      where: { kiotVietId: BigInt(kiotVietId) },
    });
    return customer?.id;
  }

  private async findSaleChannelId(kiotVietId: number) {
    if (!kiotVietId) return null;
    const channel = await this.prismaService.saleChannel.findUnique({
      where: { kiotVietId },
    });
    return channel?.id;
  }

  private async findProductId(kiotVietId: number) {
    const product = await this.prismaService.product.findUnique({
      where: { kiotVietId: BigInt(kiotVietId) },
    });
    return product?.id;
  }
}
