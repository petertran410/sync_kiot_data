import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { Prisma } from '@prisma/client';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
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

  async processInvoiceWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const invoiceData of data) {
          const savedInvoice = await this.upsertInvoice(invoiceData);

          if (savedInvoice) {
            await this.larkInvoiceSyncService.syncInvoicesToLarkBase([
              savedInvoice,
            ]);
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process invoice webhook failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertInvoice(invoiceData: any) {
    try {
      const kiotVietId = BigInt(invoiceData.Id);
      const branchId = await this.findBranchId(invoiceData.BranchId);
      const customerId = await this.findCustomerId(invoiceData.CustomerId);
      const orderId = await this.findOrderId(invoiceData.OrderCode);
      const soldById = invoiceData.SoldById
        ? BigInt(invoiceData.SoldById)
        : null;
      const saleChannelId = await this.findSaleChannelId(
        invoiceData.SaleChannelId,
      );

      const invoice = await this.prismaService.invoice.upsert({
        where: { kiotVietId },
        update: {
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : null,
          discountRatio: invoiceData.DiscountRatio,
          description: invoiceData.Description,
          modifiedDate: invoiceData.ModifiedDate
            ? new Date(invoiceData.ModifiedDate)
            : null,
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
        create: {
          kiotVietId,
          code: invoiceData.Code,
          orderCode: invoiceData.OrderCode,
          purchaseDate: new Date(invoiceData.PurchaseDate),
          branchId,
          soldById,
          customerId,
          orderId,
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : null,
          discountRatio: invoiceData.DiscountRatio,
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          description: invoiceData.Description,
          usingCod: invoiceData.UsingCod || false,
          saleChannelId,
          isApplyVoucher: invoiceData.IsApplyVoucher || false,
          retailerId: invoiceData.RetailerId,
          createdDate: invoiceData.CreatedDate
            ? new Date(invoiceData.CreatedDate)
            : new Date(),
          modifiedDate: invoiceData.ModifiedDate
            ? new Date(invoiceData.ModifiedDate)
            : null,
          customerCode: invoiceData.CustomerCode,
          customerName: invoiceData.CustomerName,
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      await this.upsertInvoiceDetails(invoice.id, invoiceData.InvoiceDetails);
      await this.upsertInvoiceDelivery(invoice.id, invoiceData.InvoiceDelivery);
      await this.upsertInvoiceSurcharges(
        invoice.id,
        invoiceData.InvoiceSurcharges,
      );
      await this.upsertPayments(invoice.id, invoiceData.Payments);

      this.logger.log(`✅ Upserted invoice ${invoice.code}`);
      return invoice;
    } catch (error) {
      this.logger.error(`❌ Upsert invoice failed: ${error.message}`);
      return null;
    }
  }

  private async upsertInvoiceDetails(invoiceId: number, details: any[]) {
    if (!details || details.length === 0) return;

    for (let i = 0; i < details.length; i++) {
      const detail = details[i];
      const productId = await this.findProductId(detail.ProductId);
      if (!productId) continue;

      await this.prismaService.invoiceDetail.upsert({
        where: {
          invoiceId_lineNumber: {
            invoiceId,
            lineNumber: i + 1,
          },
        },
        update: {
          quantity: detail.Quantity,
          price: new Prisma.Decimal(detail.Price || 0),
          discount: detail.Discount
            ? new Prisma.Decimal(detail.Discount)
            : null,
          discountRatio: detail.DiscountRatio,
          note: detail.Note,
          serialNumbers: detail.SerialNumbers,
          subTotal: new Prisma.Decimal(detail.SubTotal || 0),
        },
        create: {
          invoiceId,
          productId,
          productCode: detail.ProductCode,
          productName: detail.ProductName,
          quantity: detail.Quantity,
          price: new Prisma.Decimal(detail.Price || 0),
          discount: detail.Discount
            ? new Prisma.Decimal(detail.Discount)
            : null,
          discountRatio: detail.DiscountRatio,
          note: detail.Note,
          serialNumbers: detail.SerialNumbers,
          lineNumber: i + 1,
          subTotal: new Prisma.Decimal(detail.SubTotal || 0),
        },
      });
    }
  }

  private async upsertInvoiceDelivery(invoiceId: number, delivery: any) {
    if (!delivery) return;

    await this.prismaService.invoiceDelivery.upsert({
      where: { invoiceId },
      update: {
        deliveryCode: delivery.DeliveryCode,
        status: delivery.Status,
        type: delivery.Type,
        price: delivery.Price ? new Prisma.Decimal(delivery.Price) : null,
        receiver: delivery.Receiver,
        contactNumber: delivery.ContactNumber,
        address: delivery.Address,
        locationId: delivery.LocationId,
        locationName: delivery.LocationName,
        wardName: delivery.WardName,
        usingPriceCod: delivery.UsingPriceCod || false,
        priceCodPayment: delivery.PriceCodPayment
          ? new Prisma.Decimal(delivery.PriceCodPayment)
          : null,
        weight: delivery.Weight,
        length: delivery.Length,
        width: delivery.Width,
        height: delivery.Height,
      },
      create: {
        invoiceId,
        deliveryCode: delivery.DeliveryCode,
        status: delivery.Status,
        type: delivery.Type,
        price: delivery.Price ? new Prisma.Decimal(delivery.Price) : null,
        receiver: delivery.Receiver,
        contactNumber: delivery.ContactNumber,
        address: delivery.Address,
        locationId: delivery.LocationId,
        locationName: delivery.LocationName,
        wardName: delivery.WardName,
        usingPriceCod: delivery.UsingPriceCod || false,
        priceCodPayment: delivery.PriceCodPayment
          ? new Prisma.Decimal(delivery.PriceCodPayment)
          : null,
        weight: delivery.Weight,
        length: delivery.Length,
        width: delivery.Width,
        height: delivery.Height,
      },
    });
  }

  private async upsertInvoiceSurcharges(invoiceId: number, surcharges: any[]) {
    if (!surcharges || surcharges.length === 0) return;

    for (const surcharge of surcharges) {
      if (!surcharge.Id) continue;

      const kiotVietId = BigInt(surcharge.Id);
      const surchargeId = await this.findSurchargeId(surcharge.SurchargeId);

      await this.prismaService.invoiceSurcharge.upsert({
        where: { kiotVietId },
        update: {
          surchargeName: surcharge.SurchargeName,
          surValue: surcharge.SurValue
            ? new Prisma.Decimal(surcharge.SurValue)
            : null,
          price: surcharge.Price ? new Prisma.Decimal(surcharge.Price) : null,
        },
        create: {
          kiotVietId,
          invoiceId,
          surchargeId,
          surchargeName: surcharge.SurchargeName,
          surValue: surcharge.SurValue
            ? new Prisma.Decimal(surcharge.SurValue)
            : null,
          price: surcharge.Price ? new Prisma.Decimal(surcharge.Price) : null,
        },
      });
    }
  }

  private async upsertPayments(invoiceId: number, payments: any[]) {
    if (!payments || payments.length === 0) return;

    for (const payment of payments) {
      if (!payment.Id) continue;

      const kiotVietId = BigInt(payment.Id);
      const accountId = await this.findBankAccountId(payment.AccountId);

      await this.prismaService.payment.upsert({
        where: { kiotVietId },
        update: {
          amount: new Prisma.Decimal(payment.Amount || 0),
          method: payment.Method,
          status: payment.Status,
          transDate: new Date(payment.TransDate),
          bankAccountInfo: payment.BankAccount,
          description: payment.Description,
          statusValue: payment.StatusValue,
        },
        create: {
          kiotVietId,
          code: payment.Code,
          amount: new Prisma.Decimal(payment.Amount || 0),
          method: payment.Method,
          status: payment.Status,
          transDate: new Date(payment.TransDate),
          accountId,
          bankAccountInfo: payment.BankAccount,
          invoiceId,
          description: payment.Description,
          statusValue: payment.StatusValue,
        },
      });
    }
  }

  private async findOrderId(orderCode: string): Promise<number | null> {
    if (!orderCode) return null;
    const order = await this.prismaService.order.findUnique({
      where: { code: orderCode },
    });
    return order?.id || null;
  }

  private async findSurchargeId(kiotVietId: number): Promise<number | null> {
    if (!kiotVietId) return null;
    const surcharge = await this.prismaService.surcharge.findUnique({
      where: { kiotVietId },
    });
    return surcharge?.id || null;
  }

  private async findBankAccountId(kiotVietId: number): Promise<number | null> {
    if (!kiotVietId) return null;
    const account = await this.prismaService.bankAccount.findUnique({
      where: { kiotVietId },
    });
    return account?.id || null;
  }
}
