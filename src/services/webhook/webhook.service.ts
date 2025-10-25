import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { Prisma } from '@prisma/client';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);
  private readonly LARK_WEBHOOK_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/UgifaGlVqw56jvh9gx6l6Dhzg6f';

  constructor(
    private readonly prismaService: PrismaService,
    private readonly httpService: HttpService,
  ) {}

  async processOrderWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const orderData of data) {
          const savedOrder = await this.upsertOrder(orderData);

          if (savedOrder) {
            this.logger.log(`✅ Upserted order ${savedOrder.code}`);
          }
        }
      }

      await this.sendToLarkWebhook(webhookData);
    } catch (error) {
      this.logger.error(`❌ Process webhook failed: ${error.message}`);
      throw error;
    }
  }

  async processInvoiceWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const invoiceData of data) {
          const savedInvoice = await this.upsertInvoice(invoiceData);

          if (savedInvoice) {
            this.logger.log(`✅ Upserted invoice ${savedInvoice.code}`);
          }
        }
      }

      await this.sendToLarkWebhook(webhookData);
    } catch (error) {
      this.logger.error(`❌ Process invoice webhook failed: ${error.message}`);
      throw error;
    }
  }

  private async sendToLarkWebhook(webhookData: any): Promise<void> {
    try {
      await firstValueFrom(
        this.httpService.post(this.LARK_WEBHOOK_URL, webhookData, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );
      this.logger.log(`✅ Sent webhook data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
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
          saleChannelId,
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          larkSyncStatus: 'PENDING',
        },
      });

      return order;
    } catch (error) {
      this.logger.error(`❌ Upsert order failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertInvoice(invoiceData: any) {
    try {
      const kiotVietId = BigInt(invoiceData.Id);
      const branchId = await this.findBranchId(invoiceData.BranchId);
      const customerId = await this.findCustomerId(invoiceData.CustomerId);
      const soldById = invoiceData.SoldById
        ? BigInt(invoiceData.SoldById)
        : null;
      const orderId = invoiceData.OrderId ? invoiceData.OrderId : null;
      const saleChannelId = await this.findSaleChannelId(
        invoiceData.SaleChannelId,
      );

      const invoice = await this.prismaService.invoice.upsert({
        where: { kiotVietId },
        update: {
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : 0,
          discountRatio: invoiceData.DiscountRatio,
          createdDate: invoiceData.CreatedDate
            ? new Date(invoiceData.CreatedDate)
            : new Date(),
          modifiedDate: invoiceData.ModifiedDate
            ? new Date(invoiceData.ModifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
        create: {
          kiotVietId,
          code: invoiceData.Code,
          purchaseDate: new Date(invoiceData.PurchaseDate),
          branchId,
          soldById,
          customerId,
          orderId,
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : 0,
          discountRatio: invoiceData.DiscountRatio,
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          description: invoiceData.Description,
          saleChannelId,
          createdDate: invoiceData.CreatedDate
            ? new Date(invoiceData.CreatedDate)
            : new Date(),
          modifiedDate: invoiceData.ModifiedDate
            ? new Date(invoiceData.ModifiedDate)
            : new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      return invoice;
    } catch (error) {
      this.logger.error(`❌ Upsert invoice failed: ${error.message}`);
      throw error;
    }
  }

  private async findBranchId(kiotVietBranchId: number): Promise<number | null> {
    if (!kiotVietBranchId) return null;
    const branch = await this.prismaService.branch.findUnique({
      where: { kiotVietId: kiotVietBranchId },
    });
    return branch?.id || null;
  }

  private async findCustomerId(
    kiotVietCustomerId: number,
  ): Promise<number | null> {
    if (!kiotVietCustomerId) return null;
    const customer = await this.prismaService.customer.findUnique({
      where: { kiotVietId: kiotVietCustomerId },
    });
    return customer?.id || null;
  }

  private async findSaleChannelId(
    kiotVietSaleChannelId: number,
  ): Promise<number | null> {
    if (!kiotVietSaleChannelId) return null;
    const saleChannel = await this.prismaService.saleChannel.findUnique({
      where: { kiotVietId: kiotVietSaleChannelId },
    });
    return saleChannel?.id || null;
  }
}
