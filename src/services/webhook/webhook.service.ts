import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { JobQueueService } from '../queue/job-queue.service';
import { Prisma } from '@prisma/client';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly jobQueueService: JobQueueService,
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

            await this.jobQueueService.addJob(
              'order',
              savedOrder.id,
              savedOrder.kiotVietId,
            );
          }
        }
      }
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

            if (
              savedInvoice.larkSyncStatus === 'PENDING' ||
              savedInvoice.larkSyncStatus === 'FAILED'
            ) {
              await this.jobQueueService.addJob(
                'invoice',
                savedInvoice.id,
                savedInvoice.kiotVietId,
              );
            } else {
              this.logger.log(
                `⏭️ Invoice ${savedInvoice.code} already ${savedInvoice.larkSyncStatus}, no sync needed`,
              );
            }
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process invoice webhook failed: ${error.message}`);
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
      const invoiceCode = invoiceData.Code;

      const branchId = await this.findBranchId(invoiceData.BranchId);
      const customerId = await this.findCustomerId(invoiceData.CustomerId);
      const soldById = invoiceData.SoldById
        ? BigInt(invoiceData.SoldById)
        : null;
      const orderId = invoiceData.OrderId ? invoiceData.OrderId : null;
      const saleChannelId = await this.findSaleChannelId(
        invoiceData.SaleChannelId,
      );

      const existingInvoice = await this.prismaService.invoice.findFirst({
        where: {
          OR: [{ kiotVietId }, { code: invoiceCode }],
        },
      });

      const basePayload = {
        kiotVietId,
        code: invoiceCode,
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
        lastSyncedAt: new Date(),
      };

      if (existingInvoice) {
        // Detect thay đổi thực sự
        const hasChanged = this.detectInvoiceChanges(
          existingInvoice,
          basePayload,
        );

        const updatePayload = {
          ...basePayload,
          // Reset về PENDING nếu:
          // 1. Có thay đổi thực sự, HOẶC
          // 2. Chưa SYNCED (PENDING/FAILED)
          larkSyncStatus:
            hasChanged || existingInvoice.larkSyncStatus !== 'SYNCED'
              ? ('PENDING' as const)
              : existingInvoice.larkSyncStatus,
        };

        return await this.prismaService.invoice.update({
          where: { id: existingInvoice.id },
          data: updatePayload,
        });
      }

      return await this.prismaService.invoice.create({
        data: {
          ...basePayload,
          larkSyncStatus: 'PENDING' as const,
        },
      });
    } catch (error) {
      this.logger.error(`❌ Upsert invoice failed: ${error.message}`);
      throw error;
    }
  }

  private detectInvoiceChanges(existing: any, newData: any): boolean {
    const fieldsToCompare = [
      'total',
      'totalPayment',
      'discount',
      'discountRatio',
      'status',
      'statusValue',
      'description',
      'branchId',
      'customerId',
      'soldById',
      'orderId',
      'saleChannelId',
    ];

    for (const field of fieldsToCompare) {
      const existingValue = existing[field];
      const newValue = newData[field];

      if (existingValue instanceof Prisma.Decimal) {
        if (!existingValue.equals(newValue)) {
          this.logger.log(
            `Invoice changed: ${field} (${existingValue} → ${newValue})`,
          );
          return true;
        }
        continue;
      }

      if (typeof existingValue === 'bigint') {
        if (existingValue !== BigInt(newValue || 0)) {
          this.logger.log(
            `Invoice changed: ${field} (${existingValue} → ${newValue})`,
          );
          return true;
        }
        continue;
      }

      if (existingValue !== newValue) {
        this.logger.log(
          `Invoice changed: ${field} (${existingValue} → ${newValue})`,
        );
        return true;
      }
    }

    return false;
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
    return saleChannel?.id || 1;
  }
}
