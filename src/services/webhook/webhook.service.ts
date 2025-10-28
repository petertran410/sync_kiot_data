import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { Prisma } from '@prisma/client';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { KiotVietAuthService } from '../kiot-viet/auth.service';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);
  private readonly LARK_WEBHOOK_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/UgifaGlVqw56jvh9gx6l6Dhzg6f';
  private readonly LARK_WEBHOOK_CUSTOMER_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/PjojaSOgJwMtLJhk8NXl3eYFgqf';

  constructor(
    private readonly prismaService: PrismaService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
    private readonly configService: ConfigService,
  ) {}

  async processOrderWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const orderData of data) {
          const detailedOrder = await this.fetchOrderDetail(orderData.Id);

          if (detailedOrder) {
            await this.sendToLarkWebhook(detailedOrder);
          }

          const savedOrder = await this.upsertOrder(orderData, detailedOrder);

          if (savedOrder) {
            this.logger.log(`✅ Upserted order ${savedOrder.code}`);
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

        console.log(data);

        for (const invoiceData of data) {
          const detailedInvoice = await this.fetchInvoiceDetail(invoiceData.Id);

          if (detailedInvoice) {
            await this.sendToLarkWebhook(detailedInvoice);
          }

          const savedInvoice = await this.upsertInvoice(
            invoiceData,
            detailedInvoice,
          );

          if (savedInvoice) {
            this.logger.log(`✅ Upserted invoice ${savedInvoice.code}`);
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process invoice webhook failed: ${error.message}`);
      throw error;
    }
  }

  async processCustomerWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const customerData of data) {
          const detailedCustomer = await this.fetchCustomerDetail(
            customerData.Id,
          );

          if (detailedCustomer) {
            await this.sendToLarkCustomerWebhook(detailedCustomer);
          }

          const savedCustomer = await this.upsertCustomer(
            customerData,
            detailedCustomer,
          );

          if (savedCustomer) {
            this.logger.log(`✅ Upserted customer ${savedCustomer.code}`);
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process customer webhook failed: ${error.message}`);
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

  private async sendToLarkCustomerWebhook(webhookData: any): Promise<void> {
    try {
      await firstValueFrom(
        this.httpService.post(this.LARK_WEBHOOK_CUSTOMER_URL, webhookData, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );
      this.logger.log(`✅ Sent webhook data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
    }
  }

  private async upsertOrder(orderData: any, detailedOrder: any) {
    try {
      const kiotVietId = BigInt(orderData.Id);
      const branchId = await this.findBranchId(orderData.BranchId);
      const customerId = await this.findCustomerId(orderData.CustomerId);
      const soldById = orderData.SoldById ? BigInt(orderData.SoldById) : null;
      const saleChannel = await this.findSaleChannelId(orderData.SaleChannelId);
      const orderCode = orderData.Code;
      const shouldSyncToLark = orderCode && orderCode.includes('DH0');

      const order = await this.prismaService.order.upsert({
        where: { kiotVietId },
        update: {
          status: orderData.Status,
          statusValue: orderData.StatusValue,
          total: new Prisma.Decimal(orderData.Total || 0),
          totalPayment: new Prisma.Decimal(orderData.TotalPayment || 0),
          customerCode: detailedOrder?.customerCode ?? orderData.CustomerCode,
          customerName: detailedOrder?.customerName ?? orderData.CustomerName,
          saleChannelId: saleChannel.id,
          saleChannelName: saleChannel.name,
          discount: orderData.Discount
            ? new Prisma.Decimal(orderData.Discount)
            : null,
          discountRatio: orderData.DiscountRatio,
          description: detailedOrder?.description ?? orderData.Description,
          usingCod: detailedOrder?.usingCod ?? false,
          createdDate: detailedOrder?.createdDate
            ? new Date(detailedOrder.createdDate)
            : orderData.CreatedDate
              ? new Date(orderData.CreatedDate)
              : new Date(),
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          lastSyncedAt: new Date(),
          larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
        },
        create: {
          kiotVietId,
          code: orderData.Code,
          purchaseDate: new Date(orderData.PurchaseDate),
          branchId,
          soldById,
          customerId,
          customerCode: detailedOrder?.customerCode ?? orderData.CustomerCode,
          customerName: detailedOrder?.customerName ?? orderData.CustomerName,
          saleChannelId: saleChannel.id,
          saleChannelName: saleChannel.name,
          total: new Prisma.Decimal(orderData.Total || 0),
          totalPayment: new Prisma.Decimal(orderData.TotalPayment || 0),
          discount: orderData.Discount
            ? new Prisma.Decimal(orderData.Discount)
            : null,
          discountRatio: orderData.DiscountRatio,
          status: orderData.Status,
          statusValue: orderData.StatusValue,
          description: detailedOrder?.description ?? orderData.Description,
          usingCod: detailedOrder?.usingCod ?? false,
          createdDate: detailedOrder?.createdDate
            ? new Date(detailedOrder.createdDate)
            : orderData.CreatedDate
              ? new Date(orderData.CreatedDate)
              : new Date(),
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
        },
      });

      if (
        detailedOrder?.orderDetails &&
        detailedOrder.orderDetails.length > 0
      ) {
        for (let i = 0; i < detailedOrder.orderDetails.length; i++) {
          const detail = detailedOrder.orderDetails[i];
          const product = await this.prismaService.product.findUnique({
            where: { kiotVietId: BigInt(detail.productId) },
            select: { id: true, code: true, name: true },
          });

          if (product) {
            await this.prismaService.orderDetail.upsert({
              where: {
                orderId_lineNumber: { orderId: order.id, lineNumber: i + 1 },
              },
              update: {
                quantity: detail.quantity,
                price: new Prisma.Decimal(detail.price),
                discount: detail.discount
                  ? new Prisma.Decimal(detail.discount)
                  : null,
                discountRatio: detail.discountRatio,
                note: detail.note ?? null,
                isMaster: detail.isMaster ?? true,
                productCode: product.code,
                productName: product.name,
              },
              create: {
                orderId: order.id,
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                quantity: detail.quantity,
                price: new Prisma.Decimal(detail.price),
                discount: detail.discount
                  ? new Prisma.Decimal(detail.discount)
                  : null,
                discountRatio: detail.discountRatio,
                note: detail.note ?? null,
                isMaster: detail.isMaster ?? true,
                lineNumber: i + 1,
              },
            });
          }
        }
      }

      if (detailedOrder?.orderDelivery) {
        const delivery = detailedOrder.orderDelivery;
        await this.prismaService.orderDelivery.upsert({
          where: { orderId: order.id },
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
          },
          create: {
            orderId: order.id,
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
          },
        });
      }

      if (detailedOrder?.payments && detailedOrder.payments.length > 0) {
        for (const payment of detailedOrder.payments) {
          const bankAccount = payment.accountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.accountId },
                select: { id: true },
              })
            : null;

          await this.prismaService.payment.upsert({
            where: { kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0) },
            update: {
              orderId: order.id,
              code: payment.code,
              amount: new Prisma.Decimal(payment.amount),
              method: payment.method,
              status: payment.status,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id ?? null,
              description: payment.description,
            },
            create: {
              kiotVietId: payment.id ? BigInt(payment.id) : null,
              orderId: order.id,
              code: payment.code,
              amount: new Prisma.Decimal(payment.amount),
              method: payment.method,
              status: payment.status,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id ?? null,
              description: payment.description,
            },
          });
        }
      }

      if (
        detailedOrder?.invoiceOrderSurcharges &&
        detailedOrder.invoiceOrderSurcharges.length > 0
      ) {
        for (const surcharge of detailedOrder.invoiceOrderSurcharges) {
          const surchargeRecord = surcharge.surchargeId
            ? await this.prismaService.surcharge.findFirst({
                where: { kiotVietId: surcharge.surchargeId },
                select: { id: true },
              })
            : null;

          await this.prismaService.orderSurcharge.upsert({
            where: {
              kiotVietId: surcharge.id ? BigInt(surcharge.id) : BigInt(0),
            },
            update: {
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
            },
            create: {
              kiotVietId: surcharge.id ? BigInt(surcharge.id) : null,
              orderId: order.id,
              surchargeId: surchargeRecord?.id ?? null,
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
            },
          });
        }
      }

      return order;
    } catch (error) {
      this.logger.error(`❌ Upsert order failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertInvoice(invoiceData: any, detailedInvoice: any) {
    try {
      const kiotVietId = BigInt(invoiceData.Id);
      const branchId = await this.findBranchId(invoiceData.BranchId);
      const customerId = await this.findCustomerId(invoiceData.CustomerId);
      const soldById = invoiceData.SoldById
        ? BigInt(invoiceData.SoldById)
        : null;
      // const orderId = detailedInvoice?.orderId ?? invoiceData.OrderId ?? null;
      const orderId = detailedInvoice?.orderId
        ? await this.findOrderId(detailedInvoice.orderId)
        : invoiceData.OrderId
          ? await this.findOrderId(invoiceData.OrderId)
          : null;

      const saleChannel = await this.findSaleChannelId(
        invoiceData.SaleChannelId,
      );
      const invoiceCode = invoiceData.Code;
      const shouldSyncToLark = invoiceCode && invoiceCode.includes('HD0');

      const invoice = await this.prismaService.invoice.upsert({
        where: { kiotVietId },
        update: {
          orderCode: detailedInvoice?.orderCode ?? null,
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : 0,
          orderId,
          discountRatio: invoiceData.DiscountRatio,
          description: detailedInvoice?.description ?? invoiceData.Description,
          usingCod: detailedInvoice?.usingCod ?? false,
          customerCode: detailedInvoice?.customerCode ?? null,
          customerName: detailedInvoice?.customerName ?? null,
          createdDate: detailedInvoice?.createdDate
            ? new Date(detailedInvoice.createdDate)
            : invoiceData.CreatedDate
              ? new Date(invoiceData.CreatedDate)
              : new Date(),
          modifiedDate: detailedInvoice?.modifiedDate
            ? new Date(detailedInvoice.modifiedDate)
            : invoiceData.ModifiedDate
              ? new Date(invoiceData.ModifiedDate)
              : new Date(),
          lastSyncedAt: new Date(),
          larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
        },
        create: {
          kiotVietId,
          code: invoiceData.Code,
          orderCode: detailedInvoice?.orderCode ?? null,
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
          description: detailedInvoice?.description ?? invoiceData.Description,
          usingCod: detailedInvoice?.usingCod ?? false,
          customerCode: detailedInvoice?.customerCode ?? null,
          customerName: detailedInvoice?.customerName ?? null,
          saleChannelId: saleChannel.id,
          createdDate: detailedInvoice?.createdDate
            ? new Date(detailedInvoice.createdDate)
            : invoiceData.CreatedDate
              ? new Date(invoiceData.CreatedDate)
              : new Date(),
          modifiedDate: detailedInvoice?.modifiedDate
            ? new Date(detailedInvoice.modifiedDate)
            : invoiceData.ModifiedDate
              ? new Date(invoiceData.ModifiedDate)
              : new Date(),
          larkSyncStatus: shouldSyncToLark ? 'PENDING' : 'SKIP',
        },
      });

      if (
        detailedInvoice?.invoiceDetails &&
        detailedInvoice.invoiceDetails.length > 0
      ) {
        for (let i = 0; i < detailedInvoice.invoiceDetails.length; i++) {
          const detail = detailedInvoice.invoiceDetails[i];
          const product = await this.prismaService.product.findUnique({
            where: { kiotVietId: BigInt(detail.productId) },
            select: { id: true, code: true, name: true },
          });

          if (product) {
            await this.prismaService.invoiceDetail.upsert({
              where: {
                invoiceId_lineNumber: {
                  invoiceId: invoice.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                quantity: detail.quantity,
                price: new Prisma.Decimal(detail.price),
                discount: detail.discount
                  ? new Prisma.Decimal(detail.discount)
                  : null,
                discountRatio: detail.discountRatio,
                note: detail.note ?? null,
                serialNumbers: detail.serialNumbers ?? null,
                subTotal: new Prisma.Decimal(
                  detail.price * detail.quantity - (detail.discount || 0),
                ),
                productCode: product.code,
                productName: product.name,
              },
              create: {
                invoiceId: invoice.id,
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                quantity: detail.quantity,
                price: new Prisma.Decimal(detail.price),
                discount: detail.discount
                  ? new Prisma.Decimal(detail.discount)
                  : null,
                discountRatio: detail.discountRatio,
                note: detail.note ?? null,
                serialNumbers: detail.serialNumbers ?? null,
                subTotal: new Prisma.Decimal(
                  detail.price * detail.quantity - (detail.discount || 0),
                ),
                lineNumber: i + 1,
              },
            });
          }
        }
      }

      if (detailedInvoice?.invoiceDelivery) {
        const detail = detailedInvoice.invoiceDelivery;
        await this.prismaService.invoiceDelivery.upsert({
          where: { invoiceId: invoice.id },
          update: {
            deliveryCode: detail.deliveryCode,
            status: detail.status,
            type: detail.type,
            price: detail.price ? new Prisma.Decimal(detail.price) : null,
            receiver: detail.receiver,
            contactNumber: detail.contactNumber,
            address: detail.address,
            locationId: detail.locationId,
            locationName: detail.locationName,
            wardName: detail.wardName,
            usingPriceCod: detail.usingPriceCod || false,
            priceCodPayment: detail.priceCodPayment
              ? new Prisma.Decimal(detail.priceCodPayment)
              : null,
            weight: detail.weight,
            length: detail.length,
            width: detail.width,
            height: detail.height,
          },
          create: {
            invoiceId: invoice.id,
            deliveryCode: detail.deliveryCode,
            status: detail.status,
            type: detail.type,
            price: detail.price ? new Prisma.Decimal(detail.price) : null,
            receiver: detail.receiver,
            contactNumber: detail.contactNumber,
            address: detail.address,
            locationId: detail.locationId,
            locationName: detail.locationName,
            wardName: detail.wardName,
            usingPriceCod: detail.usingPriceCod || false,
            priceCodPayment: detail.priceCodPayment
              ? new Prisma.Decimal(detail.priceCodPayment)
              : null,
            weight: detail.weight,
            length: detail.length,
            width: detail.width,
            height: detail.height,
          },
        });
      }

      if (detailedInvoice?.payments && detailedInvoice.payments.length > 0) {
        for (const payment of detailedInvoice.payments) {
          const bankAccount = payment.accountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.accountId },
                select: { id: true },
              })
            : null;

          await this.prismaService.payment.upsert({
            where: { kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0) },
            update: {
              invoiceId: invoice.id,
              code: payment.code,
              amount: new Prisma.Decimal(payment.amount),
              method: payment.method,
              status: payment.status,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id ?? null,
              description: payment.description,
            },
            create: {
              kiotVietId: payment.id ? BigInt(payment.id) : null,
              invoiceId: invoice.id,
              code: payment.code,
              amount: new Prisma.Decimal(payment.amount),
              method: payment.method,
              status: payment.status,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id ?? null,
              description: payment.description,
            },
          });
        }
      }

      if (
        detailedInvoice?.invoiceOrderSurcharges &&
        detailedInvoice.invoiceOrderSurcharges.length > 0
      ) {
        for (const surcharge of detailedInvoice.invoiceOrderSurcharges) {
          const surchargeRecord = surcharge.surchargeId
            ? await this.prismaService.surcharge.findFirst({
                where: { kiotVietId: surcharge.surchargeId },
                select: { id: true },
              })
            : null;

          await this.prismaService.invoiceSurcharge.upsert({
            where: {
              kiotVietId: surcharge.id ? BigInt(surcharge.id) : BigInt(0),
            },
            update: {
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
            },
            create: {
              kiotVietId: surcharge.id ? BigInt(surcharge.id) : null,
              invoiceId: invoice.id,
              surchargeId: surchargeRecord?.id ?? null,
              surchargeName: surcharge.surchargeName,
              surValue: surcharge.surValue
                ? new Prisma.Decimal(surcharge.surValue)
                : null,
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
            },
          });
        }
      }

      return invoice;
    } catch (error) {
      this.logger.error(`❌ Upsert invoice failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertCustomer(customerData: any, detailedCustomer: any) {
    try {
      const kiotVietId = BigInt(customerData.Id);

      const branchId = detailedCustomer?.branchId
        ? await this.findBranchId(detailedCustomer.branchId)
        : null;

      const customer = await this.prismaService.customer.upsert({
        where: { kiotVietId },
        update: {
          code: customerData.Code,
          name: customerData.Name,
          type: customerData.Type ?? null,
          gender: customerData.Gender ?? null,
          birthDate: customerData.BirthDate
            ? new Date(customerData.BirthDate)
            : null,
          contactNumber: customerData.ContactNumber ?? null,
          address: customerData.Address ?? null,
          locationName: customerData.LocationName ?? null,
          email: customerData.Email ?? null,
          organization: customerData.Organization ?? null,
          taxCode: customerData.TaxCode ?? null,
          comments: customerData.Comments ?? null,
          debt: detailedCustomer?.debt
            ? new Prisma.Decimal(detailedCustomer.debt)
            : null,
          totalInvoiced: detailedCustomer?.totalInvoiced
            ? new Prisma.Decimal(detailedCustomer.totalInvoiced)
            : null,
          totalPoint: detailedCustomer?.totalPoint ?? null,
          totalRevenue: detailedCustomer?.totalRevenue
            ? new Prisma.Decimal(detailedCustomer.totalRevenue)
            : null,
          rewardPoint: detailedCustomer?.rewardPoint
            ? BigInt(detailedCustomer.rewardPoint)
            : null,
          groups: detailedCustomer?.groups ?? null,
          branchId,
          modifiedDate: customerData.ModifiedDate
            ? new Date(customerData.ModifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
        create: {
          kiotVietId,
          code: customerData.Code,
          name: customerData.Name,
          type: customerData.Type ?? null,
          gender: customerData.Gender ?? null,
          birthDate: customerData.BirthDate
            ? new Date(customerData.BirthDate)
            : null,
          contactNumber: customerData.ContactNumber ?? null,
          address: customerData.Address ?? null,
          locationName: customerData.LocationName ?? null,
          email: customerData.Email ?? null,
          organization: customerData.Organization ?? null,
          taxCode: customerData.TaxCode ?? null,
          comments: customerData.Comments ?? null,
          debt: detailedCustomer?.debt
            ? new Prisma.Decimal(detailedCustomer.debt)
            : null,
          totalInvoiced: detailedCustomer?.totalInvoiced
            ? new Prisma.Decimal(detailedCustomer.totalInvoiced)
            : null,
          totalPoint: detailedCustomer?.totalPoint ?? null,
          totalRevenue: detailedCustomer?.totalRevenue
            ? new Prisma.Decimal(detailedCustomer.totalRevenue)
            : null,
          rewardPoint: detailedCustomer?.rewardPoint
            ? BigInt(detailedCustomer.rewardPoint)
            : null,
          groups: detailedCustomer?.groups ?? null,
          branchId,
          createdDate: detailedCustomer?.createdDate
            ? new Date(detailedCustomer.createdDate)
            : new Date(),
          modifiedDate: customerData.ModifiedDate
            ? new Date(customerData.ModifiedDate)
            : new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      if (detailedCustomer?.groups) {
        await this.syncCustomerGroups(
          customer.id,
          kiotVietId,
          detailedCustomer.groups,
        );
      }

      return customer;
    } catch (error) {
      this.logger.error(`❌ Upsert customer failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchCustomerDetail(customerId: number): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
      const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

      const url = `${baseUrl}/customers/${customerId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      this.logger.log('📦 Fetched customer detail:');
      this.logger.log(JSON.stringify(response.data, null, 2));

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch customer detail: ${error.message}`);
      return null;
    }
  }

  private async fetchOrderDetail(orderId: number): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
      const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

      const url = `${baseUrl}/orders/${orderId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      this.logger.log('📦 Fetched order detail:');
      this.logger.log(JSON.stringify(response.data, null, 2));

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch order detail: ${error.message}`);
      return null;
    }
  }

  private async fetchInvoiceDetail(invoiceId: number): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
      const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

      const url = `${baseUrl}/invoices/${invoiceId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      this.logger.log('📦 Fetched invoice detail:');
      this.logger.log(JSON.stringify(response.data, null, 2));

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch invoice detail: ${error.message}`);
      return null;
    }
  }

  private async syncCustomerGroups(
    customerId: number,
    kiotVietCustomerId: bigint,
    groupsString: string,
  ): Promise<void> {
    try {
      if (!groupsString) return;

      const groupNames = groupsString.split(',').map((g) => g.trim());

      const customerGroups = await this.prismaService.customerGroup.findMany({
        where: {
          name: {
            in: groupNames,
          },
        },
      });

      for (const group of customerGroups) {
        await this.prismaService.customerGroupRelation.upsert({
          where: {
            customerId_customerGroupId: {
              customerId,
              customerGroupId: group.id,
            },
          },
          update: {},
          create: {
            customerId,
            customerGroupId: group.id,
          },
        });
      }
    } catch (error) {
      this.logger.warn(`⚠️ Sync customer groups failed: ${error.message}`);
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
  ): Promise<{ id: number | null; name: string }> {
    if (!kiotVietSaleChannelId) {
      const defaultChannel = await this.prismaService.saleChannel.findUnique({
        where: { id: 1 },
        select: { name: true },
      });
      return { id: null, name: defaultChannel?.name || 'Bán trực tiếp' };
    }

    const saleChannel = await this.prismaService.saleChannel.findUnique({
      where: { kiotVietId: kiotVietSaleChannelId },
      select: { id: true, name: true },
    });

    if (saleChannel) {
      return { id: saleChannel.id, name: saleChannel.name };
    }

    const defaultChannel = await this.prismaService.saleChannel.findUnique({
      where: { id: 1 },
      select: { name: true },
    });
    return { id: 1, name: defaultChannel?.name || 'Bán trực tiếp' };
  }

  private async findOrderId(kiotVietOrderId: number): Promise<number | null> {
    if (!kiotVietOrderId) return null;
    const order = await this.prismaService.order.findUnique({
      where: { kiotVietId: BigInt(kiotVietOrderId) },
    });
    return order?.id || null;
  }
}
