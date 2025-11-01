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
  private readonly LARK_WEBHOOK_PRODUCT_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/P8dQa9E8DwdYXThRnkRlVgcdgnc';
  private readonly LARK_WEBHOOK_STOCK_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/KqIvamQWVwPwZghWn7nlAXXzg5V';
  private readonly LARK_WEBHOOK_PRICEBOOK_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/P6UgaRD1nwkhBVhAU6UlhvpWgXd';
  private readonly LARK_WEBHOOK_PRICEBOOK_DETAIL_URL =
    'https://dieptra2018.sg.larksuite.com/base/workflow/webhook/event/IG2qaSfewwlQLmhuNoflfOi5gWf';

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

  async processProductWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const productData of data) {
          const detailedProduct = await this.fetchProductDetail(productData.Id);

          if (detailedProduct) {
            await this.sendToLarkProductWebhook(detailedProduct);
          }

          const savedProduct = await this.upsertProduct(
            productData,
            detailedProduct,
          );

          if (savedProduct) {
            this.logger.log(`✅ Upserted product ${savedProduct.code}`);
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process product webhook failed: ${error.message}`);
      throw error;
    }
  }

  async processStockWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const stockData of data) {
          const savedStock = await this.upsertStock(stockData);

          if (savedStock) {
            await this.sendToLarkStockWebhook(savedStock);
            this.logger.log(
              `✅ Upserted stock for product ${stockData.ProductCode}`,
            );
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process stock webhook failed: ${error.message}`);
      throw error;
    }
  }

  async processPriceBookWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const priceBookData of data) {
          const detailedPriceBook = await this.fetchPriceBookDetail(
            priceBookData.Id,
          );

          if (detailedPriceBook) {
            await this.sendToLarkPricebookWebhook(detailedPriceBook);
          }

          const savedPriceBook = await this.upsertPriceBook(
            priceBookData,
            detailedPriceBook,
          );

          if (savedPriceBook) {
            this.logger.log(`✅ Upserted pricebook ${savedPriceBook.name}`);
          }
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Process pricebook webhook failed: ${error.message}`,
      );
      throw error;
    }
  }

  async processPriceBookDetailWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const detailData of data) {
          const savedDetail = await this.upsertPriceBookDetail(detailData);

          if (savedDetail) {
            await this.sendToLarkPricebookDetailWebhook(savedDetail);
            this.logger.log(
              `✅ Upserted priceBookDetail for product ${savedDetail.productName} in pricebook ${savedDetail.priceBookName}`,
            );
          }
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Process pricebook detail webhook failed: ${error.message}`,
      );
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
      this.logger.log(
        `✅ Sent webhook order & invoice data to Lark successfully`,
      );
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
      this.logger.log(`✅ Sent webhook customer data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
    }
  }

  private async sendToLarkProductWebhook(webhookData: any): Promise<void> {
    try {
      await firstValueFrom(
        this.httpService.post(this.LARK_WEBHOOK_PRODUCT_URL, webhookData, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );
      this.logger.log(`✅ Sent webhook product data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
    }
  }

  private async sendToLarkStockWebhook(webhookData: any): Promise<void> {
    try {
      await firstValueFrom(
        this.httpService.post(this.LARK_WEBHOOK_STOCK_URL, webhookData, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );
      this.logger.log(`✅ Sent webhook stock data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
    }
  }

  private async sendToLarkPricebookWebhook(webhookData: any): Promise<void> {
    try {
      await firstValueFrom(
        this.httpService.post(this.LARK_WEBHOOK_PRICEBOOK_URL, webhookData, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );
      this.logger.log(`✅ Sent webhook pricebook data to Lark successfully`);
    } catch (error) {
      this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
    }
  }

  private async sendToLarkPricebookDetailWebhook(
    webhookData: any,
  ): Promise<void> {
    try {
      const sanitizedData = JSON.parse(
        JSON.stringify(webhookData, (key, value) =>
          typeof value === 'bigint' ? value.toString() : value,
        ),
      );

      await firstValueFrom(
        this.httpService.post(
          this.LARK_WEBHOOK_PRICEBOOK_DETAIL_URL,
          sanitizedData,
          {
            headers: { 'Content-Type': 'application/json' },
          },
        ),
      );
      this.logger.log(
        `✅ Sent webhook pricebook detail data to Lark successfully`,
      );
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
          retailerId: 310831,
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
          retailerId: 310831,
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
          retailerId: 310831,
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
          retailerId: 310831,
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
            select: { id: true, code: true, name: true, kiotVietId: true },
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
                invoiceId: Number(invoice.kiotVietId),
                productId: Number(product.kiotVietId),
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
                invoiceId: Number(invoice.kiotVietId),
                productId: Number(product.kiotVietId),
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
          retailerId: 310831,
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
          retailerId: 310831,
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

  private async upsertProduct(productData: any, detailedProduct: any) {
    try {
      const kiotVietId = BigInt(productData.Id);

      const category = await this.prismaService.category.findFirst({
        where: { kiotVietId: productData.CategoryId },
        select: {
          id: true,
          name: true,
          parent_name: true,
          child_name: true,
          branch_name: true,
        },
      });
      const tradeMarkId = detailedProduct?.tradeMarkId
        ? await this.findTradeMarkId(detailedProduct.tradeMarkId)
        : productData.TradeMarkId
          ? await this.findTradeMarkId(productData.TradeMarkId)
          : null;

      const product = await this.prismaService.product.upsert({
        where: { kiotVietId },
        update: {
          code: productData.Code,
          name: productData.Name,
          fullName: productData.FullName ?? productData.Name,
          categoryId: category?.id ?? null,
          categoryName: productData.CategoryName ?? category?.name ?? null,
          parent_name: category?.parent_name ?? null,
          child_name: category?.child_name ?? null,
          branch_name: category?.branch_name ?? null,
          allowsSale: productData.AllowsSale ?? true,
          hasVariants: productData.HasVariants ?? false,
          basePrice: productData.BasePrice
            ? new Prisma.Decimal(productData.BasePrice)
            : null,
          retailerId: 310831,
          tradeMarkId,
          minQuantity: detailedProduct.minQuantity,
          maxQuantity: detailedProduct.maxQuantity,
          weight: productData.Weight ?? null,
          unit: productData.Unit ?? null,
          masterProductId: productData.masterProductId
            ? BigInt(productData.masterProductId)
            : null,
          masterUnitId: productData.MasterUnitId
            ? BigInt(productData.MasterUnitId)
            : null,
          conversionValue: productData.ConversionValue ?? null,
          createdDate: detailedProduct?.createdDate
            ? new Date(detailedProduct.createdDate)
            : new Date(),
          modifiedDate: productData.ModifiedDate
            ? new Date(productData.ModifiedDate)
            : new Date(),
          lastSyncedAt: new Date(),
          larkSyncStatus: 'PENDING',
        },
        create: {
          kiotVietId,
          code: productData.Code,
          name: productData.Name,
          fullName: productData.FullName ?? productData.Name,
          categoryId: category?.id ?? null,
          categoryName: productData.CategoryName ?? category?.name ?? null,
          retailerId: 310831,
          parent_name: category?.parent_name ?? null,
          child_name: category?.child_name ?? null,
          branch_name: category?.branch_name ?? null,
          allowsSale: productData.AllowsSale ?? true,
          hasVariants: productData.HasVariants ?? false,
          basePrice: productData.BasePrice
            ? new Prisma.Decimal(productData.BasePrice)
            : null,
          tradeMarkId,
          minQuantity: detailedProduct.minQuantity,
          maxQuantity: detailedProduct.maxQuantity,
          weight: productData.Weight ?? null,
          unit: productData.Unit ?? null,
          masterProductId: productData.masterProductId
            ? BigInt(productData.masterProductId)
            : null,
          masterUnitId: productData.MasterUnitId
            ? BigInt(productData.MasterUnitId)
            : null,
          conversionValue: productData.ConversionValue ?? null,
          createdDate: detailedProduct?.createdDate
            ? new Date(detailedProduct.createdDate)
            : new Date(),
          modifiedDate: productData.ModifiedDate
            ? new Date(productData.ModifiedDate)
            : new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      if (
        detailedProduct?.attributes &&
        detailedProduct.attributes.length > 0
      ) {
        for (let i = 0; i < detailedProduct.attributes.length; i++) {
          const attr = detailedProduct.attributes[i];
          await this.prismaService.productAttribute.upsert({
            where: {
              productId_lineNumber: {
                productId: product.id,
                lineNumber: i + 1,
              },
            },
            update: {
              attributeName: attr.attributeName,
              attributeValue: attr.attributeValue,
              lastSyncedAt: new Date(),
              lineNumber: i + 1,
            },
            create: {
              productId: product.id,
              attributeName: attr.attributeName,
              attributeValue: attr.attributeValue,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
          });
        }
      }

      if (
        detailedProduct?.inventories &&
        detailedProduct.inventories.length > 0
      ) {
        for (let i = 0; i < detailedProduct.inventories.length; i++) {
          const inv = detailedProduct.inventories[i];
          const branch = await this.prismaService.branch.findFirst({
            where: { kiotVietId: inv.branchId },
            select: { id: true },
          });

          if (branch) {
            await this.prismaService.productInventory.upsert({
              where: {
                productId_lineNumber: {
                  productId: product.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                branchId: branch.id,
                branchName: inv.branchName ?? null,
                productCode: productData.Code,
                productName: productData.Name,
                onHand: inv.onHand ?? 0,
                reserved: inv.reserved ?? 0,
                minQuantity: inv.minQuantity ?? 0,
                maxQuantity: inv.maxQuantity ?? 0,
                cost: inv.cost ? new Prisma.Decimal(inv.cost) : null,
                lastSyncedAt: new Date(),
                lineNumber: i + 1,
              },
              create: {
                productId: product.id,
                branchId: branch.id,
                branchName: inv.branchName ?? null,
                productCode: productData.Code,
                productName: productData.Name,
                onHand: inv.onHand ?? 0,
                reserved: inv.reserved ?? 0,
                minQuantity: inv.minQuantity ?? 0,
                maxQuantity: inv.maxQuantity ?? 0,
                cost: inv.cost ? new Prisma.Decimal(inv.cost) : null,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
        }
      }

      if (
        detailedProduct?.priceBooks &&
        detailedProduct.priceBooks.length > 0
      ) {
        for (let i = 0; i < detailedProduct.priceBooks.length; i++) {
          const pb = detailedProduct.priceBooks[i];

          const priceBook = await this.prismaService.priceBook.findFirst({
            where: { kiotVietId: pb.priceBookId },
            select: { id: true, name: true },
          });

          if (priceBook) {
            await this.prismaService.priceBookDetail.upsert({
              where: {
                productId_lineNumber: {
                  productId: product.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                priceBookId: priceBook.id,
                priceBookName: pb.priceBookName ?? priceBook.name,
                productName: productData.Name,
                price: pb.price
                  ? new Prisma.Decimal(pb.price)
                  : new Prisma.Decimal(0),
                lastSyncedAt: new Date(),
              },
              create: {
                productId: product.id,
                priceBookId: priceBook.id,
                priceBookName: pb.priceBookName ?? priceBook.name,
                productName: productData.Name,
                price: pb.price
                  ? new Prisma.Decimal(pb.price)
                  : new Prisma.Decimal(0),
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
        }
      }

      if (detailedProduct?.images && detailedProduct.images.length > 0) {
        for (let i = 0; i < detailedProduct.images.length; i++) {
          const img = detailedProduct.images[i];
          await this.prismaService.productImage.upsert({
            where: {
              productId_lineNumber: {
                productId: product.id,
                lineNumber: i + 1,
              },
            },
            update: {
              imageUrl: img.image ? { url: img.image } : Prisma.JsonNull,
              lastSyncedAt: new Date(),
            },
            create: {
              productId: product.id,
              imageUrl: img.image ? { url: img.image } : Prisma.JsonNull,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
          });
        }
      }

      return product;
    } catch (error) {
      this.logger.error(`❌ Upsert product failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertStock(stockData: any) {
    try {
      let product = await this.prismaService.product.findUnique({
        where: { kiotVietId: BigInt(stockData.ProductId) },
        select: { id: true, code: true, name: true },
      });

      if (!product) {
        this.logger.warn(
          `⚠️ Product not found: ${stockData.ProductCode}, creating new product...`,
        );

        const detailedProduct = await this.fetchProductDetail(
          stockData.ProductId,
        );

        if (detailedProduct) {
          await this.sendToLarkProductWebhook(detailedProduct);
        }

        if (detailedProduct) {
          await this.upsertProduct(
            {
              Id: stockData.ProductId,
              Code: stockData.ProductCode,
              Name: stockData.ProductName,
              FullName: detailedProduct.fullName,
              CategoryId: detailedProduct.categoryId,
              CategoryName: detailedProduct.categoryName,
              AllowsSale: detailedProduct.allowsSale,
              HasVariants: detailedProduct.hasVariants,
              BasePrice: detailedProduct.basePrice,
              Weight: detailedProduct.weight,
              Unit: detailedProduct.unit,
              MasterUnitId: detailedProduct.masterUnitId,
              ConversionValue: detailedProduct.conversionValue,
              ModifiedDate: detailedProduct.modifiedDate,
            },
            detailedProduct,
          );

          product = await this.prismaService.product.findUnique({
            where: { kiotVietId: BigInt(stockData.ProductId) },
            select: { id: true, code: true, name: true },
          });

          if (!product) {
            this.logger.error(
              `❌ Failed to create product: ${stockData.ProductCode}`,
            );
            return null;
          }

          this.logger.log(
            `✅ Created new product: ${stockData.ProductCode}, now updating stock...`,
          );
        } else {
          this.logger.error(
            `❌ Could not fetch product detail: ${stockData.ProductCode}`,
          );
          return null;
        }
      }

      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: stockData.BranchId },
        select: { id: true, name: true },
      });

      if (!branch) {
        this.logger.warn(`⚠️ Branch not found: ${stockData.BranchId}`);
        return null;
      }

      const existingInventory =
        await this.prismaService.productInventory.findFirst({
          where: {
            productId: product.id,
            branchId: branch.id,
          },
        });

      if (existingInventory) {
        return await this.prismaService.productInventory.update({
          where: {
            id: existingInventory.id,
          },
          data: {
            branchName: stockData.BranchName ?? branch.name,
            productCode: stockData.ProductCode ?? product.code,
            productName: stockData.ProductName ?? product.name,
            onHand: stockData.OnHand ?? 0,
            reserved: stockData.Reserved ?? 0,
            cost: stockData.Cost ? new Prisma.Decimal(stockData.Cost) : null,
            lastSyncedAt: new Date(),
          },
        });
      } else {
        const maxLineNumber =
          await this.prismaService.productInventory.findFirst({
            where: { productId: product.id },
            orderBy: { lineNumber: 'desc' },
            select: { lineNumber: true },
          });

        const newLineNumber = (maxLineNumber?.lineNumber ?? 0) + 1;

        return await this.prismaService.productInventory.create({
          data: {
            productId: product.id,
            branchId: branch.id,
            branchName: stockData.BranchName ?? branch.name,
            productCode: stockData.ProductCode ?? product.code,
            productName: stockData.ProductName ?? product.name,
            onHand: stockData.OnHand ?? 0,
            reserved: stockData.Reserved ?? 0,
            cost: stockData.Cost ? new Prisma.Decimal(stockData.Cost) : null,
            lineNumber: newLineNumber,
            lastSyncedAt: new Date(),
          },
        });
      }
    } catch (error) {
      this.logger.error(`❌ Upsert stock failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertPriceBook(priceBookData: any, detailedPriceBook: any) {
    try {
      const kiotVietId = priceBookData.Id;

      const priceBook = await this.prismaService.priceBook.upsert({
        where: { kiotVietId },
        update: {
          name: priceBookData.Name,
          isActive: priceBookData.IsActive ?? true,
          isGlobal: priceBookData.IsGlobal ?? false,
          startDate: priceBookData.StartDate
            ? new Date(priceBookData.StartDate)
            : null,
          endDate: priceBookData.EndDate
            ? new Date(priceBookData.EndDate)
            : null,
          forAllCusGroup: priceBookData.ForAllCusGroup ?? false,
          forAllUser: priceBookData.ForAllUser ?? false,
          retailerId: 310831,
          modifiedDate: new Date(),
          lastSyncedAt: new Date(),
        },
        create: {
          kiotVietId,
          name: priceBookData.Name,
          isActive: priceBookData.IsActive ?? true,
          isGlobal: priceBookData.IsGlobal ?? false,
          startDate: priceBookData.StartDate
            ? new Date(priceBookData.StartDate)
            : null,
          endDate: priceBookData.EndDate
            ? new Date(priceBookData.EndDate)
            : null,
          forAllCusGroup: priceBookData.ForAllCusGroup ?? false,
          forAllUser: priceBookData.ForAllUser ?? false,
          retailerId: 310831,
          lastSyncedAt: new Date(),
        },
      });

      if (
        priceBookData.PriceBookBranches &&
        priceBookData.PriceBookBranches.length > 0
      ) {
        for (let i = 0; i < priceBookData.PriceBookBranches.length; i++) {
          const branchData = priceBookData.PriceBookBranches[i];
          const branch = await this.prismaService.branch.findUnique({
            where: { kiotVietId: branchData.BranchId },
            select: { id: true },
          });

          if (branch) {
            await this.prismaService.priceBookBranch.upsert({
              where: {
                priceBookId_lineNumber: {
                  priceBookId: priceBook.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                kiotVietId: BigInt(branchData.Id),
                branchId: branch.id,
                branchName: branchData.BranchName,
                retailerId: branchData.RetailerId ?? null,
                lastSyncedAt: new Date(),
              },
              create: {
                kiotVietId: BigInt(branchData.Id),
                priceBookId: priceBook.id,
                branchId: branch.id,
                branchName: branchData.BranchName,
                retailerId: branchData.RetailerId ?? null,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
        }
      }

      if (
        priceBookData.PriceBookCustomerGroups &&
        priceBookData.PriceBookCustomerGroups.length > 0
      ) {
        for (let i = 0; i < priceBookData.PriceBookCustomerGroups.length; i++) {
          const groupData = priceBookData.PriceBookCustomerGroups[i];
          const customerGroup =
            await this.prismaService.customerGroup.findUnique({
              where: { kiotVietId: groupData.CustomerGroupId },
              select: { id: true },
            });

          if (customerGroup) {
            await this.prismaService.priceBookCustomerGroup.upsert({
              where: {
                priceBookId_lineNumber: {
                  priceBookId: priceBook.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                kiotVietId: BigInt(groupData.Id),
                customerGroupId: customerGroup.id,
                customerGroupName: groupData.CustomerGroupName,
                retailerId: groupData.RetailerId ?? null,
                lastSyncedAt: new Date(),
              },
              create: {
                kiotVietId: BigInt(groupData.Id),
                priceBookId: priceBook.id,
                customerGroupId: customerGroup.id,
                customerGroupName: groupData.CustomerGroupName,
                retailerId: groupData.RetailerId ?? null,
                lineNumber: i + 1,
                lastSyncedAt: new Date(),
              },
            });
          }
        }
      }

      if (
        priceBookData.PriceBookUsers &&
        priceBookData.PriceBookUsers.length > 0
      ) {
        for (let i = 0; i < priceBookData.PriceBookUsers.length; i++) {
          const userData = priceBookData.PriceBookUsers[i];
          await this.prismaService.priceBookUser.upsert({
            where: {
              priceBookId_lineNumber: {
                priceBookId: priceBook.id,
                lineNumber: i + 1,
              },
            },
            update: {
              kiotVietId: BigInt(userData.Id),
              userId: BigInt(userData.UserId),
              userName: userData.UserName,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
            create: {
              kiotVietId: BigInt(userData.Id),
              priceBookId: priceBook.id,
              userId: BigInt(userData.UserId),
              userName: userData.UserName,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
          });
        }
      }

      return priceBook;
    } catch (error) {
      this.logger.error(`❌ Upsert pricebook failed: ${error.message}`);
      throw error;
    }
  }

  private async upsertPriceBookDetail(detailData: any) {
    try {
      const priceBook = await this.prismaService.priceBook.findUnique({
        where: { kiotVietId: detailData.PriceBookId },
        select: { id: true, name: true },
      });

      if (!priceBook) {
        this.logger.warn(`⚠️ PriceBook not found: ${detailData.PriceBookId}`);
        return null;
      }

      let product = await this.prismaService.product.findUnique({
        where: { kiotVietId: BigInt(detailData.ProductId) },
        select: { id: true, code: true, name: true, kiotVietId: true },
      });

      if (!product) {
        this.logger.warn(
          `⚠️ Product not found: ProductId=${detailData.ProductId}, creating new product...`,
        );

        const detailedProduct = await this.fetchProductDetail(
          detailData.ProductId,
        );

        if (detailedProduct) {
          await this.sendToLarkProductWebhook(detailedProduct);
        }

        if (detailedProduct) {
          const savedProduct = await this.upsertProduct(
            {
              Id: detailData.ProductId,
              Code: detailedProduct.code,
              Name: detailedProduct.name,
              FullName: detailedProduct.fullName,
              CategoryId: detailedProduct.categoryId,
              CategoryName: detailedProduct.categoryName,
              AllowsSale: detailedProduct.allowsSale,
              HasVariants: detailedProduct.hasVariants,
              BasePrice: detailedProduct.basePrice,
              Weight: detailedProduct.weight,
              Unit: detailedProduct.unit,
              MasterUnitId: detailedProduct.masterUnitId,
              ConversionValue: detailedProduct.conversionValue,
              ModifiedDate: detailedProduct.modifiedDate,
            },
            detailedProduct,
          );

          if (savedProduct) {
            product = await this.prismaService.product.findUnique({
              where: { kiotVietId: BigInt(detailData.ProductId) },
              select: { id: true, code: true, name: true, kiotVietId: true },
            });
          }
        }

        if (!product) {
          this.logger.error(
            `❌ Failed to create product: ProductId=${detailData.ProductId}`,
          );
          return null;
        }
      }

      const existingDetail = await this.prismaService.priceBookDetail.findFirst(
        {
          where: {
            priceBookId: priceBook.id,
            productId: product.id,
          },
          select: { lineNumber: true },
        },
      );

      let lineNumber: number;
      if (existingDetail) {
        lineNumber = existingDetail.lineNumber ?? 1;
      } else {
        const maxLineNumber =
          await this.prismaService.priceBookDetail.findFirst({
            where: { productId: product.id },
            orderBy: { lineNumber: 'desc' },
            select: { lineNumber: true },
          });
        lineNumber = (maxLineNumber?.lineNumber ?? 0) + 1;
      }

      return await this.prismaService.priceBookDetail.upsert({
        where: {
          productId_lineNumber: {
            productId: product.id,
            lineNumber: lineNumber,
          },
        },
        update: {
          priceBookId: priceBook.id,
          priceBookName: priceBook.name,
          productName: product.name,
          productId: product.id,
          productKiotId: product.kiotVietId,
          price: detailData.Price
            ? new Prisma.Decimal(detailData.Price)
            : new Prisma.Decimal(0),
          lastSyncedAt: new Date(),
        },
        create: {
          priceBookId: priceBook.id,
          priceBookName: priceBook.name,
          productId: product.id,
          productKiotId: product.kiotVietId,
          productName: product.name,
          price: detailData.Price
            ? new Prisma.Decimal(detailData.Price)
            : new Prisma.Decimal(0),
          lineNumber: lineNumber,
          lastSyncedAt: new Date(),
        },
      });
    } catch (error) {
      this.logger.error(`❌ Upsert priceBookDetail failed: ${error.message}`);
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

  private async fetchProductDetail(productId: number): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
      const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

      const queryParams = new URLSearchParams({
        includeInventory: 'true',
        includePricebook: 'true',
      });

      const url = `${baseUrl}/products/${productId}?${queryParams}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      this.logger.log('📦 Fetched product detail:');
      this.logger.log(JSON.stringify(response.data, null, 2));

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch product detail: ${error.message}`);
      return null;
    }
  }

  private async fetchPriceBookDetail(priceBookId: number): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
      const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

      const queryParams = new URLSearchParams({
        includePriceBookBranch: 'true',
        includePriceBookCustomerGroups: 'true',
        includePriceBookUsers: 'true',
      });

      const url = `${baseUrl}/pricebooks/${priceBookId}?${queryParams}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      this.logger.log('📦 Fetched pricebook detail:');
      this.logger.log(JSON.stringify(response.data, null, 2));

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch pricebook detail: ${error.message}`);
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

  private async findTradeMarkId(
    kiotVietTradeMarkId: number,
  ): Promise<number | null> {
    if (!kiotVietTradeMarkId) return null;
    const tradeMark = await this.prismaService.tradeMark.findUnique({
      where: { kiotVietId: kiotVietTradeMarkId },
    });
    return tradeMark?.id || null;
  }
}
