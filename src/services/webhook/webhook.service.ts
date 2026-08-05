import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietAuthService } from '../kiot-viet/auth.service';
import { ConfigService } from '@nestjs/config';
import { Prisma } from '@prisma/client';
import { firstValueFrom } from 'rxjs';
import { HttpService } from '@nestjs/axios';
import { lineSubTotal } from '../kiot-viet/shared/line-math';
import { RetailerContext } from '../kiot-viet/shared/retailer-context';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);

  /** Upper bound on every outbound KiotViet detail fetch. */
  private readonly httpTimeoutMs: number;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly configService: ConfigService,
     private readonly httpService: HttpService,
     private readonly retailer: RetailerContext,
     private readonly larkCustomerSync: LarkCustomerSyncService,
  ) {
    const raw = this.configService.get('KIOT_HTTP_TIMEOUT_MS');
    const parsed = Number(raw);
    this.httpTimeoutMs = Number.isFinite(parsed) && parsed > 0 ? parsed : 20000;
  }

  async processOrderWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const orderData of data) {
          const detailedOrder = await this.fetchOrderDetail(orderData.Id);
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

        for (const invoiceData of data) {
          const detailedInvoice = await this.fetchInvoiceDetail(invoiceData.Id);
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

  async processCustomerWebhook(webhookData: any): Promise<string> {
    try {
      const notifications = webhookData?.Notifications || [];
      let dbUpdated = 0;
      let larkSynced = 0;
      let larkSkipped = 0;

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const customerData of data) {
          const detailedCustomer = await this.fetchCustomerDetail(
            customerData.Id,
          );
          const savedCustomer = await this.upsertCustomer(
            customerData,
            detailedCustomer,
          );

          if (savedCustomer) {
            dbUpdated++;
            this.logger.log(`Upserted customer ${savedCustomer.code} in DB`);

            // Await the exact Customer rather than kicking the general queue. If
            // Lark is unavailable this throws, and WebhookWorkerService retries
            // the durable event until both DB and Lark have converged.
            const larkResult = await this.larkCustomerSync.syncCustomerById(
              savedCustomer.id,
            );
            if (larkResult === 'synced') larkSynced++;
            if (larkResult === 'skipped') larkSkipped++;
          }
        }
      }

      return `customers: ${dbUpdated} DB updated, ${larkSynced} Lark synced, ${larkSkipped} Lark skipped`;
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

        for (const productData of data) {
          const detailedProduct = await this.fetchProductDetail(productData.Id);
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

  async processPriceBookWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const priceBookData of data) {
          const detailedPriceBook = await this.fetchPriceBookDetail(
            priceBookData.Id,
          );
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

        for (const detailData of data) {
          const savedDetail = await this.upsertPriceBookDetail(detailData);

          if (savedDetail) {
            this.logger.log(
              `✅ Upserted priceBookDetail for product ${savedDetail.productName}`,
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

  async processStockWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        for (const stockData of data) {
          const savedStock = await this.upsertStock(stockData);

          if (savedStock) {
            this.logger.log(
              `✅ Upserted stock for product ${savedStock.productCode}`,
            );
          }
        }
      }
    } catch (error) {
      this.logger.error(`❌ Process stock webhook failed: ${error.message}`);
      throw error;
    }
  }

  // private async sendToLarkWebhook(webhookData: any): Promise<void> {
  //   try {
  //     await firstValueFrom(
  //       this.httpService.post(this.LARK_WEBHOOK_URL, webhookData, {
  //         headers: { 'Content-Type': 'application/json' },
  //       }),
  //     );
  //     this.logger.log(
  //       `✅ Sent webhook order & invoice data to Lark successfully`,
  //     );
  //   } catch (error) {
  //     this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
  //   }
  // }

  // private async sendToLarkCustomerWebhook(webhookData: any): Promise<void> {
  //   try {
  //     await firstValueFrom(
  //       this.httpService.post(this.LARK_WEBHOOK_CUSTOMER_URL, webhookData, {
  //         headers: { 'Content-Type': 'application/json' },
  //       }),
  //     );
  //     this.logger.log(`✅ Sent webhook customer data to Lark successfully`);
  //   } catch (error) {
  //     this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
  //   }
  // }

  // private async sendToLarkProductWebhook(webhookData: any): Promise<void> {
  //   try {
  //     await firstValueFrom(
  //       this.httpService.post(this.LARK_WEBHOOK_PRODUCT_URL, webhookData, {
  //         headers: { 'Content-Type': 'application/json' },
  //       }),
  //     );
  //     this.logger.log(`✅ Sent webhook product data to Lark successfully`);
  //   } catch (error) {
  //     this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
  //   }
  // }

  // private async sendToLarkPricebookWebhook(webhookData: any): Promise<void> {
  //   try {
  //     await firstValueFrom(
  //       this.httpService.post(this.LARK_WEBHOOK_PRICEBOOK_URL, webhookData, {
  //         headers: { 'Content-Type': 'application/json' },
  //       }),
  //     );
  //     this.logger.log(`✅ Sent webhook pricebook data to Lark successfully`);
  //   } catch (error) {
  //     this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
  //   }
  // }

  // private async sendToLarkPricebookDetailWebhook(
  //   webhookData: any,
  // ): Promise<void> {
  //   try {
  //     const sanitizedData = JSON.parse(
  //       JSON.stringify(webhookData, (key, value) =>
  //         typeof value === 'bigint' ? value.toString() : value,
  //       ),
  //     );

  //     await firstValueFrom(
  //       this.httpService.post(
  //         this.LARK_WEBHOOK_PRICEBOOK_DETAIL_URL,
  //         sanitizedData,
  //         {
  //           headers: { 'Content-Type': 'application/json' },
  //         },
  //       ),
  //     );
  //     this.logger.log(
  //       `✅ Sent webhook pricebook detail data to Lark successfully`,
  //     );
  //   } catch (error) {
  //     this.logger.error(`❌ Failed to send to Lark: ${error.message}`);
  //   }
  // }

  /**
   * Upsert a Payment row.
   *
   * KiotViet does not always send a payment id. The previous code handled that by
   * looking up `kiotVietId: BigInt(0)` while *creating* the row with
   * `kiotVietId: null`. Postgres never treats NULL as a conflict, so the lookup
   * could never match what had been written — every redelivery of the same webhook
   * inserted ANOTHER payment row, silently inflating payment totals on replay.
   *
   * With an id we upsert on it. Without one we fall back to a natural key scoped to
   * the parent document, so a replay updates the same row instead of duplicating it.
   */
  private async upsertPaymentRow(args: {
    payment: any;
    parentField: 'orderId' | 'invoiceId' | 'returnId' | 'purchaseOrderId';
    parentId: number;
    accountId: number | null;
  }): Promise<any> {
    const { payment, parentField, parentId, accountId } = args;

    const data: any = {
      [parentField]: parentId,
      code: payment.code ?? null,
      amount: new Prisma.Decimal(payment.amount ?? 0),
      method: payment.method ?? 'Unknown',
      status: payment.status ?? null,
      transDate: payment.transDate ? new Date(payment.transDate) : new Date(),
      accountId,
      description: payment.description ?? null,
      statusValue: payment.statusValue ?? null,
    };

    if (payment.id) {
      return this.prismaService.payment.upsert({
        where: { kiotVietId: BigInt(payment.id) },
        update: data,
        create: { kiotVietId: BigInt(payment.id), ...data },
      });
    }

    // No id from KiotViet: identify the row by parent + code, or by parent +
    // method + transDate + amount when the code is absent too.
    const existing = await this.prismaService.payment.findFirst({
      where: {
        [parentField]: parentId,
        ...(payment.code
          ? { code: payment.code }
          : {
              method: data.method,
              transDate: data.transDate,
              amount: data.amount,
            }),
      },
      select: { id: true },
    });

    if (existing) {
      return this.prismaService.payment.update({
        where: { id: existing.id },
        data,
      });
    }
    return this.prismaService.payment.create({
      data: { kiotVietId: null, ...data },
    });
  }

  /**
   * Upsert an Order/Invoice surcharge row. Same NULL-conflict problem as payments.
   *
   * This also fixes a second bug: the previous `update` blocks omitted both the
   * parent id and `surchargeId`, so those columns could never be corrected once
   * written — and the `surchargeRecord` lookup they performed was discarded on
   * every update.
   */
  private async upsertSurchargeRow(args: {
    surcharge: any;
    table: 'orderSurcharge' | 'invoiceSurcharge';
    parentField: 'orderId' | 'invoiceId';
    parentId: number;
    surchargeId: number | null;
  }): Promise<any> {
    const { surcharge, table, parentField, parentId, surchargeId } = args;
    const delegate = (this.prismaService as any)[table];

    const data: any = {
      [parentField]: parentId,
      surchargeId,
      surchargeName: surcharge.surchargeName ?? null,
      surValue: surcharge.surValue
        ? new Prisma.Decimal(surcharge.surValue)
        : null,
      price: surcharge.price ? new Prisma.Decimal(surcharge.price) : null,
    };

    if (surcharge.id) {
      return delegate.upsert({
        where: { kiotVietId: BigInt(surcharge.id) },
        update: data,
        create: { kiotVietId: BigInt(surcharge.id), ...data },
      });
    }

    const existing = await delegate.findFirst({
      where: { [parentField]: parentId, surchargeName: data.surchargeName },
      select: { id: true },
    });
    if (existing) {
      return delegate.update({ where: { id: existing.id }, data });
    }
    return delegate.create({ data: { kiotVietId: null, ...data } });
  }

  private async upsertOrder(orderData: any, detailedOrder: any) {
    try {
      const kiotVietId = BigInt(orderData.Id);
      const branchId = await this.findBranchId(orderData.BranchId);
      const customerId = await this.findCustomerId(orderData.CustomerId);
      const soldById = await this.findSoldById(orderData.SoldById);
      const soldByKiotVietId = this.toSoldByKiotVietId(orderData.SoldById);
      const soldByName =
        detailedOrder?.soldByName ?? orderData.SoldByName ?? null;
      const saleChannel = await this.findSaleChannelId(orderData.SaleChannelId);
      const orderCode = orderData.Code;
      const shouldSyncToLark = orderCode && orderCode.includes('DH0');

      const order = await this.prismaService.order.upsert({
        where: { kiotVietId },
        update: {
          kiotVietId,
          code: orderData.Code,
          purchaseDate: new Date(orderData.PurchaseDate),
          branchId,
          soldById,
          soldByKiotVietId,
          soldByName,
          customerId,
          customerCode: detailedOrder?.customerCode ?? orderData.CustomerCode,
          customerName: detailedOrder?.customerName ?? orderData.CustomerName,
          retailerId: this.retailer.resolve(
            orderData.RetailerId ?? detailedOrder?.retailerId,
          ),
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
        create: {
          kiotVietId,
          code: orderData.Code,
          purchaseDate: new Date(orderData.PurchaseDate),
          branchId,
          soldById,
          soldByKiotVietId,
          soldByName,
          customerId,
          customerCode: detailedOrder?.customerCode ?? orderData.CustomerCode,
          customerName: detailedOrder?.customerName ?? orderData.CustomerName,
          retailerId: this.retailer.resolve(
            orderData.RetailerId ?? detailedOrder?.retailerId,
          ),
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

          const savedPayment = await this.upsertPaymentRow({
            payment,
            parentField: 'orderId',
            parentId: order.id,
            accountId: bankAccount?.id ?? null,
          });

          if (savedPayment.method === 'Voucher') {
            this.logger.log(
              `Voucher payment ${savedPayment.code} for order ${order.code}`,
            );
          }
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

          await this.upsertSurchargeRow({
            surcharge,
            table: 'orderSurcharge',
            parentField: 'orderId',
            parentId: order.id,
            surchargeId: surchargeRecord?.id ?? null,
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
      const soldById = await this.findSoldById(invoiceData.SoldById);
      const soldByKiotVietId = this.toSoldByKiotVietId(invoiceData.SoldById);
      const soldByName =
        detailedInvoice?.soldByName ?? invoiceData.SoldByName ?? null;
      const orderId = detailedInvoice?.orderId
        ? await this.findOrderId(detailedInvoice.orderId)
        : invoiceData.OrderId
          ? await this.findOrderId(invoiceData.OrderId)
          : null;
      const saleChannel = await this.findSaleChannelId(
        invoiceData.SaleChannelId,
      );
      const invoiceCode = invoiceData.Code;
      const shouldSyncToLark =
        invoiceCode &&
        (invoiceCode.includes('HD0') || invoiceCode.includes('HD1'));

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
          soldById,
          soldByKiotVietId,
          soldByName,
          saleChannelId: saleChannel.id ? saleChannel.id : 1,
          discountRatio: invoiceData.DiscountRatio
            ? invoiceData.DiscountRatio
            : 0,
          description: detailedInvoice?.description ?? invoiceData.Description,
          retailerId: this.retailer.resolve(
            invoiceData.RetailerId ?? detailedInvoice?.retailerId,
          ),
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
          soldByKiotVietId,
          soldByName,
          customerId,
          retailerId: this.retailer.resolve(
            invoiceData.RetailerId ?? detailedInvoice?.retailerId,
          ),
          orderId,
          total: new Prisma.Decimal(invoiceData.Total || 0),
          totalPayment: new Prisma.Decimal(invoiceData.TotalPayment || 0),
          discount: invoiceData.Discount
            ? new Prisma.Decimal(invoiceData.Discount)
            : 0,
          discountRatio: invoiceData.DiscountRatio
            ? invoiceData.DiscountRatio
            : 0,
          status: invoiceData.Status,
          statusValue: invoiceData.StatusValue,
          description: detailedInvoice?.description ?? invoiceData.Description,
          usingCod: detailedInvoice?.usingCod ?? false,
          customerCode: detailedInvoice?.customerCode ?? null,
          customerName: detailedInvoice?.customerName ?? null,
          saleChannelId: saleChannel.id ? saleChannel.id : 1,
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

          const acsNumber: number = i + 1;

          // Previously gated by shouldSyncInvoiceDetail(), which hardcoded two
          // product codes and six Vietnamese keywords. That rule is retired, so
          // every detail row is queued like the other entities do.
          const detailLarkSyncStatus = 'PENDING';

          if (product) {
            await this.prismaService.invoiceDetail.upsert({
              where: {
                invoiceId_lineNumber: {
                  invoiceId: invoice.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                invoiceId: invoice.id,
                productId: product.id,
                invoiceKiotVietId: invoice.kiotVietId,
                productKiotVietId: product.kiotVietId,
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
                  lineSubTotal({
                    subTotal: detail.subTotal,
                    price: detail.price,
                    discount: detail.discount,
                    quantity: detail.quantity,
                  }),
                ),
                lineNumber: i + 1,
                larkSyncStatus: detailLarkSyncStatus,
                uniqueKey: `${invoice.kiotVietId}.${acsNumber}`,
              },
              create: {
                invoiceId: invoice.id,
                productId: product.id,
                invoiceKiotVietId: invoice.kiotVietId,
                productKiotVietId: product.kiotVietId,
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
                  lineSubTotal({
                    subTotal: detail.subTotal,
                    price: detail.price,
                    discount: detail.discount,
                    quantity: detail.quantity,
                  }),
                ),
                lineNumber: i + 1,
                larkSyncStatus: detailLarkSyncStatus,
                uniqueKey: `${invoice.kiotVietId}.${acsNumber}`,
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

          // Shares the order path's helper, which also fixes the missing
          // `statusValue` here: the order payment path mapped it, this one did not,
          // so the same column was populated or left null depending on which
          // webhook happened to write the row.
          await this.upsertPaymentRow({
            payment,
            parentField: 'invoiceId',
            parentId: invoice.id,
            accountId: bankAccount?.id ?? null,
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

          await this.upsertSurchargeRow({
            surcharge,
            table: 'invoiceSurcharge',
            parentField: 'invoiceId',
            parentId: invoice.id,
            surchargeId: surchargeRecord?.id ?? null,
          });
        }
      }

      return invoice;
    } catch (error) {
      this.logger.error(`❌ Upsert invoice failed: ${error.message}`);
      throw error;
    }
  }

  private larkStatusForContact(contactNumber: unknown): 'PENDING' | 'SKIP' {
    return typeof contactNumber === 'string' && contactNumber.trim().length > 0
      ? 'PENDING'
      : 'SKIP';
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
          wardName: customerData.WardName ?? null,
          email: customerData.Email ?? null,
          organization: customerData.Organization ?? null,
          taxCode: customerData.TaxCode ?? null,
          identificationNumber:
            detailedCustomer?.identificationNumber ??
            customerData.IdentificationNumber ??
            null,
          comments: customerData.Comments ?? null,
          debt: detailedCustomer?.debt
            ? new Prisma.Decimal(detailedCustomer.debt)
            : 0,
          totalInvoiced: detailedCustomer?.totalInvoiced
            ? new Prisma.Decimal(detailedCustomer.totalInvoiced)
            : 0,
          totalPoint: detailedCustomer?.totalPoint ?? null,
          totalRevenue: detailedCustomer?.totalRevenue
            ? new Prisma.Decimal(detailedCustomer.totalRevenue)
            : 0,
          retailerId: this.retailer.resolve(
            customerData.RetailerId ?? detailedCustomer?.retailerId,
          ),
          rewardPoint: detailedCustomer?.rewardPoint
            ? BigInt(detailedCustomer.rewardPoint)
            : 0,
          groups: detailedCustomer?.groups ?? null,
          branchId,
           modifiedDate: customerData.ModifiedDate
             ? new Date(customerData.ModifiedDate)
             : new Date(),
           lastSyncedAt: new Date(),
           larkSyncStatus: this.larkStatusForContact(customerData.ContactNumber),
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
          wardName: customerData.WardName ?? null,
          email: customerData.Email ?? null,
          organization: customerData.Organization ?? null,
          taxCode: customerData.TaxCode ?? null,
          identificationNumber:
            detailedCustomer?.identificationNumber ??
            customerData.IdentificationNumber ??
            null,
          comments: customerData.Comments ?? null,
          debt: detailedCustomer?.debt
            ? new Prisma.Decimal(detailedCustomer.debt)
            : null,
          retailerId: this.retailer.resolve(
            customerData.RetailerId ?? detailedCustomer?.retailerId,
          ),
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
           larkSyncStatus: this.larkStatusForContact(customerData.ContactNumber),
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

      const tradeMarkInfo = detailedProduct?.tradeMarkId
        ? await this.findTradeMarkId(detailedProduct.tradeMarkId)
        : productData.TradeMarkId
          ? await this.findTradeMarkId(productData.TradeMarkId)
          : { id: null, name: null };

      const minQuantity =
        detailedProduct?.minQuantity ?? productData.MinQuantity ?? null;

      const maxQuantity =
        detailedProduct?.maxQuantity ?? productData.MaxQuantity ?? null;

      const product = await this.prismaService.product.upsert({
        where: { kiotVietId },
        update: {
          kiotVietId,
          code: productData.Code,
          name: productData.Name,
          fullName: productData.FullName ?? productData.Name,
          categoryId: category?.id ?? null,
          categoryName: productData.CategoryName ?? category?.name ?? null,
          parent_name: category?.parent_name ?? null,
          child_name: category?.child_name ?? null,
          branch_name: category?.branch_name ?? null,
          type: detailedProduct?.type ?? productData.Type ?? null,
          isManufactured: (detailedProduct?.productFormulas?.length ?? 0) > 0,
          allowsSale: productData.AllowsSale ?? true,
          hasVariants: productData.HasVariants ?? false,
          basePrice: productData.BasePrice
            ? new Prisma.Decimal(productData.BasePrice)
            : null,
          retailerId: this.retailer.resolve(
            productData.RetailerId ?? detailedProduct?.retailerId,
          ),
          tradeMarkId: tradeMarkInfo.id,
          tradeMarkName: tradeMarkInfo.name || '',
          minQuantity,
          maxQuantity,
          weight: productData.Weight ?? null,
          unit: productData.Unit ?? null,
          // Webhook payloads use PascalCase; the /products/{id} detail response uses
          // lower-camel. The previous code read ONLY `productData.masterProductId`,
          // which is lower-camel amid otherwise-PascalCase siblings, so it resolved
          // to null on every product webhook. Read every shape.
          masterProductId: (() => {
            const v =
              productData.MasterProductId ??
              productData.masterProductId ??
              detailedProduct?.masterProductId;
            return v ? BigInt(v) : null;
          })(),
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
          retailerId: this.retailer.resolve(
            productData.RetailerId ?? detailedProduct?.retailerId,
          ),
          parent_name: category?.parent_name ?? null,
          child_name: category?.child_name ?? null,
          branch_name: category?.branch_name ?? null,
          type: detailedProduct?.type ?? productData.Type ?? null,
          isManufactured: (detailedProduct?.productFormulas?.length ?? 0) > 0,
          allowsSale: productData.AllowsSale ?? true,
          hasVariants: productData.HasVariants ?? false,
          basePrice: productData.BasePrice
            ? new Prisma.Decimal(productData.BasePrice)
            : null,
          tradeMarkId: tradeMarkInfo.id,
          tradeMarkName: tradeMarkInfo.name || '',
          minQuantity,
          maxQuantity,
          weight: productData.Weight ?? null,
          unit: productData.Unit ?? null,
          // Webhook payloads use PascalCase; the /products/{id} detail response uses
          // lower-camel. The previous code read ONLY `productData.masterProductId`,
          // which is lower-camel amid otherwise-PascalCase siblings, so it resolved
          // to null on every product webhook. Read every shape.
          masterProductId: (() => {
            const v =
              productData.MasterProductId ??
              productData.masterProductId ??
              detailedProduct?.masterProductId;
            return v ? BigInt(v) : null;
          })(),
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
                productId_branchId: {
                  productId: product.id,
                  branchId: branch.id,
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
                priceBookId_productId: {
                  priceBookId: priceBook.id,
                  productId: product.id,
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

      // Sync product formulas (định mức nguyên vật liệu) từ webhook detail
      const formulas = detailedProduct?.productFormulas ?? [];
      if (formulas.length > 0) {
        for (let i = 0; i < formulas.length; i++) {
          const f = formulas[i];
          await this.prismaService.productFormula.upsert({
            where: {
              productId_lineNumber: {
                productId: product.id,
                lineNumber: i + 1,
              },
            },
            update: {
              productKiotVietId: product.kiotVietId,
              materialId: BigInt(f.materialId),
              materialCode: f.materialCode,
              materialName: f.materialName ?? null,
              materialFullName: f.materialFullName ?? null,
              quantity: f.quantity ?? null,
              basePrice: f.basePrice ? new Prisma.Decimal(f.basePrice) : null,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
            create: {
              productId: product.id,
              productKiotVietId: product.kiotVietId,
              materialId: BigInt(f.materialId),
              materialCode: f.materialCode,
              materialName: f.materialName ?? null,
              materialFullName: f.materialFullName ?? null,
              quantity: f.quantity ?? null,
              basePrice: f.basePrice ? new Prisma.Decimal(f.basePrice) : null,
              lineNumber: i + 1,
              lastSyncedAt: new Date(),
            },
          });
        }

        // Đánh dấu các nguyên liệu được tham chiếu là hàng sản xuất (isMaterial=true)
        const materialCodes: string[] = Array.from(
          new Set(
            formulas
              .map((f: any) => f.materialCode?.trim())
              .filter((c: any): c is string => !!c),
          ),
        );
        if (materialCodes.length > 0) {
          await this.prismaService.product.updateMany({
            where: { code: { in: materialCodes } },
            data: { isMaterial: true },
          });
        }
      } else {
        // Archive mode: retain formulas previously synced from KiotViet.
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
          retailerId: this.retailer.resolve(
            priceBookData.RetailerId ?? detailedPriceBook?.retailerId,
          ),
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
          retailerId: this.retailer.resolve(
            priceBookData.RetailerId ?? detailedPriceBook?.retailerId,
          ),
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

        // if (detailedProduct) {
        //   await this.sendToLarkProductWebhook(detailedProduct);
        // }

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

      // Keyed on the natural key (priceBookId, productId). The previous
      // find-existing-else-max+1 dance computed a `lineNumber` that is no longer
      // the conflict target, at the cost of two extra queries per detail row.
      return await this.prismaService.priceBookDetail.upsert({
        where: {
          priceBookId_productId: {
            priceBookId: priceBook.id,
            productId: product.id,
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
          // Without a timeout a hung KiotViet response stalls the worker forever.
          timeout: this.httpTimeoutMs,
        }),
      );

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
          // Without a timeout a hung KiotViet response stalls the worker forever.
          timeout: this.httpTimeoutMs,
        }),
      );

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
          // Without a timeout a hung KiotViet response stalls the worker forever.
          timeout: this.httpTimeoutMs,
        }),
      );

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
        includeMaterial: 'true',
      });

      const url = `${baseUrl}/products/${productId}?${queryParams}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
          // Without a timeout a hung KiotViet response stalls the worker forever.
          timeout: this.httpTimeoutMs,
        }),
      );

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
          // Without a timeout a hung KiotViet response stalls the worker forever.
          timeout: this.httpTimeoutMs,
        }),
      );

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
      return { id: 1, name: defaultChannel?.name || 'Bán trực tiếp' };
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

  /**
   * Resolve `soldById` to a value the FK will accept.
   *
   * `Order.soldById` / `Invoice.soldById` are FKs to `User.kiotVietId`, but
   * `GET /users` only returns active staff — ~98% of orders reference a deleted
   * user id that is absent from the User table. Writing it directly makes the
   * `upsert` throw a FK violation, which fails the whole webhook event (retries
   * to dead). Returns the id only when the user exists locally, else null so the
   * row is kept. Mirrors the guard added to the sync path.
   */
  private async findSoldById(rawId: unknown): Promise<bigint | null> {
    if (!rawId) return null;
    let kvId: bigint;
    try {
      kvId = BigInt(rawId as any);
    } catch {
      return null;
    }
    const user = await this.prismaService.user.findUnique({
      where: { kiotVietId: kvId },
      select: { kiotVietId: true },
    });
    return user ? kvId : null;
  }

  /**
   * The raw KiotViet staff id, kept alongside `soldById` so a deleted staff
   * member's identity survives even when the FK could not be satisfied.
   */
  private toSoldByKiotVietId(rawId: unknown): bigint | null {
    if (!rawId) return null;
    try {
      return BigInt(rawId as any);
    } catch {
      return null;
    }
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
  ): Promise<{ id: number | null; name: string | null }> {
    if (!kiotVietTradeMarkId) return { id: null, name: null };

    const tradeMark = await this.prismaService.tradeMark.findUnique({
      where: { kiotVietId: kiotVietTradeMarkId },
      select: { id: true, name: true },
    });

    return {
      id: tradeMark?.id || null,
      name: tradeMark?.name || null,
    };
  }
}
