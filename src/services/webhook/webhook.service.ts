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

        console.log(data);

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

        console.log(data);

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

  async processCustomerWebhook(webhookData: any): Promise<void> {
    try {
      const notifications = webhookData?.Notifications || [];

      for (const notification of notifications) {
        const data = notification?.Data || [];

        console.log(data);

        for (const customerData of data) {
          const savedCustomer = await this.upsertCustomer(customerData);

          if (savedCustomer) {
            this.logger.log(`✅ Upserted customer ${savedCustomer.code}`);
          }
        }
      }

      await this.sendToLarkWebhook(webhookData);
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

  private async upsertOrder(orderData: any) {
    try {
      const kiotVietId = BigInt(orderData.Id);
      const branchId = await this.findBranchId(orderData.BranchId);
      const customerId = await this.findCustomerId(orderData.CustomerId);
      const soldById = orderData.SoldById ? BigInt(orderData.SoldById) : null;
      const saleChannel = await this.findSaleChannelId(orderData.SaleChannelId);

      const order = await this.prismaService.order.upsert({
        where: { kiotVietId },
        update: {
          status: orderData.Status,
          statusValue: orderData.StatusValue,
          total: new Prisma.Decimal(orderData.Total || 0),
          totalPayment: new Prisma.Decimal(orderData.TotalPayment || 0),
          customerCode: orderData.CustomerCode,
          customerName: orderData.CustomerName,
          saleChannelId: saleChannel.id,
          saleChannelName: saleChannel.name,
          discount: orderData.Discount
            ? new Prisma.Decimal(orderData.Discount)
            : null,
          discountRatio: orderData.DiscountRatio,
          createdDate: orderData.CreatedDate
            ? new Date(orderData.CreatedDate)
            : new Date(),
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
          customerCode: orderData.CustomerCode,
          customerName: orderData.CustomerName,
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
          description: orderData.Description,
          createdDate: orderData.CreatedDate
            ? new Date(orderData.CreatedDate)
            : new Date(),
          modifiedDate: orderData.ModifiedDate
            ? new Date(orderData.ModifiedDate)
            : null,
          larkSyncStatus: 'PENDING',
        },
      });

      if (orderData.OrderDetails && orderData.OrderDetails.length > 0) {
        for (let i = 0; i < orderData.OrderDetails.length; i++) {
          const detail = orderData.OrderDetails[i];
          const product = await this.prismaService.product.findUnique({
            where: { kiotVietId: BigInt(detail.ProductId) },
            select: { id: true, code: true, name: true },
          });

          if (product) {
            await this.prismaService.orderDetail.upsert({
              where: {
                orderId_lineNumber: { orderId: order.id, lineNumber: i + 1 },
              },
              update: {
                quantity: detail.Quantity,
                price: new Prisma.Decimal(detail.Price),
                discount: detail.Discount
                  ? new Prisma.Decimal(detail.Discount)
                  : null,
                discountRatio: detail.DiscountRatio,
                productCode: product.code,
                productName: product.name,
              },
              create: {
                orderId: order.id,
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                quantity: detail.Quantity,
                price: new Prisma.Decimal(detail.Price),
                discount: detail.Discount
                  ? new Prisma.Decimal(detail.Discount)
                  : null,
                discountRatio: detail.DiscountRatio,
                lineNumber: i + 1,
              },
            });
          }
        }
      }

      const deliveryData = orderData.DeliveryPackage || orderData.OrderDelivery;
      if (deliveryData) {
        await this.prismaService.orderDelivery.upsert({
          where: { orderId: order.id },
          update: {
            deliveryCode: deliveryData.DeliveryCode || deliveryData.ServiceType,
            type: deliveryData.ServiceType
              ? parseInt(deliveryData.ServiceType)
              : null,
            receiver: deliveryData.Receiver,
            contactNumber: deliveryData.ContactNumber,
            address: deliveryData.Address,
            locationId: deliveryData.LocationId,
            locationName: deliveryData.LocationName,
            wardName: deliveryData.WardName,
            weight: deliveryData.Weight,
            length: deliveryData.Length,
            width: deliveryData.Width,
            height: deliveryData.Height,
          },
          create: {
            orderId: order.id,
            deliveryCode: deliveryData.DeliveryCode || deliveryData.ServiceType,
            type: deliveryData.ServiceType
              ? parseInt(deliveryData.ServiceType)
              : null,
            receiver: deliveryData.Receiver,
            contactNumber: deliveryData.ContactNumber,
            address: deliveryData.Address,
            locationId: deliveryData.LocationId,
            locationName: deliveryData.LocationName,
            wardName: deliveryData.WardName,
            weight: deliveryData.Weight,
            length: deliveryData.Length,
            width: deliveryData.Width,
            height: deliveryData.Height,
          },
        });
      }

      if (orderData.Payments && orderData.Payments.length > 0) {
        for (const payment of orderData.Payments) {
          const bankAccount = payment.AccountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.AccountId },
                select: { id: true },
              })
            : null;

          await this.prismaService.payment.upsert({
            where: { kiotVietId: payment.Id ? BigInt(payment.Id) : BigInt(0) },
            update: {
              orderId: order.id,
              code: payment.Code,
              amount: new Prisma.Decimal(payment.Amount),
              method: payment.Method,
              status: payment.Status,
              transDate: new Date(payment.TransDate),
              accountId: bankAccount?.id ?? null,
              description: payment.Description,
            },
            create: {
              kiotVietId: payment.Id ? BigInt(payment.Id) : null,
              orderId: order.id,
              code: payment.Code,
              amount: new Prisma.Decimal(payment.Amount),
              method: payment.Method,
              status: payment.Status,
              transDate: new Date(payment.TransDate),
              accountId: bankAccount?.id ?? null,
              description: payment.Description,
            },
          });
        }
      }

      if (
        orderData.invoiceOrderSurcharges &&
        orderData.invoiceOrderSurcharges.length > 0
      ) {
        for (const surcharge of orderData.invoiceOrderSurcharges) {
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

  private async upsertInvoice(invoiceData: any) {
    try {
      const kiotVietId = BigInt(invoiceData.Id);
      const branchId = await this.findBranchId(invoiceData.BranchId);
      const customerId = await this.findCustomerId(invoiceData.CustomerId);
      const soldById = invoiceData.SoldById
        ? BigInt(invoiceData.SoldById)
        : null;
      const orderId = invoiceData.OrderId ? invoiceData.OrderId : null;
      const saleChannel = await this.findSaleChannelId(
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
          saleChannelId: saleChannel.id,
          createdDate: invoiceData.CreatedDate
            ? new Date(invoiceData.CreatedDate)
            : new Date(),
          modifiedDate: invoiceData.ModifiedDate
            ? new Date(invoiceData.ModifiedDate)
            : new Date(),
          larkSyncStatus: 'PENDING',
        },
      });

      if (invoiceData.InvoiceDetails && invoiceData.InvoiceDetails.length > 0) {
        for (let i = 0; i < invoiceData.InvoiceDetails.length; i++) {
          const detail = invoiceData.InvoiceDetails[i];
          const product = await this.prismaService.product.findUnique({
            where: { kiotVietId: BigInt(detail.ProductId) },
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
                quantity: detail.Quantity,
                price: new Prisma.Decimal(detail.Price),
                discount: detail.Discount
                  ? new Prisma.Decimal(detail.Discount)
                  : null,
                discountRatio: detail.DiscountRatio,
                subTotal: new Prisma.Decimal(
                  detail.Price * detail.Quantity - (detail.Discount || 0),
                ),
                productCode: product.code,
                productName: product.name,
              },
              create: {
                invoiceId: invoice.id,
                productId: product.id,
                productCode: product.code,
                productName: product.name,
                quantity: detail.Quantity,
                price: new Prisma.Decimal(detail.Price),
                discount: detail.Discount
                  ? new Prisma.Decimal(detail.Discount)
                  : null,
                discountRatio: detail.DiscountRatio,
                subTotal: new Prisma.Decimal(
                  detail.Price * detail.Quantity - (detail.Discount || 0),
                ),
                lineNumber: i + 1,
              },
            });
          }
        }
      }

      if (invoiceData.InvoiceDelivery) {
        const detail = invoiceData.InvoiceDelivery;
        await this.prismaService.invoiceDelivery.upsert({
          where: { invoiceId: invoice.id },
          update: {
            deliveryCode: detail.DeliveryCode,
            status: detail.Status,
            type: detail.Type,
            price: detail.Price ? new Prisma.Decimal(detail.Price) : null,
            receiver: detail.Receiver,
            contactNumber: detail.ContactNumber,
            address: detail.Address,
            locationId: detail.LocationId,
            locationName: detail.LocationName,
            wardName: detail.WardName,
            usingPriceCod: detail.UsingPriceCod || false,
            priceCodPayment: detail.PriceCodPayment
              ? new Prisma.Decimal(detail.PriceCodPayment)
              : null,
            weight: detail.Weight,
            length: detail.Length,
            width: detail.Width,
            height: detail.Height,
          },
          create: {
            invoiceId: invoice.id,
            deliveryCode: detail.DeliveryCode,
            status: detail.Status,
            type: detail.Type,
            price: detail.Price ? new Prisma.Decimal(detail.Price) : null,
            receiver: detail.Receiver,
            contactNumber: detail.ContactNumber,
            address: detail.Address,
            locationId: detail.LocationId,
            locationName: detail.LocationName,
            wardName: detail.WardName,
            usingPriceCod: detail.UsingPriceCod || false,
            priceCodPayment: detail.PriceCodPayment
              ? new Prisma.Decimal(detail.PriceCodPayment)
              : null,
            weight: detail.Weight,
            length: detail.Length,
            width: detail.Width,
            height: detail.Height,
          },
        });
      }

      if (invoiceData.Payments && invoiceData.Payments.length > 0) {
        for (const payment of invoiceData.Payments) {
          const bankAccount = payment.AccountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.AccountId },
                select: { id: true },
              })
            : null;

          await this.prismaService.payment.upsert({
            where: { kiotVietId: payment.Id ? BigInt(payment.Id) : BigInt(0) },
            update: {
              invoiceId: invoice.id,
              code: payment.Code,
              amount: new Prisma.Decimal(payment.Amount),
              method: payment.Method,
              status: payment.Status,
              transDate: new Date(payment.TransDate),
              accountId: bankAccount?.id ?? null,
              description: payment.Description,
            },
            create: {
              kiotVietId: payment.Id ? BigInt(payment.Id) : null,
              invoiceId: invoice.id,
              code: payment.Code,
              amount: new Prisma.Decimal(payment.Amount),
              method: payment.Method,
              status: payment.Status,
              transDate: new Date(payment.TransDate),
              accountId: bankAccount?.id ?? null,
              description: payment.Description,
            },
          });
        }
      }

      if (
        invoiceData.invoiceOrderSurcharges &&
        invoiceData.invoiceOrderSurcharges.length > 0
      ) {
        for (const surcharge of invoiceData.invoiceOrderSurcharges) {
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

  private async upsertCustomer(customerData: any) {
    try {
      const kiotVietId = BigInt(customerData.Id);

      const detailedCustomer = await this.fetchCustomerDetail(customerData.Id);

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
      const accessToken =
        await this.httpService['authService'].getAccessToken();
      const baseUrl = 'https://public.kiotapi.com';
      const shopName = process.env.KIOT_SHOP_NAME;

      const url = `${baseUrl}/customers/${customerId}`;

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Retailer: shopName,
            Authorization: `Bearer ${accessToken}`,
          },
        }),
      );

      return response.data;
    } catch (error) {
      this.logger.warn(`⚠️ Could not fetch customer detail: ${error.message}`);
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
}
