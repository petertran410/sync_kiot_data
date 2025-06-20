// src/services/kiot-viet/invoice/invoice.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
  private readonly baseUrl: string;
  private readonly BATCH_SIZE = 500;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async fetchInvoices(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/invoices`, {
          headers,
          params: {
            ...params,
            includePayment: true,
            includeInvoiceDelivery: true,
            includeRemoveIds: true,
            SaleChannel: true,
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch invoices: ${error.message}`);
      throw error;
    }
  }

  private async batchSaveInvoices(invoices: any[]) {
    if (!invoices || invoices.length === 0) return { created: 0, updated: 0 };

    const kiotVietIds = invoices.map((i) => BigInt(i.id));
    const existingInvoices = await this.prismaService.invoice.findMany({
      where: { kiotVietId: { in: kiotVietIds } },
      select: { kiotVietId: true, id: true },
    });

    const existingMap = new Map<string, number>(
      existingInvoices.map((i) => [i.kiotVietId.toString(), i.id]),
    );

    let createdCount = 0;
    let updatedCount = 0;

    for (const invoiceData of invoices) {
      try {
        const kiotVietId = BigInt(invoiceData.id);
        const existingId = existingMap.get(kiotVietId.toString());

        if (existingId) {
          await this.updateInvoice(existingId, invoiceData);
          updatedCount++;
        } else {
          await this.createInvoice(invoiceData);
          createdCount++;
        }
      } catch (error) {
        this.logger.error(
          `Failed to save invoice ${invoiceData.code}: ${error.message}`,
        );
        // Continue processing other invoices instead of stopping
        continue;
      }
    }

    return { created: createdCount, updated: updatedCount };
  }

  private async createInvoice(invoiceData: any) {
    const invoice = await this.prismaService.invoice.create({
      data: await this.prepareInvoiceCreateData(invoiceData),
    });

    await this.handleInvoiceRelations(invoice.id, invoiceData);
  }

  private async updateInvoice(invoiceId: number, invoiceData: any) {
    await this.prismaService.invoice.update({
      where: { id: invoiceId },
      data: await this.prepareInvoiceUpdateData(invoiceData),
    });

    await this.handleInvoiceRelations(invoiceId, invoiceData);
  }

  // CORRECTED: Based on exact response structure from documentation
  private async prepareInvoiceCreateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceCreateInput> {
    // CORRECTED: Handle required branch relationship first
    if (!invoiceData.branchId) {
      throw new Error(
        `Invoice ${invoiceData.code} is missing required branchId`,
      );
    }

    const branch = await this.prismaService.branch.findFirst({
      where: { kiotVietId: invoiceData.branchId },
    });

    if (!branch) {
      throw new Error(
        `Required branch with kiotVietId ${invoiceData.branchId} not found for invoice ${invoiceData.code}`,
      );
    }

    // CORRECTED: Using exact field names from API documentation response
    const data: Prisma.InvoiceCreateInput = {
      kiotVietId: BigInt(invoiceData.id), // id: long
      code: invoiceData.code, // code: string
      orderCode: invoiceData.orderCode || null, // Not in list response but might exist
      purchaseDate: new Date(invoiceData.purchaseDate), // purchaseDate: datetime
      branch: { connect: { id: branch.id } }, // branchId: int (handled as relationship)
      total: new Prisma.Decimal(invoiceData.total || 0), // total: decimal
      totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0), // totalPayment: decimal
      discount: invoiceData.discount
        ? new Prisma.Decimal(invoiceData.discount)
        : null, // May not be in list response
      discountRatio: invoiceData.discountRatio || null, // May not be in list response
      status: invoiceData.status, // status: int
      description: invoiceData.description || null, // May not be in list response
      usingCod: invoiceData.usingCod || false, // usingCod: boolean
      isApplyVoucher: invoiceData.isApplyVoucher || false, // Not in response structure, may not exist
      retailerId: invoiceData.retailerId || null, // Not explicitly mentioned but might exist
      createdDate: invoiceData.createdDate // createdDate: datetime
        ? new Date(invoiceData.createdDate)
        : new Date(),
      modifiedDate: invoiceData.modifiedDate // modifiedDate: datetime
        ? new Date(invoiceData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // CORRECTED: Handle soldById (from response structure)
    if (invoiceData.soldById) {
      const soldByUser = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(invoiceData.soldById) },
      });
      if (soldByUser) {
        data.soldBy = { connect: { kiotVietId: BigInt(invoiceData.soldById) } };
      } else {
        this.logger.warn(
          `User with kiotVietId ${invoiceData.soldById} not found for invoice ${invoiceData.code}. Creating invoice without soldBy reference.`,
        );
      }
    }

    // CORRECTED: Handle customerId (from response structure)
    if (invoiceData.customerId) {
      const customer = await this.prismaService.customer.findFirst({
        where: { kiotVietId: BigInt(invoiceData.customerId) },
      });
      if (customer) {
        data.customer = { connect: { id: customer.id } };
      } else {
        this.logger.warn(
          `Customer with kiotVietId ${invoiceData.customerId} not found for invoice ${invoiceData.code}`,
        );
      }
    }

    // Handle orderId if present (may not be in list response, but could be in detail)
    if (invoiceData.orderId) {
      const order = await this.prismaService.order.findFirst({
        where: { kiotVietId: BigInt(invoiceData.orderId) },
      });
      if (order) {
        data.order = { connect: { id: order.id } };
      } else {
        this.logger.warn(
          `Order with kiotVietId ${invoiceData.orderId} not found for invoice ${invoiceData.code}`,
        );
      }
    }

    // Handle saleChannelId if present (not in basic list response structure shown)
    if (invoiceData.saleChannelId) {
      const saleChannel = await this.prismaService.saleChannel.findFirst({
        where: { kiotVietId: invoiceData.saleChannelId },
      });
      if (saleChannel) {
        data.saleChannel = { connect: { id: saleChannel.id } };
      } else {
        this.logger.warn(
          `Sale channel with kiotVietId ${invoiceData.saleChannelId} not found for invoice ${invoiceData.code}`,
        );
      }
    }

    return data;
  }

  // CORRECTED: Update method following same pattern
  private async prepareInvoiceUpdateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceUpdateInput> {
    // CORRECTED: Using exact field names from API documentation response
    const data: Prisma.InvoiceUpdateInput = {
      code: invoiceData.code, // code: string
      orderCode: invoiceData.orderCode || null,
      purchaseDate: new Date(invoiceData.purchaseDate), // purchaseDate: datetime
      total: new Prisma.Decimal(invoiceData.total || 0), // total: decimal
      totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0), // totalPayment: decimal
      discount: invoiceData.discount
        ? new Prisma.Decimal(invoiceData.discount)
        : null,
      discountRatio: invoiceData.discountRatio || null,
      status: invoiceData.status, // status: int
      description: invoiceData.description || null,
      usingCod: invoiceData.usingCod || false, // usingCod: boolean
      isApplyVoucher: invoiceData.isApplyVoucher || false,
      retailerId: invoiceData.retailerId || null,
      modifiedDate: invoiceData.modifiedDate // modifiedDate: datetime
        ? new Date(invoiceData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // Handle relationships same as create
    if (invoiceData.soldById) {
      const soldByUser = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(invoiceData.soldById) },
      });
      if (soldByUser) {
        data.soldBy = { connect: { kiotVietId: BigInt(invoiceData.soldById) } };
      } else {
        this.logger.warn(
          `User with kiotVietId ${invoiceData.soldById} not found for invoice ${invoiceData.code}. Updating invoice without soldBy reference.`,
        );
        data.soldBy = { disconnect: true };
      }
    }

    if (invoiceData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: invoiceData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
      } else {
        throw new Error(
          `Required branch with kiotVietId ${invoiceData.branchId} not found for invoice ${invoiceData.code}`,
        );
      }
    }

    if (invoiceData.customerId) {
      const customer = await this.prismaService.customer.findFirst({
        where: { kiotVietId: BigInt(invoiceData.customerId) },
      });
      if (customer) {
        data.customer = { connect: { id: customer.id } };
      }
    }

    if (invoiceData.orderId) {
      const order = await this.prismaService.order.findFirst({
        where: { kiotVietId: BigInt(invoiceData.orderId) },
      });
      if (order) {
        data.order = { connect: { id: order.id } };
      }
    }

    if (invoiceData.saleChannelId) {
      const saleChannel = await this.prismaService.saleChannel.findFirst({
        where: { kiotVietId: invoiceData.saleChannelId },
      });
      if (saleChannel) {
        data.saleChannel = { connect: { id: saleChannel.id } };
      }
    }

    return data;
  }

  private async handleInvoiceRelations(invoiceId: number, invoiceData: any) {
    if (invoiceData.payments && invoiceData.payments.length > 0) {
      for (const payment of invoiceData.payments) {
        try {
          const paymentData: any = {
            kiotVietId: payment.id ? BigInt(payment.id) : null,
            invoice: { connect: { id: invoiceId } },
            code: payment.code,
            amount: new Prisma.Decimal(payment.amount || 0),
            method: payment.method,
            status: payment.status,
            transDate: payment.transDate
              ? new Date(payment.transDate)
              : new Date(),
            bankAccountInfo: payment.bankAccount,
            description: payment.description || null,
          };

          if (payment.accountId) {
            const bankAccount = await this.prismaService.bankAccount.findFirst({
              where: { kiotVietId: payment.accountId },
            });
            if (bankAccount) {
              paymentData.bankAccount = { connect: { id: bankAccount.id } };
            }
          }

          if (payment.id) {
            await this.prismaService.payment.upsert({
              where: { kiotVietId: BigInt(payment.id) },
              create: paymentData,
              update: {
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount || 0),
                method: payment.method,
                status: payment.status,
                transDate: payment.transDate
                  ? new Date(payment.transDate)
                  : new Date(),
                bankAccountInfo: payment.bankAccount,
                description: payment.description || null,
              },
            });
          } else {
            // If no ID, just create
            await this.prismaService.payment.create({
              data: paymentData,
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save payment: ${error.message}`);
        }
      }
    }
  }

  private async handleInvoiceChildEntities(
    invoiceId: number,
    invoiceDetailData: any,
  ) {
    // Handle InvoiceDetails
    if (
      invoiceDetailData.invoiceDetails &&
      invoiceDetailData.invoiceDetails.length > 0
    ) {
      await this.prismaService.invoiceDetail.deleteMany({
        where: { invoiceId },
      });

      for (const detail of invoiceDetailData.invoiceDetails) {
        const product = await this.prismaService.product.findFirst({
          where: { kiotVietId: BigInt(detail.productId) },
        });

        if (product) {
          await this.prismaService.invoiceDetail.create({
            data: {
              kiotVietId: detail.id ? BigInt(detail.id) : null,
              invoice: { connect: { id: invoiceId } },
              product: { connect: { id: product.id } },
              quantity: detail.quantity,
              price: new Prisma.Decimal(detail.price),
              discount: detail.discount
                ? new Prisma.Decimal(detail.discount)
                : null,
              discountRatio: detail.discountRatio,
              note: detail.note,
              serialNumbers: detail.serialNumbers,
            },
          });
        }
      }
    }

    // Handle InvoiceDelivery
    if (invoiceDetailData.invoiceDelivery) {
      const delivery = invoiceDetailData.invoiceDelivery;
      await this.prismaService.invoiceDelivery.upsert({
        where: { invoiceId },
        create: {
          kiotVietId: delivery.id ? BigInt(delivery.id) : null,
          invoice: { connect: { id: invoiceId } },
          deliveryCode: delivery.deliveryCode,
          status: delivery.status,
          type: delivery.type,
          price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
          receiver: delivery.receiver,
          contactNumber: delivery.contactNumber,
          address: delivery.address,
          locationId: delivery.locationId,
          locationName: delivery.locationName,
          wardName: delivery.wardName,
          usingPriceCod: delivery.usingPriceCod || false,
          priceCodPayment: delivery.priceCodPayment
            ? new Prisma.Decimal(delivery.priceCodPayment)
            : null,
          weight: delivery.weight,
          length: delivery.length,
          width: delivery.width,
          height: delivery.height,
          partnerDeliveryId: delivery.partnerDeliveryId
            ? BigInt(delivery.partnerDeliveryId)
            : null,
        },
        update: {
          deliveryCode: delivery.deliveryCode,
          status: delivery.status,
          type: delivery.type,
          price: delivery.price ? new Prisma.Decimal(delivery.price) : null,
          receiver: delivery.receiver,
          contactNumber: delivery.contactNumber,
          address: delivery.address,
          locationId: delivery.locationId,
          locationName: delivery.locationName,
          wardName: delivery.wardName,
          usingPriceCod: delivery.usingPriceCod || false,
          priceCodPayment: delivery.priceCodPayment
            ? new Prisma.Decimal(delivery.priceCodPayment)
            : null,
          weight: delivery.weight,
          length: delivery.length,
          width: delivery.width,
          height: delivery.height,
          partnerDeliveryId: delivery.partnerDeliveryId
            ? BigInt(delivery.partnerDeliveryId)
            : null,
        },
      });
    }

    // Handle InvoiceSurcharges
    if (
      invoiceDetailData.invoiceOrderSurcharges &&
      invoiceDetailData.invoiceOrderSurcharges.length > 0
    ) {
      await this.prismaService.invoiceSurcharge.deleteMany({
        where: { invoiceId },
      });

      for (const surcharge of invoiceDetailData.invoiceOrderSurcharges) {
        const surchargeRef = surcharge.surchargeId
          ? await this.prismaService.surcharge.findFirst({
              where: { kiotVietId: surcharge.surchargeId },
            })
          : null;

        await this.prismaService.invoiceSurcharge.create({
          data: {
            kiotVietId: surcharge.id ? BigInt(surcharge.id) : null,
            invoice: { connect: { id: invoiceId } },
            surcharge: surchargeRef
              ? { connect: { id: surchargeRef.id } }
              : undefined,
            surchargeName: surcharge.surchargeName,
            surValue: surcharge.surValue
              ? new Prisma.Decimal(surcharge.surValue)
              : null,
            price: surcharge.price ? new Prisma.Decimal(surcharge.price) : null,
            createdDate: surcharge.createdDate
              ? new Date(surcharge.createdDate)
              : new Date(),
          },
        });
      }
    }
  }

  async fetchInvoiceDetail(invoiceId: number) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/invoices/${invoiceId}`, {
          headers,
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(
        `Failed to fetch invoice detail ${invoiceId}: ${error.message}`,
      );
      throw error;
    }
  }

  // Rest of sync methods remain the same...
  async syncRecentInvoices(days: number = 7): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'invoice_recent' },
        create: {
          name: 'invoice_recent',
          entities: ['invoice'],
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

      this.logger.log(`Starting recent invoice sync for last ${days} days...`);

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchInvoices({
          lastModifiedFrom,
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.batchSaveInvoices(
            response.data,
          );
          totalProcessed += created + updated;

          this.logger.log(
            `Invoice recent sync progress: ${totalProcessed} processed`,
          );
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'invoice_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed },
        },
      });

      this.logger.log(
        `Invoice recent sync completed: ${totalProcessed} processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'invoice_recent' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Invoice recent sync failed: ${error.message}`);
      throw error;
    }
  }

  async syncHistoricalInvoices(): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: 'invoice_historical' },
        create: {
          name: 'invoice_historical',
          entities: ['invoice'],
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
      let batchCount = 0;
      let hasMoreData = true;
      const invoiceBatch: any[] = [];

      this.logger.log('Starting historical invoice sync...');

      while (hasMoreData) {
        const response = await this.fetchInvoices({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          invoiceBatch.push(...response.data);

          if (invoiceBatch.length >= this.BATCH_SIZE) {
            const { created, updated } =
              await this.batchSaveInvoices(invoiceBatch);
            totalProcessed += created + updated;
            batchCount++;

            await this.prismaService.syncControl.update({
              where: { name: 'invoice_historical' },
              data: {
                progress: {
                  totalProcessed,
                  batchCount,
                  lastProcessedItem: currentItem + response.data.length,
                },
              },
            });

            this.logger.log(
              `Invoice historical sync batch ${batchCount}: ${totalProcessed} invoices processed`,
            );
            invoiceBatch.length = 0;
          }
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      // Process remaining invoices in batch
      if (invoiceBatch.length > 0) {
        const { created, updated } = await this.batchSaveInvoices(invoiceBatch);
        totalProcessed += created + updated;
        batchCount++;
      }

      await this.prismaService.syncControl.update({
        where: { name: 'invoice_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, batchCount },
        },
      });

      this.logger.log(
        `Invoice historical sync completed: ${totalProcessed} invoices processed`,
      );
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'invoice_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Invoice historical sync failed: ${error.message}`);
      throw error;
    }
  }
}
