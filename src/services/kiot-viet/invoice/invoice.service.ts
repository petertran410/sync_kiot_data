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

  private missingBranches = new Set<number>();

  private async handleMissingBranch(branchId: number, invoiceCode: string) {
    if (!this.missingBranches.has(branchId)) {
      this.missingBranches.add(branchId);
      this.logger.warn(
        `New missing branch detected: ${branchId} (first seen in invoice ${invoiceCode})`,
      );
    }
  }

  private async prepareInvoiceCreateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceCreateInput> {
    const total = invoiceData.total || 0;
    const discount = invoiceData.discount || 0;
    const totalCostOfGoods = total + discount || 0;

    const data: Prisma.InvoiceCreateInput = {
      kiotVietId: BigInt(invoiceData.id),
      code: invoiceData.code,
      orderCode: invoiceData.orderCode || null,
      purchaseDate: new Date(invoiceData.purchaseDate),
      total: new Prisma.Decimal(invoiceData.total || 0),
      totalCostOfGoods: new Prisma.Decimal(totalCostOfGoods || 0),
      totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
      discount: invoiceData.discount
        ? new Prisma.Decimal(invoiceData.discount)
        : null,
      discountRatio: invoiceData.discountRatio || null,
      status: invoiceData.status,
      description: invoiceData.description || null,
      usingCod: invoiceData.usingCod || false,
      isApplyVoucher: invoiceData.isApplyVoucher || false,
      retailerId: invoiceData.retailerId || null,
      modifiedDate: invoiceData.modifiedDate
        ? new Date(invoiceData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    if (invoiceData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: invoiceData.branchId },
      });

      if (branch) {
        data.branch = { connect: { id: branch.id } };
      } else {
        await this.handleMissingBranch(invoiceData.branchId, invoiceData.code);
        this.logger.warn(
          `Branch ${invoiceData.branchId} not found for invoice ${invoiceData.code}. Creating invoice without branch reference.`,
        );
      }
    }

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

  private async prepareInvoiceUpdateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceUpdateInput> {
    const total = invoiceData.total || 0;
    const discount = invoiceData.discount || 0;
    const totalCostOfGoods = total + discount || 0;

    const data: Prisma.InvoiceUpdateInput = {
      code: invoiceData.code,
      orderCode: invoiceData.orderCode || null,
      purchaseDate: new Date(invoiceData.purchaseDate),
      total: new Prisma.Decimal(invoiceData.total || 0),
      totalCostOfGoods: new Prisma.Decimal(totalCostOfGoods || 0),
      totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
      discount: invoiceData.discount
        ? new Prisma.Decimal(invoiceData.discount)
        : null,
      discountRatio: invoiceData.discountRatio || null,
      status: invoiceData.status,
      description: invoiceData.description || null,
      usingCod: invoiceData.usingCod || false,
      isApplyVoucher: invoiceData.isApplyVoucher || false,
      retailerId: invoiceData.retailerId || null,
      modifiedDate: invoiceData.modifiedDate
        ? new Date(invoiceData.modifiedDate)
        : new Date(),
      lastSyncedAt: new Date(),
    };

    // Handle branch - set to null if not found (FIXED: same as create)
    if (invoiceData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: invoiceData.branchId },
      });

      if (branch) {
        data.branch = { connect: { id: branch.id } };
      } else {
        await this.handleMissingBranch(invoiceData.branchId, invoiceData.code);
        this.logger.warn(
          `Branch ${invoiceData.branchId} not found for invoice ${invoiceData.code}. Updating invoice without branch reference.`,
        );
        data.branch = { disconnect: true }; // Explicitly set to null
      }
    }

    // Handle soldBy user - set to null if not found (FIXED: same as create)
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
        data.soldBy = { disconnect: true }; // Explicitly set to null
      }
    }

    // Handle other relationships (existing logic)
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

  async reportMissingBranches() {
    if (this.missingBranches.size > 0) {
      this.logger.warn(
        `Total missing branches: ${this.missingBranches.size}`,
        Array.from(this.missingBranches),
      );
    }
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

          // Handle bankAccount relationship
          if (payment.accountId) {
            const bankAccount = await this.prismaService.bankAccount.findFirst({
              where: { kiotVietId: payment.accountId },
            });
            if (bankAccount) {
              paymentData.bankAccount = { connect: { id: bankAccount.id } };
            }
          }

          // FIXED: Use UPSERT instead of CREATE
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
            await this.prismaService.payment.create({
              data: paymentData,
            });
          }
        } catch (error) {
          this.logger.error(`Failed to save payment: ${error.message}`);
        }
      }
    }

    // 2. Handle InvoiceDelivery (NEW - if included in list response)
    if (invoiceData.invoiceDelivery) {
      const delivery = invoiceData.invoiceDelivery;
      try {
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
      } catch (error) {
        this.logger.error(`Failed to save invoice delivery: ${error.message}`);
      }
    }

    // 3. For InvoiceDetails and InvoiceSurcharges - Need detail API call
    // The list API doesn't include these, so we need to fetch detail
    await this.handleDetailEntities(invoiceId, invoiceData.id);
  }

  private async handleDetailEntities(
    invoiceId: number,
    kiotVietInvoiceId: number,
  ) {
    try {
      const detailData = await this.fetchInvoiceDetail(kiotVietInvoiceId);

      if (!detailData) return;

      if (detailData.invoiceDetails && detailData.invoiceDetails.length > 0) {
        await this.prismaService.invoiceDetail.deleteMany({
          where: { invoiceId },
        });

        for (const detail of detailData.invoiceDetails) {
          const product = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(detail.productId) },
          });

          if (product) {
            const quantity = detail.quantity || 0;
            const price = detail.price || 0;
            const discount = detail.discount || 0;
            const subTotal = (price - discount) * quantity;

            await this.prismaService.invoiceDetail.create({
              data: {
                kiotVietId: detail.id ? BigInt(detail.id) : null,
                invoice: { connect: { id: invoiceId } },
                product: { connect: { id: product.id } },
                quantity: quantity,
                price: new Prisma.Decimal(price),
                discount: discount ? new Prisma.Decimal(discount) : null,
                discountRatio: detail.discountRatio,
                note: detail.note,
                serialNumbers: detail.serialNumbers,
                subTotal: new Prisma.Decimal(subTotal || 0),
              },
            });
          }
        }
      }

      // Handle InvoiceSurcharges
      if (
        detailData.invoiceOrderSurcharges &&
        detailData.invoiceOrderSurcharges.length > 0
      ) {
        await this.prismaService.invoiceSurcharge.deleteMany({
          where: { invoiceId },
        });

        for (const surcharge of detailData.invoiceOrderSurcharges) {
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
              price: surcharge.price
                ? new Prisma.Decimal(surcharge.price)
                : null,
              createdDate: surcharge.createdDate
                ? new Date(surcharge.createdDate)
                : new Date(),
            },
          });
        }
      }
    } catch (error) {
      this.logger.error(
        `Failed to handle detail entities for invoice ${kiotVietInvoiceId}: ${error.message}`,
      );
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
      return null;
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
