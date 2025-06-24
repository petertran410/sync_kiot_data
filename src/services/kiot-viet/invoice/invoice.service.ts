// src/services/kiot-viet/invoice/invoice.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { KiotVietAuthService } from '../auth.service';
import { LarkBaseService } from '../../lark/lark-base.service';
import { Prisma } from '@prisma/client';
import * as dayjs from 'dayjs';

interface InvoiceToCreate {
  createData: Prisma.InvoiceCreateInput;
  invoiceData: any;
}

interface InvoiceToUpdate {
  id: number;
  data: Prisma.InvoiceUpdateInput;
  invoiceData: any;
}

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
  private readonly PAGE_SIZE = 50;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly authService: KiotVietAuthService,
    private readonly larkBaseService: LarkBaseService,
  ) {}

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
      let hasMoreData = true;
      let batchCount = 0;

      while (hasMoreData) {
        const response = await this.fetchInvoices({
          currentItem,
          pageSize: this.PAGE_SIZE,
        });

        if (response.data && response.data.length > 0) {
          const { created, updated } = await this.saveInvoicesToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          // IMMEDIATE LarkBase sync
          await this.syncInvoicesToLarkBaseImmediate(response.data);

          this.logger.log(
            `Historical sync batch ${++batchCount}: ${totalProcessed} invoices processed`,
          );
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedInvoices(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      const duplicatesRemoved = await this.removeDuplicateInvoices();

      // Mark historical sync as COMPLETED and DISABLED
      await this.prismaService.syncControl.update({
        where: { name: 'invoice_historical' },
        data: {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved, batchCount },
        },
      });

      this.logger.log(
        `Historical sync completed: ${totalProcessed} invoices processed`,
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

      this.logger.error(`Historical sync failed: ${error.message}`);
      throw error;
    }
  }

  private async handleRemovedInvoices(removedIds: number[]): Promise<void> {
    try {
      for (const removedId of removedIds) {
        await this.prismaService.invoice.updateMany({
          where: { kiotVietId: BigInt(removedId) },
          data: { status: 2 },
        });
      }
      this.logger.log(`Marked ${removedIds.length} invoices as cancelled`);
    } catch (error) {
      this.logger.error(`Failed to handle removed invoices: ${error.message}`);
    }
  }

  private async removeDuplicateInvoices(): Promise<number> {
    try {
      const duplicates = await this.prismaService.$queryRaw`
        SELECT kiotVietId, MIN(id) as keep_id
        FROM "Invoice"
        GROUP BY kiotVietId
        HAVING COUNT(*) > 1
      `;

      let removedCount = 0;

      for (const duplicate of duplicates as any[]) {
        const duplicateInvoices = await this.prismaService.invoice.findMany({
          where: { kiotVietId: duplicate.kiotVietId },
          orderBy: { id: 'asc' },
        });

        // Keep the first one, delete the rest
        for (let i = 1; i < duplicateInvoices.length; i++) {
          await this.prismaService.invoice.delete({
            where: { id: duplicateInvoices[i].id },
          });
          removedCount++;
        }
      }

      if (removedCount > 0) {
        this.logger.log(`Removed ${removedCount} duplicate invoices`);
      }

      return removedCount;
    } catch (error) {
      this.logger.error(
        `Failed to remove duplicate invoices: ${error.message}`,
      );
      return 0;
    }
  }

  // ADD: Immediate LarkBase sync method
  private async syncInvoicesToLarkBaseImmediate(
    invoices: any[],
  ): Promise<void> {
    if (!invoices || invoices.length === 0) return;

    try {
      // Fetch related data for proper mapping
      const invoicesWithRelations = await Promise.all(
        invoices.map(async (invoice) => {
          const dbInvoice = await this.prismaService.invoice.findFirst({
            where: { kiotVietId: BigInt(invoice.id) },
            include: {
              branch: true,
              customer: true,
              soldBy: true,
              order: true,
              invoiceDelivery: true,
              invoiceSurcharges: true,
            },
          });
          return dbInvoice || invoice;
        }),
      );

      const result = await this.larkBaseService.directCreateInvoices(
        invoicesWithRelations,
      );

      this.logger.debug(
        `LarkBase immediate sync: ${result.success} success, ${result.failed} failed`,
      );
    } catch (error) {
      this.logger.error(`LarkBase immediate sync failed: ${error.message}`);

      return error;
    }
  }

  async syncRecentInvoices(days: number = 4): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'invoice_historical', isRunning: true },
      });

      if (historicalSync) {
        this.logger.log('Historical sync is running. Skipping recent sync.');
        return;
      }

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
          const { created, updated } = await this.saveInvoicesToDatabase(
            response.data,
          );
          totalProcessed += created + updated;

          await this.markInvoicesForLarkBaseSync(response.data);

          this.logger.log(
            `Recent sync progress: ${totalProcessed} invoices processed`,
          );
        }

        if (response.removedId && response.removedId.length > 0) {
          await this.handleRemovedInvoices(response.removedId);
        }

        hasMoreData = response.data && response.data.length === this.PAGE_SIZE;
        if (hasMoreData) currentItem += this.PAGE_SIZE;
      }

      const duplicatesRemoved = await this.removeDuplicateInvoices();

      await this.prismaService.syncControl.update({
        where: { name: 'invoice_recent' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { totalProcessed, duplicatesRemoved },
        },
      });

      this.logger.log(
        `Recent sync completed: ${totalProcessed} invoices processed, ${duplicatesRemoved} duplicates removed`,
      );

      await this.syncPendingToLarkBase();
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

      this.logger.error(`Recent sync failed: ${error.message}`);
      throw error;
    }
  }

  async syncPendingToLarkBase(): Promise<{ success: number; failed: number }> {
    try {
      const pendingInvoices = await this.prismaService.invoice.findMany({
        where: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: { lt: 3 },
        },
        include: {
          branch: true,
          customer: true,
          soldBy: true,
          order: true,
          invoiceDelivery: true,
          invoiceSurcharges: true,
        },
        take: 100,
      });

      if (pendingInvoices.length === 0) {
        this.logger.log('No pending invoices to sync to LarkBase');
        return { success: 0, failed: 0 };
      }

      this.logger.log(
        `Syncing ${pendingInvoices.length} pending invoices to LarkBase`,
      );

      // Explicitly type the arrays
      const recordsToCreate: typeof pendingInvoices = [];
      const recordsToUpdate: typeof pendingInvoices = [];

      for (const invoice of pendingInvoices) {
        if (invoice.larkRecordId) {
          recordsToUpdate.push(invoice);
        } else {
          recordsToCreate.push(invoice);
        }
      }

      let totalSuccess = 0;
      let totalFailed = 0;

      if (recordsToCreate.length > 0) {
        const createResult = await this.larkBaseCreateBatch(recordsToCreate);
        totalSuccess += createResult.success;
        totalFailed += createResult.failed;
      }

      if (recordsToUpdate.length > 0) {
        const updateResult = await this.larkBaseUpdateBatch(recordsToUpdate);
        totalSuccess += updateResult.success;
        totalFailed += updateResult.failed;
      }

      this.logger.log(
        `LarkBase sync completed: ${totalSuccess} success, ${totalFailed} failed`,
      );

      return { success: totalSuccess, failed: totalFailed };
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      return { success: 0, failed: 0 };
    }
  }

  private async larkBaseCreateBatch(
    invoices: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response =
        await this.larkBaseService.directCreateInvoices(invoices);

      if (response.success > 0 && response.records) {
        for (const [index, invoice] of invoices.entries()) {
          if (response.records[index]) {
            await this.prismaService.invoice.update({
              where: { id: invoice.id },
              data: {
                larkRecordId: response.records[index].record_id,
                larkSyncStatus: 'SYNCED',
                larkSyncedAt: new Date(),
                larkSyncRetries: 0,
              },
            });
          }
        }
      }

      if (response.failed > 0) {
        const failedInvoices = invoices.slice(response.success);
        for (const invoice of failedInvoices) {
          await this.prismaService.invoice.update({
            where: { id: invoice.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase create batch failed: ${error.message}`);

      for (const invoice of invoices) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: invoices.length };
    }
  }

  private async larkBaseUpdateBatch(
    invoices: any[],
  ): Promise<{ success: number; failed: number }> {
    try {
      const response =
        await this.larkBaseService.directUpdateInvoices(invoices);

      if (response.success > 0) {
        const successfulInvoices = invoices.slice(0, response.success);
        for (const invoice of successfulInvoices) {
          await this.prismaService.invoice.update({
            where: { id: invoice.id },
            data: {
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });
        }
      }

      if (response.failed > 0) {
        const failedInvoices = invoices.slice(response.success);
        for (const invoice of failedInvoices) {
          await this.prismaService.invoice.update({
            where: { id: invoice.id },
            data: {
              larkSyncStatus: 'FAILED',
              larkSyncRetries: { increment: 1 },
            },
          });
        }
      }

      return { success: response.success, failed: response.failed };
    } catch (error) {
      this.logger.error(`LarkBase update batch failed: ${error.message}`);

      for (const invoice of invoices) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncRetries: { increment: 1 },
          },
        });
      }

      return { success: 0, failed: invoices.length };
    }
  }

  private async markInvoicesForLarkBaseSync(invoices: any[]): Promise<void> {
    try {
      const kiotVietIds = invoices.map((i) => BigInt(i.id));

      await this.prismaService.invoice.updateMany({
        where: { kiotVietId: { in: kiotVietIds } },
        data: {
          larkSyncStatus: 'PENDING',
          larkSyncRetries: 0,
        },
      });

      this.logger.debug(`Marked ${invoices.length} invoices for LarkBase sync`);
    } catch (error) {
      this.logger.error(
        `Failed to mark invoices for LarkBase sync: ${error.message}`,
      );
    }
  }

  private async fetchInvoices(params: any): Promise<any> {
    try {
      const accessToken = await this.authService.getAccessToken();
      const baseUrl = this.configService.get<string>('KIOT_BASE_URL');

      const queryParams = new URLSearchParams();
      if (params.currentItem !== undefined) {
        queryParams.append('currentItem', params.currentItem.toString());
      }
      if (params.pageSize) {
        queryParams.append('pageSize', params.pageSize.toString());
      }
      if (params.lastModifiedFrom) {
        queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
      }
      queryParams.append('includeInvoiceSurcharge', 'true');

      const url = `${baseUrl}/invoices?${queryParams.toString()}`;

      const response = await this.httpService
        .get(url, {
          headers: {
            Retailer: this.configService.get<string>('KIOT_SHOP_NAME'),
            Authorization: `Bearer ${accessToken}`,
          },
        })
        .toPromise();

      return response?.data;
    } catch (error) {
      this.logger.error(`Failed to fetch invoices: ${error.message}`);
      throw error;
    }
  }

  private async saveInvoicesToDatabase(
    invoices: any[],
  ): Promise<{ created: number; updated: number }> {
    // Explicitly type the arrays
    const invoicesToCreate: InvoiceToCreate[] = [];
    const invoicesToUpdate: InvoiceToUpdate[] = [];

    for (const invoiceData of invoices) {
      const existingInvoice = await this.prismaService.invoice.findUnique({
        where: { kiotVietId: BigInt(invoiceData.id) },
      });

      if (existingInvoice) {
        const updateData = await this.prepareInvoiceUpdateData(invoiceData);
        invoicesToUpdate.push({
          id: existingInvoice.id,
          data: updateData,
          invoiceData: invoiceData,
        });
      } else {
        const createData = await this.prepareInvoiceCreateData(invoiceData);
        if (createData) {
          invoicesToCreate.push({
            createData: createData,
            invoiceData: invoiceData,
          });
        }
      }
    }

    return await this.processDatabaseOperations(
      invoicesToCreate,
      invoicesToUpdate,
    );
  }

  private async processDatabaseOperations(
    invoicesToCreate: InvoiceToCreate[],
    invoicesToUpdate: InvoiceToUpdate[],
  ) {
    let createdCount = 0;
    let updatedCount = 0;

    // Create invoices
    for (const invoiceToCreate of invoicesToCreate) {
      try {
        const invoice = await this.prismaService.invoice.create({
          data: invoiceToCreate.createData,
        });

        await this.handleInvoiceRelations(
          invoice.id,
          invoiceToCreate.invoiceData,
        );
        createdCount++;
      } catch (error) {
        this.logger.error(`Failed to create invoice: ${error.message}`);
      }
    }

    // Update invoices
    for (const invoiceToUpdate of invoicesToUpdate) {
      try {
        await this.prismaService.invoice.update({
          where: { id: invoiceToUpdate.id },
          data: invoiceToUpdate.data,
        });

        await this.handleInvoiceRelations(
          invoiceToUpdate.id,
          invoiceToUpdate.invoiceData,
        );
        updatedCount++;
      } catch (error) {
        this.logger.error(`Failed to update invoice: ${error.message}`);
      }
    }

    return { created: createdCount, updated: updatedCount };
  }
  private async prepareInvoiceCreateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceCreateInput | null> {
    try {
      const data: Prisma.InvoiceCreateInput = {
        kiotVietId: BigInt(invoiceData.id),
        code: invoiceData.code,
        orderCode: invoiceData.orderCode || null,
        purchaseDate: new Date(invoiceData.purchaseDate),
        total: parseFloat(invoiceData.total),
        totalCostOfGoods: invoiceData.totalCostOfGoods
          ? parseFloat(invoiceData.totalCostOfGoods)
          : 0,
        totalPayment: parseFloat(invoiceData.totalPayment),
        discount: invoiceData.discount
          ? parseFloat(invoiceData.discount)
          : null,
        discountRatio: invoiceData.discountRatio || null,
        status: invoiceData.status,
        description: invoiceData.description || null,
        usingCod: invoiceData.usingCod || false,
        isApplyVoucher: invoiceData.isApplyVoucher || false,
        retailerId: invoiceData.retailerId || null,
        createdDate: invoiceData.createdDate
          ? new Date(invoiceData.createdDate)
          : new Date(),
        modifiedDate: invoiceData.modifiedDate
          ? new Date(invoiceData.modifiedDate)
          : new Date(),
        lastSyncedAt: new Date(),
        larkSyncStatus: 'PENDING',
      };

      // Handle soldBy relationship (not soldById)
      if (invoiceData.soldById) {
        const user = await this.prismaService.user.findFirst({
          where: { kiotVietId: BigInt(invoiceData.soldById) },
        });
        if (user) {
          data.soldBy = { connect: { id: user.id } };
        }
      }

      // Handle branch relationship
      if (invoiceData.branchId) {
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: invoiceData.branchId },
        });
        if (branch) {
          data.branch = { connect: { id: branch.id } };
        }
      }

      // Handle customer relationship
      if (invoiceData.customerId) {
        const customer = await this.prismaService.customer.findFirst({
          where: { kiotVietId: BigInt(invoiceData.customerId) },
        });
        if (customer) {
          data.customer = { connect: { id: customer.id } };
        }
      }

      // Handle order relationship
      if (invoiceData.orderId) {
        const order = await this.prismaService.order.findFirst({
          where: { kiotVietId: BigInt(invoiceData.orderId) },
        });
        if (order) {
          data.order = { connect: { id: order.id } };
        }
      }

      // Handle sale channel relationship
      if (invoiceData.saleChannelId) {
        const saleChannel = await this.prismaService.saleChannel.findFirst({
          where: { kiotVietId: invoiceData.saleChannelId },
        });
        if (saleChannel) {
          data.saleChannel = { connect: { id: saleChannel.id } };
        }
      }

      return data;
    } catch (error) {
      this.logger.error(
        `Failed to prepare invoice create data: ${error.message}`,
      );
      return null;
    }
  }

  private async prepareInvoiceUpdateData(
    invoiceData: any,
  ): Promise<Prisma.InvoiceUpdateInput> {
    const data: Prisma.InvoiceUpdateInput = {
      code: invoiceData.code,
      orderCode: invoiceData.orderCode || null,
      purchaseDate: new Date(invoiceData.purchaseDate),
      total: parseFloat(invoiceData.total),
      totalCostOfGoods: invoiceData.totalCostOfGoods
        ? parseFloat(invoiceData.totalCostOfGoods)
        : 0,
      totalPayment: parseFloat(invoiceData.totalPayment),
      discount: invoiceData.discount ? parseFloat(invoiceData.discount) : null,
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
      larkSyncStatus: 'PENDING',
    };

    // Handle soldBy relationship (not soldById)
    if (invoiceData.soldById) {
      const user = await this.prismaService.user.findFirst({
        where: { kiotVietId: BigInt(invoiceData.soldById) },
      });
      if (user) {
        data.soldBy = { connect: { id: user.id } };
      }
    }

    // Handle relationships (same as create)
    if (invoiceData.branchId) {
      const branch = await this.prismaService.branch.findFirst({
        where: { kiotVietId: invoiceData.branchId },
      });
      if (branch) {
        data.branch = { connect: { id: branch.id } };
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

  private async handleInvoiceRelations(
    invoiceId: number,
    invoiceData: any,
  ): Promise<void> {
    // Handle invoice details
    if (invoiceData.invoiceDetails && invoiceData.invoiceDetails.length > 0) {
      await this.prismaService.invoiceDetail.deleteMany({
        where: { invoiceId },
      });

      for (const detail of invoiceData.invoiceDetails) {
        try {
          const product = await this.prismaService.product.findFirst({
            where: { kiotVietId: BigInt(detail.productId) },
          });

          if (product) {
            await this.prismaService.invoiceDetail.create({
              data: {
                kiotVietId: detail.kiotVietId
                  ? BigInt(detail.kiotVietId)
                  : null,
                invoiceId,
                productId: product.id,
                quantity: parseFloat(detail.quantity),
                price: parseFloat(detail.price),
                discount: detail.discount ? parseFloat(detail.discount) : null,
                discountRatio: detail.discountRatio || null,
                note: detail.note || null,
                serialNumbers: detail.serialNumbers || null,
                subTotal: parseFloat(detail.subTotal || 0),
              },
            });
          }
        } catch (error) {
          this.logger.error(
            `Failed to create invoice detail: ${error.message}`,
          );
        }
      }
    }

    // Handle invoice delivery
    if (invoiceData.invoiceDelivery) {
      await this.prismaService.invoiceDelivery.deleteMany({
        where: { invoiceId },
      });

      try {
        await this.prismaService.invoiceDelivery.create({
          data: {
            kiotVietId: invoiceData.invoiceDelivery.kiotVietId
              ? BigInt(invoiceData.invoiceDelivery.kiotVietId)
              : null,
            invoiceId,
            deliveryCode: invoiceData.invoiceDelivery.deliveryCode || null,
            status: invoiceData.invoiceDelivery.status,
            type: invoiceData.invoiceDelivery.type || null,
            price: invoiceData.invoiceDelivery.price
              ? parseFloat(invoiceData.invoiceDelivery.price)
              : null,
            receiver: invoiceData.invoiceDelivery.receiver || null,
            contactNumber: invoiceData.invoiceDelivery.contactNumber || null,
            address: invoiceData.invoiceDelivery.address || null,
            locationId: invoiceData.invoiceDelivery.locationId || null,
            locationName: invoiceData.invoiceDelivery.locationName || null,
            wardName: invoiceData.invoiceDelivery.wardName || null,
            usingPriceCod: invoiceData.invoiceDelivery.usingPriceCod || false,
            priceCodPayment: invoiceData.invoiceDelivery.priceCodPayment
              ? parseFloat(invoiceData.invoiceDelivery.priceCodPayment)
              : null,
            weight: invoiceData.invoiceDelivery.weight || null,
            length: invoiceData.invoiceDelivery.length || null,
            width: invoiceData.invoiceDelivery.width || null,
            height: invoiceData.invoiceDelivery.height || null,
            partnerDeliveryId: invoiceData.invoiceDelivery.partnerDeliveryId
              ? BigInt(invoiceData.invoiceDelivery.partnerDeliveryId)
              : null,
          },
        });
      } catch (error) {
        this.logger.error(
          `Failed to create invoice delivery: ${error.message}`,
        );
      }
    }

    // Handle invoice surcharges
    if (
      invoiceData.invoiceSurcharges &&
      invoiceData.invoiceSurcharges.length > 0
    ) {
      await this.prismaService.invoiceSurcharge.deleteMany({
        where: { invoiceId },
      });

      for (const surcharge of invoiceData.invoiceSurcharges) {
        try {
          const surchargeRecord = await this.prismaService.surcharge.findFirst({
            where: { kiotVietId: surcharge.surchargeId },
          });

          await this.prismaService.invoiceSurcharge.create({
            data: {
              kiotVietId: surcharge.kiotVietId
                ? BigInt(surcharge.kiotVietId)
                : null,
              invoiceId,
              surchargeId: surchargeRecord?.id || null,
              surchargeName: surcharge.surchargeName || null,
              surValue: surcharge.surValue
                ? parseFloat(surcharge.surValue)
                : null,
              price: surcharge.price ? parseFloat(surcharge.price) : null,
              createdDate: surcharge.createdDate
                ? new Date(surcharge.createdDate)
                : new Date(),
            },
          });
        } catch (error) {
          this.logger.error(
            `Failed to create invoice surcharge: ${error.message}`,
          );
        }
      }
    }

    // Handle payments
    if (invoiceData.payments && invoiceData.payments.length > 0) {
      await this.prismaService.payment.deleteMany({
        where: { invoiceId },
      });

      for (const payment of invoiceData.payments) {
        try {
          const bankAccount = payment.accountId
            ? await this.prismaService.bankAccount.findFirst({
                where: { kiotVietId: payment.accountId },
              })
            : null;

          await this.prismaService.payment.create({
            data: {
              kiotVietId: payment.kiotVietId
                ? BigInt(payment.kiotVietId)
                : null,
              code: payment.code || null,
              amount: parseFloat(payment.amount),
              method: payment.method,
              status: payment.status || null,
              transDate: new Date(payment.transDate),
              accountId: bankAccount?.id || null,
              bankAccountInfo: payment.bankAccount || null,
              invoiceId,
              description: payment.description || null,
            },
          });
        } catch (error) {
          this.logger.error(`Failed to create payment: ${error.message}`);
        }
      }
    }
  }

  async checkAndRunAppropriateSync(): Promise<void> {
    const historicalSync = await this.prismaService.syncControl.findFirst({
      where: { name: 'invoice_historical' },
    });

    if (!historicalSync) {
      this.logger.log(
        'No historical sync record found. Starting full historical sync...',
      );
      await this.syncHistoricalInvoices();
      return;
    }

    // If historical sync is completed and disabled, run recent sync
    if (historicalSync.status === 'completed' && !historicalSync.isEnabled) {
      this.logger.log('Historical sync completed. Running recent sync...');
      await this.syncRecentInvoices();
      return;
    }

    // If historical sync is enabled but not running, start it
    if (historicalSync.isEnabled && !historicalSync.isRunning) {
      this.logger.log('Starting historical sync...');
      await this.syncHistoricalInvoices();
      return;
    }

    // If historical sync is running, skip
    if (historicalSync.isRunning) {
      this.logger.log('Historical sync is already running. Skipping...');
      return;
    }

    // Default to recent sync
    this.logger.log('Running recent sync...');
    await this.syncRecentInvoices();
  }
}
