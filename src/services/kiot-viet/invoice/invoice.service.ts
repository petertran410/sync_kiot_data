// src/services/kiot-viet/invoice/invoice.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';
import { LarkInvoiceSyncService } from '../../lark/invoice/lark-invoice-sync.service';

interface KiotVietInvoice {
  id: number;
  code: string;
  purchaseDate: string;
  branchId: number;
  branchName?: string;
  soldById?: number;
  soldByName?: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  orderId?: number;
  orderCode?: string;
  total: number;
  totalPayment: number;
  discount?: number;
  discountRatio?: number;
  status: number;
  statusValue?: string;
  description?: string;
  usingCod?: boolean;
  modifiedDate?: string;
  createdDate?: string;
  saleChannelId?: number;
  isApplyVoucher?: boolean;
  retailerId?: number;
  invoiceDetails?: Array<{
    id?: number;
    productId: number;
    productCode: string;
    productName: string;
    quantity: number;
    price: number;
    discount?: number;
    discountRatio?: number;
    note?: string;
    serialNumbers?: string;
    subTotal?: number;
  }>;
  invoiceDelivery?: {
    deliveryCode?: string;
    status: number;
    type?: number;
    price?: number;
    receiver?: string;
    contactNumber?: string;
    address?: string;
    locationId?: number;
    locationName?: string;
    wardName?: string;
    usingPriceCod?: boolean;
    priceCodPayment?: number;
    weight?: number;
    length?: number;
    width?: number;
    height?: number;
    partnerDeliveryId?: number;
  };
  payments?: Array<{
    id?: number;
    code?: string;
    amount: number;
    method: string;
    status?: number;
    transDate: string;
    accountId?: number;
    description?: string;
  }>;
  invoiceOrderSurcharges?: Array<{
    id?: number;
    surchargeId?: number;
    surchargeName?: string;
    surValue?: number;
    price?: number;
  }>;
}

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  // ============================================================================
  // SYNC CONTROL & TRACKING
  // ============================================================================

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'invoice_historical' },
      });

      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'invoice_recent' },
      });

      // Priority: Historical sync first
      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical invoice sync...');
        await this.syncHistoricalInvoices();
        return;
      }

      // Then recent sync
      if (recentSync?.isEnabled && !recentSync.isRunning) {
        this.logger.log('Starting recent invoice sync...');
        await this.syncRecentInvoices(7); // Last 7 days
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent invoice sync...');
      await this.syncRecentInvoices(7);
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('invoice_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical invoice sync enabled');
  }

  // ============================================================================
  // HISTORICAL SYNC
  // ============================================================================

  async syncHistoricalInvoices(): Promise<void> {
    const syncName = 'invoice_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalInvoices = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedInvoiceIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
      });

      this.logger.log('🚀 Starting historical invoice sync...');

      // Get total count first
      const totalResponse = await this.fetchInvoicePage(0);
      totalInvoices = totalResponse.total;
      lastValidTotal = totalInvoices;

      this.logger.log(`📊 Total invoices in system: ${totalInvoices}`);

      // Process all pages
      while (currentItem < totalInvoices) {
        const pageNumber = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        try {
          // Fetch page
          const response = await this.fetchInvoicePage(currentItem);

          if (!response.data || response.data.length === 0) {
            consecutiveEmptyPages++;
            this.logger.warn(
              `⚠️ Empty page ${pageNumber}, consecutive: ${consecutiveEmptyPages}`,
            );

            if (consecutiveEmptyPages >= 3) {
              this.logger.warn(
                '❌ Too many consecutive empty pages, stopping sync',
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // Filter duplicates
          const newInvoices = response.data.filter(
            (invoice: any) => !processedInvoiceIds.has(invoice.id),
          );

          if (newInvoices.length < response.data.length) {
            this.logger.warn(
              `⚠️ Found ${response.data.length - newInvoices.length} duplicate invoices in page ${pageNumber}`,
            );
          }

          if (newInvoices.length === 0) {
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Add to processed set
          newInvoices.forEach((invoice: any) =>
            processedInvoiceIds.add(invoice.id),
          );

          this.logger.log(
            `📊 Processing ${newInvoices.length} invoices (Page: ${pageNumber}, Processed: ${processedCount}/${totalInvoices})`,
          );

          // Enrich with details
          this.logger.log(
            `🔍 Enriching ${newInvoices.length} invoices with details...`,
          );
          const enrichedInvoices =
            await this.enrichInvoicesWithDetails(newInvoices);

          // Save to database
          this.logger.log(
            `💾 Saving ${enrichedInvoices.length} invoices to database...`,
          );
          const savedInvoices =
            await this.saveInvoicesToDatabase(enrichedInvoices);

          // Sync to LarkBase
          await this.syncInvoicesToLarkBase(savedInvoices);

          processedCount += newInvoices.length;
          currentItem += this.PAGE_SIZE;

          // Update progress
          const progress = Math.round((processedCount / totalInvoices) * 100);
          this.logger.log(
            `📈 Progress: ${processedCount}/${totalInvoices} (${progress}%)`,
          );

          await this.updateSyncControl(syncName, {
            progress: {
              current: processedCount,
              total: totalInvoices,
              percentage: progress,
            },
          });

          // Rate limiting
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } catch (error) {
          consecutiveErrorPages++;
          this.logger.error(
            `❌ Error processing page ${pageNumber}: ${error.message}`,
          );

          if (consecutiveErrorPages >= 3) {
            this.logger.error('❌ Too many consecutive errors, stopping sync');
            throw error;
          }

          currentItem += this.PAGE_SIZE;
          await new Promise((resolve) => setTimeout(resolve, 5000));
        }
      }

      // Sync control update
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        progress: {
          current: processedCount,
          total: totalInvoices,
          percentage: 100,
        },
      });

      this.logger.log(
        `✅ Historical sync completed: ${processedCount} invoices processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      throw error;
    }
  }

  // ============================================================================
  // RECENT SYNC
  // ============================================================================

  async syncRecentInvoices(days: number = 7): Promise<void> {
    const syncName = 'invoice_recent';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
      });

      this.logger.log(`🚀 Starting recent invoice sync (last ${days} days)...`);

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      let currentItem = 0;
      let totalProcessed = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchRecentInvoices(fromDate, currentItem);

        if (!response.data || response.data.length === 0) {
          hasMoreData = false;
          break;
        }

        this.logger.log(
          `📊 Processing ${response.data.length} recent invoices...`,
        );

        // Enrich with details
        const enrichedInvoices = await this.enrichInvoicesWithDetails(
          response.data,
        );

        // Save to database
        const savedInvoices =
          await this.saveInvoicesToDatabase(enrichedInvoices);

        // Sync to LarkBase
        await this.syncInvoicesToLarkBase(savedInvoices);

        totalProcessed += response.data.length;
        currentItem += response.data.length;

        // Check if more data
        hasMoreData = response.data.length === this.PAGE_SIZE;

        // Rate limiting
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        metadata: {
          daysSync: days,
          invoicesProcessed: totalProcessed,
        },
      });

      this.logger.log(
        `✅ Recent sync completed: ${totalProcessed} invoices processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Recent sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      throw error;
    }
  }

  // ============================================================================
  // KIOTVIET API METHODS
  // ============================================================================

  private async fetchInvoicePage(currentItem: number): Promise<any> {
    try {
      const headers = await this.authService.getRequestHeaders();

      const response = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/invoices`, {
          headers,
          params: {
            pageSize: this.PAGE_SIZE,
            currentItem,
            includeInvoiceDelivery: true,
            includePayment: true,
            orderBy: 'id',
            orderDirection: 'Asc',
          },
          timeout: 30000,
        }),
      );

      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch invoice page: ${error.message}`);
      throw error;
    }
  }

  private async fetchRecentInvoices(
    fromDate: Date,
    currentItem: number,
  ): Promise<any> {
    try {
      const headers = await this.authService.getRequestHeaders();

      const response = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/invoices`, {
          headers,
          params: {
            pageSize: this.PAGE_SIZE,
            currentItem,
            includeInvoiceDelivery: true,
            includePayment: true,
            lastModifiedFrom: fromDate.toISOString(),
            orderBy: 'modifiedDate',
            orderDirection: 'Desc',
          },
          timeout: 30000,
        }),
      );

      return response.data;
    } catch (error) {
      this.logger.error(`Failed to fetch recent invoices: ${error.message}`);
      throw error;
    }
  }

  private async enrichInvoicesWithDetails(invoices: any[]): Promise<any[]> {
    const enrichedInvoices: any[] = [];

    for (const invoice of invoices) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/invoices/${invoice.id}`, {
            headers,
            timeout: 15000,
          }),
        );

        enrichedInvoices.push(response.data);
      } catch (error) {
        this.logger.warn(
          `Failed to enrich invoice ${invoice.code}: ${error.message}`,
        );
        enrichedInvoices.push(invoice);
      }
    }

    return enrichedInvoices;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  private async saveInvoicesToDatabase(invoices: any[]): Promise<any[]> {
    const savedInvoices: any[] = [];

    for (const invoiceData of invoices) {
      try {
        // Get internal IDs
        const customer = invoiceData.customerId
          ? await this.prismaService.customer.findFirst({
              where: { kiotVietId: BigInt(invoiceData.customerId) },
              select: { id: true },
            })
          : null;

        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: invoiceData.branchId },
          select: { id: true },
        });

        const soldBy = invoiceData.soldById
          ? await this.prismaService.user.findFirst({
              where: { kiotVietId: invoiceData.soldById },
              select: { id: true },
            })
          : null;

        const saleChannel = invoiceData.saleChannelId
          ? await this.prismaService.saleChannel.findFirst({
              where: { kiotVietId: invoiceData.saleChannelId },
              select: { id: true },
            })
          : null;

        // Map status
        const statusMap = {
          1: 'COMPLETED',
          2: 'CANCELLED',
          3: 'PROCESSING',
          5: 'DELIVERY_FAILED',
        };

        // Save invoice
        const invoice = await this.prismaService.invoice.upsert({
          where: { kiotVietId: BigInt(invoiceData.id) },
          update: {
            code: invoiceData.code,
            purchaseDate: new Date(invoiceData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.id ?? null,
            customerId: customer?.id ?? null,
            customerCode: invoiceData.customerCode || null,
            customerName: invoiceData.customerName || null,
            orderId: invoiceData.orderId || null,
            orderCode: invoiceData.orderCode || null,
            total: new Prisma.Decimal(invoiceData.total || 0),
            totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
            discount: invoiceData.discount
              ? new Prisma.Decimal(invoiceData.discount)
              : null,
            discountRatio: invoiceData.discountRatio || null,
            status: statusMap[invoiceData.status] || 'COMPLETED',
            statusValue: invoiceData.statusValue || null,
            description: invoiceData.description || null,
            usingCod: invoiceData.usingCod || false,
            saleChannelId: saleChannel?.id ?? null,
            isApplyVoucher: invoiceData.isApplyVoucher || false,
            retailerId: invoiceData.retailerId || null,
            modifiedDate: invoiceData.modifiedDate
              ? new Date(invoiceData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING' as const,
          },
          create: {
            kiotVietId: BigInt(invoiceData.id),
            code: invoiceData.code,
            purchaseDate: new Date(invoiceData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.id ?? null,
            customerId: customer?.id ?? null,
            customerCode: invoiceData.customerCode || null,
            customerName: invoiceData.customerName || null,
            orderId: invoiceData.orderId || null,
            orderCode: invoiceData.orderCode || null,
            total: new Prisma.Decimal(invoiceData.total || 0),
            totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
            discount: invoiceData.discount
              ? new Prisma.Decimal(invoiceData.discount)
              : null,
            discountRatio: invoiceData.discountRatio || null,
            status: statusMap[invoiceData.status] || 'COMPLETED',
            statusValue: invoiceData.statusValue || null,
            description: invoiceData.description || null,
            usingCod: invoiceData.usingCod || false,
            saleChannelId: saleChannel?.id ?? null,
            isApplyVoucher: invoiceData.isApplyVoucher || false,
            retailerId: invoiceData.retailerId || null,
            modifiedDate: invoiceData.modifiedDate
              ? new Date(invoiceData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING' as const,
            totalCostOfGoods: '',
          } satisfies Prisma.InvoiceUncheckedCreateInput,
        });

        // Save invoice details
        if (
          invoiceData.invoiceDetails &&
          invoiceData.invoiceDetails.length > 0
        ) {
          for (const detail of invoiceData.invoiceDetails) {
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(detail.productId) },
              select: { id: true },
            });

            if (product) {
              await this.prismaService.invoiceDetail.upsert({
                where: {
                  kiotVietId: detail.id ? BigInt(detail.id) : BigInt(0),
                },
                update: {
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : null,
                  discountRatio: detail.discountRatio,
                  note: detail.note,
                  serialNumbers: detail.serialNumbers,
                  subTotal: new Prisma.Decimal(
                    detail.price * detail.quantity - (detail.discount || 0),
                  ),
                },
                create: {
                  kiotVietId: detail.id ? BigInt(detail.id) : null,
                  invoiceId: invoice.id,
                  productId: product.id,
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  discount: detail.discount
                    ? new Prisma.Decimal(detail.discount)
                    : null,
                  discountRatio: detail.discountRatio,
                  note: detail.note,
                  serialNumbers: detail.serialNumbers,
                  subTotal: new Prisma.Decimal(
                    detail.price * detail.quantity - (detail.discount || 0),
                  ),
                },
              });
            }
          }
        }

        // Save invoice delivery if exists
        if (invoiceData.invoiceDelivery) {
          const delivery = invoiceData.invoiceDelivery;

          await this.prismaService.invoiceDelivery.upsert({
            where: { invoiceId: invoice.id },
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
            create: {
              invoiceId: invoice.id,
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

        savedInvoices.push(invoice);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save invoice ${invoiceData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${savedInvoices.length} invoices to database`);
    return savedInvoices;
  }

  // ============================================================================
  // LARKBASE SYNC
  // ============================================================================

  async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${invoices.length} invoices...`,
      );

      const invoicesToSync = invoices.filter(
        (i) => i.larkSyncStatus === 'PENDING' || i.larkSyncStatus === 'FAILED',
      );

      if (invoicesToSync.length === 0) {
        this.logger.log('📋 No invoices need LarkBase sync');
        return;
      }

      await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoicesToSync);

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);

      // Don't mark as FAILED on connection issues
      if (
        !error.message.includes('connect') &&
        !error.message.includes('400')
      ) {
        const invoiceIds = invoices.map((i) => i.id);
        await this.prismaService.invoice.updateMany({
          where: { id: { in: invoiceIds } },
          data: {
            larkSyncStatus: 'FAILED',
            larkSyncedAt: new Date(),
          },
        });
      }

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateSyncControl(name: string, updates: any) {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['invoice'],
        syncMode: name.includes('historical') ? 'historical' : 'recent',
        ...updates,
      },
      update: updates,
    });
  }
}
