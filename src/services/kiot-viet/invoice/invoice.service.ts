import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkInvoiceSyncService } from '../../lark/invoice/lark-invoice-sync.service';
import { Prisma } from '@prisma/client';

interface KiotVietInvoice {
  id: number;
  code: string;
  orderCode?: string;
  purchaseDate?: string;
  branchId?: number;
  branchName?: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  soldById?: number;
  soldByName?: string;
  total?: number;
  totalPayment?: number;
  status?: number;
  statusValue?: string;
  createdDate?: string;
  modifiedDate?: string;
  usingCod?: boolean;
  description?: string;
  invoiceOrderSurcharges?: Array<{
    id?: number;
    invoiceId: number;
    surchargeId?: number;
    surchargeName?: string;
    surValue?: number;
    price?: number;
    createdDate?: string;
  }>;
  invoiceDetails: Array<{
    productId: number;
    productCode?: string;
    productName?: string;
    quantity: number;
    price: number;
    discount?: number;
    discountRatio?: number;
    note?: string;
    serialNumbers?: string;
    productBatchExpire?: Array<{
      id: number;
      productId: number;
      batchName?: string;
      fullNameVirgule?: string;
      createdDate?: string;
      expireDate?: string;
    }>;
  }>;
  invoiceDelivery?: {
    deliveryCode?: string;
    type?: number;
    status?: number;
    price?: number;
    receiver?: string;
    contactNumber?: string;
    address?: string;
    locationId?: number;
    locationName?: string;
    usingPriceCod?: boolean;
    priceCodPayment?: number;
    weight?: number;
    length?: number;
    width?: number;
    height?: number;
    partnerDeliveryId?: number;
    partnerDelivery?: {
      code?: string;
      name?: string;
      address?: string;
      contactNumber?: string;
      email?: string;
    };
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
  SaleChannel?: {
    IsNotDelete?: boolean;
    RetailerId?: number;
    Position?: number;
    IsActivate?: boolean;
    CreatedBy?: number;
    CreatedDate?: string;
    Id: number;
    Name: string;
  };
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
  // SYNC CONTROL & TRACKING - ENHANCED
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
        await this.syncRecentInvoices(3);
        return;
      }

      // Default: recent sync
      this.logger.log('Running default recent invoice sync...');
      await this.syncRecentInvoices(3);
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
  // HISTORICAL SYNC - ENHANCED WITH ADVANCED ERROR HANDLING
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
        error: null,
      });

      this.logger.log('🚀 Starting historical invoice sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalInvoices > 0) {
          if (currentItem >= totalInvoices) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalInvoices} customers`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalInvoices) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalInvoices} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const invoiceListResponse = await this.fetchInvoicesListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            includeInvoiceDelivery: true,
            includePayment: true,
            includeTotal: true,
          });

          // VALIDATION: Check response structure
          if (!invoiceListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          // Reset consecutive error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: invoices } = invoiceListResponse;

          if (total !== undefined && total !== null) {
            if (totalInvoices === 0) {
              totalInvoices = total;
              this.logger.log(`📊 Total invoices detected: ${totalInvoices}`);
            } else if (total !== totalInvoices) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalInvoices} -> ${total}. Using latest.`,
              );
              totalInvoices = total;
            }
            lastValidTotal = total;
          }

          if (!invoices || invoices.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalInvoices > 0 && currentItem >= totalInvoices) {
              this.logger.log('✅ Reached end of data (empty page past total)');
              break;
            }

            // If too many consecutive empty pages, stop
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            // Move to next page
            currentItem += this.PAGE_SIZE;
            continue;
          }

          const newInvoices = invoices.filter((invoice) => {
            if (processedInvoiceIds.has(invoice.id)) {
              this.logger.debug(
                `⚠️ Duplicate invoice ID detected: ${invoice.id} (${invoice.code})`,
              );
              return false;
            }
            processedInvoiceIds.add(invoice.id);
            return true;
          });

          if (newInvoices.length !== invoices.length) {
            this.logger.warn(
              `🔄 Filtered out ${invoices.length - newInvoices.length} duplicate invoices on page ${currentPage}`,
            );
          }

          if (newInvoices.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all invoices already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Process invoices
          this.logger.log(
            `🔄 Processing ${newInvoices.length} invoices from page ${currentPage}...`,
          );

          const invoicesWithDetails =
            await this.enrichInvoicesWithDetails(newInvoices);
          const savedInvoices =
            await this.saveInvoicesToDatabase(invoicesWithDetails);

          processedCount += savedInvoices.length;
          currentItem += this.PAGE_SIZE;

          if (totalInvoices > 0) {
            const completionPercentage = (processedCount / totalInvoices) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalInvoices} (${completionPercentage.toFixed(1)}%)`,
            );

            // Early completion check
            if (processedCount >= totalInvoices) {
              this.logger.log('🎉 All invoices processed successfully!');
              break;
            }
          }

          if (savedInvoices.length > 0) {
            try {
              await this.syncInvoicesToLarkBase(savedInvoices);
              this.logger.log(
                `🚀 Synced ${savedInvoices.length} invoices to LarkBase`,
              );
            } catch (larkError) {
              this.logger.warn(
                `⚠️ LarkBase sync failed for page ${currentPage}: ${larkError.message}`,
              );
            }
          }

          if (totalInvoices > 0) {
            if (
              currentItem >= totalInvoices &&
              processedCount >= totalInvoices * 0.95
            ) {
              this.logger.log(
                '✅ Sync completed - reached expected data range',
              );
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          // Exponential backoff
          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`⏳ Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }

      // Final completion logging
      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalInvoices },
      });

      await this.updateSyncControl('invoice_recent', {
        isEnabled: true,
        isRunning: false,
        status: 'idle',
      });

      const completionRate =
        totalInvoices > 0 ? (processedCount / totalInvoices) * 100 : 100;

      this.logger.log(
        `✅ Historical invoice sync completed: ${processedCount}/${totalInvoices} (${completionRate.toFixed(1)}% completion rate)`,
      );
      this.logger.log(
        `🔄 AUTO-TRANSITION: Historical sync disabled, Recent sync enabled for future cycles`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical invoice sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalInvoices },
      });

      throw error;
    }
  }

  // ============================================================================
  // RECENT SYNC
  // ============================================================================

  async syncRecentInvoices(days: number = 3): Promise<void> {
    const syncName = 'invoice_recent';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(`🔄 Starting recent invoice sync (${days} days)...`);

      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - days);

      const recentInvoices = await this.fetchRecentInvoices(fromDate);

      if (recentInvoices.length === 0) {
        this.logger.log('📋 No recent invoices updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(`📊 Processing ${recentInvoices.length} recent invoices`);

      const invoicesWithDetails =
        await this.enrichInvoicesWithDetails(recentInvoices);
      const savedInvoices =
        await this.saveInvoicesToDatabase(invoicesWithDetails);
      await this.syncInvoicesToLarkBase(savedInvoices);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
      });

      this.logger.log(
        `✅ Recent invoice sync completed: ${invoicesWithDetails.length} invoices processed`,
      );
    } catch (error) {
      this.logger.error(`❌ Recent invoice sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
      });

      throw error;
    }
  }

  // ============================================================================
  // API METHODS - ENHANCED WITH BETTER TIMEOUT & RETRY LOGIC
  // ============================================================================

  async fetchInvoicesListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      orderBy?: string;
      orderDirection?: string;
      includeInvoiceDelivery?: boolean;
      includePayment?: boolean;
      includeTotal?: boolean;
    },
    maxRetries: number = 5, // Keep existing retry count
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchInvoicesList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          this.logger.log(`⏳ Retrying after ${delay / 1000}s delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchInvoicesList(params: {
    currentItem?: number;
    pageSize?: number;
    orderBy?: string;
    orderDirection?: string;
    includeInvoiceDelivery?: boolean;
    includePayment?: boolean;
    includeTotal?: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      orderBy: params.orderBy || 'createdDate',
      orderDirection: params.orderDirection || 'DESC',
      includeInvoiceDelivery: (
        params.includeInvoiceDelivery || true
      ).toString(),
      includePayment: (params.includePayment || true).toString(),
      includeTotal: (params.includeTotal || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/invoices?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  async fetchRecentInvoices(fromDate: Date): Promise<KiotVietInvoice[]> {
    const headers = await this.authService.getRequestHeaders();
    const fromDateStr = fromDate.toISOString();

    const queryParams = new URLSearchParams({
      lastModifiedFrom: fromDateStr,
      currentItem: '0',
      pageSize: '100',
      orderBy: 'purchaseDate',
      orderDirection: 'DESC',
      includeInvoiceDelivery: 'true',
      includePayment: 'true',
      includeTotal: 'true',
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/invoices?${queryParams}`, {
        headers,
        timeout: 60000,
      }),
    );

    return response.data?.data || [];
  }

  private async enrichInvoicesWithDetails(
    invoices: KiotVietInvoice[],
  ): Promise<KiotVietInvoice[]> {
    this.logger.log(`🔍 Enriching ${invoices.length} invoices with details...`);

    const enrichedInvoices: any[] = [];
    for (const invoice of invoices) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/invoices/${invoice.id}`, {
            headers,
          }),
        );

        if (response.data) {
          enrichedInvoices.push(response.data);
        } else {
          enrichedInvoices.push(invoice);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich invoice ${invoice.code}: ${error.message}`,
        );
        enrichedInvoices.push(invoice);
      }
    }

    return enrichedInvoices;
  }

  private async saveInvoicesToDatabase(invoices: any[]): Promise<any[]> {
    this.logger.log(`💾 Saving ${invoices.length} invoices to database...`);

    const savedInvoices: any[] = [];

    for (const invoiceData of invoices) {
      try {
        const customer = invoiceData.customerId
          ? await this.prismaService.customer.findFirst({
              where: { kiotVietId: BigInt(invoiceData.customerId) },
              select: { id: true },
            })
          : null;

        // Branch lookup - SAFE
        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: invoiceData.branchId },
          select: { id: true },
        });

        const soldBy = invoiceData.soldById
          ? await this.prismaService.user.findFirst({
              where: { kiotVietId: BigInt(invoiceData.soldById) },
              select: { kiotVietId: true },
            })
          : null;

        // SaleChannel lookup - SAFE
        const saleChannel = invoiceData.saleChannelId
          ? await this.prismaService.saleChannel.findFirst({
              where: { kiotVietId: invoiceData.saleChannelId },
              select: { id: true },
            })
          : null;

        // Order lookup - SAFE
        const order = invoiceData.orderId
          ? await this.prismaService.order.findFirst({
              where: { kiotVietId: BigInt(invoiceData.orderId) },
              select: { id: true },
            })
          : null;

        const invoice = await this.prismaService.invoice.upsert({
          where: { kiotVietId: BigInt(invoiceData.id) },
          update: {
            code: invoiceData.code,
            purchaseDate: new Date(invoiceData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            customerId: customer?.id ?? null,
            customerCode: invoiceData.customerCode || null,
            customerName: invoiceData.customerName || null,
            orderId: order?.id ?? null,
            orderCode: invoiceData.orderCode || null,
            total: new Prisma.Decimal(invoiceData.total || 0),
            totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
            discount: invoiceData.discount
              ? new Prisma.Decimal(invoiceData.discount)
              : null,
            discountRatio: invoiceData.discountRatio || null,
            status: invoiceData.status,
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
            larkRecordId: null,
            larkSyncStatus: 'PENDING' as const,
          },
          create: {
            kiotVietId: BigInt(invoiceData.id),
            code: invoiceData.code,
            purchaseDate: new Date(invoiceData.purchaseDate),
            branchId: branch?.id ?? null,
            soldById: soldBy?.kiotVietId ?? null,
            customerId: customer?.id ?? null,
            customerCode: invoiceData.customerCode || null,
            customerName: invoiceData.customerName || null,
            orderId: order?.id ?? null,
            orderCode: invoiceData.orderCode || null,
            total: new Prisma.Decimal(invoiceData.total || 0),
            totalPayment: new Prisma.Decimal(invoiceData.totalPayment || 0),
            discount: invoiceData.discount
              ? new Prisma.Decimal(invoiceData.discount)
              : null,
            discountRatio: invoiceData.discountRatio || null,
            status: invoiceData.status,
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
            larkSyncStatus: 'PENDING',
          },
        });

        // ============================================================================
        // SAVE INVOICE DETAILS
        // ============================================================================
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
                  invoiceId: invoice.id,
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

        // ============================================================================
        // SAVE INVOICE DELIVERY
        // ============================================================================
        if (
          invoiceData.invoiceDelivery &&
          invoiceData.invoiceDelivery.length > 0
        ) {
          for (const detail of invoiceData.invoiceDelivery) {
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
                partnerDeliveryId: detail.partnerDeliveryId
                  ? BigInt(detail.partnerDeliveryId)
                  : null,
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
                partnerDeliveryId: detail.partnerDeliveryId
                  ? BigInt(detail.partnerDeliveryId)
                  : null,
              },
            });
          }
        }

        if (invoiceData.payments && invoiceData.payments.length > 0) {
          for (const payment of invoiceData.payments) {
            const bankAccount = payment.accountId
              ? await this.prismaService.bankAccount.findFirst({
                  where: { kiotVietId: payment.accountId },
                  select: { id: true },
                })
              : null;

            await this.prismaService.payment.upsert({
              where: {
                kiotVietId: payment.id ? BigInt(payment.id) : BigInt(0),
              },
              update: {
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount),
                method: payment.method,
                status: payment.status,
                transDate: new Date(payment.transDate),
                accountId: bankAccount?.id ?? null,
                description: payment.description,
                invoiceId: invoice.id,
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
                createdDate: new Date(),
              },
            });
          }
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

  private async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${invoices.length} invoices...`,
      );

      const invoicesToSync = invoices.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (invoicesToSync.length === 0) {
        this.logger.log('No invoices need LarkBase sync');
        return;
      }

      await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoicesToSync);
      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase sync FAILED: ${error.message}`);
      this.logger.error(`🛑 STOPPING sync to prevent data duplication`);

      const invoiceIds = invoices.map((c) => c.id);
      await this.prismaService.invoice.updateMany({
        where: { id: { in: invoiceIds } },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncedAt: new Date(),
        },
      });

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

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
