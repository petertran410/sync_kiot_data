import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { LarkInvoiceSyncService } from '../../lark/invoice/lark-invoice-sync.service';
import { Prisma } from '@prisma/client';

// Interface definitions remain the same...
interface InvoiceResponse {
  data: any[];
  total: number;
  pageSize: number;
  currentItem: number;
}

interface CreateInvoiceDto {
  branchId: number;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  soldById?: number;
  soldByName?: string;
  description?: string;
  invoiceOrderSurcharges?: Array<{
    id?: number;
    surchargeId?: number;
    surchargeName?: string;
    surValue?: number;
    price?: number;
  }>;
  invoiceDetails: Array<{
    productId: number;
    productCode?: string;
    productName?: string;
    quantity: number;
    price: number;
    discount?: number;
    discountRatio?: number;
  }>;
  invoiceDelivery?: {
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
}

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 50; // ✅ REDUCED: 100 → 50 for better timeout handling

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

      // COMPLETION DETECTION with more flexible thresholds
      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000; // ✅ KEPT: 2 seconds delay
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;
        this.logger.log(
          `📄 Fetching invoices page: ${currentPage} (currentItem: ${currentItem})`,
        );

        try {
          const invoiceListResponse = await this.fetchInvoicesListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id',
            orderDirection: 'ASC',
            includeInvoiceDelivery: true,
            includePayment: true,
            includeTotal: true,
          });

          // VALIDATION: Check response structure
          if (!invoiceListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;
            consecutiveErrorPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.error(
                `❌ Received ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses. Stopping sync.`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Reset consecutive error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // Update total only if we get a valid response with total
          if (
            invoiceListResponse.total &&
            invoiceListResponse.total > lastValidTotal
          ) {
            totalInvoices = invoiceListResponse.total;
            lastValidTotal = totalInvoices;
          }

          const invoices = invoiceListResponse.data || [];

          if (invoices.length === 0) {
            this.logger.log('📋 No more invoices to process');
            break;
          }

          // ✅ ENHANCED: Rate limiting with 2s delay
          await new Promise((resolve) => setTimeout(resolve, 2000));

          // Process and save invoices
          const invoicesWithDetails =
            await this.enrichInvoicesWithDetails(invoices);
          const savedInvoices =
            await this.saveInvoicesToDatabase(invoicesWithDetails);

          processedCount += savedInvoices.length;
          currentItem += invoices.length;

          // Progress logging
          const progressPercentage =
            totalInvoices > 0 ? (processedCount / totalInvoices) * 100 : 0;

          this.logger.log(
            `📈 Progress: ${processedCount}/${totalInvoices} (${progressPercentage.toFixed(1)}%)`,
          );

          // Update progress in sync control
          await this.updateSyncControl(syncName, {
            progress: {
              current: processedCount,
              total: totalInvoices,
              percentage: Math.round(progressPercentage),
            },
          });

          // Dynamic completion check
          if (totalInvoices > 0 && processedCount >= totalInvoices) {
            this.logger.log('🎉 All invoices processed successfully!');
            break;
          }

          // Safety limit to prevent infinite loops
          if (currentItem > totalInvoices * 1.5) {
            this.logger.warn(
              `⚠️ Safety limit reached. Processed: ${processedCount}/${totalInvoices}`,
            );
            break;
          }
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

  async syncRecentInvoices(days: number = 7): Promise<void> {
    const syncName = 'invoice_recent';
    const SYNC_TIMEOUT_MS = 15 * 60 * 1000; // ✅ INCREASED: 5 minutes → 15 minutes

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log(
        `🔄 Starting recent invoice sync (${days} days) with 15min timeout...`,
      );

      // ✅ ENHANCED: Extended timeout protection
      const syncPromise = this.performRecentInvoiceSync(days);
      const timeoutPromise = new Promise<never>((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error(
                `Invoice sync timeout after ${SYNC_TIMEOUT_MS / 60000} minutes`,
              ),
            ),
          SYNC_TIMEOUT_MS,
        ),
      );

      await Promise.race([syncPromise, timeoutPromise]);

      this.logger.log('✅ Recent invoice sync completed successfully');
    } catch (error) {
      this.logger.error(`❌ Recent sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { errorDetails: error.message },
      });

      throw error;
    }
  }

  // ✅ ENHANCED: Optimized core sync logic with better chunking
  private async performRecentInvoiceSync(days: number): Promise<void> {
    const fromDate = new Date();
    fromDate.setDate(fromDate.getDate() - days);

    this.logger.log(
      `📊 Fetching recent invoices since: ${fromDate.toISOString()}`,
    );

    // ✅ NEW: Chunked processing with progress tracking
    const recentInvoices = await this.fetchRecentInvoicesChunked(fromDate);

    if (recentInvoices.length === 0) {
      this.logger.log('📋 No recent invoice updates found');
      await this.updateSyncControl('invoice_recent', {
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

    const totalProcessed = savedInvoices.length;
    const duplicatesRemoved = recentInvoices.length - totalProcessed;

    await this.updateSyncControl('invoice_recent', {
      isRunning: false,
      status: 'completed',
      completedAt: new Date(),
      lastRunAt: new Date(),
      progress: { totalProcessed, duplicatesRemoved },
    });
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
          const delay = 2000 * attempt; // ✅ INCREASED: 1s → 2s progressive delay
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
        timeout: 45000, // ✅ INCREASED: 30s → 45s timeout
      }),
    );

    return response.data;
  }

  // ✅ NEW: Chunked fetching with progress tracking
  async fetchRecentInvoicesChunked(fromDate: Date): Promise<any[]> {
    try {
      const allInvoices: any[] = [];
      let currentItem = 0;
      let hasMoreData = true;
      let pageCount = 0;

      while (hasMoreData) {
        pageCount++;
        this.logger.log(
          `📄 Fetching invoices page ${pageCount} (offset: ${currentItem})`,
        );

        const response = await this.fetchRecentInvoicesPage(
          fromDate,
          currentItem,
        );

        if (!response.data || response.data.length === 0) {
          this.logger.log('📋 No more data available');
          hasMoreData = false;
          break;
        }

        allInvoices.push(...response.data);
        currentItem += response.data.length;

        this.logger.log(
          `📊 Page ${pageCount}: ${response.data.length} invoices (total: ${allInvoices.length})`,
        );

        hasMoreData = response.data.length === this.PAGE_SIZE;

        // ✅ INCREASED: Rate limiting delay 1s → 2s
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }

      this.logger.log(
        `✅ Fetched total ${allInvoices.length} recent invoices in ${pageCount} pages`,
      );
      return allInvoices;
    } catch (error) {
      this.logger.error(`Failed to fetch recent invoices: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // KIOTVIET API METHODS - EXISTING FUNCTIONS ENHANCED
  // ============================================================================

  async fetchRecentInvoices(fromDate: Date): Promise<any[]> {
    try {
      const allInvoices: any[] = [];
      let currentItem = 0;
      let hasMoreData = true;

      while (hasMoreData) {
        const response = await this.fetchRecentInvoicesPage(
          fromDate,
          currentItem,
        );

        if (!response.data || response.data.length === 0) {
          hasMoreData = false;
          break;
        }

        allInvoices.push(...response.data);
        currentItem += response.data.length;

        // Check if more data
        hasMoreData = response.data.length === this.PAGE_SIZE;

        // Rate limiting
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      return allInvoices;
    } catch (error) {
      this.logger.error(`Failed to fetch recent invoices: ${error.message}`);
      throw error;
    }
  }

  private async fetchRecentInvoicesPage(
    fromDate: Date,
    currentItem: number,
  ): Promise<any> {
    try {
      const headers = await this.authService.getRequestHeaders();

      const response = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/invoices`, {
          headers,
          params: {
            pageSize: this.PAGE_SIZE, // Now using 50 instead of 100
            currentItem,
            includeInvoiceDelivery: 'true',
            includePayment: 'true',
            lastModifiedFrom: fromDate.toISOString(),
            orderBy: 'modifiedDate',
            orderDirection: 'DESC',
          },
          timeout: 75000, // ✅ INCREASED: 60s → 75s timeout for recent invoices
        }),
      );
      return response.data;
    } catch (error) {
      this.logger.error(
        `Failed to fetch recent invoices page: ${error.message}`,
      );
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
            soldById: soldBy?.kiotVietId ?? null, // ✅ FIXED: Use kiotVietId, not id
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
            larkSyncStatus: 'PENDING' as const,
          } satisfies Prisma.InvoiceUncheckedCreateInput,
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

        // ============================================================================
        // SAVE INVOICE DELIVERY
        // ============================================================================
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

        // ============================================================================
        // ✅ FIX 2: SAVE PAYMENTS - CORRECTED accountId REFERENCE
        // ============================================================================
        if (invoiceData.payments && invoiceData.payments.length > 0) {
          for (const payment of invoiceData.payments) {
            // ✅ FIXED: Lookup BankAccount by kiotVietId from API data
            const bankAccount = payment.accountId
              ? await this.prismaService.bankAccount.findFirst({
                  where: { kiotVietId: payment.accountId }, // KiotViet API accountId = BankAccount.kiotVietId
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
                accountId: bankAccount?.id ?? null, // ✅ FIXED: Use internal BankAccount.id
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
                accountId: bankAccount?.id ?? null, // ✅ FIXED: Use internal BankAccount.id
                description: payment.description,
              },
            });
          }
        }

        // ============================================================================
        // SAVE INVOICE SURCHARGES (OPTIONAL)
        // ============================================================================
        if (
          invoiceData.invoiceOrderSurcharges &&
          invoiceData.invoiceOrderSurcharges.length > 0
        ) {
          for (const surcharge of invoiceData.invoiceOrderSurcharges) {
            // Lookup surcharge by ID
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
          `Failed to save invoice ${invoiceData.id}: ${error.message}`,
        );
        // Log detailed error for debugging
        this.logger.debug(
          `Invoice data: ${JSON.stringify({
            id: invoiceData.id,
            soldById: invoiceData.soldById,
            customerId: invoiceData.customerId,
            branchId: invoiceData.branchId,
            payments: invoiceData.payments?.map((p) => ({
              id: p.id,
              accountId: p.accountId,
            })),
          })}`,
        );
      }
    }

    return savedInvoices;
  }

  // ============================================================================
  // LARKBASE INTEGRATION
  // ============================================================================

  private async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    if (invoices.length === 0) return;

    try {
      await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoices);
    } catch (error) {
      this.logger.error(`LarkBase sync failed: ${error.message}`);
      // Don't throw error to prevent blocking database sync
    }
  }

  // ============================================================================
  // SYNC CONTROL UTILITIES
  // ============================================================================

  private async updateSyncControl(
    name: string,
    data: Partial<{
      isRunning: boolean;
      isEnabled: boolean;
      status: string;
      error: string | null;
      startedAt: Date;
      completedAt: Date;
      lastRunAt: Date;
      progress: any; // ✅ FIX: Use progress instead of metadata
    }>,
  ): Promise<void> {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['invoice'],
        syncMode: name.includes('historical') ? 'historical' : 'recent',
        status: 'idle',
        ...data,
      },
      update: data,
    });
  }
}
