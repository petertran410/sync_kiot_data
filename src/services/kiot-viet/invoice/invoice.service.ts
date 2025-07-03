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
    let processedInvoiceIds = new Set<number>(); // Track processed IDs to avoid duplicates

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical invoice sync...');

      // COMPLETION DETECTION with more flexible thresholds
      const MAX_CONSECUTIVE_EMPTY_PAGES = 5; // Increased from 3
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000; // 2 seconds delay between retries
      const MAX_TOTAL_RETRIES = 10; // Total retries allowed across the entire sync

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
                `❌ Received ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses. Trying final validation...`,
              );

              // Try to validate with current data before failing
              if (processedCount > 0) {
                this.logger.log(
                  `✅ Partial sync completed with ${processedCount} invoices processed`,
                );
                break;
              } else {
                throw new Error(
                  `API returned ${MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty responses with no data processed`,
                );
              }
            }

            // Wait before retrying
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          // Reset error counters on successful response
          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          // VALIDATION: Check data structure
          const { total, data: invoices } = invoiceListResponse;

          if (total !== undefined && total !== null) {
            totalInvoices = total;
            lastValidTotal = total;
          } else if (lastValidTotal > 0) {
            totalInvoices = lastValidTotal;
          }

          this.logger.log(`📊 Total invoices in system: ${totalInvoices}`);

          // Handle empty data array
          if (!invoices || invoices.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received. Count: ${consecutiveEmptyPages + 1}`,
            );
            consecutiveEmptyPages++;

            // More flexible empty page handling
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              const progressPercentage =
                totalInvoices > 0 ? (processedCount / totalInvoices) * 100 : 0;

              if (progressPercentage >= 95) {
                this.logger.log(
                  `✅ Sync nearly complete (${progressPercentage.toFixed(1)}%). Ending gracefully.`,
                );
                break;
              } else if (processedCount > 0) {
                this.logger.log(
                  `⚠️ Partial completion (${progressPercentage.toFixed(1)}%). Ending with partial data.`,
                );
                break;
              } else {
                throw new Error(
                  `Too many empty pages with minimal progress: ${processedCount}/${totalInvoices}`,
                );
              }
            }

            // Smart pagination increment on empty pages
            if (currentItem < totalInvoices * 0.9) {
              // Only skip if we're not near the end
              currentItem += this.PAGE_SIZE;
            }
            continue;
          }

          // Handle duplicate detection at page level
          const newInvoices = invoices.filter(
            (invoice) => !processedInvoiceIds.has(invoice.id),
          );
          const duplicateCount = invoices.length - newInvoices.length;

          if (duplicateCount > 0) {
            this.logger.warn(
              `⚠️ Found ${duplicateCount} duplicate invoices in page ${currentPage}. Processing ${newInvoices.length} new invoices.`,
            );
          }

          if (newInvoices.length === 0) {
            this.logger.warn(
              `⚠️ All invoices in current page already processed. Skipping...`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Add to processed set
          newInvoices.forEach((invoice) => processedInvoiceIds.add(invoice.id));

          this.logger.log(
            `📊 Processing ${newInvoices.length} invoices (Page: ${currentPage}, Processed: ${processedCount}/${totalInvoices})`,
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

          // Progress tracking
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

          // Rate limiting
          await new Promise((resolve) => setTimeout(resolve, 1000));
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
        this.logger.log('📋 No recent invoice updates found');
        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
        });
        return;
      }

      this.logger.log(
        `🔄 Processing ${recentInvoices.length} recent invoices...`,
      );

      // Enrich with details
      const enrichedInvoices =
        await this.enrichInvoicesWithDetails(recentInvoices);

      // Save to database
      const savedInvoices = await this.saveInvoicesToDatabase(enrichedInvoices);

      // Sync to LarkBase
      await this.syncInvoicesToLarkBase(savedInvoices);

      const totalProcessed = savedInvoices.length;
      const duplicatesRemoved = recentInvoices.length - totalProcessed;

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { totalProcessed, duplicatesRemoved },
      });

      this.logger.log(
        `✅ Recent sync completed: ${recentInvoices.length} invoices processed`,
      );
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

  // ============================================================================
  // API METHODS with Retry Logic - NEW ENHANCED FUNCTIONS
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
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined; // Initialize as undefined

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchInvoicesList(params);
      } catch (error) {
        lastError = error as Error; // Cast to Error type
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 1000 * attempt; // Progressive delay
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError; // Now guaranteed to be defined
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
      orderBy: params.orderBy || 'id',
      orderDirection: params.orderDirection || 'DESC',
      includeInvoiceDelivery: (
        params.includeInvoiceDelivery || true
      ).toString(),
      includePayment: (params.includePayment || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/invoices?${queryParams}`, {
        headers,
        timeout: 30000, // Increased timeout
      }),
    );

    return response.data;
  }

  // ============================================================================
  // KIOTVIET API METHODS - EXISTING FUNCTIONS ENHANCED
  // ============================================================================

  private async fetchRecentInvoices(fromDate: Date): Promise<any[]> {
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
