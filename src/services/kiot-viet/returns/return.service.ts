import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

interface KiotVietReturn {
  id: number;
  code: string;
  invoiceId?: number;
  returnDate: string;
  branchId?: number;
  branchName?: string;
  receivedById?: number;
  soldByName: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  returnTotal: number;
  returnDiscount?: number;
  returnFee?: number;
  totalPayment: number;
  status: number;
  statusValue?: string;
  createdDate: string;
  modifiedDate?: string;
  retailerId?: number;
  payments?: Array<{
    id: number;
    code: string;
    amount: number;
    method: string;
    status?: number;
    statusValue?: string;
    transDate: string;
    bankAccount?: string;
    accountId?: number;
    description?: string;
  }>;
  returnDetails?: Array<{
    productId: number;
    lineNumber: number;
    productCode: string;
    productName: string;
    quantity: number;
    price: number;
    note?: string;
    usePoint?: boolean;
    subTotal: number;
  }>;
}

@Injectable()
export class KiotVietReturnService {
  private readonly logger = new Logger(KiotVietReturnService.name);
  private readonly baseUrl: string;
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

  async checkAndRunAppropriateSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'return_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical return sync');
        await this.syncHistoricalReturns();
        return;
      }
      this.logger.log('Running default historical return sync...');
      await this.syncHistoricalReturns();
    } catch (error) {
      this.logger.log(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('return_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical return sync enabled');
  }

  async syncHistoricalReturns(): Promise<void> {
    const syncName = 'return_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalReturns = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let processedReturnIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical return sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalReturns > 0) {
          const progressPercentage = (processedCount / totalReturns) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${processedCount}/${totalReturns} - ${progressPercentage.toFixed(1)}% completed)`,
          );

          if (processedCount >= totalReturns) {
            this.logger.log(
              `✅ All returns processed successfully! Final count: ${processedCount}/${totalReturns}`,
            );
            break;
          }
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        try {
          const returnListResponse = await this.fetchReturnsWithRetry({
            orderBy: 'createdDate',
            orderDirection: 'DESC',
            pageSize: this.PAGE_SIZE,
            currentItem,
            includePayment: true,
          });

          if (!returnListResponse) {
            this.logger.warn('⚠️ Received null response from KiotViet API');
            consecutiveEmptyPages++;
            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 API returned null ${consecutiveEmptyPages} times - ending pagination`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            currentItem += this.PAGE_SIZE;
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: returns } = returnListResponse;

          if (total !== undefined && total !== null) {
            if (totalReturns === 0) {
              totalReturns = total;
              this.logger.log(`📊 Total categories detected: ${totalReturns}`);
            } else if (total! == totalReturns) {
              this.logger.warn(
                `⚠️ Total count updated: ${totalReturns} → ${total}`,
              );
              totalReturns = total;
            }
          }

          if (!returns || returns.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalReturns > 0 && processedCount >= totalReturns) {
              this.logger.log(
                '✅ All expected returns processed - pagination complete',
              );
              break;
            }

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `🔚 Stopping after ${consecutiveEmptyPages} consecutive empty pages`,
              );
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          const newReturns = returns.filter((returnData) => {
            if (!returnData.id || !returnData.code) {
              this.logger.warn(
                `⚠️ Skipping invalid return: id=${returnData.id}, code='${returnData.code}'`,
              );
              return false;
            }

            if (processedReturnIds.has(returnData.id)) {
              this.logger.debug(
                `⚠️ Duplicate return ID detected: ${returnData.id} (${returnData.code})`,
              );
              return false;
            }

            processedReturnIds.add(returnData.id);
            return true;
          });

          if (newReturns.length !== returns.length) {
            this.logger.warn(
              `🔄 Filtered out ${returns.length - newReturns.length} invalid/duplicate returns on page ${currentPage}`,
            );
          }

          if (newReturns.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all returns were filtered out`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newReturns.length} returns from page ${currentPage}...`,
          );

          const returnsWithDetails =
            await this.enrichReturnsWithDetails(newReturns);
          const savedReturns =
            await this.saveReturnsToDatabase(returnsWithDetails);

          processedCount += savedReturns.length;

          if (totalReturns > 0) {
            const completionPercentage = (processedCount / totalReturns) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalReturns} (${completionPercentage.toFixed(1)}%)`,
            );
          } else {
            this.logger.log(`📈 Progress: ${processedCount} returns processed`);
          }

          currentItem += this.PAGE_SIZE;

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          this.logger.error(
            `❌ Error fetching page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            this.logger.error(
              `💥 Too many consecutive errors (${consecutiveErrorPages}). Stopping sync.`,
            );
            throw error;
          }

          await new Promise((resolve) =>
            setTimeout(resolve, RETRY_DELAY_MS * consecutiveErrorPages),
          );
        }

        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { processedCount, expectedTotal: totalReturns },
        });

        const completionRate =
          totalReturns > 0 ? (processedCount / totalReturns) * 100 : 100;

        this.logger.log(
          `✅ Historical order sync completed: ${processedCount}/${totalReturns} (${completionRate.toFixed(1)}% completion rate)`,
        );
      }
    } catch (error) {
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      this.logger.error(`❌ Historical return sync failed: ${error.message}`);
      throw error;
    }
  }

  async fetchReturnsWithRetry(
    params: {
      pageSize?: number;
      currentItem?: number;
      includePayment?: boolean;
      fromReturnDate?: string;
      toReturnDate?: string;
      lastModifiedFrom?: string;
      orderBy?: string;
      orderDirection?: string;
    },
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchReturns(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 1000 * Math.pow(2, attempt - 1);
          this.logger.log(`⏳ Retrying after ${delay / 1000}s delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  private async fetchReturns(params: {
    pageSize?: number;
    currentItem?: number;
    includePayment?: boolean;
    fromReturnDate?: string;
    toReturnDate?: string;
    lastModifiedFrom?: string;
    orderBy?: string;
    orderDirection?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
      includePayment: (params.includePayment || true).toString(),
    });

    if (params.fromReturnDate) {
      queryParams.append('fromReturnDate', params.fromReturnDate);
    }

    if (params.toReturnDate) {
      queryParams.append('toReturnDate', params.toReturnDate);
    }

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/returns?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    return response.data;
  }

  private async enrichReturnsWithDetails(
    returns: KiotVietReturn[],
  ): Promise<KiotVietReturn[]> {
    this.logger.log(`🔍 Enriching ${returns.length} returns with details...`);

    const enrichedReturns: KiotVietReturn[] = [];
    for (const returnData of returns) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/returns/${returnData.id}`, {
            headers,
            timeout: 15000,
          }),
        );

        if (response.data && response.data.id) {
          enrichedReturns.push(response.data);
        } else {
          this.logger.warn(
            `⚠️ No detailed data for return ${returnData.id}, using basic data`,
          );
          enrichedReturns.push(returnData);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich return ${returnData.id}: ${error.message}`,
        );
        enrichedReturns.push(returnData);
      }
    }

    return enrichedReturns;
  }

  private async saveReturnsToDatabase(
    returns: KiotVietReturn[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${returns.length} returns to database...`);

    const savedReturns: any[] = [];

    for (const returnData of returns) {
      try {
        const invoice = returnData.invoiceId
          ? await this.prismaService.invoice.findFirst({
              where: { kiotVietId: BigInt(returnData.invoiceId) },
              select: { id: true, code: true },
            })
          : null;

        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: returnData.branchId },
          select: { id: true, name: true },
        });

        const customer = returnData.customerId
          ? await this.prismaService.customer.findFirst({
              where: { kiotVietId: BigInt(returnData.customerId) },
              select: { id: true, name: true, code: true },
            })
          : null;

        const returnRecord = await this.prismaService.return.upsert({
          where: { kiotVietId: BigInt(returnData.id) },
          update: {
            code: returnData.code,
            invoiceId: returnData.invoiceId || null,
            returnDate: new Date(returnData.returnDate),
            branchId: branch?.id,
            branchName: branch?.name,
            receivedById: returnData.receivedById
              ? BigInt(returnData.receivedById)
              : null,
            soldByName: returnData.soldByName,
            customerId: customer?.id || null,
            customerCode: customer?.code,
            customerName: customer?.name,
            returnTotal: new Prisma.Decimal(returnData.returnTotal),
            totalPayment: new Prisma.Decimal(returnData.totalPayment),
            status: returnData.status,
            createdDate: returnData.createdDate
              ? new Date(returnData.createdDate)
              : new Date(),
            modifiedDate: returnData.modifiedDate
              ? new Date(returnData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: BigInt(returnData.id),
            code: returnData.code,
            invoiceId: returnData.invoiceId || null,
            returnDate: new Date(returnData.returnDate),
            branchId: branch?.id,
            branchName: branch?.name,
            receivedById: returnData.receivedById
              ? BigInt(returnData.receivedById)
              : null,
            soldByName: returnData.soldByName,
            customerId: customer?.id || null,
            customerCode: customer?.code,
            customerName: customer?.name,
            returnTotal: new Prisma.Decimal(returnData.returnTotal),
            totalPayment: new Prisma.Decimal(returnData.totalPayment),
            status: returnData.status,
            createdDate: returnData.createdDate
              ? new Date(returnData.createdDate)
              : new Date(),
            modifiedDate: returnData.modifiedDate
              ? new Date(returnData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        if (returnData.returnDetails && returnData.returnDetails.length > 0) {
          for (let i = 0; i < returnData.returnDetails.length; i++) {
            const detail = returnData.returnDetails[i];
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: BigInt(detail.productId) },
              select: {
                id: true,
                code: true,
                name: true,
              },
            });

            if (product) {
              await this.prismaService.returnDetail.upsert({
                where: {
                  returnId_lineNumber: {
                    returnId: returnRecord.id,
                    lineNumber: i + 1,
                  },
                },
                update: {
                  lineNumber: i + 1,
                  productId: product.id,
                  productCode: product.code,
                  productName: product.name,
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  note: detail.note || null,
                  usePoint: detail.usePoint || false,
                  subTotal: new Prisma.Decimal(detail.subTotal),
                },
                create: {
                  returnId: returnRecord.id,
                  lineNumber: i + 1,
                  productId: product.id,
                  productCode: product.code,
                  productName: product.name,
                  quantity: detail.quantity,
                  price: new Prisma.Decimal(detail.price),
                  note: detail.note || null,
                  usePoint: detail.usePoint || false,
                  subTotal: new Prisma.Decimal(detail.subTotal),
                },
              });
            }
          }
        }

        if (returnData.payments && returnData.payments.length > 0) {
          for (const payment of returnData.payments) {
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
                returnId: returnRecord.id,
              },
              create: {
                kiotVietId: payment.id ? BigInt(payment.id) : null,
                code: payment.code,
                amount: new Prisma.Decimal(payment.amount),
                method: payment.method,
                status: payment.status,
                transDate: new Date(payment.transDate),
                accountId: bankAccount?.id ?? null,
                description: payment.description,
                returnId: returnRecord.id,
              },
            });
          }
        }

        savedReturns.push(returnRecord);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save return ${returnData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${savedReturns.length} returns to database`);
    return savedReturns;
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['return'],
          syncMode: 'historical',
          isRunning: false,
          isEnabled: true,
          status: 'idle',
          ...data,
        },
        update: {
          ...data,
          lastRunAt:
            data.status === 'completed' || data.status === 'failed'
              ? new Date()
              : undefined,
        },
      });
    } catch (error) {
      this.logger.error(
        `Failed to update sync control '${name}': ${error.message}`,
      );
      throw error;
    }
  }
}
