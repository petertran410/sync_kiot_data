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
    let lastValidTotal = 0;
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
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalReturns > 0) {
          if (currentItem >= totalReturns) {
            this.logger.log(
              `✅ Pagination complete. Processed ${processedCount}/${totalReturns} returns`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalReturns) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalReturns} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );

          try {
            const response = await this.fetchReturnsWithRetry({
              pageSize: this.PAGE_SIZE,
              currentItem,
              orderBy: 'createdBy',
              orderDirection: 'DESC',
              includePayment: true,
            });

            if (!response) {
              consecutiveEmptyPages++;
              this.logger.warn('⚠️ Received null response from KiotViet API');

              if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
                this.logger.log(
                  `🔚 Reached end after ${consecutiveEmptyPages} empty pages`,
                );
                break;
              }

              await new Promise((resolve) =>
                setTimeout(resolve, RETRY_DELAY_MS),
              );
              continue;
            }

            consecutiveEmptyPages = 0;
            consecutiveErrorPages = 0;

            const { total, data: returns } = response;

            if (total !== undefined && total !== null) {
              if (totalReturns === 0) {
                totalReturns = total;
                this.logger.log(`📊 Total orders detected: ${totalReturns}`);
              } else if (total !== totalReturns) {
                this.logger.warn(
                  `⚠️ Total count changed: ${totalReturns} -> ${total}. Using latest.`,
                );
                totalReturns = total;
              }
              lastValidTotal = total;
            }

            if (!returns || returns.length === 0) {
              this.logger.warn(
                `⚠️ Empty page received at position ${currentItem}`,
              );

              consecutiveEmptyPages++;

              if (totalReturns > 0 && currentItem >= totalReturns) {
                this.logger.log(
                  '✅ Reached end of data (empty page past total)',
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

            const uniqueReturns = returns.filter(
              (returnData: KiotVietReturn) => {
                if (processedReturnIds.has(returnData.id)) {
                  this.logger.debug(
                    `⚠️ Duplicate return ID detected: ${returnData.id} (${returnData.code})`,
                  );
                  return false;
                }
                processedReturnIds.add(returnData.id);
                return true;
              },
            );

            if (uniqueReturns.length === returns.length) {
              this.logger.warn(
                `🔄 Filtered out ${returns.length - uniqueReturns.length} duplicate returns on page ${currentPage}`,
              );
            }

            if (uniqueReturns.length === 0) {
              this.logger.log(
                `⏭️ Skipping page ${currentPage} - all returns already processed`,
              );
              currentItem += this.PAGE_SIZE;
              continue;
            }

            this.logger.log(
              `🔄 Processing ${uniqueReturns.length} returns from page ${currentPage}...`,
            );

            const returnsWithDetails =
              await this.enrichReturnsWithDetails(uniqueReturns);
            const savedReturns =
              await this.saveReturnsToDatabase(returnsWithDetails);

            processedCount += savedReturns.length;
            currentItem += this.PAGE_SIZE;

            if (totalReturns > 0) {
              const completionPercentage =
                (processedCount / totalReturns) * 100;
              this.logger.log(
                `📈 Progress: ${processedCount}/${totalReturns} (${completionPercentage.toFixed(1)}%)`,
              );

              if (processedCount >= totalReturns) {
                this.logger.log('🎉 All returns processed successfully!');
                break;
              }
            }

            if (totalReturns > 0) {
              if (
                currentItem >= totalReturns &&
                processedCount >= totalReturns * 0.95
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
            totalReturns++;

            this.logger.error(
              `❌ API error on page ${currentPage}: ${error.message}`,
            );

            if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
              throw new Error(`Too many consecutive errors: ${error.message}`);
            }

            if (totalRetries >= MAX_TOTAL_RETRIES) {
              throw new Error(
                `Maximum total retries exceeded: ${error.message}`,
              );
            }

            const delay =
              RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
            this.logger.log(`⏳ Retrying after ${delay}ms delay...`);
            await new Promise((resolve) => setTimeout(resolve, delay));
          }
        }

        await this.updateSyncControl(syncName, {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: { processedCount, expectedTotal: totalReturns },
        });

        await this.updateSyncControl('return_historical', {
          isEnabled: false,
          isRunning: false,
          status: 'idle',
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
        return await this.fetchReturnsList(params);
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

  private async fetchReturnsList(params: {
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

    const enrichedReturns: any[] = [];
    for (const returnData of returns) {
      try {
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/returns/${returnData.id}`, {
            headers,
          }),
        );
        if (response.data) {
          enrichedReturns.push(response.data);
        } else {
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
        // const [invoice, branch, customer] = await Promise.all([
        //   returnData.invoiceId
        //     ? this.prismaService.invoice.findFirst({
        //         where: { kiotVietId: BigInt(returnData.invoiceId) },
        //         select: { id: true, code: true },
        //       })
        //     : Promise.resolve(null),
        //   this.prismaService.branch.findFirst({
        //     where: { kiotVietId: returnData.branchId },
        //     select: { id: true, name: true },
        //   }),
        //   returnData.customerId
        //     ? this.prismaService.customer.findFirst({
        //         where: { kiotVietId: BigInt(returnData.customerId) },
        //         select: { id: true, code: true, name: true },
        //       })
        //     : Promise.resolve(null),
        // ]);

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
            invoiceId: invoice?.id || null,
            invoiceCode: invoice?.code,
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
            statusValue: returnData.statusValue,
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
            invoiceId: invoice?.id || null,
            invoiceCode: invoice?.code,
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
            statusValue: returnData.statusValue,
            createdDate: returnData.createdDate
              ? new Date(returnData.createdDate)
              : new Date(),
            modifiedDate: returnData.modifiedDate
              ? new Date(returnData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedReturns.push(returnRecord);

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

            // const acsNumber: number = i + 1;

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
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save return ${returnData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`💾 Saved ${savedReturns.length} returns to database`);
    return savedReturns;
  }

  private async saveReturnPayments(
    returnId: number,
    payments: Array<{
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
    }>,
  ): Promise<void> {
    try {
      for (const payment of payments) {
        // Find bank account if exists
        const bankAccount = payment.accountId
          ? await this.prismaService.bankAccount.findFirst({
              where: { kiotVietId: payment.accountId },
              select: { id: true },
            })
          : null;

        await this.prismaService.payment.upsert({
          where: {
            kiotVietId: BigInt(payment.id),
          },
          update: {
            code: payment.code,
            amount: new Prisma.Decimal(payment.amount),
            method: payment.method,
            status: payment.status || null,
            statusValue: payment.statusValue || null,
            transDate: new Date(payment.transDate),
            accountId: bankAccount?.id || null,
            bankAccountInfo: payment.bankAccount || null,
            description: payment.description || null,
            returnId: returnId,
          },
          create: {
            kiotVietId: BigInt(payment.id),
            returnId: returnId,
            code: payment.code,
            amount: new Prisma.Decimal(payment.amount),
            method: payment.method,
            status: payment.status || null,
            statusValue: payment.statusValue || null,
            transDate: new Date(payment.transDate),
            accountId: bankAccount?.id || null,
            bankAccountInfo: payment.bankAccount || null,
            description: payment.description || null,
          },
        });
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to save return payments for return ${returnId}: ${error.message}`,
      );
    }
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

  // ============================================================================
  // LEGACY METHODS (for backward compatibility)
  // ============================================================================

  async fetchReturns(params?: any) {
    return this.fetchReturnsWithRetry(params || {});
  }

  async syncReturns(): Promise<void> {
    this.logger.warn(
      '⚠️ Using legacy syncReturns method. Consider using syncHistoricalReturns instead.',
    );
    return this.syncHistoricalReturns();
  }
}
