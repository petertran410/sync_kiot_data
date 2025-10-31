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
      const runningReturnSyncs = await this.prismaService.syncControl.findMany({
        where: { OR: [{ name: 'return_historical' }], isRunning: true },
      });

      if (runningReturnSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningReturnSyncs.length} Return syncs still running: ${runningReturnSyncs.map((r) => r.name).join(', ')}`,
        );
        this.logger.warn('Skipping return sync to avoid conflicts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'return_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical return sync');
        await this.syncHistoricalReturns();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log(
          'Historical return sync is running, skipping recent sync',
        );
      }
      // this.logger.log('Running default historical return sync...');
      // await this.syncHistoricalReturns();
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

    this.logger.log('Historical return sync enabled');
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

      this.logger.log('Starting historical return sync...');

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
              `Pagination complete. Processed: ${processedCount}/${totalReturns} returns`,
            );
            break;
          }

          const progressPercentage = (processedCount / totalReturns) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalReturns} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        const dateEnd = new Date();
        dateEnd.setDate(dateEnd.getDate() + 1);
        const dateEndStr = dateEnd.toISOString().split('T')[0];

        try {
          const returnListResponse = await this.fetchReturnsWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            orderBy: 'id',
            orderDirection: 'DESC',
            includePayment: true,
            fromReturnDate: '2024-12-01',
            toReturnDate: dateEndStr,
          });

          if (!returnListResponse) {
            this.logger.warn('Received null response from KiotViet API');
            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `API returned null ${consecutiveEmptyPages} times - ending pagination`,
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
              this.logger.log(`Total returns detected: ${totalReturns}`);

              totalReturns = total;
            } else if (total! == totalReturns) {
              this.logger.warn(
                `Total count updated: ${totalReturns} → ${total}. Using latest`,
              );
              totalReturns = total;
            }
            lastValidTotal = total;
          }

          if (!returns || returns.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalReturns > 0 && processedCount >= totalReturns) {
              this.logger.log('Reached end of data (empty page past total)');
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

          const existingReturnIds = new Set(
            (
              await this.prismaService.return.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newReturns = returns.filter((returnData) => {
            if (
              !existingReturnIds.has(returnData.id) &&
              !processedReturnIds.has(returnData.id)
            ) {
              processedReturnIds.add(returnData.id);
              return true;
            }
            return false;
          });

          const existingReturns = returns.filter((returnData) => {
            if (
              existingReturnIds.has(returnData.id) &&
              !processedReturnIds.has(returnData.id)
            ) {
              processedReturnIds.add(returnData.id);
              return true;
            }
            return false;
          });

          if (newReturns.length === 0 && existingReturns.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all returns already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedReturns: any[] = [];

          if (newReturns.length > 0) {
            this.logger.log(
              `Processing ${newReturns} NEW returns from page ${currentPage}...`,
            );

            const savedReturns = await this.saveReturnsToDatabase(newReturns);
            pageProcessedCount += savedReturns.length;
            allSavedReturns.push(...savedReturns);
          }

          if (existingReturns.length > 0) {
            this.logger.log(
              `Processing ${newReturns} EXISTING returns from page ${currentPage}...`,
            );

            const savedReturns =
              await this.saveReturnsToDatabase(existingReturns);
            pageProcessedCount += savedReturns.length;
            allSavedReturns.push(...savedReturns);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (totalReturns > 0) {
            const completionPercentage = (processedCount / totalReturns) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalReturns} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalReturns) {
              this.logger.log('All returns processed successfully');
              break;
            }
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `API error on page ${currentPage}: ${error.message}`,
          );

          if (consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES) {
            throw new Error(
              `Multiple consecutive API failures: ${error.message}`,
            );
          }

          if (totalRetries >= MAX_TOTAL_RETRIES) {
            throw new Error(`Maximum total retries exceeded: ${error.message}`);
          }

          const delay = RETRY_DELAY_MS * Math.pow(2, consecutiveErrorPages - 1);
          this.logger.log(`Retrying after ${delay}ms delay...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        await this.updateSyncControl(syncName, {
          isRunning: false,
          isEnabled: false,
          status: 'completed',
          completedAt: new Date(),
          lastRunAt: new Date(),
          progress: { processedCount, expectedTotal: totalReturns },
        });

        const completionRate =
          totalReturns > 0 ? (processedCount / totalReturns) * 100 : 100;

        this.logger.log(
          `Historical return sync completed: ${processedCount}/${totalReturns} (${completionRate.toFixed(1)}% completion rate)`,
        );
      }
    } catch (error) {
      this.logger.error(`Historical return sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalReturns },
      });

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
      // lastModifiedFrom?: string;
      orderBy?: string;
      orderDirection?: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchReturnsList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          this.logger.log(`Retrying after ${delay / 1000}s delay...`);
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
    // lastModifiedFrom?: string;
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

    // if (params.lastModifiedFrom) {
    //   queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    // }

    if (params.orderBy) {
      queryParams.append('orderBy', params.orderBy);
      queryParams.append('orderDirection', params.orderDirection || 'ASC');
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/returns?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async saveReturnsToDatabase(returns: any[]): Promise<any[]> {
    this.logger.log(`Saving ${returns.length} returns to database...`);

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

    this.logger.log(`Saved ${savedReturns.length} returns to database`);
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
