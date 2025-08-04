// src/services/kiot-viet/return/return.service.ts - Replace existing file

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

// ============================================================================
// INTERFACES
// ============================================================================
interface KiotVietReturn {
  id: number;
  code: string;
  invoiceId?: number;
  returnDate: string;
  branchId: number;
  branchName: string;
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
  private readonly MAX_RETRIES = 3;
  private readonly MAX_EMPTY_PAGES = 3;
  private readonly MAX_ERROR_PAGES = 5;

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

  // ============================================================================
  // MAIN HISTORICAL SYNC METHOD
  // ============================================================================

  async syncHistoricalReturns(): Promise<void> {
    const syncName = 'return_historical';

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        progress: { processedCount: 0 },
      });

      this.logger.log('🚀 Starting historical return sync...');

      let currentItem = 0;
      let processedCount = 0;
      let totalReturns = 0;
      let consecutiveEmptyPages = 0;
      let consecutiveErrorPages = 0;
      let processedReturnIds = new Set<number>();

      while (
        consecutiveEmptyPages < this.MAX_EMPTY_PAGES &&
        consecutiveErrorPages < this.MAX_ERROR_PAGES
      ) {
        try {
          this.logger.log(
            `📄 Fetching page ${Math.floor(currentItem / this.PAGE_SIZE) + 1} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchReturnsWithRetry({
            pageSize: this.PAGE_SIZE,
            currentItem,
            includePayment: true,
          });

          if (!response?.data || response.data.length === 0) {
            consecutiveEmptyPages++;
            this.logger.log(
              `📄 Empty page ${consecutiveEmptyPages}/${this.MAX_EMPTY_PAGES}`,
            );

            if (consecutiveEmptyPages >= this.MAX_EMPTY_PAGES) {
              this.logger.log('📄 No more data, completing sync');
              break;
            }

            currentItem += this.PAGE_SIZE;
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const returns = response.data;
          totalReturns = response.total || returns.length;

          // Filter out duplicates
          const uniqueReturns = returns.filter((returnData: KiotVietReturn) => {
            if (processedReturnIds.has(returnData.id)) {
              this.logger.debug(
                `⚠️ Duplicate return ID detected: ${returnData.id} (${returnData.code})`,
              );
              return false;
            }
            processedReturnIds.add(returnData.id);
            return true;
          });

          if (uniqueReturns.length === 0) {
            this.logger.log('📄 No unique returns found in this page');
            currentItem += this.PAGE_SIZE;
            continue;
          }

          // Validate and save to database
          const validReturns = this.validateReturns(uniqueReturns);
          const savedReturns = await this.saveReturnsToDatabase(validReturns);

          processedCount += savedReturns.length;

          // Update progress
          await this.updateSyncControl(syncName, {
            progress: { processedCount, expectedTotal: totalReturns },
          });

          this.logger.log(
            `✅ Page processed: ${savedReturns.length}/${uniqueReturns.length} returns saved. Total: ${processedCount}`,
          );

          currentItem += this.PAGE_SIZE;

          // Rate limiting
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          this.logger.error(
            `❌ Page ${Math.floor(currentItem / this.PAGE_SIZE) + 1} failed (attempt ${consecutiveErrorPages}/${this.MAX_ERROR_PAGES}): ${error.message}`,
          );

          if (consecutiveErrorPages >= this.MAX_ERROR_PAGES) {
            throw new Error(`Too many consecutive errors: ${error.message}`);
          }

          // Wait before retry
          await new Promise((resolve) =>
            setTimeout(resolve, 2000 * consecutiveErrorPages),
          );
          currentItem += this.PAGE_SIZE;
        }
      }

      const completionRate =
        totalReturns > 0
          ? Math.round((processedCount / totalReturns) * 100)
          : 100;

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        progress: { processedCount, expectedTotal: totalReturns },
      });

      this.logger.log(
        `✅ Historical return sync completed: ${processedCount}/${totalReturns} (${completionRate}% completion rate)`,
      );
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

  // ============================================================================
  // API METHODS WITH RETRY
  // ============================================================================

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
        return await this.fetchReturnsFromAPI(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delayMs = 1000 * Math.pow(2, attempt - 1);
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError;
  }

  private async fetchReturnsFromAPI(params: {
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

  // ============================================================================
  // VALIDATION
  // ============================================================================

  private validateReturns(returns: KiotVietReturn[]): KiotVietReturn[] {
    const validReturns: KiotVietReturn[] = [];

    for (const returnData of returns) {
      if (this.validateReturnData(returnData)) {
        validReturns.push(returnData);
      }
    }

    this.logger.log(
      `✅ Validation complete: ${validReturns.length}/${returns.length} returns valid`,
    );

    return validReturns;
  }

  private validateReturnData(returnData: KiotVietReturn): boolean {
    // Required field validation
    if (!returnData.id || typeof returnData.id !== 'number') {
      this.logger.warn(`⚠️ Invalid return ID: ${returnData.id}`);
      return false;
    }

    if (
      !returnData.code ||
      typeof returnData.code !== 'string' ||
      returnData.code.trim() === ''
    ) {
      this.logger.warn(
        `⚠️ Invalid return code for ID ${returnData.id}: '${returnData.code}'`,
      );
      return false;
    }

    if (!returnData.returnDate) {
      this.logger.warn(`⚠️ Missing return date for ID ${returnData.id}`);
      return false;
    }

    if (!returnData.branchId || typeof returnData.branchId !== 'number') {
      this.logger.warn(
        `⚠️ Invalid branch ID for return ${returnData.id}: ${returnData.branchId}`,
      );
      return false;
    }

    // Amount validation
    if (
      returnData.returnTotal === null ||
      returnData.returnTotal === undefined ||
      typeof returnData.returnTotal !== 'number'
    ) {
      this.logger.warn(
        `⚠️ Invalid return total for return ${returnData.id}: ${returnData.returnTotal}`,
      );
      return false;
    }

    return true;
  }

  // ============================================================================
  // DATABASE OPERATIONS
  // ============================================================================

  private async saveReturnsToDatabase(
    returns: KiotVietReturn[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${returns.length} returns to database...`);

    const savedReturns: any[] = [];

    for (const returnData of returns) {
      try {
        const [invoice, branch, customer] = await Promise.all([
          returnData.invoiceId
            ? this.prismaService.invoice.findFirst({
                where: { kiotVietId: BigInt(returnData.invoiceId) },
                select: { id: true, code: true },
              })
            : Promise.resolve(null),
          this.prismaService.branch.findFirst({
            where: { kiotVietId: returnData.branchId },
            select: { id: true, name: true },
          }),
          returnData.customerId
            ? this.prismaService.customer.findFirst({
                where: { kiotVietId: BigInt(returnData.customerId) },
                select: { id: true, code: true, name: true },
              })
            : Promise.resolve(null),
        ]);

        if (!branch) {
          this.logger.warn(
            `⚠️ Branch ${returnData.branchId} not found for return ${returnData.id}`,
          );
          continue;
        }

        const returnRecord = await this.prismaService.return.upsert({
          where: { kiotVietId: BigInt(returnData.id) },
          update: {
            code: returnData.code,
            invoiceId: invoice?.id || null,
            invoiceCode: invoice?.code,
            returnDate: new Date(returnData.returnDate),
            branchId: branch.id,
            branchName: branch.name,
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
            branchId: branch.id,
            branchName: branch.name,
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

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('return_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical return sync enabled');
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
