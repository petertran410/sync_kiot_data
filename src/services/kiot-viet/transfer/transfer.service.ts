import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, firstValueFrom } from 'rxjs';

interface KiotVietTransfer {
  id: number;
  code: string;
  description?: string;
  dispatchedDate?: string;
  fromBranchId?: number;
  isActive?: boolean;
  receivedDate?: string;
  retailerId?: number;
  status?: number;
  toBranchId?: number;
  lastSyncedAt?: string;
  transferDetails?: Array<{
    productId?: number;
    productCode?: string;
    sendQuantity?: number;
    receiveQuantity?: number;
    price?: number;
    sendPrice?: number;
    receivePrice?: number;
  }>;
}

@Injectable()
export class KiotVietTransferService {
  private readonly logger = new Logger(KiotVietTransferService.name);
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
      const runningTransferSyncs =
        await this.prismaService.syncControl.findMany({
          where: {
            OR: [{ name: 'transfer_historical' }],
            isRunning: true,
          },
        });

      if (runningTransferSyncs.length > 0) {
        this.logger.warn(
          `Found ${runningTransferSyncs.length} Transfer syncs still running: ${runningTransferSyncs.map((s) => s.name).join(',')}`,
        );
        this.logger.warn('Skipping transfer sync to avoid conficts');
        return;
      }

      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'transfer_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical transfer sync...');
        await this.syncHistoricalTransfers();
        return;
      }

      if (historicalSync?.isRunning) {
        this.logger.log('Historical transfer sync is running');
        return;
      }

      this.logger.log('Running detailf historical transfer sync...');
      await this.syncHistoricalTransfers();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('transfer_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('Historical transfer sync enabled');
  }

  async syncHistoricalTransfers(): Promise<void> {
    const syncName = 'transfer_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalTransfers = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedTransferIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('Starting historical transfer sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalTransfers > 0) {
          if (currentItem >= totalTransfers) {
            this.logger.log(
              `Pagination complete. Processed ${processedCount}/${totalTransfers} transfers`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalTransfers) * 100;
          this.logger.log(
            `Fetching page ${currentPage} (${currentItem}/${totalTransfers} - ${progressPercentage.toFixed(1)}%)`,
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
          const response = await this.fetchTransfersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            fromReceivedDate: '2024-12-1',
            toReceivedDate: dateEndStr,
            fromTransferDate: '2024-12-1',
            toTransferDate: dateEndStr,
          });

          if (!response) {
            this.logger.warn('Received null response from KiotViet API');

            consecutiveEmptyPages++;

            if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY_PAGES) {
              this.logger.log(
                `Reached end after ${consecutiveEmptyPages} empty pages`,
              );
              break;
            }

            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
            continue;
          }

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { data: transfers, total } = response;

          if (total != undefined && total !== null) {
            if (totalTransfers === 0) {
              this.logger.log(
                `Total transfers detected: ${total}. Starting processing...`,
              );

              totalTransfers = total;
            } else if (total !== totalTransfers) {
              this.logger.warn(
                `Total count changed: ${totalTransfers} -> ${total}. Using latest.`,
              );
              totalTransfers = total;
            }
            lastValidTotal = total;
          }

          if (!transfers || transfers.length === 0) {
            this.logger.warn(`Empty page received at position ${currentItem}`);
            consecutiveEmptyPages++;

            if (totalTransfers > 0 && currentItem >= totalTransfers) {
              this.logger.log('Reached end of data (empty page past total');
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

          const existingTransferIds = new Set(
            (
              await this.prismaService.transfer.findMany({
                select: { kiotVietId: true },
              })
            ).map((c) => Number(c.kiotVietId)),
          );

          const newTransfers = transfers.filter((transfer) => {
            if (
              !existingTransferIds.has(transfer.id) &&
              !processedTransferIds.has(transfer.id)
            ) {
              processedTransferIds.add(transfer.id);
              return true;
            }
            return false;
          });

          const existingTransfers = transfers.filter((transfer) => {
            if (
              existingTransferIds.has(transfer.id) &&
              !processedTransferIds.has(transfer.id)
            ) {
              processedTransferIds.add(transfer.id);
              return true;
            }
            return false;
          });

          if (newTransfers.length === 0 && existingTransfers.length === 0) {
            this.logger.log(
              `Skipping page ${currentPage} - all transfers already processed in this run`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          let pageProcessedCount = 0;
          let allSavedTransfers: any[] = [];

          if (newTransfers.length > 0) {
            this.logger.log(
              `Processing ${newTransfers.length} NEW transfers from page ${currentPage}...`,
            );

            const savedTransfers =
              await this.saveTransfersToDatabase(newTransfers);
            pageProcessedCount += savedTransfers.length;
            allSavedTransfers.push(...savedTransfers);
          }

          if (existingTransfers.length > 0) {
            this.logger.log(
              `Processing ${existingTransfers.length} EXISTING transfers from page ${currentPage}...`,
            );

            const savedTransfers =
              await this.saveTransfersToDatabase(existingTransfers);
            pageProcessedCount += savedTransfers.length;
            allSavedTransfers.push(...savedTransfers);
          }

          processedCount += pageProcessedCount;
          currentItem += this.PAGE_SIZE;

          if (totalTransfers > 0) {
            const completionPercentage =
              (processedCount / totalTransfers) * 100;
            this.logger.log(
              `Progress: ${processedCount}/${totalTransfers} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalTransfers) {
              this.logger.log('All transfers processed successfully');
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
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalTransfers },
      });

      const completionRate =
        totalTransfers > 0 ? (processedCount / totalTransfers) * 100 : 100;

      this.logger.log(
        `Historical transfer sync completed: ${processedCount}/${totalTransfers} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.log(`Historical transfer sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalTransfers },
      });

      throw error;
    }
  }

  async fetchTransfersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      fromReceivedDate: string;
      toReceivedDate: string;
      fromTransferDate: string;
      toTransferDate: string;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchTransfersList(params);
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

  async fetchTransfersList(params: {
    currentItem?: number;
    pageSize?: number;
    fromReceivedDate: string;
    toReceivedDate: string;
    fromTransferDate: string;
    toTransferDate: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
    });

    if (params.fromReceivedDate) {
      queryParams.append('fromReceivedDate', params.fromReceivedDate);
    }

    if (params.toReceivedDate) {
      queryParams.append('toReceivedDate', params.toReceivedDate);
    }

    if (params.fromTransferDate) {
      queryParams.append('fromTransferDate', params.fromTransferDate);
    }

    if (params.toTransferDate) {
      queryParams.append('toTransferDate', params.toTransferDate);
    }

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/transfers?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async saveTransfersToDatabase(
    transfers: KiotVietTransfer[],
  ): Promise<any[]> {
    this.logger.log(`Saving ${transfers.length} transfers to database...`);

    const savedTransfers: any[] = [];

    for (const transferData of transfers) {
      try {
        const branch = await this.prismaService.branch.findFirst({
          where: {
            OR: [
              { kiotVietId: transferData.fromBranchId },
              { kiotVietId: transferData.toBranchId },
            ],
          },
          select: {
            id: true,
            name: true,
          },
        });

        const transfer = await this.prismaService.transfer.upsert({
          where: { kiotVietId: BigInt(transferData.id) },
          update: {
            code: transferData.code.trim(),
            description: transferData.description ?? '',
            dispatchedDate: transferData.dispatchedDate
              ? new Date(transferData.dispatchedDate)
              : new Date(),
            fromBranchId: branch?.id ?? null,
            isActive: transferData.isActive ?? false,
            receivedDate: transferData.receivedDate
              ? new Date(transferData.receivedDate)
              : new Date(),
            retailerId: transferData.retailerId ?? null,
            status: transferData.status ?? null,
            toBranchId: branch?.id ?? null,
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: BigInt(transferData.id),
            code: transferData.code.trim(),
            description: transferData.description ?? '',
            dispatchedDate: transferData.dispatchedDate
              ? new Date(transferData.dispatchedDate)
              : new Date(),
            fromBranchId: branch?.id ?? null,
            isActive: transferData.isActive ?? false,
            receivedDate: transferData.receivedDate
              ? new Date(transferData.receivedDate)
              : new Date(),
            retailerId: transferData.retailerId ?? null,
            status: transferData.status ?? null,
            toBranchId: branch?.id ?? null,
            lastSyncedAt: new Date(),
          },
        });

        if (
          transferData.transferDetails &&
          transferData.transferDetails.length > 0
        ) {
          for (let i = 0; i < transferData.transferDetails.length; i++) {
            const detail = transferData.transferDetails[i];
            const product = await this.prismaService.product.findFirst({
              where: { kiotVietId: detail.productId },
              select: {
                id: true,
                name: true,
                fullName: true,
                code: true,
              },
            });
            await this.prismaService.transferDetail.upsert({
              where: {
                transferId_lineNumber: {
                  transferId: transfer.id,
                  lineNumber: i + 1,
                },
              },
              update: {
                productId: product?.id,
                productCode: product?.code,
                productName: product?.name,
                sendQuantity: detail.sendQuantity ?? 0,
                receivedQuantity: detail.receiveQuantity ?? 0,
                sendPrice: detail.sendPrice ?? 0,
                receivePrice: detail.receivePrice ?? 0,
                price: detail.price ?? 0,
                lineNumber: i + 1,
              },
              create: {
                transferId: transfer.id,
                productId: product?.id,
                productCode: product?.code,
                productName: product?.name,
                sendQuantity: detail.sendQuantity ?? 0,
                receivedQuantity: detail.receiveQuantity ?? 0,
                sendPrice: detail.sendPrice ?? 0,
                receivePrice: detail.receivePrice ?? 0,
                price: detail.price ?? 0,
                lineNumber: i + 1,
              },
            });
          }
        }

        savedTransfers.push(transfer);
      } catch (error) {
        this.logger.error(
          `Failed to save transfer ${transferData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`Saved ${savedTransfers.length} transfers successfully`);
    return savedTransfers;
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['transfer'],
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
