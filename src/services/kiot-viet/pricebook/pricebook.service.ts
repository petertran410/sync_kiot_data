// src/services/kiot-viet/pricebook/pricebook.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';

interface KiotVietPriceBook {
  id: number;
  kiotVietId: number;
  name: string;
  isActive?: boolean;
  isGlobal?: boolean;
  startDate?: string;
  endDate?: string;
  forAllCusGroup?: boolean;
  forAllUser?: boolean;
  retailerId?: number;
  createdDate?: string;
  modifiedDate?: string;
  priceBookBranches?: Array<{
    id: number;
    priceBookId: number;
    branchId: number;
    branchName?: string;
    retailerId?: number;
  }>;
  priceBookCustomerGroups?: Array<{
    id: number;
    priceBookId: number;
    customerGroupId: number;
    customerGroupName?: string;
    retailerId?: number;
  }>;
  priceBookUsers?: Array<{
    id: number;
    priceBookId: number;
    userId: number;
    userName?: string;
  }>;
}

@Injectable()
export class KiotVietPriceBookService {
  private readonly logger = new Logger(KiotVietPriceBookService.name);
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

  // ============================================================================
  // HISTORICAL SYNC - FIXED VERSION
  // ============================================================================

  async syncHistoricalPriceBooks(): Promise<void> {
    const syncName = 'pricebook_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalPriceBooks = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let processedPricebookIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical pricebook sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalPriceBooks > 0) {
          if (currentItem >= totalPriceBooks) {
            this.logger.log(
              `✅ Pagination complete. Processed: ${processedCount}/${totalPriceBooks} pricebooks`,
            );
            break;
          }

          const progressPercentage = (currentItem / totalPriceBooks) * 100;
          this.logger.log(
            `📄 Fetching page ${currentPage} (${currentItem}/${totalPriceBooks} - ${progressPercentage.toFixed(1)}%)`,
          );
        } else {
          this.logger.log(
            `📄 Fetching page ${currentPage} (currentItem: ${currentItem})`,
          );
        }

        this.logger.log(
          `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})...`,
        );

        try {
          // FIXED: Use correct endpoint for pricebook metadata
          const pricebookListResponse = await this.fetchPriceBookMetadata({
            pageSize: this.PAGE_SIZE,
            currentItem,
          });

          if (!pricebookListResponse) {
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

          consecutiveEmptyPages = 0;
          consecutiveErrorPages = 0;

          const { total, data: pricebooks } = pricebookListResponse;

          if (total !== undefined && total !== null) {
            if (totalPriceBooks === 0) {
              totalPriceBooks = total;
              this.logger.log(
                `📊 Total pricebooks detected: ${totalPriceBooks}`,
              );
            } else if (total !== totalPriceBooks) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalPriceBooks} -> ${total}. Using latest.`,
              );
              totalPriceBooks = total;
            }
          }

          if (!pricebooks || pricebooks.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );
            consecutiveEmptyPages++;

            if (totalPriceBooks > 0 && currentItem >= totalPriceBooks) {
              this.logger.log('✅ Reached end of data (empty page past total)');
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

          const newPricebooks = pricebooks.filter((pricebook) => {
            if (processedPricebookIds.has(pricebook.id)) {
              this.logger.debug(
                `⚠️ Duplicate pricebook ID detected: ${pricebook.id} (${pricebook.name})`,
              );
              return false;
            }
            processedPricebookIds.add(pricebook.id);
            return true;
          });

          if (newPricebooks.length !== pricebooks.length) {
            this.logger.warn(
              `🔄 Filtered out ${pricebooks.length - newPricebooks.length} duplicate pricebooks on page ${currentPage}`,
            );
          }

          if (newPricebooks.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all pricebooks already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newPricebooks.length} pricebooks from page ${currentPage}...`,
          );

          const pricebooksWithDetails =
            await this.enrichPriceBooksWithDetails(newPricebooks);
          const savedPricebooks = await this.savePriceBooksToDatabase(
            pricebooksWithDetails,
          );

          processedCount += savedPricebooks.length;
          currentItem += this.PAGE_SIZE;

          if (totalPriceBooks > 0) {
            const completionPercentage =
              (processedCount / totalPriceBooks) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalPriceBooks} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalPriceBooks) {
              this.logger.log('🎉 All pricebooks processed successfully!');
              break;
            }
          }

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
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'completed',
        completedAt: new Date(),
        error: null,
      });

      this.logger.log(
        `✅ Historical pricebook sync completed: ${processedCount}/${totalPriceBooks} (${totalPriceBooks > 0 ? ((processedCount / totalPriceBooks) * 100).toFixed(1) : 0}% completion rate)`,
      );
    } catch (error) {
      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        completedAt: new Date(),
      });

      this.logger.error(
        `❌ Historical pricebook sync failed: ${error.message}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // FIXED API METHODS - SEPARATE METADATA FROM DETAILS
  // ============================================================================

  /**
   * Fetch pricebook metadata (id, name, isActive, etc.)
   * This is the correct endpoint for pricebook list
   */
  private async fetchPriceBookMetadata(params: {
    pageSize?: number;
    currentItem?: number;
    lastModifiedFrom?: string;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      currentItem: (params.currentItem || 0).toString(),
    });

    if (params.lastModifiedFrom) {
      queryParams.append('lastModifiedFrom', params.lastModifiedFrom);
    }

    // CRITICAL: This endpoint returns pricebook metadata, not pricing data
    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/pricebooks?${queryParams}`, {
        headers,
        timeout: 30000,
      }),
    );

    // ENHANCED: Validate response structure to ensure we're getting metadata
    if (response.data?.data && Array.isArray(response.data.data)) {
      const firstItem = response.data.data[0];
      if (firstItem && firstItem.productId) {
        // ERROR: Receiving pricing data instead of pricebook metadata
        throw new Error(
          'API endpoint mismatch: Received pricing data instead of pricebook metadata. ' +
            'Check if endpoint should be /pricebooks instead of /pricebooks/{id}/details',
        );
      }
    }

    return response.data;
  }

  private async fetchPriceBookDetails(priceBookId: number): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const response = await firstValueFrom(
      this.httpService.get(
        `${this.baseUrl}/pricebooks/${priceBookId}/details`,
        {
          headers,
          timeout: 30000,
        },
      ),
    );

    return response.data;
  }

  // ============================================================================
  // ENRICH WITH DETAILS - FIXED IMPLEMENTATION
  // ============================================================================

  private async enrichPriceBooksWithDetails(
    pricebooks: KiotVietPriceBook[],
  ): Promise<KiotVietPriceBook[]> {
    this.logger.log(
      `🔍 Enriching ${pricebooks.length} pricebooks with details...`,
    );

    const enrichedPricebooks: KiotVietPriceBook[] = [];

    for (const pricebook of pricebooks) {
      try {
        // Get detailed pricebook metadata from individual endpoint
        const headers = await this.authService.getRequestHeaders();
        const response = await firstValueFrom(
          this.httpService.get(`${this.baseUrl}/pricebooks/${pricebook.id}`, {
            headers,
            timeout: 15000,
          }),
        );

        if (response.data && response.data.id) {
          enrichedPricebooks.push(response.data);
        } else {
          enrichedPricebooks.push(pricebook);
        }

        // Rate limiting
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `⚠️ Failed to enrich pricebook ${pricebook.id}: ${error.message}`,
        );
        enrichedPricebooks.push(pricebook);
      }
    }

    return enrichedPricebooks;
  }

  // ============================================================================
  // DATABASE SAVE - FIXED VALIDATION
  // ============================================================================

  private async savePriceBooksToDatabase(
    priceBooks: KiotVietPriceBook[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${priceBooks.length} pricebooks to database...`);

    const savedPricebooks: any[] = [];

    for (const priceBookData of priceBooks) {
      try {
        // ENHANCED: Strict validation with actual API response structure
        if (
          !priceBookData.id ||
          !priceBookData.name ||
          priceBookData.name.trim() === ''
        ) {
          continue;
        }

        if (typeof priceBookData.id !== 'number' || priceBookData.id <= 0) {
          continue;
        }

        const pricebook = await this.prismaService.priceBook.upsert({
          where: { kiotVietId: priceBookData.id },
          update: {
            name: priceBookData.name.trim(),
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
          create: {
            kiotVietId: priceBookData.id,
            name: priceBookData.name.trim(),
            isActive: priceBookData.isActive ?? true,
            isGlobal: priceBookData.isGlobal ?? false,
            startDate: priceBookData.startDate
              ? new Date(priceBookData.startDate)
              : null,
            endDate: priceBookData.endDate
              ? new Date(priceBookData.endDate)
              : null,
            forAllCusGroup: priceBookData.forAllCusGroup ?? false,
            forAllUser: priceBookData.forAllUser ?? false,
            retailerId: priceBookData.retailerId ?? null,
            createdDate: priceBookData.createdDate
              ? new Date(priceBookData.createdDate)
              : new Date(),
            modifiedDate: priceBookData.modifiedDate
              ? new Date(priceBookData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
          },
        });

        savedPricebooks.push(pricebook);

        // ENHANCED: Save relationship data with proper error handling
        if (
          priceBookData.priceBookBranches &&
          priceBookData.priceBookBranches.length > 0
        ) {
          await this.savePriceBookBranches(
            pricebook.id,
            priceBookData.priceBookBranches,
          );
        }

        if (
          priceBookData.priceBookCustomerGroups &&
          priceBookData.priceBookCustomerGroups.length > 0
        ) {
          await this.savePriceBookCustomerGroups(
            pricebook.id,
            priceBookData.priceBookCustomerGroups,
          );
        }

        if (
          priceBookData.priceBookUsers &&
          priceBookData.priceBookUsers.length > 0
        ) {
          await this.savePriceBookUsers(
            pricebook.id,
            priceBookData.priceBookUsers,
          );
        }
      } catch (error) {
        this.logger.error(
          `❌ Failed to save pricebook ${priceBookData.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `💾 Saved ${savedPricebooks.length} pricebooks to database`,
    );
    return savedPricebooks;
  }

  // ============================================================================
  // SAVE RELATED DATA - ENHANCED ERROR HANDLING
  // ============================================================================

  private async savePriceBookBranches(
    priceBookId: number,
    branches: any[],
  ): Promise<void> {
    if (!branches || branches.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookBranch.deleteMany({
        where: { priceBookId },
      });

      let processedCount = 0;
      let skippedCount = 0;

      // Create new relationships
      for (const branchData of branches) {
        if (!branchData.branchId || typeof branchData.branchId !== 'number') {
          skippedCount++;
          continue;
        }

        const branch = await this.prismaService.branch.findFirst({
          where: { kiotVietId: branchData.branchId },
          select: { id: true, name: true },
        });

        if (branch) {
          try {
            await this.prismaService.priceBookBranch.create({
              data: {
                kiotVietId: branchData.id ? BigInt(branchData.id) : null,
                priceBookId,
                branchId: branch.id,
              },
            });
            processedCount++;
          } catch (createError) {
            this.logger.error(
              `❌ Failed to create pricebook-branch relationship: ${createError.message}`,
            );
            skippedCount++;
          }
        } else {
          skippedCount++;
        }
      }

      this.logger.log(
        `✅ PriceBook ${priceBookId} branches: ${processedCount} processed, ${skippedCount} skipped`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook branches: ${error.message}`,
      );
    }
  }

  private async savePriceBookCustomerGroups(
    priceBookId: number,
    customerGroups: any[],
  ): Promise<void> {
    if (!customerGroups || customerGroups.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookCustomerGroup.deleteMany({
        where: { priceBookId },
      });

      let processedCount = 0;
      let skippedCount = 0;

      // Create new relationships
      for (const groupData of customerGroups) {
        if (
          !groupData.customerGroupId ||
          typeof groupData.customerGroupId !== 'number'
        ) {
          skippedCount++;
          continue;
        }

        const customerGroup = await this.prismaService.customerGroup.findFirst({
          where: { kiotVietId: groupData.customerGroupId },
          select: { id: true, name: true },
        });

        if (customerGroup) {
          try {
            await this.prismaService.priceBookCustomerGroup.create({
              data: {
                kiotVietId: groupData.id ? BigInt(groupData.id) : null,
                priceBookId,
                customerGroupId: customerGroup.id,
              },
            });
            processedCount++;
          } catch (createError) {
            this.logger.error(
              `❌ Failed to create pricebook-customergroup relationship: ${createError.message}`,
            );
            skippedCount++;
          }
        } else {
          skippedCount++;
        }
      }

      this.logger.log(
        `✅ PriceBook ${priceBookId} customer groups: ${processedCount} processed, ${skippedCount} skipped`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to save pricebook customer groups: ${error.message}`,
      );
    }
  }

  private async savePriceBookUsers(
    priceBookId: number,
    users: any[],
  ): Promise<void> {
    if (!users || users.length === 0) return;

    try {
      // Clear existing relationships
      await this.prismaService.priceBookUser.deleteMany({
        where: { priceBookId },
      });

      // Create new relationships
      for (const userData of users) {
        // Note: User entity might not exist in current schema
        // Skip if user management is not implemented
        this.logger.debug(
          `User relationship for pricebook ${priceBookId}: ${userData.userId}`,
        );
      }
    } catch (error) {
      this.logger.error(`❌ Failed to save pricebook users: ${error.message}`);
    }
  }

  // ============================================================================
  // RETRY WRAPPER
  // ============================================================================

  async fetchPriceBooksWithRetry(
    params: {
      pageSize?: number;
      currentItem?: number;
      lastModifiedFrom?: string;
    },
    maxRetries: number = 3,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchPriceBookMetadata(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 2000 * attempt));
        }
      }
    }

    throw lastError;
  }

  // ============================================================================
  // SYNC CONTROL
  // ============================================================================

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('pricebook_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical pricebook sync enabled');
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['pricebook'],
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
