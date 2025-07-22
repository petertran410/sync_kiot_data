import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { async, first, firstValueFrom } from 'rxjs';
import { Prisma, LarkSyncStatus } from '@prisma/client';
import { LarkSupplierSyncService } from 'src/services/lark/supplier/lark-supplier-sync.service';

interface KiotVietSupplier {
  id: number;
  code: string;
  name: string;
  contactNumber?: string;
  email?: string;
  address?: string;
  locationName?: string;
  wardName?: string;
  organization?: string;
  taxCode?: string;
  comments?: string;
  groups?: string;
  isActive?: boolean;
  debt?: number;
  totalInvoiced?: number;
  totalInvoicedWithoutReturn?: number;
  retailerId?: number;
  branchId?: number;
  createdDate?: string;
  modifiedDate?: string;
}

@Injectable()
export class KiotVietSupplierService {
  private readonly logger = new Logger(KiotVietSupplierService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkSupplierSyncService: LarkSupplierSyncService,
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
        where: { name: 'supplier_historical' },
      });

      if (historicalSync?.isEnabled && !historicalSync.isRunning) {
        this.logger.log('Starting historical supplier sync...');
        await this.syncHistoricalSuppliers();
        return;
      }

      this.logger.log('Running default historical supplier sync...');
      await this.syncHistoricalSuppliers();
    } catch (error) {
      this.logger.error(`Sync check failed: ${error.message}`);
      throw error;
    }
  }

  async enableHistoricalSync(): Promise<void> {
    await this.updateSyncControl('supplier_historical', {
      isEnabled: true,
      isRunning: false,
      status: 'idle',
    });

    this.logger.log('✅ Historical supplier sync enabled');
  }

  async syncHistoricalSuppliers(): Promise<void> {
    const syncName = 'supplier_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalSuppliers = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let lastValidTotal = 0;
    let processedSupplierIds = new Set<number>();

    try {
      await this.updateSyncControl(syncName, {
        isRunning: true,
        status: 'running',
        startedAt: new Date(),
        error: null,
      });

      this.logger.log('🚀 Starting historical supplier sync...');

      const MAX_CONSECUTIVE_EMPTY_PAGES = 5;
      const MAX_CONSECUTIVE_ERROR_PAGES = 3;
      const RETRY_DELAY_MS = 2000;
      const MAX_TOTAL_RETRIES = 10;

      let totalRetries = 0;

      while (true) {
        const currentPage = Math.floor(currentItem / this.PAGE_SIZE) + 1;

        if (totalSuppliers > 0) {
          if (currentItem >= totalSuppliers) {
            this.logger.log(
              `✅ Pagination complete. Processed ${processedCount}/${totalSuppliers} supplier`,
            );
            break;
          }
        }

        try {
          this.logger.log(
            `📄 Fetching page ${currentPage} (items ${currentItem} - ${currentItem + this.PAGE_SIZE - 1})`,
          );

          const response = await this.fetchSuppliersListWithRetry({
            currentItem,
            pageSize: this.PAGE_SIZE,
            includeTotal: true,
            includeSupplierGroup: true,
          });

          consecutiveErrorPages = 0;

          const { data: suppliers, total } = response;

          if (total !== undefined && total !== null) {
            if (totalSuppliers === 0) {
              this.logger.log(
                `📊 Total suppliers detected: ${total}. Starting processing...`,
              );

              totalSuppliers = total;
            } else if (total !== totalSuppliers && total !== lastValidTotal) {
              this.logger.warn(
                `⚠️ Total count changed: ${totalSuppliers} → ${total}. Using latest.`,
              );

              totalSuppliers = total;
            }
            lastValidTotal = total;
          }

          if (!suppliers || suppliers.length === 0) {
            this.logger.warn(
              `⚠️ Empty page received at position ${currentItem}`,
            );

            consecutiveEmptyPages++;

            if (totalSuppliers > 0 && currentItem >= totalSuppliers) {
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

          const newSuppliers = suppliers.filter((supplier) => {
            if (processedSupplierIds.has(supplier.id)) {
              this.logger.debug(
                `⚠️ Duplicate supplier ID detected: ${supplier.id} (${supplier.code})`,
              );
              return false;
            }
            processedSupplierIds.add(supplier.id);
            return true;
          });

          if (newSuppliers.length !== suppliers.length) {
            this.logger.warn(
              `🔄 Filtered out ${suppliers.length - newSuppliers.length} duplicate suppliers on page ${currentPage}`,
            );
          }

          if (newSuppliers.length === 0) {
            this.logger.log(
              `⏭️ Skipping page ${currentPage} - all suppliers already processed`,
            );
            currentItem += this.PAGE_SIZE;
            continue;
          }

          this.logger.log(
            `🔄 Processing ${newSuppliers.length} suppliers from page ${currentPage}...`,
          );

          const suppliersWithDetails =
            await this.enrichSuppliersWithDetails(newSuppliers);
          const savedSuppliers =
            await this.saveSuppliersToDatabase(suppliersWithDetails);
          await this.syncSuppliersToLarkBase(savedSuppliers);

          processedCount += savedSuppliers.length;
          currentItem += this.PAGE_SIZE;

          if (totalSuppliers > 0) {
            const completionPercentage =
              (processedCount / totalSuppliers) * 100;
            this.logger.log(
              `📈 Progress: ${processedCount}/${totalSuppliers} (${completionPercentage.toFixed(1)}%)`,
            );

            if (processedCount >= totalSuppliers) {
              this.logger.log('🎉 All suppliers processed successfully!');
              break;
            }
          }

          consecutiveEmptyPages = 0;
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          consecutiveErrorPages++;
          totalRetries++;

          this.logger.error(
            `❌ Page ${currentPage} failed (attempt ${consecutiveErrorPages}/${MAX_CONSECUTIVE_ERROR_PAGES}): ${error.message}`,
          );

          if (
            consecutiveErrorPages >= MAX_CONSECUTIVE_ERROR_PAGES ||
            totalRetries >= MAX_TOTAL_RETRIES
          ) {
            throw new Error(
              `Too many consecutive errors (${consecutiveErrorPages}) or total retries (${totalRetries}). Last error: ${error.message}`,
            );
          }

          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
        }
      }

      await this.updateSyncControl(syncName, {
        isRunning: false,
        isEnabled: false,
        status: 'completed',
        completedAt: new Date(),
        lastRunAt: new Date(),
        progress: { processedCount, expectedTotal: totalSuppliers },
      });

      const completionRate =
        totalSuppliers > 0 ? (processedCount / totalSuppliers) * 100 : 100;

      this.logger.log(
        `✅ Historical supplier sync completed: ${processedCount}/${totalSuppliers} (${completionRate.toFixed(1)}% completion rate)`,
      );
    } catch (error) {
      this.logger.error(`❌ Historical supplier sync failed: ${error.message}`);

      await this.updateSyncControl(syncName, {
        isRunning: false,
        status: 'failed',
        error: error.message,
        progress: { processedCount, expectedTotal: totalSuppliers },
      });

      throw error;
    }
  }

  async fetchSuppliersListWithRetry(
    params: {
      currentItem?: number;
      pageSize?: number;
      includeTotal?: boolean;
      includeSupplierGroup?: boolean;
    },
    maxRetries: number = 5,
  ): Promise<any> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.fetchSuppliersList(params);
      } catch (error) {
        lastError = error as Error;
        this.logger.warn(
          `⚠️ API attempt ${attempt}/${maxRetries} failed: ${error.message}`,
        );

        if (attempt < maxRetries) {
          const delay = 2000 * attempt;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  async fetchSuppliersList(params: {
    currentItem?: number;
    pageSize?: number;
    includeTotal?: boolean;
    includeSupplierGroup?: boolean;
  }): Promise<any> {
    const headers = await this.authService.getRequestHeaders();

    const queryParams = new URLSearchParams({
      currentItem: (params.currentItem || 0).toString(),
      pageSize: (params.pageSize || this.PAGE_SIZE).toString(),
      includeTotal: (params.includeTotal || true).toString(),
      includeSupplierGroup: (params.includeSupplierGroup || true).toString(),
    });

    const response = await firstValueFrom(
      this.httpService.get(`${this.baseUrl}/suppliers?${queryParams}`, {
        headers,
        timeout: 45000,
      }),
    );

    return response.data;
  }

  private async enrichSuppliersWithDetails(
    suppliers: KiotVietSupplier[],
  ): Promise<KiotVietSupplier[]> {
    this.logger.log(
      `🔍 Enriching ${suppliers.length} suppliers with details...`,
    );

    const enrichedSuppliers: KiotVietSupplier[] = [];

    for (const supplier of suppliers) {
      try {
        const headers = await this.authService.getRequestHeaders();

        const queryParams = new URLSearchParams({
          includeTotal: 'true',
          includeSupplierGroup: 'true',
        });

        const response = await firstValueFrom(
          this.httpService.get(
            `${this.baseUrl}/suppliers/${supplier.id}?${queryParams}`,
            { headers, timeout: 30000 },
          ),
        );

        if (response.data) {
          enrichedSuppliers.push(response.data);
        } else {
          enrichedSuppliers.push(supplier);
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.warn(
          `Failed to enrich supplier ${supplier.code}: ${error.message}`,
        );

        enrichedSuppliers.push(supplier);
      }
    }
    return enrichedSuppliers;
  }

  private async saveSuppliersToDatabase(
    suppliers: KiotVietSupplier[],
  ): Promise<any[]> {
    this.logger.log(`💾 Saving ${suppliers.length} suppliers to database...`);

    const savedSuppliers: any[] = [];

    for (const supplierData of suppliers) {
      try {
        const supplier = await this.prismaService.supplier.upsert({
          where: { kiotVietId: BigInt(supplierData.id) },
          update: {
            code: supplierData.code.trim(),
            name: supplierData.name.trim(),
            contactNumber: supplierData.contactNumber || null,
            email: supplierData.email || null,
            address: supplierData.address || null,
            locationName: supplierData.locationName || null,
            wardName: supplierData.wardName || null,
            organization: supplierData.organization || null,
            taxCode: supplierData.taxCode || null,
            comments: supplierData.comments || null,
            groups: supplierData.groups || null,
            isActive: supplierData.isActive ?? true,
            debt: supplierData.debt
              ? new Prisma.Decimal(supplierData.debt)
              : null,
            totalInvoiced: supplierData.totalInvoiced
              ? new Prisma.Decimal(supplierData.totalInvoiced)
              : null,
            totalInvoicedWithoutReturn: supplierData.totalInvoicedWithoutReturn
              ? new Prisma.Decimal(supplierData.totalInvoicedWithoutReturn)
              : null,
            retailerId: supplierData.retailerId ?? null,
            branchId: supplierData.branchId ?? null,
            createdDate: supplierData.createdDate
              ? new Date(supplierData.createdDate)
              : new Date(),
            modifiedDate: supplierData.modifiedDate
              ? new Date(supplierData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
          create: {
            kiotVietId: BigInt(supplierData.id),
            code: supplierData.code.trim(),
            name: supplierData.name.trim(),
            contactNumber: supplierData.contactNumber || null,
            email: supplierData.email || null,
            address: supplierData.address || null,
            locationName: supplierData.locationName || null,
            wardName: supplierData.wardName || null,
            organization: supplierData.organization || null,
            taxCode: supplierData.taxCode || null,
            comments: supplierData.comments || null,
            groups: supplierData.groups || null,
            isActive: supplierData.isActive ?? true,
            debt: supplierData.debt
              ? new Prisma.Decimal(supplierData.debt)
              : null,
            totalInvoiced: supplierData.totalInvoiced
              ? new Prisma.Decimal(supplierData.totalInvoiced)
              : null,
            totalInvoicedWithoutReturn: supplierData.totalInvoicedWithoutReturn
              ? new Prisma.Decimal(supplierData.totalInvoicedWithoutReturn)
              : null,
            retailerId: supplierData.retailerId ?? null,
            branchId: supplierData.branchId ?? null,
            createdDate: supplierData.createdDate
              ? new Date(supplierData.createdDate)
              : new Date(),
            modifiedDate: supplierData.modifiedDate
              ? new Date(supplierData.modifiedDate)
              : new Date(),
            lastSyncedAt: new Date(),
            larkSyncStatus: 'PENDING',
          },
        });

        savedSuppliers.push(supplier);
      } catch (error) {
        this.logger.error(
          `❌ Failed to save supplier ${supplierData.code}: ${error.message}`,
        );
      }
    }

    this.logger.log(`✅ Saved ${savedSuppliers.length} suppliers successfully`);
    return savedSuppliers;
  }

  async syncSuppliersToLarkBase(suppliers: any[]): Promise<void> {
    try {
      this.logger.log(
        `🚀 Starting LarkBase sync for ${suppliers.length} suppliers...`,
      );

      const suppliersToSync = suppliers.filter(
        (s) => s.larkSyncStatus === 'PENDING' || s.larkSyncStatus === 'FAILED',
      );

      if (suppliersToSync.length === 0) {
        this.logger.log('📋 No suppliers need LarkBase sync');
        return;
      }

      await this.larkSupplierSyncService.syncSuppliersToLarkBase(
        suppliersToSync,
      );

      this.logger.log(`✅ LarkBase sync completed successfully`);
    } catch (error) {
      this.logger.error(`❌ LarkBase supplier sync failed: ${error.message}`);

      try {
        const supplierIds = suppliers
          .map((s) => s.id)
          .filter((id) => id !== undefined);

        if (supplierIds.length > 0) {
          await this.prismaService.supplier.updateMany({
            where: { id: { in: supplierIds } },
            data: {
              larkSyncedAt: new Date(),
              larkSyncStatus: 'FAILED',
            },
          });
        }
      } catch (updateError) {
        this.logger.error(
          `Failed to update supplier status: ${updateError.message}`,
        );
      }

      throw new Error(`LarkBase sync failed: ${error.message}`);
    }
  }

  private async updateSyncControl(name: string, data: any): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: ['supplier'],
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
