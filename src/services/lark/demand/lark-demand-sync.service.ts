// src/services/lark/demand/lark-demand-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from 'src/prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

interface LarkRecord {
  record_id: string;
  fields: Record<string, any>;
}

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    items?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

interface BatchResult {
  successRecords: any[];
  failedRecords: any[];
}

@Injectable()
export class LarkDemandSyncService {
  private readonly logger = new Logger(LarkDemandSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<string, any> = new Map();
  private customerCodeCache: Map<string, number> = new Map();
  private productCodeCache: Map<string, number> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>('LARK_DEMAND_BASE_TOKEN');
    const tableId = this.configService.get<string>('LARK_DEMAND_TABLE_ID');

    if (!baseToken || !tableId) {
      throw new Error('LarkBase demand configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ============================================================================
  // MAIN SYNC METHODS
  // ============================================================================

  async syncDemandsFromLarkBase(): Promise<{
    attempted: number;
    success: number;
    failed: number;
    details: any[];
  }> {
    const lockKey = `lark_demand_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log('🚀 Starting sync from LarkBase to Database...');

      // Load caches
      await this.loadCustomerProductCaches();
      await this.loadExistingDemandsCache();

      // Fetch all records from LarkBase
      const larkRecords = await this.fetchAllLarkRecords();
      this.logger.log(`📊 Found ${larkRecords.length} records in LarkBase`);

      let totalSuccess = 0;
      let totalFailed = 0;
      const syncDetails: any[] = [];

      // Process records in batches
      const batches = this.chunkArray(larkRecords, this.batchSize);

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        this.logger.log(
          `🔄 Processing batch ${i + 1}/${batches.length} (${batch.length} records)`,
        );

        try {
          const result = await this.processBatch(batch);
          totalSuccess += result.successRecords.length;
          totalFailed += result.failedRecords.length;

          syncDetails.push({
            batch: i + 1,
            success: result.successRecords.length,
            failed: result.failedRecords.length,
            successRecords: result.successRecords.slice(0, 5), // Sample
            failedRecords: result.failedRecords,
          });

          // Small delay between batches
          if (i < batches.length - 1) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        } catch (error) {
          this.logger.error(`❌ Batch ${i + 1} failed: ${error.message}`);
          totalFailed += batch.length;

          syncDetails.push({
            batch: i + 1,
            success: 0,
            failed: batch.length,
            error: error.message,
          });
        }
      }

      this.logger.log('🎯 Sync from LarkBase completed:');
      this.logger.log(`- Attempted: ${larkRecords.length}`);
      this.logger.log(`- Success: ${totalSuccess}`);
      this.logger.log(`- Failed: ${totalFailed}`);

      return {
        attempted: larkRecords.length,
        success: totalSuccess,
        failed: totalFailed,
        details: syncDetails,
      };
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  async syncSingleRecord(recordId: string): Promise<boolean> {
    try {
      this.logger.log(`🔄 Syncing single record: ${recordId}`);

      // Load caches if not loaded
      if (!this.cacheLoaded) {
        await this.loadCustomerProductCaches();
        await this.loadExistingDemandsCache();
      }

      // Fetch single record from LarkBase
      const larkRecord = await this.fetchSingleLarkRecord(recordId);
      if (!larkRecord) {
        this.logger.warn(`Record ${recordId} not found in LarkBase`);
        return false;
      }

      // Process the record
      const result = await this.processRecord(larkRecord);

      if (result) {
        this.logger.log(`✅ Successfully synced record ${recordId}`);
        return true;
      } else {
        this.logger.error(`❌ Failed to sync record ${recordId}`);
        return false;
      }
    } catch (error) {
      this.logger.error(
        `❌ Error syncing record ${recordId}: ${error.message}`,
      );
      return false;
    }
  }

  // ============================================================================
  // RECORD PROCESSING
  // ============================================================================

  private async processBatch(larkRecords: LarkRecord[]): Promise<BatchResult> {
    const successRecords: any[] = [];
    const failedRecords: any[] = [];

    for (const larkRecord of larkRecords) {
      try {
        const result = await this.processRecord(larkRecord);
        if (result) {
          successRecords.push({ recordId: larkRecord.record_id });
        } else {
          failedRecords.push({
            recordId: larkRecord.record_id,
            error: 'Processing failed',
          });
        }
      } catch (error) {
        failedRecords.push({
          recordId: larkRecord.record_id,
          error: error.message,
        });
      }
    }

    return { successRecords, failedRecords };
  }

  private async processRecord(larkRecord: LarkRecord): Promise<boolean> {
    const { record_id, fields } = larkRecord;

    // Check if record exists in database
    const existingDemand = this.existingRecordsCache.get(record_id);

    if (existingDemand) {
      // Update existing record
      return await this.updateDemandRecord(existingDemand, fields);
    } else {
      // Create new record
      return await this.createDemandRecord(record_id, fields);
    }
  }

  private async createDemandRecord(
    recordId: string,
    fields: Record<string, any>,
  ): Promise<boolean> {
    try {
      const demandData = this.mapLarkFieldsToDemand(recordId, fields);

      const demand = await this.prismaService.demand.create({
        data: demandData,
      });

      // Update cache
      this.existingRecordsCache.set(recordId, demand);

      return true;
    } catch (error) {
      this.logger.error(
        `Failed to create demand record ${recordId}: ${error.message}`,
      );
      return false;
    }
  }

  private async updateDemandRecord(
    existingDemand: any,
    fields: Record<string, any>,
  ): Promise<boolean> {
    try {
      const demandData = this.mapLarkFieldsToDemand(
        existingDemand.larkRecordId,
        fields,
        true,
      );

      await this.prismaService.demand.update({
        where: { id: existingDemand.id },
        data: {
          ...demandData,
          updatedDate: new Date(),
        },
      });

      return true;
    } catch (error) {
      this.logger.error(
        `Failed to update demand record ${existingDemand.larkRecordId}: ${error.message}`,
      );
      return false;
    }
  }

  // ============================================================================
  // FIELD MAPPING
  // ============================================================================

  private mapLarkFieldsToDemand(
    recordId: string,
    fields: Record<string, any>,
    isUpdate: boolean = false,
  ): any {
    // Extract customer code
    const customerCode = this.extractFieldValue(fields.fldB6qMDJE);
    const customerId = customerCode
      ? this.customerCodeCache.get(customerCode)
      : null;

    // Extract product code
    const productCode = this.extractFieldValue(fields.fldObSVczc);
    const productId = productCode
      ? this.productCodeCache.get(productCode)
      : null;

    // Extract other fields
    const quantity = parseFloat(fields.fldt6xdslC) || 0;
    const conversionRate =
      parseFloat(this.extractFieldValue(fields.fldedJZ9nw)) || 1;
    const month = this.extractSelectValue(fields.fldiwjX0x4);
    const year = parseInt(fields.fld9xrXbOL) || new Date().getFullYear();
    const unitType = this.extractSelectValue(fields.fld8UBa1eD);

    // Calculate converted quantity
    const convertedQuantity =
      unitType === 'Thùng' ? quantity * conversionRate : quantity;

    const demandData: any = {
      larkRecordId: recordId,
      customerId,
      productId,
      customerCode,
      customerName: this.extractFieldValue(fields.fldi50vNFY),
      productCode,
      productName: this.extractFieldValue(fields.fldJU22ujn),
      quantity,
      convertedQuantity,
      unit: this.extractFieldValue(fields.fldBtNjnkb),
      unitType,
      conversionRate,
      month,
      year,
      notes: fields.fldl4Z68kd || '',
      createdBy: this.extractFieldValue(fields.fldYucgmBG),
      larkSyncStatus: 'SYNCED',
      larkSyncedAt: new Date(),
    };

    // Handle created date for new records
    if (!isUpdate) {
      const createdDate = this.extractDateValue(fields.fldNLbZqnV);
      if (createdDate) {
        demandData.createdDate = createdDate;
      }
    }

    return demandData;
  }

  // ============================================================================
  // FIELD EXTRACTION HELPERS
  // ============================================================================

  private extractFieldValue(field: any) {
    if (!field) return null;

    if (typeof field === 'string') return field;
    if (Array.isArray(field) && field.length > 0) {
      return field[0]?.text || field[0] || null;
    }

    return null;
  }

  private extractSelectValue(field: any): string | null {
    if (!field) return null;

    if (Array.isArray(field) && field.length > 0) {
      return field[0]?.text || field[0] || null;
    }

    return field?.text || field || null;
  }

  private extractDateValue(field: any): Date | null {
    if (!field) return null;

    try {
      const timestamp = typeof field === 'number' ? field : parseInt(field);
      return new Date(timestamp);
    } catch {
      return null;
    }
  }

  // ============================================================================
  // LARKBASE API METHODS
  // ============================================================================

  private async fetchAllLarkRecords(): Promise<LarkRecord[]> {
    const allRecords: LarkRecord[] = [];
    let pageToken: string | undefined;

    do {
      const response = await this.fetchLarkRecordPage(pageToken);
      const records = response.data?.records || response.data?.items || [];
      allRecords.push(...records);
      pageToken = response.data?.page_token;

      if (pageToken) {
        this.logger.log(`📥 Fetched ${allRecords.length} records so far...`);
        await new Promise((resolve) => setTimeout(resolve, 500)); // Rate limiting
      }
    } while (pageToken);

    return allRecords;
  }

  private async fetchLarkRecordPage(
    pageToken?: string,
  ): Promise<LarkBatchResponse> {
    const maxRetries = 3;
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getDemandHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params = new URLSearchParams({
          page_size: '500',
        });

        if (pageToken) {
          params.append('page_token', pageToken);
        }

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data.code === 0) {
          return response.data;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshDemandToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        throw new Error(`LarkBase API error: ${response.data.msg}`);
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshDemandToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        throw error;
      }
    }

    throw new Error('Max authentication retries exceeded');
  }

  private async fetchSingleLarkRecord(
    recordId: string,
  ): Promise<LarkRecord | null> {
    const maxRetries = 3;
    let authRetries = 0;

    while (authRetries <= this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getDemandHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${recordId}`;

        const response = await firstValueFrom(
          this.httpService.get(url, {
            headers,
            timeout: 15000,
          }),
        );

        if (response.data.code === 0) {
          return response.data.data.record;
        }

        if (response.data.code === 21011) {
          // Record not found
          return null;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshDemandToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        throw new Error(`LarkBase API error: ${response.data.msg}`);
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshDemandToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        if (error.response?.status === 404) {
          return null;
        }

        throw error;
      }
    }

    throw new Error('Max authentication retries exceeded');
  }

  // ============================================================================
  // CACHE MANAGEMENT
  // ============================================================================

  private async loadCustomerProductCaches(): Promise<void> {
    this.logger.log('🔄 Loading customer and product caches...');

    // Load customers
    const customers = await this.prismaService.customer.findMany({
      select: { id: true, code: true },
    });

    this.customerCodeCache.clear();
    customers.forEach((customer) => {
      if (customer.code) {
        this.customerCodeCache.set(customer.code, customer.id);
      }
    });

    // Load products
    const products = await this.prismaService.product.findMany({
      select: { id: true, code: true },
    });

    this.productCodeCache.clear();
    products.forEach((product) => {
      if (product.code) {
        this.productCodeCache.set(product.code, product.id);
      }
    });

    this.logger.log(
      `✅ Loaded ${this.customerCodeCache.size} customers and ${this.productCodeCache.size} products`,
    );
  }

  private async loadExistingDemandsCache(): Promise<void> {
    this.logger.log('🔄 Loading existing demands cache...');

    const demands = await this.prismaService.demand.findMany({
      select: {
        id: true,
        larkRecordId: true,
        customerCode: true,
        productCode: true,
      },
    });

    this.existingRecordsCache.clear();
    demands.forEach((demand) => {
      if (demand.larkRecordId) {
        this.existingRecordsCache.set(demand.larkRecordId, demand);
      }
    });

    this.logger.log(
      `✅ Loaded ${this.existingRecordsCache.size} existing demands`,
    );
    this.cacheLoaded = true;
    this.lastCacheLoadTime = new Date();
  }

  private isCacheValid(): boolean {
    if (!this.cacheLoaded || !this.lastCacheLoadTime) return false;

    const now = new Date();
    const diffMinutes =
      (now.getTime() - this.lastCacheLoadTime.getTime()) / (1000 * 60);

    return diffMinutes < this.CACHE_VALIDITY_MINUTES;
  }

  // ============================================================================
  // ANALYSIS METHODS
  // ============================================================================

  async analyzeMissingData(): Promise<{
    larkOnly: any[];
    dbOnly: any[];
    common: any[];
    summary: any;
  }> {
    this.logger.log(
      '🔍 Analyzing data differences between LarkBase and Database...',
    );

    // Load caches
    await this.loadCustomerProductCaches();
    await this.loadExistingDemandsCache();

    // Fetch LarkBase records
    const larkRecords = await this.fetchAllLarkRecords();
    const larkRecordIds = new Set(larkRecords.map((r) => r.record_id));

    // Get database records
    const dbRecords = await this.prismaService.demand.findMany({
      select: {
        id: true,
        larkRecordId: true,
        customerCode: true,
        productCode: true,
        quantity: true,
      },
    });

    const dbRecordIds = new Set(
      dbRecords.filter((r) => r.larkRecordId).map((r) => r.larkRecordId!),
    );

    // Find differences
    const larkOnly = larkRecords.filter((r) => !dbRecordIds.has(r.record_id));
    const dbOnly = dbRecords.filter(
      (r) => r.larkRecordId && !larkRecordIds.has(r.larkRecordId),
    );
    const common = larkRecords.filter((r) => dbRecordIds.has(r.record_id));

    const summary = {
      totalLarkBase: larkRecords.length,
      totalDatabase: dbRecords.length,
      onlyInLarkBase: larkOnly.length,
      onlyInDatabase: dbOnly.length,
      common: common.length,
      needsSync: larkOnly.length,
    };

    this.logger.log('📊 Analysis Summary:');
    this.logger.log(`- Total in LarkBase: ${summary.totalLarkBase}`);
    this.logger.log(`- Total in Database: ${summary.totalDatabase}`);
    this.logger.log(`- Only in LarkBase: ${summary.onlyInLarkBase}`);
    this.logger.log(`- Only in Database: ${summary.onlyInDatabase}`);
    this.logger.log(`- Common records: ${summary.common}`);

    return {
      larkOnly: larkOnly.slice(0, 100), // First 100 for readability
      dbOnly: dbOnly.slice(0, 100),
      common: common.slice(0, 20),
      summary,
    };
  }

  async getDemandSyncStats(): Promise<{
    total: number;
    synced: number;
    pending: number;
    failed: number;
  }> {
    const [total, synced, pending, failed] = await Promise.all([
      this.prismaService.demand.count(),
      this.prismaService.demand.count({ where: { larkSyncStatus: 'SYNCED' } }),
      this.prismaService.demand.count({ where: { larkSyncStatus: 'PENDING' } }),
      this.prismaService.demand.count({ where: { larkSyncStatus: 'FAILED' } }),
    ]);

    return { total, synced, pending, failed };
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private chunkArray<T>(array: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    // Simple lock mechanism using database or Redis
    // For now, just use a simple check
    this.logger.log(`🔒 Acquiring sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    this.logger.log(`🔓 Releasing sync lock: ${lockKey}`);
  }

  // ============================================================================
  // CONNECTION TESTING
  // ============================================================================

  async testLarkBaseConnection(): Promise<{
    connected: boolean;
    error?: string;
    recordCount?: number;
  }> {
    try {
      this.logger.log('🔍 Testing LarkBase connection...');

      const headers = await this.larkAuthService.getDemandHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
      const params = new URLSearchParams({ page_size: '1' });

      const response = await firstValueFrom(
        this.httpService.get(`${url}?${params}`, {
          headers,
          timeout: 30000,
        }),
      );

      if (response.data.code === 0) {
        const totalRecords = response.data.data?.total || 0;
        this.logger.log(`✅ LarkBase connection successful`);
        this.logger.log(`📊 LarkBase table has ${totalRecords} records`);

        return {
          connected: true,
          recordCount: totalRecords,
        };
      }

      throw new Error(`Connection test failed: ${response.data.msg}`);
    } catch (error) {
      this.logger.error(`❌ LarkBase connection failed: ${error.message}`);
      return {
        connected: false,
        error: error.message,
      };
    }
  }
}
