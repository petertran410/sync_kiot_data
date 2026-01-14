import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';
import { LarkSyncStatus } from '@prisma/client';

const LARK_SUPPLIER_FIELDS = {
  SUPPLIER_CODE: 'Mã Nhà Cung Cấp',
  KIOTVIET_ID: 'kiotVietId',
  SUPPLIER_NAME: 'Tên Nhà Cung Cấp',
  PHONE_NUMBER: 'Số Điện Thoại',
  ADDRESS: 'Địa Chỉ',
  WARD: 'Phường',
  EMAIL: 'Email',
  GROUPS: 'Nhóm',
  ORGANIZATION: 'Tên Công Ty',
  TAX_CODE: 'Mã Số Thuế',
  LOCATION_NAME: 'Khu Vực',
  COMMENTS: 'Ghi Chú',
  ACTIVE: 'Hoạt Động',
  MODIFIED_DATE: 'Ngày Cập Nhật',
  CREATED_DATE: 'Ngày Tạo',
  RETAILER: 'Gian Hàng',
  CREATOR: 'Người Tạo',
  BRANCH: 'Branch',
  DEBT: 'Nợ Hiện Tại',
  TOTAL_INVOICED: 'Tổng Mua',
  TOTAL_INVOICED_WITHOUT_RETURN: 'Tổng Mua Trừ Trả Hàng',
} as const;

const IS_ACTIVE = {
  YES: 'Có',
  NO: 'Không',
} as const;

const BRANCH_NAME = {
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
  KHO_BAN_HANG: 'Kho Bán Hàng',
} as const;

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
export class LarkSupplierSyncService {
  private readonly logger = new Logger(LarkSupplierSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<number, string> = new Map();
  private supplierCodeCache: Map<string, string> = new Map();
  private cacheLoaded: boolean = false;
  private lastCacheLoadTime: Date | null = null;
  private readonly CACHE_VALIDITY_MINUTES = 30;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_SUPPLIER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_SUPPLIER_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase supplier configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncSuppliersToLarkBase(suppliers: any[]): Promise<void> {
    const lockKey = `lark_supplier_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${suppliers.length} suppliers...`,
      );

      const suppliersToSync = suppliers.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (suppliers.length === 0) {
        this.logger.log('No suppliers need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (suppliers.length < 5) {
        this.logger.log(
          `Small sync (${suppliers.length} suppliers) - using lightweight mode`,
        );
        await this.syncWithoutCache(suppliersToSync);
        await this.releaseSyncLock(lockKey);
        return;
      }

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(suppliersToSync);
        await this.releaseSyncLock(lockKey);
      }

      const { newSuppliers, updateSuppliers } =
        await this.categorizeSuppliers(suppliersToSync);

      this.logger.log(
        `Categorization: ${newSuppliers.length} new, ${updateSuppliers.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 500;

      if (newSuppliers.length > 0) {
        for (let i = 0; i < newSuppliers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newSuppliers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new suppliers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newSuppliers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewSuppliers(batch);
        }
      }

      if (updateSuppliers.length > 0) {
        for (let i = 0; i < updateSuppliers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateSuppliers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update suppliers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(updateSuppliers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateSuppliers(batch);
        }
      }

      this.logger.log('LarkBase supplier sync completed!');
    } catch (error) {
      await this.releaseSyncLock(lockKey);
      this.logger.error(`❌ LarkBase supplier sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(
    maxRetries: number = 3,
  ): Promise<boolean> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (this.isCacheValid() && this.existingRecordsCache.size > 5000) {
          this.logger.log(
            `Using cache available (${this.existingRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastCacheLoadTime) {
          const cacheAgeMinutes =
            (Date.now() - this.lastCacheLoadTime.getTime()) / (1000 * 60);
          if (cacheAgeMinutes < 45 && this.existingRecordsCache.size > 500) {
            this.logger.log(
              `Recent cache (${cacheAgeMinutes.toFixed(1)}min old, ${this.existingRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearCache();
        await this.loadExistingRecords();

        if (this.existingRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingRecordsCache.size} records`,
          );
          this.lastCacheLoadTime = new Date();
          return true;
        }

        this.logger.warn(`Cache empty on attempt ${attempt}`);
      } catch (error) {
        this.logger.warn(
          `Cache loading attempt ${attempt} failed: ${error.message}`,
        );
        if (attempt < maxRetries) {
          const delay = attempt * 1500;
          this.logger.log(`Waiting ${delay / 1000}s before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }
    return false;
  }

  private isCacheValid(): boolean {
    if (!this.cacheLoaded || !this.lastCacheLoadTime) {
      return false;
    }

    const cacheAge = Date.now() - this.lastCacheLoadTime.getTime();
    const maxAge = this.CACHE_VALIDITY_MINUTES * 60 * 1000;

    return cacheAge < maxAge && this.existingRecordsCache.size > 0;
  }

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getSupplierHeaders();
      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheBuilt = 0;
      let stringConversions = 0;
      const pageSize = 500;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const params = new URLSearchParams({
          page_size: String(pageSize),
        });

        if (pageToken) {
          params.append('page_token', pageToken);
        }

        const startTime = Date.now();

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 90000,
          }),
        );

        const loadTime = Date.now() - startTime;

        if (response.data.code === 0) {
          const records = response.data.data?.items || [];

          for (const record of records) {
            const kiotVietIdField =
              record.fields[LARK_SUPPLIER_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const supplierCode =
              record.fields[LARK_SUPPLIER_FIELDS.SUPPLIER_CODE];
            if (supplierCode) {
              this.supplierCodeCache.set(
                String(supplierCode).trim(),
                record.record_id,
              );
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token;

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheBuilt}/${totalLoaded} records processed (${stringConversions} string conversions) (${loadTime}ms/page)`,
            );
          }
        } else {
          throw new Error(
            `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
          );
        }
      } while (pageToken);

      this.cacheLoaded = true;

      const successRate =
        totalLoaded > 0 ? Math.round((cacheBuilt / totalLoaded) * 100) : 0;

      this.logger.log(
        `Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.supplierCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async categorizeSuppliers(suppliers: any[]): Promise<any> {
    const newSuppliers: any[] = [];
    const updateSuppliers: any[] = [];

    for (const supplier of suppliers) {
      const kiotVietId = supplier.kiotVietId
        ? typeof supplier.kiotVietId === 'bigint'
          ? Number(supplier.kiotVietId)
          : Number(supplier.kiotVietId)
        : 0;

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Supplier ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && supplier.code) {
        existingRecordId = this.supplierCodeCache.get(
          String(supplier.code).trim(),
        );
      }

      if (existingRecordId) {
        updateSuppliers.push({
          ...supplier,
          larkRecordId: existingRecordId,
        });
      } else {
        newSuppliers.push(supplier);
      }
    }

    return { newSuppliers, updateSuppliers };
  }

  private async syncWithoutCache(suppliers: any[]): Promise<void> {
    const existingSuppliers = await this.prismaService.supplier.findMany({
      where: { kiotVietId: { in: suppliers.map((t) => t.kiotVietId) } },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingSuppliers.forEach((o) => {
      if (o.larkRecordId) {
        quickCache.set(Number(o.kiotVietId), o.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newSuppliers, updateSuppliers } =
        await this.categorizeSuppliers(suppliers);

      if (newSuppliers.length > 0) {
        await this.processNewSuppliers(newSuppliers);
      }

      if (updateSuppliers.length > 0) {
        await this.processUpdateSuppliers(updateSuppliers);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async processNewSuppliers(suppliers: any[]): Promise<void> {
    if (suppliers.length === 0) return;

    this.logger.log(`Creating ${suppliers.length} new suppliers...`);

    const batches = this.chunkArray(suppliers, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const supplier of batch) {
        const kiotVietId = this.safeBigIntToNumber(supplier.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(supplier);
        } else {
          this.logger.warn(
            `Skipping duplicate supplier ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all supplier already exist`);
        continue;
      }
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} suppliers)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateSuppliers(verifiedBatch);

      totalCreated += successRecords.length;
      totalFailed += failedRecords.length;

      if (successRecords.length > 0) {
        await this.updateDatabaseStatus(successRecords, 'SYNCED');

        successRecords.forEach((record) => {
          const kiotVietId = this.safeBigIntToNumber(record.kiotVietId);
          this.pendingCreation.delete(kiotVietId);
        });
      }

      if (failedRecords.length > 0) {
        await this.updateDatabaseStatus(failedRecords, 'FAILED');
      }

      this.logger.log(
        `Batch ${i + 1}/${batches.length}: ${successRecords.length}/${batch.length} created`,
      );
    }

    this.logger.log(
      `Create complete: ${totalCreated} success, ${totalFailed} failed`,
    );
  }

  private async processUpdateSuppliers(suppliers: any[]): Promise<void> {
    if (suppliers.length === 0) return;

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 20;

    for (let i = 0; i < suppliers.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = suppliers.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (supplier) => {
          try {
            const updated = await this.updateSingleSupplier(supplier);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([supplier], 'SYNCED');
            } else {
              createFallbacks.push(supplier);
            }
          } catch (error) {
            createFallbacks.push(supplier);
          }
        }),
      );
    }

    if (createFallbacks.length > 0) {
      await this.processNewSuppliers(createFallbacks);
    }
  }

  private async batchCreateSuppliers(suppliers: any[]): Promise<BatchResult> {
    const records = suppliers.map((supplier) => ({
      fields: this.mapSupplierToLarkBase(supplier),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getSupplierHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdRecords = response.data.data?.records || [];
          const successCount = createdRecords.length;
          const successRecords = suppliers.slice(0, successCount);
          const failedRecords = suppliers.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const supplier = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(supplier.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (supplier.code) {
              this.supplierCodeCache.set(
                String(supplier.code),
                createdRecord.record_id,
              );
            }
          }

          return { successRecords, failedRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(
          `Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: suppliers };
      } catch (error) {
        this.logger.error('Batch create error details:', {
          status: error.response?.status,
          statusText: error.response?.statusText,
          data: error.response?.data,
          config: {
            url: error.config?.url,
            method: error.config?.method,
            data: JSON.parse(error.config?.data || '{}'),
          },
        });

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: suppliers };
      }
    }

    return { successRecords: [], failedRecords: suppliers };
  }

  private async updateSingleSupplier(supplier: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getSupplierHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${supplier.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapSupplierToLarkBase(supplier) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${supplier.larkRecordId} for supplier ${supplier.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.forceTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        if (error.response?.status === 404) {
          this.logger.warn(`Record not found: ${supplier.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private mapSupplierToLarkBase(supplier: any): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_SUPPLIER_FIELDS.KIOTVIET_ID] = this.safeBigIntToNumber(
      supplier.kiotVietId,
    );

    if (supplier.name) {
      fields[LARK_SUPPLIER_FIELDS.SUPPLIER_NAME] = supplier.name;
    }

    if (supplier.code) {
      fields[LARK_SUPPLIER_FIELDS.SUPPLIER_CODE] = supplier.code;
    }

    if (supplier.contactNumber) {
      fields[LARK_SUPPLIER_FIELDS.PHONE_NUMBER] = supplier.contactNumber || '';
    }

    if (supplier.retailedId) {
      fields[LARK_SUPPLIER_FIELDS.RETAILER] = '2svn';
    }

    if (supplier.address) {
      fields[LARK_SUPPLIER_FIELDS.ADDRESS] = supplier.address || '';
    }

    if (supplier.email) {
      fields[LARK_SUPPLIER_FIELDS.EMAIL] = supplier.email || '';
    }

    if (supplier.locationName) {
      fields[LARK_SUPPLIER_FIELDS.LOCATION_NAME] = supplier.locationName || '';
    }

    if (supplier.wardName) {
      fields[LARK_SUPPLIER_FIELDS.WARD] = supplier.wardName || '';
    }

    if (supplier.organization) {
      fields[LARK_SUPPLIER_FIELDS.ORGANIZATION] = supplier.organization || '';
    }

    if (supplier.taxCode) {
      fields[LARK_SUPPLIER_FIELDS.TAX_CODE] = supplier.taxCode || '';
    }

    if (supplier.comments) {
      fields[LARK_SUPPLIER_FIELDS.COMMENTS] = supplier.comments || '';
    }

    if (supplier.groups) {
      fields[LARK_SUPPLIER_FIELDS.GROUPS] = supplier.groups || '';
    }

    if (supplier.isActive) {
      fields[LARK_SUPPLIER_FIELDS.ACTIVE] = supplier.isActive
        ? IS_ACTIVE.YES
        : IS_ACTIVE.NO;
    }

    if (supplier.debt !== null && supplier.debt !== undefined) {
      fields[LARK_SUPPLIER_FIELDS.DEBT] = Number(supplier.debt || 0);
    }

    if (
      supplier.totalInvoiced !== null &&
      supplier.totalInvoiced !== undefined
    ) {
      fields[LARK_SUPPLIER_FIELDS.TOTAL_INVOICED] = Number(
        supplier.totalInvoiced || 0,
      );
    }

    if (
      supplier.totalInvoicedWithoutReturn !== null &&
      supplier.totalInvoicedWithoutReturn !== undefined
    ) {
      fields[LARK_SUPPLIER_FIELDS.TOTAL_INVOICED_WITHOUT_RETURN] = Number(
        supplier.totalInvoicedWithoutReturn || 0,
      );
    }

    if (supplier.branchId !== null && supplier.branchId !== undefined) {
      const branchMapping: Record<number, string> = {
        635934: BRANCH_NAME.CUA_HANG_DIEP_TRA,
        154833: BRANCH_NAME.KHO_HA_NOI,
        402819: BRANCH_NAME.KHO_SAI_GON,
        631163: BRANCH_NAME.VAN_PHONG_HA_NOI,
        635935: BRANCH_NAME.KHO_BAN_HANG,
      };

      fields[LARK_SUPPLIER_FIELDS.BRANCH] =
        branchMapping[supplier.branchId] || null;
    }

    if (supplier.createdDate) {
      fields[LARK_SUPPLIER_FIELDS.CREATED_DATE] = new Date(
        supplier.createdDate,
      ).getTime();
    }

    if (supplier.modifiedDate) {
      fields[LARK_SUPPLIER_FIELDS.MODIFIED_DATE] = new Date(
        supplier.modifiedDate,
      ).getTime();
    }

    return fields;
  }

  private safeBigIntToNumber(value: any): number {
    if (typeof value === 'bigint') {
      return Number(value);
    }
    if (typeof value === 'number') {
      return value;
    }
    if (typeof value === 'string') {
      const parsed = parseInt(value, 10);
      return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
  }

  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  private async updateDatabaseStatus(
    suppliers: any[],
    status: LarkSyncStatus,
  ): Promise<void> {
    if (suppliers.length === 0) return;

    try {
      const supplierIds = suppliers
        .map((s) => s.id)
        .filter((id) => id !== undefined);

      if (supplierIds.length > 0) {
        await this.prismaService.supplier.updateMany({
          where: { id: { in: supplierIds } },
          data: {
            larkSyncStatus: status,
            larkSyncedAt: new Date(),
          },
        });
      }
    } catch (error) {
      this.logger.error(`Failed to update database status: ${error.message}`);
    }
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getSupplierHeaders();
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
          this.logger.log(
            `📊 LarkBase table has ${totalRecords} existing records`,
          );
          return;
        }

        throw new Error(`Connection test failed: ${response.data.msg}`);
      } catch (error) {
        if (retryCount < maxRetries) {
          const delay = (retryCount + 1) * 2000;
          this.logger.warn(
            `⚠️ Connection attempt ${retryCount + 1} failed: ${error.message}`,
          );
          this.logger.log(`🔄 Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          this.logger.error(
            '❌ LarkBase connection test failed after all retries',
          );
          throw new Error(`Cannot connect to LarkBase: ${error.message}`);
        }
      }
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    const syncName = 'supplier_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: syncName,
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();

      if (lockAge < 10 * 60 * 1000) {
        const isProcessActive = await this.isLockProcessActive(existingLock);

        if (isProcessActive) {
          throw new Error('Another sync is already running');
        } else {
          this.logger.warn(
            `🔓 Clearing inactive lock (age: ${Math.round(lockAge / 1000)}s)`,
          );
          await this.forceReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['supplier'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
    });

    this.logger.debug(
      `🔒 Acquired sync lock: ${lockKey} (PID: ${process.pid})`,
    );
  }

  private async isLockProcessActive(lockRecord: any): Promise<boolean> {
    try {
      if (!lockRecord.progress?.processId) {
        return false;
      }

      const currentHostname = require('os').hostname();
      if (lockRecord.progress.hostname !== currentHostname) {
        return false;
      }

      const lockAge = Date.now() - lockRecord.startedAt.getTime();
      if (lockAge > 5 * 60 * 1000) {
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Could not verify lock process: ${error.message}`);
      return false;
    }
  }

  private async waitForLockAvailability(
    syncName: string,
    maxWaitMs: number = 30000,
  ): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const existingLock = await this.prismaService.syncControl.findFirst({
        where: { name: syncName, isRunning: true },
      });

      if (!existingLock) {
        return;
      }

      this.logger.debug(
        `⏳ Waiting for lock release... (${Math.round((Date.now() - startTime) / 1000)}s)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    throw new Error(`Lock wait timeout after ${maxWaitMs / 1000}s`);
  }

  private async forceReleaseLock(syncName: string): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: syncName },
      data: {
        isRunning: false,
        status: 'force_released',
        error: 'Lock force released due to inactivity',
        completedAt: new Date(),
        progress: {},
      },
    });
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'supplier_lark_sync',
        isRunning: true,
      },
    });

    if (
      lockRecord &&
      lockRecord.progress &&
      typeof lockRecord.progress === 'object' &&
      'lockKey' in lockRecord.progress &&
      lockRecord.progress.lockKey === lockKey
    ) {
      await this.prismaService.syncControl.update({
        where: { id: lockRecord.id },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: {},
        },
      });

      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    }
  }

  private async forceTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;
      await this.larkAuthService.getSupplierHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.supplierCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }
}
