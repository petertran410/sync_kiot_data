import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_TRANSFER_FIELDS = {
  KIOTVIET_ID: 'kiotVietId',
  TRANSFER_CODE: 'Mã Chuyển Hàng',
  STATUS: 'Trạng Thái',
  RECEIVE_DATE: 'Ngày Nhận',
  BRANCH_SEND: 'Chi Nhánh Chuyển',
  BRANCH_RECEIVE: 'Chi Nhánh Nhận',
  SEND_DATE: 'Ngày Gửi Đi',
  DESCRIPTION: 'Ghi Chú',
};

const LARK_TRANSFER_DETAIL_FIELDS = {
  ID_TRANSFER: 'Id Chuyển Hàng',
  ID_PRODUCT: 'Id Sản Phẩm',
  RECEIVE_QUANTITY: 'Số Lượng Sản Phẩm Nhận',
  PRICE: 'Đơn Giá Sản Phẩm',
  TOTAL_PRICE_SEND: 'Tổng Tiền Chuyển',
  TOTAL_PRICE_RECEIVE: 'Tổng Tiền Nhận',
  PRODUCT_CODE: 'Mã Sản Phẩm',
  PRODUCT_NAME: 'Tên Sản Phẩm',
  SEND_QUANTITY: 'Số Lượng Gửi Đi',
  UNIQUE_KEY: 'uniqueKey',
};

const STATUS_OPTION = {
  COMPLETED: 'Đã Nhận',
  PROCESSING: 'Phiếu Tạm',
  CANCELLED: 'Đã Hủy',
  DELIVERY: 'Đang Chuyển',
};

const BRANCH_OPTION = {
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
};

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

interface BatchDetailResult {
  successDetailsRecords: any[];
  failedDetailsRecords: any[];
}

@Injectable()
export class LarkTransferSyncService {
  private readonly logger = new Logger(LarkTransferSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();
  private readonly pendingDetailCreation = new Set<number>();

  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  private existingRecordsCache = new Map<number, string>();
  private transferCodeCache = new Map<string, string>();

  private existingDetailRecordsCache = new Map<string, string>();
  private transferDetailCodeCache = new Map<string, string>();

  private cacheLoaded = false;
  private cacheDetailLoaded = false;

  private lastCacheLoadTime: Date | null = null;
  private lastDetailCacheLoadTime: Date | null = null;

  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_TRANSFER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_TRANSFER_SYNC_TABLE_ID',
    );

    const baseTokenDetail = this.configService.get<string>(
      'LARK_TRANSFER_DETAIL_SYNC_BASE_TOKEN',
    );
    const tableIdDetail = this.configService.get<string>(
      'LARK_TRANSFER_DETAIL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId || !baseTokenDetail || !tableIdDetail) {
      throw new Error('LarkBase transfer configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
    this.baseTokenDetail = baseTokenDetail;
    this.tableIdDetail = tableIdDetail;
  }

  async syncTransferToLarkBase(transfers: any[]): Promise<void> {
    const lockKey = `lark_transfer_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `Starting LarkBase sync for ${transfers.length} transfers...`,
      );

      const transfersToSync = transfers.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (transfers.length === 0) {
        this.logger.log('No transfers need LarkBase sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      if (transfers.length < 5) {
        this.logger.log(
          `Small sync (${transfers.length} tranfers) - using lightweight mode`,
        );
        await this.syncWithoutCache(transfersToSync);
        await this.releaseDetailSyncLock(lockKey);
        return;
      }

      await this.testLarkBaseConnection();

      const cacheLoaded = await this.loadExistingRecordsWithRetry();

      if (!cacheLoaded) {
        this.logger.warn('Cache loading failed - using lightweight mode');
        await this.syncWithoutCache(transfersToSync);
        await this.releaseSyncLock(lockKey);
      }

      const { newTransfers, updateTransfers } =
        await this.categorizeTransfers(transfersToSync);

      this.logger.log(
        `Categorization: ${newTransfers.length} new, ${updateTransfers.length} updates`,
      );

      const BATCH_SIZE_FOR_SYNC = 500;

      if (newTransfers.length > 0) {
        for (let i = 0; i < newTransfers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = newTransfers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new transfers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newTransfers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewTransfers(batch);
        }
      }

      if (updateTransfers.length > 0) {
        for (let i = 0; i < updateTransfers.length; i += BATCH_SIZE_FOR_SYNC) {
          const batch = updateTransfers.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing update transfers batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newTransfers.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateTransfers(batch);
        }
      }

      this.logger.log('LarkBase transfer sync completed!');
    } catch (error) {
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  async syncTransferDetailsToLarkBase(): Promise<void> {
    this.logger.log('syncTransferDetailsToLarkBase called');
    const lockKey = `lark_transfer_detail_sync_lock_${Date.now()}`;

    try {
      await this.acquireDetailSyncLock(lockKey);

      this.logger.log(`Starting LarkBase sync for transfers_details`);

      const transferDetailsToSync =
        await this.prismaService.transferDetail.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      this.logger.log(
        `DEBUG: Found ${transferDetailsToSync.length} records with PENDING/FAILED status`,
      );

      if (transferDetailsToSync.length === 0) {
        this.logger.log('DEBUG: No records to sync - checking database status');
        const statusCounts = await this.prismaService.transferDetail.groupBy({
          by: ['larkSyncStatus'],
          _count: { larkSyncStatus: true },
        });
        this.logger.log('DEBUG: Current status distribution:', statusCounts);
        return;
      }

      if (transferDetailsToSync.length < 5) {
        this.logger.log(
          `Small sync (${transferDetailsToSync.length} transferDetailsToSync) - using lightweight mode`,
        );
        await this.syncWithoutDetailCache(transferDetailsToSync);
        return;
      }

      await this.testLarkBaseDetailConnection();

      const cacheDetailLoaded = await this.loadExistingDetailRecordsWithRetry();

      if (!cacheDetailLoaded) {
        await this.syncWithoutDetailCache(transferDetailsToSync);
        return;
      }

      const { newTransfersDetails, updateTransfersDetails } =
        await this.categorizeTransferDetails(transferDetailsToSync);

      const BATCH_SIZE_FOR_SYNC = 500;

      if (newTransfersDetails.length > 0) {
        for (
          let i = 0;
          i < newTransfersDetails.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = newTransfersDetails.slice(i, i + BATCH_SIZE_FOR_SYNC);
          this.logger.log(
            `Processing new transfer_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newTransfersDetails.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processNewTransferDetails(batch);
        }
      }

      if (updateTransfersDetails.length > 0) {
        for (
          let i = 0;
          i < updateTransfersDetails.length;
          i += BATCH_SIZE_FOR_SYNC
        ) {
          const batch = updateTransfersDetails.slice(
            i,
            i + BATCH_SIZE_FOR_SYNC,
          );
          this.logger.log(
            `Processing update transfer_details batch ${Math.floor(i / BATCH_SIZE_FOR_SYNC) + 1}/${Math.ceil(newTransfersDetails.length / BATCH_SIZE_FOR_SYNC)}`,
          );
          await this.processUpdateTransferDetails(batch);
        }
      }
    } catch (error) {
      throw error;
    } finally {
      await this.releaseDetailSyncLock(lockKey);
    }
  }

  private async loadExistingRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
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

  private async loadExistingDetailRecordsWithRetry(): Promise<boolean> {
    const maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.log(`Loading cache (attempt ${attempt}/${maxRetries})...`);

        if (
          this.isDetailCacheValid() &&
          this.existingDetailRecordsCache.size > 5000
        ) {
          this.logger.log(
            `Using cache detail available (${this.existingDetailRecordsCache.size} records) - skipping reload`,
          );
          return true;
        }

        if (this.lastDetailCacheLoadTime) {
          const detailCacheAgeMinutes =
            (Date.now() - this.lastDetailCacheLoadTime.getTime()) / (1000 * 60);
          if (
            detailCacheAgeMinutes < 45 &&
            this.existingDetailRecordsCache.size > 500
          ) {
            this.logger.log(
              `Recent detail cache (${detailCacheAgeMinutes.toFixed(1)}min old, ${this.existingDetailRecordsCache.size} records) - skipping reload`,
            );
            return true;
          }
        }

        this.clearDetailCache();
        await this.loadExistingDetailRecords();

        if (this.existingDetailRecordsCache.size > 0) {
          this.logger.log(
            `Cache loaded successfully: ${this.existingDetailRecordsCache.size} records`,
          );
          this.lastDetailCacheLoadTime = new Date();
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

  private isDetailCacheValid(): boolean {
    if (!this.cacheDetailLoaded || !this.lastDetailCacheLoadTime) {
      return false;
    }

    const cacheDetailAge = Date.now() - this.lastDetailCacheLoadTime.getTime();
    const maxAge = this.CACHE_VALIDITY_MINUTES * 60 * 1000;

    return cacheDetailAge < maxAge && this.existingDetailRecordsCache.size > 0;
  }

  private async loadExistingRecords(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getPurchaseOrderHeaders();
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
              record.fields[LARK_TRANSFER_FIELDS.KIOTVIET_ID];

            if (kiotVietIdField) {
              const kiotVietId = this.safeBigIntToNumber(kiotVietIdField);
              if (kiotVietId > 0) {
                this.existingRecordsCache.set(kiotVietId, record.record_id);
                cacheBuilt++;
              }
            }

            const transferCode =
              record.fields[LARK_TRANSFER_FIELDS.TRANSFER_CODE];
            if (transferCode) {
              this.transferCodeCache.set(
                String(transferCode).trim(),
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
        `Cache loaded: ${this.existingRecordsCache.size} by ID, ${this.transferCodeCache.size} by code (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(`Cache loading failed: ${error.message}`);
      throw error;
    }
  }

  private async loadExistingDetailRecords(): Promise<void> {
    try {
      const headers =
        await this.larkAuthService.getPurchaseOrderDetailHeaders();
      let pageToken: string | undefined;
      let totalLoaded = 0;
      let cacheDetailBuilt = 0;
      let stringConversions = 0;
      const pageSize = 500;

      do {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records`;

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
            const uniqueKeyRaw =
              record.fields[LARK_TRANSFER_DETAIL_FIELDS.UNIQUE_KEY];

            let uniqueKey = '';

            if (uniqueKeyRaw !== null && uniqueKeyRaw !== undefined) {
              if (typeof uniqueKeyRaw === 'string') {
                const trimmed = uniqueKeyRaw.trim();
                if (trimmed !== '') {
                  uniqueKey = trimmed;
                  stringConversions++;
                }
              } else if (typeof uniqueKeyRaw === 'number') {
                uniqueKey = String(uniqueKeyRaw).trim();
                if (uniqueKey !== '') {
                  stringConversions++;
                }
              }
            }

            if (uniqueKey && record.record_id) {
              this.existingDetailRecordsCache.set(uniqueKey, record.record_id);
              cacheDetailBuilt++;
            }
          }

          totalLoaded += records.length;
          pageToken = response.data.data?.page_token;

          if (totalLoaded % 1500 === 0 || !pageToken) {
            this.logger.log(
              `Cache progress: ${cacheDetailBuilt}/${totalLoaded} records processed (${stringConversions} string conversions) (${loadTime}ms/page)`,
            );
          }
        } else {
          throw new Error(
            `LarkBase API error: ${response.data.msg} (code: ${response.data.code})`,
          );
        }
      } while (pageToken);

      this.cacheDetailLoaded = true;

      const successRate =
        totalLoaded > 0
          ? Math.round((cacheDetailBuilt / totalLoaded) * 100)
          : 0;

      this.logger.log(
        `Cache loaded: ${this.existingDetailRecordsCache.size} records (${successRate}% success)`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to load existing detail records: ${error.message}`,
      );
      throw error;
    }
  }

  private async categorizeTransfers(transfers: any[]): Promise<any> {
    const newTransfers: any[] = [];
    const updateTransfers: any[] = [];

    for (const transfer of transfers) {
      const kiotVietId = transfer.kiotVietId
        ? typeof transfer.kiotVietId === 'bigint'
          ? Number(transfer.kiotVietId)
          : Number(transfer.kiotVietId)
        : 0;

      if (this.pendingCreation.has(kiotVietId)) {
        this.logger.warn(
          `Transfers ${kiotVietId} is pending creation, skipping`,
        );
        continue;
      }

      let existingRecordId = this.existingRecordsCache.get(kiotVietId);

      if (!existingRecordId && transfer.code) {
        existingRecordId = this.transferCodeCache.get(
          String(transfer.code).trim(),
        );
      }

      if (existingRecordId) {
        updateTransfers.push({
          ...transfer,
          larkRecordId: existingRecordId,
        });
      } else {
        newTransfers.push(transfer);
      }
    }

    return { newTransfers, updateTransfers };
  }

  private async categorizeTransferDetails(
    transfers_details: any[],
  ): Promise<any> {
    const newTransfersDetails: any[] = [];
    const updateTransfersDetails: any[] = [];

    const duplicateDetected = transfers_details.filter((transfers_detail) => {
      const uniqueKey = transfers_detail.uniqueKey;
      return this.existingDetailRecordsCache.has(uniqueKey);
    });

    if (duplicateDetected.length > 0) {
      this.logger.warn(
        `Detectd ${duplicateDetected.length} transfers_detail already in cache: ${duplicateDetected.map((o) => o.uniqueKey)}`,
      );
    }

    for (const transfers_detail of transfers_details) {
      const uniqueKey = transfers_detail.uniqueKey;

      if (this.pendingDetailCreation.has(uniqueKey)) {
        this.logger.warn(
          `Transfer Detail ${uniqueKey} is pending creation, skipping`,
        );
        continue;
      }

      let existingDetailRecordId =
        this.existingDetailRecordsCache.get(uniqueKey);

      if (existingDetailRecordId) {
        updateTransfersDetails.push({
          ...transfers_detail,
          larkRecordId: existingDetailRecordId,
        });
      } else {
        this.pendingDetailCreation.add(uniqueKey);
        newTransfersDetails.push(transfers_detail);
      }
    }

    return { newTransfersDetails, updateTransfersDetails };
  }

  private async syncWithoutCache(transfers: any[]): Promise<void> {
    const existingTransfers = await this.prismaService.transfer.findMany({
      where: { kiotVietId: { in: transfers.map((t) => t.kiotVietId) } },
      select: { kiotVietId: true, larkRecordId: true },
    });

    const quickCache = new Map<number, string>();
    existingTransfers.forEach((o) => {
      if (o.larkRecordId) {
        quickCache.set(Number(o.kiotVietId), o.larkRecordId);
      }
    });

    const originalCache = this.existingRecordsCache;
    this.existingRecordsCache = quickCache;

    try {
      const { newTransfers, updateTransfers } =
        await this.categorizeTransfers(transfers);

      if (newTransfers.length > 0) {
        await this.processNewTransfers(newTransfers);
      }

      if (updateTransfers.length > 0) {
        await this.processUpdateTransfers(updateTransfers);
      }
    } finally {
      this.existingRecordsCache = originalCache;
    }
  }

  private async syncWithoutDetailCache(transfers_detail: any[]): Promise<void> {
    const existingTransfersDetail =
      await this.prismaService.transferDetail.findMany({
        where: {
          uniqueKey: { in: transfers_detail.map((o) => o.uniqueKey) },
        },
        select: { uniqueKey: true, larkRecordId: true },
      });

    const quickDetailCache = new Map<string, string>();
    existingTransfersDetail.forEach((o) => {
      if (o.larkRecordId) {
        quickDetailCache.set(String(o.uniqueKey), o.larkRecordId);
      }
    });

    const originalDetailCache = this.existingDetailRecordsCache;
    this.existingDetailRecordsCache = quickDetailCache;

    try {
      const { newTransfersDetails, updateTransfersDetails } =
        await this.categorizeTransferDetails(transfers_detail);

      if (newTransfersDetails.length > 0) {
        await this.processNewTransferDetails(newTransfersDetails);
      }

      if (updateTransfersDetails.length > 0) {
        await this.processUpdateTransferDetails(updateTransfersDetails);
      }
    } finally {
      this.existingDetailRecordsCache = originalDetailCache;
    }
  }

  private async processNewTransfers(transfers: any[]): Promise<void> {
    if (transfers.length === 0) return;

    this.logger.log(`Creating ${transfers.length} new transfers...`);

    const batches = this.chunkArray(transfers, this.batchSize);
    let totalCreated = 0;
    let totalFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const transfer of batch) {
        const kiotVietId = this.safeBigIntToNumber(transfer.kiotVietId);
        if (!this.existingRecordsCache.has(kiotVietId)) {
          verifiedBatch.push(transfer);
        } else {
          this.logger.warn(
            `Skipping duplicate transfer ${kiotVietId} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        this.logger.log(`Batch ${i + 1} skipped - all transfer already exist`);
        continue;
      }
      this.logger.log(
        `Creating batch ${i + 1}/${batches.length} (${verifiedBatch.length} transfers)...`,
      );

      const { successRecords, failedRecords } =
        await this.batchCreateTransfers(verifiedBatch);

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

  private async processNewTransferDetails(details: any[]): Promise<void> {
    if (details.length === 0) return;

    const batches = this.chunkArray(details, this.batchSize);
    let totalDetailsCreated = 0;
    let totalDetailsFailed = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];

      const verifiedBatch: any[] = [];
      for (const transferDetail of batch) {
        const uniqueKey = transferDetail.uniqueKey;
        if (!this.existingDetailRecordsCache.has(uniqueKey)) {
          verifiedBatch.push(transferDetail);
        } else {
          this.logger.warn(
            `Skipping duplicate transfer_detail ${uniqueKey} in batch ${i + 1}`,
          );
        }
      }

      if (verifiedBatch.length === 0) {
        continue;
      }

      const { successDetailsRecords, failedDetailsRecords } =
        await this.batchCreateTransferDetails(verifiedBatch);

      totalDetailsCreated += successDetailsRecords.length;
      totalDetailsFailed += failedDetailsRecords.length;

      if (successDetailsRecords.length > 0) {
        await this.updateDetailDatabaseStatus(successDetailsRecords, 'SYNCED');

        successDetailsRecords.forEach((detail) => {
          this.pendingDetailCreation.delete(detail.uniqueKey);
        });
      }

      if (failedDetailsRecords.length > 0) {
        await this.updateDetailDatabaseStatus(failedDetailsRecords, 'FAILED');
      }
    }
  }

  private async processUpdateTransfers(transfers: any[]): Promise<void> {
    if (transfers.length === 0) return;

    let successCount = 0;
    let failedCount = 0;
    const createFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 20;

    for (let i = 0; i < transfers.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = transfers.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (transfer) => {
          try {
            const updated = await this.updateSingleTransfer(transfer);

            if (updated) {
              successCount++;
              await this.updateDatabaseStatus([transfer], 'SYNCED');
            } else {
              createFallbacks.push(transfer);
            }
          } catch (error) {
            createFallbacks.push(transfer);
          }
        }),
      );
    }

    if (createFallbacks.length > 0) {
      await this.processNewTransfers(createFallbacks);
    }
  }

  private async processUpdateTransferDetails(
    transfers_detail: any[],
  ): Promise<void> {
    if (transfers_detail.length === 0) return;

    let successDetailCount = 0;
    let failedDetailCount = 0;
    const createDetailFallbacks: any[] = [];

    const UPDATE_CHUNK_SIZE = 20;

    for (let i = 0; i < transfers_detail.length; i += UPDATE_CHUNK_SIZE) {
      const chunk = transfers_detail.slice(i, i + UPDATE_CHUNK_SIZE);

      await Promise.all(
        chunk.map(async (transfer_detail) => {
          try {
            const updated =
              await this.updateSingleTransferDetail(transfer_detail);

            if (updated) {
              successDetailCount++;
              await this.updateDetailDatabaseStatus(
                [transfer_detail],
                'SYNCED',
              );
            } else {
              createDetailFallbacks.push(transfer_detail);
            }
          } catch (error) {
            createDetailFallbacks.push(transfer_detail);
          }
        }),
      );
    }

    if (createDetailFallbacks.length > 0) {
      await this.processNewTransferDetails(createDetailFallbacks);
    }
  }

  private async batchCreateTransfers(transfers: any[]): Promise<BatchResult> {
    const records = transfers.map((transfer) => ({
      fields: this.mapTransferToLarkBase(transfer),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
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
          const successRecords = transfers.slice(0, successCount);
          const failedRecords = transfers.slice(successCount);

          for (
            let i = 0;
            i < Math.min(successRecords.length, createdRecords.length);
            i++
          ) {
            const transfer = successRecords[i];
            const createdRecord = createdRecords[i];

            const kiotVietId = this.safeBigIntToNumber(transfer.kiotVietId);
            if (kiotVietId > 0) {
              this.existingRecordsCache.set(
                kiotVietId,
                createdRecord.record_id,
              );
            }

            if (transfer.code) {
              this.transferCodeCache.set(
                String(transfer.code).trim(),
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

        return { successRecords: [], failedRecords: transfers };
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

        return { successRecords: [], failedRecords: transfers };
      }
    }

    return { successRecords: [], failedRecords: transfers };
  }

  private async batchCreateTransferDetails(
    details: any[],
  ): Promise<BatchDetailResult> {
    const records = details.map((detail) => ({
      fields: this.mapTransferDetailToLarkBase(detail),
    }));

    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/batch_create`;

        const response = await firstValueFrom(
          this.httpService.post<LarkBatchResponse>(
            url,
            { records },
            { headers, timeout: 30000 },
          ),
        );

        if (response.data.code === 0) {
          const createdDetailsRecords = response.data.data?.records || [];
          const successDetailCount = createdDetailsRecords.length;
          const successDetailsRecords = details.slice(0, successDetailCount);
          const failedDetailsRecords = details.slice(successDetailCount);

          for (
            let i = 0;
            i <
            Math.min(
              successDetailsRecords.length,
              createdDetailsRecords.length,
            );
            i++
          ) {
            const transfers_detail = successDetailsRecords[i];
            const createdDetailRecord = createdDetailsRecords[i];

            if (transfers_detail.uniqueKey) {
              this.existingDetailRecordsCache.set(
                transfers_detail.uniqueKey,
                createdDetailRecord.record_id,
              );
            }
          }

          return { successDetailsRecords, failedDetailsRecords };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceDetailTokenRefresh();
          await new Promise((resolve) => setTimeout(resolve, 500));
          continue;
        }

        return { successDetailsRecords: [], failedDetailsRecords: details };
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

        if (records && records.length > 0) {
          this.logger.error(
            'Sample record being sent:',
            JSON.stringify(records[0], null, 2),
          );
        }
        return { successDetailsRecords: [], failedDetailsRecords: details };
      }
    }

    return { successDetailsRecords: [], failedDetailsRecords: details };
  }

  private async updateSingleTransfer(transfers: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${transfers.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapTransferToLarkBase(transfers) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${transfers.larkRecordId} for transfers ${transfers.code}`,
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
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private async updateSingleTransferDetail(detail: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers =
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records/${detail.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapTransferDetailToLarkBase(detail) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `Updated record ${detail.larkRecordId} for transfer_detail ${detail.uniqueKey}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.forceDetailTokenRefresh();
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
          this.logger.warn(`Record not found: ${detail.larkRecordId}`);
          return false;
        }

        throw error;
      }
    }

    return false;
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getPurchaseOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data.code === 0) {
          return;
        }

        throw new Error(`Connection test failed: ${response.data.msg}`);
      } catch (error) {
        if (retryCount < maxRetries) {
          const delay = (retryCount + 1) * 1500;
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

  private async testLarkBaseDetailConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );
        const headers =
          await this.larkAuthService.getPurchaseOrderDetailHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseTokenDetail}/tables/${this.tableIdDetail}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 10000,
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
          const delay = (retryCount + 1) * 1500;
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
    const syncName = 'transfer_lark_sync';

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
        entities: ['transfer'],
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

  private async acquireDetailSyncLock(lockKey: string): Promise<void> {
    const syncName = 'transfer_detail_lark_sync';

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
          await this.forceDetailReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceDetailReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['transfer_detail'],
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

  private async forceDetailReleaseLock(syncName: string): Promise<void> {
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
        name: 'transfer_lark_sync',
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

  private async releaseDetailSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'transfer_detail_lark_sync',
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
      await this.larkAuthService.getPurchaseOrderHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async forceDetailTokenRefresh(): Promise<void> {
    try {
      this.logger.debug('🔄 Forcing LarkBase token refresh...');
      (this.larkAuthService as any).accessToken = null;
      (this.larkAuthService as any).tokenExpiry = null;
      await this.larkAuthService.getPurchaseOrderDetailHeaders();
      this.logger.debug('✅ LarkBase token refreshed successfully');
    } catch (error) {
      this.logger.error(`❌ Token refresh failed: ${error.message}`);
      throw error;
    }
  }

  private async updateDatabaseStatus(
    transfers: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    if (transfers.length === 0) return;

    const transferIds = transfers.map((c) => c.id);
    const updateData = {
      larkSyncStatus: status,
      larkSyncedAt: new Date(),
      ...(status === 'FAILED' && { larkSyncRetries: { increment: 1 } }),
      ...(status === 'SYNCED' && { larkSyncRetries: 0 }),
    };

    await this.prismaService.transfer.updateMany({
      where: { id: { in: transferIds } },
      data: updateData,
    });
  }

  private async updateDetailDatabaseStatus(
    details: any[],
    status: 'SYNCED' | 'FAILED',
  ): Promise<void> {
    const detailIds = details
      .map((detail) => detail.id)
      .filter((id) => id !== undefined);

    if (detailIds.length === 0) return;

    try {
      await this.prismaService.transferDetail.updateMany({
        where: { id: { in: detailIds } },
        data: {
          larkSyncStatus: status,
          larkSyncedAt: new Date(),
          larkSyncRetries: status === 'FAILED' ? { increment: 1 } : 0,
        },
      });
    } catch (error) {
      this.logger.error(
        `❌ Failed to update detail database status: ${error.message}`,
      );
    }
  }

  private clearCache(): void {
    this.existingRecordsCache.clear();
    this.transferCodeCache.clear();
    this.cacheLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.debug('🧹 Cache cleared');
  }

  clearDetailCache(): void {
    this.existingDetailRecordsCache.clear();
    this.transferDetailCodeCache.clear();
    this.cacheDetailLoaded = false;
    this.lastCacheLoadTime = null;
    this.logger.log('🗑️ Detail cache cleared');
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
      array.slice(i * size, i * size + size),
    );
  }

  private safeBigIntToNumber(value: any): number {
    if (value === null || value === undefined) return 0;

    if (typeof value === 'bigint') {
      return Number(value);
    }

    if (typeof value === 'number') {
      return Math.floor(value);
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return 0;
      const parsed = parseInt(trimmed, 10);
      return isNaN(parsed) ? 0 : parsed;
    }

    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    try {
      const asString = String(value).trim();
      const parsed = parseInt(asString, 10);
      return isNaN(parsed) ? 0 : parsed;
    } catch {
      return 0;
    }
  }

  private mapTransferToLarkBase(transfers: any): Record<string, any> {
    const fields: Record<string, any> = {};

    fields[LARK_TRANSFER_FIELDS.KIOTVIET_ID] = this.safeBigIntToNumber(
      transfers.kiotVietId,
    );

    fields[LARK_TRANSFER_FIELDS.TRANSFER_CODE] = transfers.code;

    if (transfers.status) {
      const statusMapping = {
        1: STATUS_OPTION.PROCESSING,
        2: STATUS_OPTION.DELIVERY,
        3: STATUS_OPTION.COMPLETED,
        4: STATUS_OPTION.CANCELLED,
      };
      if (transfers.status) {
        fields[LARK_TRANSFER_FIELDS.STATUS] =
          statusMapping[transfers.status] || STATUS_OPTION.COMPLETED;
      }
    }

    if (transfers.receivedDate) {
      fields[LARK_TRANSFER_FIELDS.RECEIVE_DATE] = new Date(
        transfers.receivedDate,
      ).getTime();
    }

    if (
      transfers.fromBranchId !== null ||
      transfers.fromBranchId !== undefined
    ) {
      const branchMapping = {
        635934: BRANCH_OPTION.CUA_HANG_DIEP_TRA,
        154833: BRANCH_OPTION.KHO_HA_NOI,
        402819: BRANCH_OPTION.KHO_SAI_GON,
        631163: BRANCH_OPTION.VAN_PHONG_HA_NOI,
      };

      fields[LARK_TRANSFER_FIELDS.BRANCH_SEND] =
        branchMapping[transfers.fromBranchId];
    }

    if (transfers.toBranchId !== null || transfers.toBranchId !== undefined) {
      const branchMapping = {
        635934: BRANCH_OPTION.CUA_HANG_DIEP_TRA,
        154833: BRANCH_OPTION.KHO_HA_NOI,
        402819: BRANCH_OPTION.KHO_SAI_GON,
        631163: BRANCH_OPTION.VAN_PHONG_HA_NOI,
      };

      fields[LARK_TRANSFER_FIELDS.BRANCH_RECEIVE] =
        branchMapping[transfers.toBranchId];
    }

    if (transfers.description) {
      fields[LARK_TRANSFER_FIELDS.DESCRIPTION] = transfers.description;
    }

    if (transfers.dispatchedDate) {
      fields[LARK_TRANSFER_FIELDS.SEND_DATE] = new Date(
        transfers.dispatchedDate,
      ).getTime();
    }

    return fields;
  }

  private mapTransferDetailToLarkBase(detail: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (!detail) {
      return fields;
    }

    if (detail.uniqueKey) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.UNIQUE_KEY] = detail.uniqueKey;
    }

    if (detail.transferId !== null || detail.transferId !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.ID_TRANSFER] = detail.transferId;
    }

    if (detail.productId !== null || detail.productId !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.ID_PRODUCT] = detail.productId;
    }

    if (
      detail.receivedQuantity !== null ||
      detail.receivedQuantity !== undefined
    ) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.RECEIVE_QUANTITY] = Number(
        detail.receivedQuantity,
      );
    }

    if (detail.price !== null || detail.price !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.PRICE] = Number(detail.price);
    }

    if (detail.sendPrice !== null || detail.sendPrice !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.TOTAL_PRICE_SEND] = Number(
        detail.sendPrice,
      );
    }

    if (detail.receivePrice !== null || detail.receivePrice !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.TOTAL_PRICE_RECEIVE] = Number(
        detail.receivePrice,
      );
    }

    if (detail.productCode !== null || detail.productCode !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.PRODUCT_CODE] = detail.productCode;
    }

    if (detail.productName !== null || detail.productName !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.PRODUCT_NAME] = detail.productName;
    }

    if (detail.sendQuantity !== null || detail.sendQuantity !== undefined) {
      fields[LARK_TRANSFER_DETAIL_FIELDS.SEND_QUANTITY] = Number(
        detail.sendQuantity,
      );
    }

    return fields;
  }
}
