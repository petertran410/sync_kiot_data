import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_PRODUCT_FIELDS = {
  PRIMARY_CODE: 'Mã Hàng Hoá',
  PRODUCT_ID: 'Id Hàng Hoá',
  CREATED_DATE: 'Ngày Tạo',
  MODIFIED_DATE: 'Ngày Cập Nhật',
  TRADEMARK: 'Thương Hiệu',
  PRODUCT_NAME: 'Tên Hàng Hoá',
  FULL_NAME: 'Tên Đầy Đủ',
  TYPE: 'Loại',
  ALLOWS_SALE: 'Cho Phép Bán',
  WEIGHT: 'Cân Nặng',
  UNIT: 'Đơn Vị',
  PRODUCT_BUSINESS: 'Hàng Kinh Doanh',
  BASE_PRICE: 'Bảng Giá Chung',
  DESCRIPTION: 'Mô Tả',
  SOURCE: 'Nguồn Gốc',
  PRODUCTS_TYPE: 'Loại Hàng',
  SUB_CATEGORY: 'Danh Mục',

  COST_PRICE_CUA_HANG_DIEP_TRA: 'Giá Vốn (Cửa Hàng Diệp Trà)',
  COST_PRICE_KHO_HA_NOI: 'Giá Vốn (Kho Hà Nội)',
  COST_PRICE_KHO_HA_NOI_2: 'Giá Vốn (Kho Hà Nội 2)',
  COST_PRICE_KHO_SAI_GON: 'Giá Vốn (Kho Sài Gòn)',
  COST_PRICE_VAN_PHONG_HA_NOI: 'Giá Vốn (Văn Phòng Hà Nội)',
  COST_PRICE_KHO_BAN_HANG: 'Giá Vốn (Kho Bán Hàng)',

  TON_KHO_CUA_HANG_DIEP_TRA: 'Tồn Kho (Cửa Hàng Diệp Trà)',
  TON_KHO_KHO_HA_NOI: 'Tồn Kho (Kho Hà Nội)',
  TON_KHO_KHO_HA_NOI_2: 'Tồn Kho (Kho Hà Nội 2)',
  TON_KHO_KHO_SAI_GON: 'Tồn Kho (Kho Sài Gòn)',
  TON_KHO_VAN_PHONG_HA_NOI: 'Tồn Kho (Văn Phòng Hà Nội)',
  TON_KHO_KHO_BAN_HANG: 'Tồn Kho (Kho Bán Hàng)',

  PRICE_HOC_VIEN_CAFE: 'Bảng Giá Học Viện Cafe',
  PRICE_LE_HCM: 'Bảng Giá Lẻ HCM',
  PRICE_DO_MINH_TAN: 'Bảng Giá Đỗ Minh Tân',
  PRICE_DO_MINH_TAN_8: 'Bảng Giá Đỗ Minh Tân 8%',
  PRICE_SHOPEE: 'Bảng Giá Shopee',
  PRICE_CHEESE_COFFEE: 'Bảng Giá Cheese Coffee',
  PRICE_CING_HU_TANG: 'Bảng Giá Cing Hu Tang',
  PRICE_CHIEN_LUOC: 'Bảng Giá Chiến Lược',
  PRICE_BUON_HN: 'Bảng Giá Buôn HN',
  PRICE_BUON_HCM: 'Bảng Giá Buôn HCM',
  PRICE_CHUOI_LABOONG: 'Bảng Giá Chuỗi Laboong',
  PRICE_CHUOI_SHANCHA: 'Bảng Giá Chuỗi ShanCha',
  PRICE_CONG_TAC_VIEN: 'Bảng Giá Cộng Tác Viên',
  PRICE_EM_HOAI_ROYALTEA: 'Bảng Giá Em Hoài RoyalTea',
  PRICE_KAFFA: 'Bảng Giá Kaffa',
  PRICE_LASIMI_SAI_GON: 'Bảng Giá Lasimi Sài Gòn',
  PRICE_DO_DO: 'Bảng giá chuỗi Đô Đô',
  PRICE_SUNDAY_BASIC: 'Bảng giá chuỗi Sunday Basic',
  PRICE_HADILAO: 'Bảng Giá Hadilao Việt Nam',
  PRICE_TRA_NON: 'Chuỗi Lá Trà Non',
  PRICE_HOANG_QUAN_HCM: 'Bảng Giá Hoàng Quân HCM',
  PRICE_HOC_VIEN_CAFE_HN: 'Bảng Giá Học Viện Cafe Hà Nội',
} as const;

const ALLOWS_SALE_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
} as const;

const PRODUCT_TYPE_OPTIONS = {
  REGULAR: 'Hàng Hoá',
  SERVICE: 'Dịch Vụ',
} as const;

const PRODUCT_BUSINESS_OPTIONS = {
  YES: 'Có',
  NO: 'Không',
};

const PRICEBOOK_FIELD_MAPPING: Record<number, string> = {
  1: LARK_PRODUCT_FIELDS.PRICE_LE_HCM,
  2: LARK_PRODUCT_FIELDS.PRICE_BUON_HCM,
  3: LARK_PRODUCT_FIELDS.PRICE_CHIEN_LUOC,
  4: LARK_PRODUCT_FIELDS.PRICE_LASIMI_SAI_GON,
  5: LARK_PRODUCT_FIELDS.PRICE_BUON_HN,
  6: LARK_PRODUCT_FIELDS.PRICE_EM_HOAI_ROYALTEA,
  7: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN,
  8: LARK_PRODUCT_FIELDS.PRICE_DO_MINH_TAN_8,
  10: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE,
  11: LARK_PRODUCT_FIELDS.PRICE_CHUOI_LABOONG,
  12: LARK_PRODUCT_FIELDS.PRICE_CONG_TAC_VIEN,
  14: LARK_PRODUCT_FIELDS.PRICE_CHEESE_COFFEE,
  15: LARK_PRODUCT_FIELDS.PRICE_CHUOI_SHANCHA,
  16: LARK_PRODUCT_FIELDS.PRICE_SHOPEE,
  17: LARK_PRODUCT_FIELDS.PRICE_KAFFA,
  18: LARK_PRODUCT_FIELDS.PRICE_CING_HU_TANG,
  19: LARK_PRODUCT_FIELDS.PRICE_DO_DO,
  20: LARK_PRODUCT_FIELDS.PRICE_SUNDAY_BASIC,
  21: LARK_PRODUCT_FIELDS.PRICE_HADILAO,
  22: LARK_PRODUCT_FIELDS.PRICE_TRA_NON,
  23: LARK_PRODUCT_FIELDS.PRICE_HOANG_QUAN_HCM,
  24: LARK_PRODUCT_FIELDS.PRICE_HOC_VIEN_CAFE_HN,
} as const;

const BRANCH_COST_MAPPING: Record<number, string> = {
  635934: LARK_PRODUCT_FIELDS.COST_PRICE_CUA_HANG_DIEP_TRA,
  154833: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI,
  402819: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_SAI_GON,
  631164: LARK_PRODUCT_FIELDS.COST_PRICE_VAN_PHONG_HA_NOI,
  631163: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_HA_NOI_2,
  635935: LARK_PRODUCT_FIELDS.COST_PRICE_KHO_BAN_HANG,
} as const;

const BRANCH_INVENTORY_MAPPING: Record<number, string> = {
  635934: LARK_PRODUCT_FIELDS.TON_KHO_CUA_HANG_DIEP_TRA,
  154833: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI,
  402819: LARK_PRODUCT_FIELDS.TON_KHO_KHO_SAI_GON,
  631164: LARK_PRODUCT_FIELDS.TON_KHO_VAN_PHONG_HA_NOI,
  631163: LARK_PRODUCT_FIELDS.TON_KHO_KHO_HA_NOI_2,
  635935: LARK_PRODUCT_FIELDS.TON_KHO_KHO_BAN_HANG,
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

@Injectable()
export class LarkProductSyncService {
  private readonly logger = new Logger(LarkProductSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;

  // In-memory lock to prevent concurrent syncs for the same product code
  private readonly productSyncLocks = new Map<string, Promise<void>>();

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_PRODUCT_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_PRODUCT_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase product configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ============================================================================
  // MAIN SYNC METHOD
  // ============================================================================

  async syncProductsToLarkBase(products: any[]): Promise<void> {
    const lockKey = `lark_product_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting batch sync for ${products.length} products...`,
      );

      const productsToSync = products.filter(
        (p) => p.larkSyncStatus === 'PENDING' || p.larkSyncStatus === 'FAILED',
      );

      if (productsToSync.length === 0) {
        this.logger.log('✅ No products need sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      this.logger.log(
        `📊 Syncing ${productsToSync.length} products (PENDING + FAILED)`,
      );

      await this.testLarkBaseConnection();

      const BATCH_SIZE = 50;
      let totalSuccess = 0;
      let totalFailed = 0;

      for (let i = 0; i < productsToSync.length; i += BATCH_SIZE) {
        const batch = productsToSync.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(productsToSync.length / BATCH_SIZE);

        this.logger.log(
          `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} products)`,
        );

        for (const product of batch) {
          try {
            await this.syncSingleProductDirect(product);
            totalSuccess++;
          } catch (error) {
            this.logger.error(
              `❌ Failed to sync product ${product.code}: ${error.message}`,
            );
            totalFailed++;
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        }

        if (i + BATCH_SIZE < productsToSync.length) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      this.logger.log('🎯 Batch sync completed!');
      this.logger.log(`✅ Success: ${totalSuccess}`);
      this.logger.log(`❌ Failed: ${totalFailed}`);
    } catch (error) {
      this.logger.error(`❌ Batch sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  /**
   * Sync a single product to Larkbase with locking to prevent duplicate creation
   * This method ensures only one sync operation runs per product code at a time
   */
  async syncSingleProductDirect(product: any): Promise<void> {
    const productCode = product.code;

    // Check if there's already a sync in progress for this product
    const existingLock = this.productSyncLocks.get(productCode);
    if (existingLock) {
      this.logger.log(
        `⏳ Product ${productCode} sync already in progress, waiting...`,
      );
      await existingLock;
      this.logger.log(`✅ Product ${productCode} sync completed by another process`);
      return;
    }

    // Create a new sync promise for this product
    const syncPromise = this.performProductSync(product);
    this.productSyncLocks.set(productCode, syncPromise);

    try {
      await syncPromise;
    } finally {
      // Always clean up the lock, even if sync failed
      this.productSyncLocks.delete(productCode);
    }
  }

  /**
   * Internal method that performs the actual product sync
   * Should only be called from syncSingleProductDirect with proper locking
   */
  private async performProductSync(product: any): Promise<void> {
    try {
      this.logger.log(`🔄 Syncing product ${product.code} to Lark...`);

      const existingRecordId = await this.searchRecordByCode(product.code);

      const larkData = this.mapProductToLarkBase(product);
      const headers = await this.larkAuthService.getProductHeaders();

      if (existingRecordId) {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        await firstValueFrom(
          this.httpService.put(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Updated product ${product.code} in Lark`);
      } else {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Created product ${product.code} in Lark`);
      }

      await this.prismaService.product.update({
        where: { id: product.id },
        data: { larkSyncStatus: 'SYNCED', larkSyncedAt: new Date() },
      });
    } catch (error) {
      this.logger.error(
        `❌ Sync product ${product.code} failed: ${error.message}`,
      );

      await this.prismaService.product.update({
        where: { id: product.id },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncRetries: { increment: 1 },
        },
      });

      throw error;
    }
  }

  private async searchRecordByCode(code: string): Promise<string | null> {
    try {
      const headers = await this.larkAuthService.getProductHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            field_names: [LARK_PRODUCT_FIELDS.PRIMARY_CODE],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_PRODUCT_FIELDS.PRIMARY_CODE,
                  operator: 'is',
                  value: [code],
                },
              ],
            },
          },
          {
            headers,
            timeout: 10000,
          },
        ),
      );

      if (response.data.code === 0) {
        const items = response.data.data?.items || [];
        if (items.length > 0) {
          return items[0].record_id;
        }
      }

      return null;
    } catch (error) {
      this.logger.warn(`Search product by code failed: ${error.message}`);
      return null;
    }
  }

  private mapProductToLarkBase(product: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (product.code) {
      fields[LARK_PRODUCT_FIELDS.PRIMARY_CODE] = product.code;
    }

    if (product.kiotVietId !== null && product.kiotVietId !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_ID] = Number(product.kiotVietId || 0);
    }

    if (product.description !== null && product.description !== undefined) {
      fields[LARK_PRODUCT_FIELDS.DESCRIPTION] = product.description;
    }

    if (product.createdDate) {
      fields[LARK_PRODUCT_FIELDS.CREATED_DATE] = new Date(
        product.createdDate,
      ).getTime();
    }

    if (product.modifiedDate) {
      fields[LARK_PRODUCT_FIELDS.MODIFIED_DATE] = new Date(
        product.modifiedDate,
      ).getTime();
    }

    if (product.tradeMarkName !== null && product.tradeMarkName !== undefined) {
      fields[LARK_PRODUCT_FIELDS.TRADEMARK] = product.tradeMarkName;
    }

    if (product.name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_NAME] = product.name;
    }

    if (product.fullName) {
      fields[LARK_PRODUCT_FIELDS.FULL_NAME] = product.fullName;
    }

    if (product.allowsSale !== null && product.allowsSale !== undefined) {
      fields[LARK_PRODUCT_FIELDS.ALLOWS_SALE] = product.allowsSale
        ? ALLOWS_SALE_OPTIONS.YES
        : ALLOWS_SALE_OPTIONS.NO;
    }

    if (product.isActive !== null && product.isActive !== undefined) {
      fields[LARK_PRODUCT_FIELDS.PRODUCT_BUSINESS] = product.isActive
        ? PRODUCT_BUSINESS_OPTIONS.YES
        : PRODUCT_BUSINESS_OPTIONS.NO;
    }

    if (product.type !== null && product.type !== undefined) {
      const typeMapping = {
        2: PRODUCT_TYPE_OPTIONS.REGULAR,
        3: PRODUCT_TYPE_OPTIONS.SERVICE,
      };
      fields[LARK_PRODUCT_FIELDS.TYPE] = typeMapping[product.type] || null;
    }

    if (product.priceBookDetails && product.priceBookDetails.length > 0) {
      for (const priceBookDetail of product.priceBookDetails) {
        const priceBookKiotVietId = priceBookDetail.priceBook?.kiotVietId;

        if (priceBookKiotVietId) {
          const larkField = PRICEBOOK_FIELD_MAPPING[priceBookKiotVietId];

          if (larkField && larkField !== 'undefined') {
            fields[larkField] = Number(priceBookDetail.price) || 0;
          }
        }
      }
    }

    if (product.inventories && product.inventories.length > 0) {
      for (const inventory of product.inventories) {
        const branchKiotVietId = inventory.branchKiotVietId;

        if (branchKiotVietId) {
          const costField = BRANCH_COST_MAPPING[branchKiotVietId];
          if (costField) {
            fields[costField] = Number(inventory.cost) || 0;
          }

          const inventoryField = BRANCH_INVENTORY_MAPPING[branchKiotVietId];
          if (inventoryField) {
            fields[inventoryField] = Number(inventory.onHand) || 0;
          }
        }
      }
    }

    if (product.basePrice) {
      fields[LARK_PRODUCT_FIELDS.BASE_PRICE] = Number(product.basePrice);
    }

    if (product.weight) {
      fields[LARK_PRODUCT_FIELDS.WEIGHT] = Number(product.weight) || null;
    }

    if (product.unit) {
      fields[LARK_PRODUCT_FIELDS.UNIT] = product.unit || null;
    }

    if (product.parent_name) {
      fields[LARK_PRODUCT_FIELDS.PRODUCTS_TYPE] = product.parent_name || null;
    }

    if (product.child_name) {
      fields[LARK_PRODUCT_FIELDS.SOURCE] = product.child_name || null;
    }

    if (product.branch_name) {
      fields[LARK_PRODUCT_FIELDS.SUB_CATEGORY] = product.branch_name || null;
    }

    return fields;
  }

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.product.count();
    const synced = await this.prismaService.product.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.product.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.product.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    return {
      total,
      synced,
      pending,
      failed,
      progress,
      canRetryFailed: failed > 0,
      summary: `${synced}/${total} synced (${progress}%)`,
    };
  }

  async retryFailedProductSyncs(): Promise<void> {
    this.logger.log('🔄 Retrying failed product syncs...');

    const failedProducts = await this.prismaService.product.findMany({
      where: {
        larkSyncStatus: 'FAILED',
        larkSyncRetries: { lt: 3 },
      },
      take: 100,
      include: {
        inventories: true,
        // priceBooks: true,
      },
    });

    if (failedProducts.length === 0) {
      this.logger.log('✅ No failed products to retry');
      return;
    }

    this.logger.log(`Found ${failedProducts.length} failed products to retry`);

    await this.prismaService.product.updateMany({
      where: { id: { in: failedProducts.map((p) => p.id) } },
      data: { larkSyncStatus: 'PENDING' },
    });

    await this.syncProductsToLarkBase(failedProducts);
  }

  async getProductSyncStats(): Promise<{
    pending: number;
    synced: number;
    failed: number;
    total: number;
  }> {
    const [pending, synced, failed, total] = await Promise.all([
      this.prismaService.product.count({
        where: { larkSyncStatus: 'PENDING' },
      }),
      this.prismaService.product.count({
        where: { larkSyncStatus: 'SYNCED' },
      }),
      this.prismaService.product.count({
        where: { larkSyncStatus: 'FAILED' },
      }),
      this.prismaService.product.count(),
    ]);

    return { pending, synced, failed, total };
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getProductHeaders();
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
            `⚠️  Connection attempt ${retryCount + 1} failed: ${error.message}`,
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
    const syncName = 'product_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: { name: syncName, isRunning: true },
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
        entities: ['product'],
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
      await new Promise((resolve) => setTimeout(resolve, 2000));
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
        name: 'product_lark_sync',
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
        where: {
          id: lockRecord.id,
        },
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
}
