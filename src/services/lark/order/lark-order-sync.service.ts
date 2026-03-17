import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_ORDER_FIELDS = {
  PRIMARY_CODE: 'Mã Đơn Hàng',
  KIOTVIET_ID: 'kiotVietId',
  BRANCH: 'Chi Nhánh',
  CUSTOMER_NAME: 'Tên Khách Hàng',
  CUSTOMER_CODE: 'Mã Khách Hàng',
  SELLER: 'Người Bán',
  CUSTOMER_NEED_PAY: 'Khách Cần Trả',
  CUSTOMER_PAID: 'Khách Đã Trả',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATIO: 'Mức Độ Giảm Giá (%)',
  STATUS: 'Tình Trạng',
  COMMENT: 'Ghi Chú',
  ORDER_DATE: 'Ngày Đặt Hàng',
  CREATED_DATE: 'Thời Gian Tạo',
  MODIFIED_DATE: 'Thời Gian Cập Nhật',
  SALE_CHANNEL: 'Kênh Bán',
} as const;

const BRANCH_OPTIONS = {
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
};

const STATUS_OPTIONS = {
  PHIEU_TAM: 'Phiếu Tạm',
  DANG_GIAO_HANG: 'Đang Giao Hàng',
  HOAN_THANH: 'Hoàn Thành',
  DA_HUY: 'Đã Hủy',
  DA_XAC_NHAN: 'Đã Xác Nhận',
};

const SALE_NAME = {
  LE_ANH_TUAN: 'Lê Anh Tuấn',
  NGUYEN_THI_PHUONG: 'Nguyễn Thị Phương',
  LINH_THUY_DUONG: 'Linh Thuỳ Dương',
  VU_HUYEN_TRANG: 'Vũ Huyền Trang',
  NGUYEN_THI_THUONG: 'Nguyễn Thị Thương',
  NGUYEN_THI_NGAN: 'Nguyễn Thị Ngân',
  NGUYEN_HUYEN_TRANG: 'Nguyễn Huyền Trang',
  MAI_THI_VAN_ANH: 'Mai Thị Vân Anh',
  BANG_ANH_VU: 'Bàng Anh Vũ',
  PHI_THI_PHUONG_THANH: 'Phí Thị Phương Thanh',
  LE_THI_HONG_LIEN: 'Lê Thị Hồng Liên',
  TRAN_XUAN_PHUONG: 'Trần Xuân Phương',
  DINH_THI_LY_LY: 'Đinh Thị Ly Ly',
  ADMIN: 'Admin',
  LE_XUAN_TUNG: 'Lê Xuân Tùng',
  TA_THI_TRANG: 'Tạ Thị Trang',
  LINH_THU_TRANG: 'Linh Thu Trang',
  LY_THI_HONG_DAO: 'Lý Thị Hồng Đào',
  NGUYEN_HUU_TOAN: 'Nguyễn Hữu Toàn',
  LE_BICH_NGOC: 'Lê Bích Ngọc',
  NGUYEN_THI_LOAN: 'Nguyễn Thị Loan',
  NGUYEN_VIET_NAM: 'Nguyễn Viết Nam',
  CUA_HANG_DIEP_TRA_ANH_TUAN: 'Cửa Hàng Diệp Trà Anh Tuấn',
  DO_THI_THUONG: 'Đỗ Thị Thương',
  NGUYEN_THI_BICH_NGOC: 'Nguyễn Thị Bích Ngọc',
  LE_BAO_NGAN: 'Lê Bảo Ngân',
  HUYNH_MAN_NHI: 'Huỳnh Mẫn Nhi',
  NGO_TRANG_NHUNG: 'Ngô Trang Nhung',
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

@Injectable()
export class LarkOrderSyncService {
  private readonly logger = new Logger(LarkOrderSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_ORDER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>('LARK_ORDER_SYNC_TABLE_ID');

    if (!baseToken || !tableId) {
      throw new Error('LarkBase order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncOrdersToLarkBase(orders: any[]): Promise<void> {
    const lockKey = `lark_order_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(`🚀 Starting batch sync for ${orders.length} orders...`);

      const ordersToSync = orders.filter(
        (o) => o.larkSyncStatus === 'PENDING' || o.larkSyncStatus === 'FAILED',
      );

      if (ordersToSync.length === 0) {
        this.logger.log('✅ No orders need sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      this.logger.log(
        `📊 Syncing ${ordersToSync.length} orders (PENDING + FAILED)`,
      );

      await this.testLarkBaseConnection();

      const BATCH_SIZE = 50;
      let totalSuccess = 0;
      let totalFailed = 0;

      for (let i = 0; i < ordersToSync.length; i += BATCH_SIZE) {
        const batch = ordersToSync.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(ordersToSync.length / BATCH_SIZE);

        this.logger.log(
          `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} orders)`,
        );

        for (const order of batch) {
          try {
            await this.syncSingleOrderDirect(order);
            totalSuccess++;
          } catch (error) {
            this.logger.error(
              `❌ Failed to sync order ${order.code}: ${error.message}`,
            );
            totalFailed++;
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        }

        if (i + BATCH_SIZE < ordersToSync.length) {
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

  async syncSingleOrderDirect(order: any): Promise<void> {
    try {
      if (this.shouldSkipSync(order.code)) {
        this.logger.log(`⏭️  Skipping test order: ${order.code}`);
        return;
      }

      this.logger.log(`🔄 Syncing order ${order.code} to Lark...`);

      const existingRecordId = await this.searchRecordByCode(order.code);

      const larkData = this.mapOrderToLarkBase(order);
      const headers = await this.larkAuthService.getOrderHeaders();

      if (existingRecordId) {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        await firstValueFrom(
          this.httpService.put(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Updated order ${order.code} in Lark`);
      } else {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Created order ${order.code} in Lark`);
      }

      await this.prismaService.order.update({
        where: { id: order.id },
        data: { larkSyncStatus: 'SYNCED', larkSyncedAt: new Date() },
      });
    } catch (error) {
      this.logger.error(`❌ Sync order ${order.code} failed: ${error.message}`);

      await this.prismaService.order.update({
        where: { id: order.id },
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
      const headers = await this.larkAuthService.getOrderHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            field_names: [LARK_ORDER_FIELDS.PRIMARY_CODE],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_ORDER_FIELDS.PRIMARY_CODE,
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
      this.logger.warn(`Search order by code failed: ${error.message}`);
      return null;
    }
  }

  private shouldSkipSync(code: string): boolean {
    if (!code) return false;
    const upperCode = code.toUpperCase();
    return upperCode.includes('SPE') || upperCode.includes('TTS');
  }

  private mapOrderToLarkBase(order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (order.code) {
      fields[LARK_ORDER_FIELDS.PRIMARY_CODE] = order.code;
    }

    if (order.kiotVietId !== null && order.kiotVietId !== undefined) {
      fields[LARK_ORDER_FIELDS.KIOTVIET_ID] = Number(order.kiotVietId);
    }

    if (order.branchId) {
      const branchMapping = {
        1: BRANCH_OPTIONS.CUA_HANG_DIEP_TRA,
        2: BRANCH_OPTIONS.KHO_HA_NOI,
        3: BRANCH_OPTIONS.KHO_SAI_GON,
        4: BRANCH_OPTIONS.VAN_PHONG_HA_NOI,
      };

      fields[LARK_ORDER_FIELDS.BRANCH] = branchMapping[order.branchId] || '';
    }

    if (order.soldById !== null && order.soldById !== undefined) {
      const sellerMapping = {
        1015579: SALE_NAME.ADMIN,
        1031177: SALE_NAME.DINH_THI_LY_LY,
        1015592: SALE_NAME.TRAN_XUAN_PHUONG,
        1015596: SALE_NAME.LE_THI_HONG_LIEN,
        1015604: SALE_NAME.PHI_THI_PHUONG_THANH,
        1015610: SALE_NAME.LE_XUAN_TUNG,
        1015613: SALE_NAME.TA_THI_TRANG,
        1015698: SALE_NAME.BANG_ANH_VU,
        1015722: SALE_NAME.MAI_THI_VAN_ANH,
        1015729: SALE_NAME.LINH_THU_TRANG,
        1015746: SALE_NAME.LY_THI_HONG_DAO,
        1015761: SALE_NAME.NGUYEN_HUYEN_TRANG,
        1015764: SALE_NAME.NGUYEN_THI_NGAN,
        1015777: SALE_NAME.NGUYEN_THI_THUONG,
        1015781: SALE_NAME.VU_HUYEN_TRANG,
        1015788: SALE_NAME.LINH_THUY_DUONG,
        1016818: SALE_NAME.NGUYEN_THI_PHUONG,
        383855: SALE_NAME.NGUYEN_HUU_TOAN,
        1032906: SALE_NAME.LE_BICH_NGOC,
        1032972: SALE_NAME.NGUYEN_THI_LOAN,
        1034030: SALE_NAME.NGUYEN_VIET_NAM,
        1030913: SALE_NAME.CUA_HANG_DIEP_TRA_ANH_TUAN,
        1034176: SALE_NAME.DO_THI_THUONG,
        1034250: SALE_NAME.NGUYEN_THI_BICH_NGOC,
        1034266: SALE_NAME.LE_BAO_NGAN,
        1033767: SALE_NAME.HUYNH_MAN_NHI,
        1042325: SALE_NAME.NGO_TRANG_NHUNG,
      };

      fields[LARK_ORDER_FIELDS.SELLER] = sellerMapping[order.soldById] || '';
    }

    if (order.customerCode) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_CODE] = order.customerCode;
    }

    if (order.saleChannelName) {
      fields[LARK_ORDER_FIELDS.SALE_CHANNEL] = order.saleChannelName || '';
    }

    if (order.customerName) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NAME] = order.customerName;
    }

    if (order.total !== null && order.total !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NEED_PAY] = Number(order.total || 0);
    }

    if (order.totalPayment !== null && order.totalPayment !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_PAID] = Number(order.totalPayment || 0);
    }

    if (order.discount !== null && order.discount !== undefined) {
      fields[LARK_ORDER_FIELDS.DISCOUNT] = Number(order.discount || 0);
    }

    if (order.discountRatio !== null && order.discountRatio !== undefined) {
      fields[LARK_ORDER_FIELDS.DISCOUNT_RATIO] = Number(
        order.discountRatio || 0,
      );
    }

    if (order.status) {
      const statusMapping = {
        1: STATUS_OPTIONS.PHIEU_TAM,
        2: STATUS_OPTIONS.DANG_GIAO_HANG,
        3: STATUS_OPTIONS.HOAN_THANH,
        4: STATUS_OPTIONS.DA_HUY,
        5: STATUS_OPTIONS.DA_XAC_NHAN,
      };

      fields[LARK_ORDER_FIELDS.STATUS] =
        statusMapping[order.status] || STATUS_OPTIONS.PHIEU_TAM;
    }

    if (order.description !== null && order.description !== undefined) {
      fields[LARK_ORDER_FIELDS.COMMENT] = order.description || '';
    }

    if (order.purchaseDate) {
      fields[LARK_ORDER_FIELDS.ORDER_DATE] = new Date(
        order.purchaseDate,
      ).getTime();
    }

    if (order.createdDate) {
      fields[LARK_ORDER_FIELDS.CREATED_DATE] = new Date(
        order.createdDate,
      ).getTime();
    }

    if (order.modifiedDate !== null && order.modifiedDate !== undefined) {
      fields[LARK_ORDER_FIELDS.MODIFIED_DATE] = new Date(
        order.modifiedDate,
      ).getTime();
    }

    return fields;
  }

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.order.count();
    const synced = await this.prismaService.order.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.order.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.order.count({
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

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getOrderHeaders();
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
    const syncName = 'order_lark_sync';

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
        entities: ['order'],
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
      if (!lockRecord.progress || typeof lockRecord.progress !== 'object') {
        return false;
      }

      const { processId, hostname } = lockRecord.progress;
      const currentHostname = require('os').hostname();

      if (hostname !== currentHostname) {
        return false;
      }

      if (!processId) {
        return false;
      }

      process.kill(processId, 0);
      return true;
    } catch (error) {
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
        name: 'order_lark_sync',
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
}
