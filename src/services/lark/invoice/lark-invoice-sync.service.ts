import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_INVOICE_FIELDS = {
  INVOICE_CODE: 'Mã Hoá Đơn',
  ORDER_CODE: 'Mã Đơn Hàng',
  KIOTVIET_ID: 'kiotVietId',
  PURCHASE_DATE: 'Ngày Mua',
  BRANCH: 'Branch',
  SOLD_BY_NAME: 'Người Bán',
  TOTAL: 'Khách Cần Trả',
  TOTAL_PAYMENT: 'Khách Đã Trả',
  TOTAL_COST_OF_GOODS: 'Tổng Tiền Hàng',
  DISCOUNT: 'Giảm Giá',
  DISCOUNT_RATE: 'Mức Độ Giảm Giá',
  STATUS: 'Tình Trạng',
  COMMENT: 'Ghi Chú',
  USING_COD: 'Sử Dụng COD',
  SALE_CHANNEL_ID: 'Kênh Bán',
  VOUCHEr_APPLY: 'Áp Mã Voucher',
  CREATED_DATE: 'Ngày Tạo',
  MODIFIED_DATE: 'Ngày Cập Nhật',
};

const BRANCH_OPTIONS = {
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
};

const SOLD_NAME_OPTIONS = {
  LE_ANH_TUAN: 'Lê Anh Tuấn',
  NGUYEN_THI_PHUONG: 'Nguyễn Thị Phương',
  LINH_THUY_DUONG: 'Linh Thuỳ Dương',
  VU_HUYEN_TRANG: 'Vũ Huyền Trang',
  NGUYEN_THI_THUONG: 'Nguyễn Thị Thương',
  NGUYEN_THI_NGAN: 'Nguyễn Thị Ngân',
  NGUYEN_HUYEN_TRANG: 'Nguyễn Huyền Trang',
  MAI_THI_VAN_ANH: 'Mai Thị Vân Anh',
  BANG_ANH_VU: 'Bàng Anh Vũ',
  TA_THI_TRANG: 'Tạ Thị Trang',
  LE_XUAN_TUNG: 'Lê Xuân Tùng',
  PHI_THI_PHUONG_THANH: 'Phí Thị Phương Thanh',
  LE_THI_HONG_LIEN: 'Lê Thị Hồng Liên',
  TRAN_XUAN_PHUONG: 'Trần Xuân Phương',
  DINH_THI_LY_LY: 'Đinh Thị Ly Ly',
};

interface LarkBaseRecord {
  record_id?: string;
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
export class LarkInvoiceSyncService {
  private readonly logger = new Logger(LarkInvoiceSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly batchSize: number = 15;

  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];
  private readonly MAX_AUTH_RETRIES = 3;

  private existingRecordsCache: Map<number, string> = new Map();
  private invoiceCodeCache: Map<string, string> = new Map();
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
      'LARK_INVOICE_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_INVOICE_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase invoice configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    const existingLock = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'invoice_lark_sync',
        isRunning: true,
      },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();
      if (lockAge < 30 * 60 * 1000) {
        throw new Error('Another sync is already running');
      }
      this.logger.warn('🔓 Clearing stale lock');
    }

    await this.prismaService.syncControl.upsert({
      where: { name: 'invoice_lark_sync' },
      create: {
        name: 'invoice_lark_sync',
        entities: ['invoice'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: { lockKey },
      },
    });

    this.logger.debug(`🔒 Acquired sync lock: ${lockKey}`);
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'invoice_lark_sync',
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

  async syncInvoicesToLarkBase(invoices: any[]): Promise<void> {
    const lockKey = `lark_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting LarkBase sync for ${invoices.length} customers (IMPROVED MODE)...`,
      );

      const invoicesToSync = invoices.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (invoicesToSync.length === 0) {
        this.logger.log('📋 No customers need LarkBase sync');
        return;
      }

      const pendingCount = invoices.filter(
        (c) => c.larkSyncStatus === 'PENDING',
      ).length;
      const failedCount = invoices.filter(
        (c) => c.larkSyncStatus === 'FAILED',
      ).length;

      this.logger.log(
        `📊 Including: ${pendingCount} PENDING + ${failedCount} FAILED customers`,
      );

      await this.testLarkBaseConnection();
    } catch (error) {}
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 10;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getInvoiceHeaders();

        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 60000,
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
}
