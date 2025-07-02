// src/services/lark/order/lark-order-sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

// ✅ EXACT field names from Đơn Hàng.rtf
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
};

interface LarkBaseRecord {
  record_id?: string;
  fields: Record<string, any>;
}

interface LarkResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{ record_id: string; fields: any }>;
    total?: number;
    has_more?: boolean;
    page_token?: string;
  };
}

@Injectable()
export class LarkOrderSyncService {
  private readonly logger = new Logger(LarkOrderSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly tableView: string;
  private readonly BATCH_SIZE = 500;
  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    this.baseToken = this.configService.get<string>(
      'LARK_ORDER_SYNC_BASE_TOKEN',
    );
    this.tableId = this.configService.get<string>('LARK_ORDER_SYNC_TABLE_ID');
    this.tableView = this.configService.get<string>(
      'LARK_ORDER_SYNC_TABLE_VIEW',
    );

    if (!this.baseToken || !this.tableId || !this.tableView) {
      throw new Error('Lark Order sync configuration missing');
    }
  }

  // ============================================================================
  // MAIN SYNC METHODS
  // ============================================================================

  async syncOrdersToLark(): Promise<void> {
    try {
      this.logger.log('🚀 Starting order sync to LarkBase...');

      // Get pending orders
      const pendingOrders = await this.prismaService.order.findMany({
        where: {
          larkSyncStatus: 'PENDING',
        },
        orderBy: { id: 'asc' },
        take: this.BATCH_SIZE,
      });

      if (pendingOrders.length === 0) {
        this.logger.log('✅ No pending orders to sync');
        return;
      }

      this.logger.log(`📦 Found ${pendingOrders.length} orders to sync`);

      // Check for existing records in Lark
      const existingRecords = await this.fetchExistingOrderRecords();
      const existingOrderCodes = new Set(
        existingRecords.map(
          (record) => record.fields[LARK_ORDER_FIELDS.PRIMARY_CODE],
        ),
      );

      // Separate new and existing orders
      const newOrders = pendingOrders.filter(
        (order) => !existingOrderCodes.has(order.code),
      );
      const existingOrders = pendingOrders.filter((order) =>
        existingOrderCodes.has(order.code),
      );

      this.logger.log(
        `📝 New orders: ${newOrders.length}, Existing orders: ${existingOrders.length}`,
      );

      // Process new orders
      if (newOrders.length > 0) {
        await this.createOrdersInBatches(newOrders);
      }

      // Process existing orders
      if (existingOrders.length > 0) {
        await this.updateExistingOrders(existingOrders, existingRecords);
      }

      this.logger.log('🎉 Order sync to LarkBase completed!');
    } catch (error) {
      this.logger.error(`💥 Order sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // CREATE NEW ORDERS
  // ============================================================================

  private async createOrdersInBatches(orders: any[]): Promise<void> {
    const batchSize = 500;

    for (let i = 0; i < orders.length; i += batchSize) {
      const batch = orders.slice(i, i + batchSize);
      this.logger.log(
        `📝 Creating orders batch ${Math.floor(i / batchSize) + 1}: ${batch.length} orders`,
      );

      const result = await this.createOrderBatch(batch);

      // Update sync status
      const successIds = result.successRecords
        .map(
          (record) =>
            batch.find(
              (order) =>
                order.code === record.fields[LARK_ORDER_FIELDS.PRIMARY_CODE],
            )?.id,
        )
        .filter(Boolean);

      if (successIds.length > 0) {
        await this.updateOrderSyncStatus(successIds, 'SUCCESS');
      }

      const failedIds = result.failedRecords.map((order) => order.id);
      if (failedIds.length > 0) {
        await this.updateOrderSyncStatus(failedIds, 'FAILED');
      }

      this.logger.log(
        `✅ Batch completed: ${result.successRecords.length} success, ${result.failedRecords.length} failed`,
      );

      // Add delay between batches
      if (i + batchSize < orders.length) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async createOrderBatch(
    orders: any[],
  ): Promise<{ successRecords: any[]; failedRecords: any[] }> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/batch_create`;

        const records = orders.map((order) => ({
          fields: this.mapOrderToLarkBase(order),
        }));

        const response = await firstValueFrom(
          this.httpService.post(url, { records }, { headers, timeout: 30000 }),
        );

        if (response.data.code === 0) {
          // Set larkRecordId for successful records
          const successRecords = response.data.data.records || [];
          for (let i = 0; i < successRecords.length && i < orders.length; i++) {
            const order = orders[i];
            const record = successRecords[i];
            if (record.record_id) {
              await this.prismaService.order.update({
                where: { id: order.id },
                data: { larkRecordId: record.record_id },
              });
            }
          }

          this.logger.debug(
            `✅ Created ${successRecords.length} order records in LarkBase`,
          );
          return { successRecords, failedRecords: [] };
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(
          `⚠️ Batch create failed: ${response.data.msg} (Code: ${response.data.code})`,
        );
        return { successRecords: [], failedRecords: orders };
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`❌ Batch create error: ${error.message}`);
        return { successRecords: [], failedRecords: orders };
      }
    }

    return { successRecords: [], failedRecords: orders };
  }

  // ============================================================================
  // UPDATE EXISTING ORDERS
  // ============================================================================

  private async updateExistingOrders(
    orders: any[],
    existingRecords: any[],
  ): Promise<void> {
    for (const order of orders) {
      const existingRecord = existingRecords.find(
        (record) =>
          record.fields[LARK_ORDER_FIELDS.PRIMARY_CODE] === order.code,
      );

      if (existingRecord) {
        const success = await this.updateSingleOrder({
          ...order,
          larkRecordId: existingRecord.record_id,
        });

        await this.updateOrderSyncStatus(
          [order.id],
          success ? 'SUCCESS' : 'FAILED',
        );
      }
    }
  }

  private async updateSingleOrder(order: any): Promise<boolean> {
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${order.larkRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: this.mapOrderToLarkBase(order) },
            { headers, timeout: 15000 },
          ),
        );

        if (response.data.code === 0) {
          this.logger.debug(
            `✅ Updated record ${order.larkRecordId} for order ${order.code}`,
          );
          return true;
        }

        if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.warn(`Update failed: ${response.data.msg}`);
        return false;
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
          continue;
        }

        this.logger.error(`Update error: ${error.message}`);
        return false;
      }
    }

    return false;
  }

  // ============================================================================
  // FETCH EXISTING RECORDS
  // ============================================================================

  private async fetchExistingOrderRecords(): Promise<any[]> {
    const allRecords: any[] = [];
    let pageToken: string | undefined;
    let authRetries = 0;

    while (authRetries < this.MAX_AUTH_RETRIES) {
      try {
        const headers = await this.larkAuthService.getOrderHeaders();
        let url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records?page_size=500`;

        if (pageToken) {
          url += `&page_token=${pageToken}`;
        }

        const response = await firstValueFrom(
          this.httpService.get(url, { headers, timeout: 30000 }),
        );

        if (response.data.code === 0) {
          const records = response.data.data.records || [];
          allRecords.push(...records);

          if (response.data.data.has_more) {
            pageToken = response.data.data.page_token;
            await new Promise((resolve) => setTimeout(resolve, 500));
          } else {
            break;
          }

          authRetries = 0; // Reset auth retries on success
        } else if (this.AUTH_ERROR_CODES.includes(response.data.code)) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
        } else {
          this.logger.error(`Failed to fetch records: ${response.data.msg}`);
          break;
        }
      } catch (error) {
        if (error.response?.status === 401 || error.response?.status === 403) {
          authRetries++;
          await this.larkAuthService.forceRefreshOrderToken();
          await new Promise((resolve) => setTimeout(resolve, 2000));
        } else {
          this.logger.error(`Fetch error: ${error.message}`);
          break;
        }
      }
    }

    this.logger.log(
      `📄 Fetched ${allRecords.length} existing order records from LarkBase`,
    );
    return allRecords;
  }

  // ============================================================================
  // MAPPING ORDER TO LARKBASE FIELDS
  // ============================================================================

  private mapOrderToLarkBase(order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // Primary field - Mã Đơn Hàng
    if (order.code) {
      fields[LARK_ORDER_FIELDS.PRIMARY_CODE] = order.code;
    }

    // KiotViet ID
    if (order.kiotVietId !== null && order.kiotVietId !== undefined) {
      fields[LARK_ORDER_FIELDS.KIOTVIET_ID] = Number(order.kiotVietId);
    }

    // Branch mapping
    if (order.branchId !== null && order.branchId !== undefined) {
      const branchMapping = {
        1: BRANCH_OPTIONS.VAN_PHONG_HA_NOI,
        2: BRANCH_OPTIONS.KHO_HA_NOI,
        3: BRANCH_OPTIONS.KHO_SAI_GON,
        4: BRANCH_OPTIONS.CUA_HANG_DIEP_TRA,
      };

      fields[LARK_ORDER_FIELDS.BRANCH] = branchMapping[order.branchId] || '';
    }

    // Seller mapping
    if (order.soldById !== null && order.soldById !== undefined) {
      const sellerMapping = {
        1015650: SALE_NAME.LE_ANH_TUAN,
        1015652: SALE_NAME.NGUYEN_THI_PHUONG,
        1015746: SALE_NAME.LY_THI_HONG_DAO,
        1015761: SALE_NAME.NGUYEN_HUYEN_TRANG,
        1015764: SALE_NAME.NGUYEN_THI_NGAN,
        1015777: SALE_NAME.NGUYEN_THI_THUONG,
        1015781: SALE_NAME.VU_HUYEN_TRANG,
        1015788: SALE_NAME.LINH_THUY_DUONG,
        1016818: SALE_NAME.NGUYEN_THI_PHUONG,
        1234567: SALE_NAME.MAI_THI_VAN_ANH,
        1234568: SALE_NAME.BANG_ANH_VU,
        1234569: SALE_NAME.TA_THI_TRANG,
        1234570: SALE_NAME.LE_XUAN_TUNG,
        1234571: SALE_NAME.PHI_THI_PHUONG_THANH,
        1234572: SALE_NAME.LE_THI_HONG_LIEN,
        1234573: SALE_NAME.TRAN_XUAN_PHUONG,
        1234574: SALE_NAME.DINH_THI_LY_LY,
        1234575: SALE_NAME.NGUYEN_HUU_TOAN,
      };

      fields[LARK_ORDER_FIELDS.SELLER] = sellerMapping[order.soldById] || '';
    }

    // Customer fields
    if (order.customerCode !== null && order.customerCode !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_CODE] = order.customerCode || '';
    }

    if (order.customerName !== null && order.customerName !== undefined) {
      fields[LARK_ORDER_FIELDS.CUSTOMER_NAME] = order.customerName || '';
    }

    // Financial fields
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
      fields[LARK_ORDER_FIELDS.DISCOUNT_RATIO] =
        Number(order.discountRatio || 0) / 100; // Convert to percentage for LarkBase
    }

    // Status mapping
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

    // Comment
    if (order.description !== null && order.description !== undefined) {
      fields[LARK_ORDER_FIELDS.COMMENT] = order.description || '';
    }

    // Date fields
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

    if (order.modifiedDate) {
      fields[LARK_ORDER_FIELDS.MODIFIED_DATE] = new Date(
        order.modifiedDate,
      ).getTime();
    }

    return fields;
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  private async updateOrderSyncStatus(
    orderIds: number[],
    status: 'SUCCESS' | 'FAILED',
  ): Promise<void> {
    await this.prismaService.order.updateMany({
      where: { id: { in: orderIds } },
      data: {
        larkSyncStatus: status,
        larkSyncedAt: status === 'SUCCESS' ? new Date() : undefined,
        larkSyncRetries: status === 'FAILED' ? { increment: 1 } : undefined,
      },
    });
  }

  async retryFailedOrderSyncs(): Promise<void> {
    this.logger.log('🔄 Retrying failed order syncs...');

    const failedOrders = await this.prismaService.order.findMany({
      where: {
        larkSyncStatus: 'FAILED',
        larkSyncRetries: { lt: 3 },
      },
      take: 100,
    });

    if (failedOrders.length === 0) {
      this.logger.log('✅ No failed orders to retry');
      return;
    }

    // Reset to PENDING to trigger sync
    await this.prismaService.order.updateMany({
      where: { id: { in: failedOrders.map((o) => o.id) } },
      data: { larkSyncStatus: 'PENDING' },
    });

    await this.syncOrdersToLark();
  }

  async getOrderSyncStats(): Promise<{
    pending: number;
    success: number;
    failed: number;
    total: number;
  }> {
    const [pending, success, failed, total] = await Promise.all([
      this.prismaService.order.count({ where: { larkSyncStatus: 'PENDING' } }),
      this.prismaService.order.count({ where: { larkSyncStatus: 'SUCCESS' } }),
      this.prismaService.order.count({ where: { larkSyncStatus: 'FAILED' } }),
      this.prismaService.order.count(),
    ]);

    return { pending, success, failed, total };
  }
}
