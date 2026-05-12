// src/services/lark/order/lark-order-sync.service.ts

import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkBaseService } from '../lark-base.service';

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
  DAM_THI_HONG_NHA: 'Đàm Thị Hồng Nhã',
  HO_SI_PHU: 'Hồ Sĩ Phú',
};

const BRANCH_MAPPING: Record<number, string> = {
  1: BRANCH_OPTIONS.CUA_HANG_DIEP_TRA,
  2: BRANCH_OPTIONS.KHO_HA_NOI,
  3: BRANCH_OPTIONS.KHO_SAI_GON,
  4: BRANCH_OPTIONS.VAN_PHONG_HA_NOI,
};

const SELLER_MAPPING: Record<number, string> = {
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
  1062483: SALE_NAME.HO_SI_PHU,
  1062484: SALE_NAME.DAM_THI_HONG_NHA,
};

@Injectable()
export class LarkOrderSyncService {
  private readonly logger = new Logger(LarkOrderSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly MAX_RETRIES = 3;

  constructor(
    private readonly larkBase: LarkBaseService,
    private readonly prisma: PrismaService,
    private readonly config: ConfigService,
  ) {
    const baseToken = this.config.get<string>('LARK_ORDER_SYNC_BASE_TOKEN');
    const tableId = this.config.get<string>('LARK_ORDER_SYNC_TABLE_ID');

    if (!baseToken || !tableId) {
      throw new Error('LarkBase order configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  // ─── REAL-TIME SYNC ───────────────────────────────────────────────

  async syncSingle(orderId: number): Promise<void> {
    try {
      const order = await this.prisma.order.findUnique({
        where: { id: orderId },
      });

      if (!order) {
        this.logger.warn(`Order #${orderId} not found`);
        return;
      }

      if (this.shouldSkipSync(order.code)) {
        this.logger.debug(`Skip order: ${order.code}`);
        return;
      }

      const fields = this.mapOrderToLarkBase(order);
      let needSearchCreate = !order.larkRecordId;

      if (order.larkRecordId) {
        try {
          await this.larkBase.updateRecord(
            this.baseToken,
            this.tableId,
            order.larkRecordId,
            fields,
          );
        } catch (updateError) {
          if (this.isRecordNotFound(updateError)) {
            this.logger.warn(
              `⚠️ Record ${order.larkRecordId} not found on Lark, re-creating...`,
            );
            await this.prisma.order.update({
              where: { id: orderId },
              data: { larkRecordId: null },
            });
            needSearchCreate = true;
          } else {
            throw updateError;
          }
        }
      }

      if (needSearchCreate) {
        let recordId = await this.larkBase.searchRecord(
          this.baseToken,
          this.tableId,
          LARK_ORDER_FIELDS.PRIMARY_CODE,
          order.code,
        );

        if (recordId) {
          await this.larkBase.updateRecord(
            this.baseToken,
            this.tableId,
            recordId,
            fields,
          );
        } else {
          recordId = await this.larkBase.createRecord(
            this.baseToken,
            this.tableId,
            fields,
          );
        }

        await this.prisma.order.update({
          where: { id: orderId },
          data: { larkRecordId: recordId },
        });
      }

      await this.prisma.order.update({
        where: { id: orderId },
        data: {
          larkSyncStatus: 'SYNCED',
          larkSyncedAt: new Date(),
          larkSyncRetries: 0,
        },
      });

      this.logger.log(`✅ Synced order ${order.code}`);
    } catch (error) {
      this.logger.error(`❌ Sync order #${orderId} failed: ${error.message}`);

      const current = await this.prisma.order.findUnique({
        where: { id: orderId },
        select: { larkSyncRetries: true },
      });

      await this.prisma.order.update({
        where: { id: orderId },
        data: {
          larkSyncStatus:
            (current?.larkSyncRetries ?? 0) + 1 >= this.MAX_RETRIES
              ? 'FAILED'
              : 'PENDING',
          larkSyncRetries: { increment: 1 },
        },
      });
    }
  }

  syncSingleAsync(orderId: number): void {
    this.syncSingle(orderId).catch((err) => {
      this.logger.error(`Async sync order #${orderId} error: ${err.message}`);
    });
  }

  // ─── BATCH SYNC (CRON / MANUAL) ──────────────────────────────────

  async syncPendingAndFailed(): Promise<{ success: number; failed: number }> {
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    const orders = await this.prisma.order.findMany({
      where: {
        purchaseDate: { gte: threeMonthsAgo }, // sync_kiot_data dùng purchaseDate
        larkSyncStatus: { in: ['PENDING', 'FAILED'] },
      },
      orderBy: { purchaseDate: 'desc' },
    });

    const validOrders = orders.filter((o) => !this.shouldSkipSync(o.code));

    if (validOrders.length === 0) {
      this.logger.log('No orders need sync');
      return { success: 0, failed: 0 };
    }

    this.logger.log(`🔄 Syncing ${validOrders.length} orders...`);

    const toUpdate = validOrders.filter((o) => o.larkRecordId);
    let toCreate = validOrders.filter((o) => !o.larkRecordId);

    let success = 0;
    let failed = 0;

    // ── BATCH UPDATE ─────────────────────────────────────────────
    if (toUpdate.length > 0) {
      const allRecordIds = toUpdate.map((o) => o.larkRecordId!);
      const existingIds = await this.larkBase.verifyRecordIds(
        this.baseToken,
        this.tableId,
        allRecordIds,
      );

      const validToUpdate = toUpdate.filter((o) =>
        existingIds.has(o.larkRecordId!),
      );
      const staleOrders = toUpdate.filter(
        (o) => !existingIds.has(o.larkRecordId!),
      );

      // Stale: reset larkRecordId → đẩy sang toCreate để fetchAllRecords match lại
      if (staleOrders.length > 0) {
        this.logger.warn(
          `⚠️ ${staleOrders.length} stale records, resetting...`,
        );
        await this.prisma.order.updateMany({
          where: { id: { in: staleOrders.map((o) => o.id) } },
          data: { larkRecordId: null },
        });
        toCreate = [
          ...toCreate,
          ...staleOrders.map((o) => ({ ...o, larkRecordId: null })),
        ];
      }

      if (validToUpdate.length > 0) {
        try {
          await this.larkBase.batchUpdateRecords(
            this.baseToken,
            this.tableId,
            validToUpdate.map((o) => ({
              record_id: o.larkRecordId!,
              fields: this.mapOrderToLarkBase(o),
            })),
          );

          await this.prisma.order.updateMany({
            where: { id: { in: validToUpdate.map((o) => o.id) } },
            data: {
              larkSyncStatus: 'SYNCED',
              larkSyncedAt: new Date(),
              larkSyncRetries: 0,
            },
          });

          success += validToUpdate.length;
          this.logger.log(`✅ Batch updated ${validToUpdate.length} orders`);
        } catch (error) {
          this.logger.error(`❌ Batch update failed: ${error.message}`);
          failed += validToUpdate.length;
        }
      }
    }

    // ── SEARCH + CREATE ───────────────────────────────────────────
    if (toCreate.length > 0) {
      this.logger.log(
        `📝 Processing ${toCreate.length} orders (fetch all + match)...`,
      );

      // 1 lần fetch toàn bộ Lark records — O(1) API calls thay vì O(n)
      const larkCodeMap = await this.larkBase.fetchAllRecords(
        this.baseToken,
        this.tableId,
        LARK_ORDER_FIELDS.PRIMARY_CODE,
      );
      this.logger.log(`📋 Fetched ${larkCodeMap.size} records from Lark`);

      const toMatchUpdate: Array<{
        order: (typeof toCreate)[0];
        larkRecordId: string;
      }> = [];
      const reallyNew: typeof toCreate = [];

      for (const order of toCreate) {
        const existingId = larkCodeMap.get(order.code);
        if (existingId) {
          toMatchUpdate.push({ order, larkRecordId: existingId });
        } else {
          reallyNew.push(order);
        }
      }

      this.logger.log(
        `🔍 Matched: ${toMatchUpdate.length} existing, ${reallyNew.length} new`,
      );

      // Update matched + lưu larkRecordId vào DB
      if (toMatchUpdate.length > 0) {
        try {
          await this.larkBase.batchUpdateRecords(
            this.baseToken,
            this.tableId,
            toMatchUpdate.map((m) => ({
              record_id: m.larkRecordId,
              fields: this.mapOrderToLarkBase(m.order),
            })),
          );

          await this.prisma.$transaction(
            toMatchUpdate.map((m) =>
              this.prisma.order.update({
                where: { id: m.order.id },
                data: {
                  larkRecordId: m.larkRecordId,
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                  larkSyncRetries: 0,
                },
              }),
            ),
          );

          success += toMatchUpdate.length;
          this.logger.log(`✅ Matched updated ${toMatchUpdate.length} orders`);
        } catch (error) {
          this.logger.error(`❌ Matched update failed: ${error.message}`);
          failed += toMatchUpdate.length;
        }
      }

      // Create new records
      if (reallyNew.length > 0) {
        try {
          const newRecordIds = await this.larkBase.batchCreateRecords(
            this.baseToken,
            this.tableId,
            reallyNew.map((o) => ({ fields: this.mapOrderToLarkBase(o) })),
          );

          const updateOps = reallyNew
            .map((order, i) => {
              if (!newRecordIds[i]) return null;
              return this.prisma.order.update({
                where: { id: order.id },
                data: {
                  larkRecordId: newRecordIds[i],
                  larkSyncStatus: 'SYNCED',
                  larkSyncedAt: new Date(),
                  larkSyncRetries: 0,
                },
              });
            })
            .filter((op): op is NonNullable<typeof op> => op !== null);

          const DB_CHUNK = 100;
          for (let i = 0; i < updateOps.length; i += DB_CHUNK) {
            await this.prisma.$transaction(updateOps.slice(i, i + DB_CHUNK));
          }

          success += updateOps.length;
          failed += reallyNew.length - updateOps.length;
          this.logger.log(`✅ Batch created ${updateOps.length} orders`);
        } catch (error) {
          this.logger.error(`❌ Batch create failed: ${error.message}`);
          failed += reallyNew.length;
        }
      }
    }

    this.logger.log(`🎯 Sync done: ${success} success, ${failed} failed`);
    return { success, failed };
  }

  async fullSync(): Promise<{ success: number; failed: number }> {
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    const result = await this.prisma.order.updateMany({
      where: { purchaseDate: { gte: threeMonthsAgo } },
      data: {
        larkSyncStatus: 'PENDING',
        larkRecordId: null, // reset để toCreate path dùng fetchAllRecords thay vì verifyRecordIds
      },
    });

    this.logger.log(`📋 fullSync: marked ${result.count} orders as PENDING`);
    return this.syncPendingAndFailed();
  }

  // ─── FIELD MAPPING ───────────────────────────────────────────────

  private mapOrderToLarkBase(order: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (order.code) fields[LARK_ORDER_FIELDS.PRIMARY_CODE] = order.code;
    if (order.kiotVietId != null)
      fields[LARK_ORDER_FIELDS.KIOTVIET_ID] = Number(order.kiotVietId);
    if (order.branchId)
      fields[LARK_ORDER_FIELDS.BRANCH] = BRANCH_MAPPING[order.branchId] || '';
    if (order.soldById != null)
      fields[LARK_ORDER_FIELDS.SELLER] =
        SELLER_MAPPING[Number(order.soldById)] || '';
    if (order.customerCode)
      fields[LARK_ORDER_FIELDS.CUSTOMER_CODE] = order.customerCode;
    if (order.customerName)
      fields[LARK_ORDER_FIELDS.CUSTOMER_NAME] = order.customerName;
    if (order.saleChannelName)
      fields[LARK_ORDER_FIELDS.SALE_CHANNEL] = order.saleChannelName;
    if (order.total != null)
      fields[LARK_ORDER_FIELDS.CUSTOMER_NEED_PAY] = Number(order.total);
    if (order.totalPayment != null)
      fields[LARK_ORDER_FIELDS.CUSTOMER_PAID] = Number(order.totalPayment);
    if (order.discount != null)
      fields[LARK_ORDER_FIELDS.DISCOUNT] = Number(order.discount);
    if (order.discountRatio != null)
      fields[LARK_ORDER_FIELDS.DISCOUNT_RATIO] = order.discountRatio;
    if (order.statusValue) fields[LARK_ORDER_FIELDS.STATUS] = order.statusValue;
    if (order.description)
      fields[LARK_ORDER_FIELDS.COMMENT] = order.description;
    if (order.purchaseDate)
      fields[LARK_ORDER_FIELDS.ORDER_DATE] = new Date(
        order.purchaseDate,
      ).getTime();
    if (order.createdDate)
      fields[LARK_ORDER_FIELDS.CREATED_DATE] = new Date(
        order.createdDate,
      ).getTime();
    if (order.modifiedDate)
      fields[LARK_ORDER_FIELDS.MODIFIED_DATE] = new Date(
        order.modifiedDate,
      ).getTime();

    return fields;
  }

  private shouldSkipSync(code: string): boolean {
    if (!code) return false;
    const upper = code.toUpperCase();
    return upper.includes('SPE') || upper.includes('TTS');
  }

  private isRecordNotFound(error: any): boolean {
    const code = error?.code ?? error?.response?.data?.code ?? 0;
    return code === 1254040 || code === 404;
  }
}
