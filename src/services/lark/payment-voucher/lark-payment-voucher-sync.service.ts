import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_VOUCHER_FIELDS = {
  PRIMARY_CODE: 'Mã Phiếu Thu',
  AMOUNT: 'Số Tiền',
  METHOD: 'Phương Thức Thanh Toán',
  STATUS: 'Trạng Thái',
  ORDER_CODE: 'Mã Đơn Hàng',
  USE_DATE: 'Ngày Sử Dụng',
  KIOTVIET_ID: 'kiotVietId',
  TRANS_DATE: 'Ngày Giao Dịch',
  DESCRIPTION: 'Ghi Chú',
  ORDER_ID: 'OrderId',
  PAYMENT_ID: 'Id Payment',
  ORDER_KIOTVIET_ID: 'kiotVietId Từ Order',
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
export class LarkPaymentVoucherSyncService {
  private readonly logger = new Logger(LarkPaymentVoucherSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_VOUCHER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_VOUCHER_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase voucher configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncSinglePaymentVoucherDirect(
    payment: any,
    order: any,
  ): Promise<void> {
    try {
      if (payment.method !== 'Voucher') {
        this.logger.log(
          `⏭️  Skipping non-voucher payment: ${payment.code} (method: ${payment.method})`,
        );
        return;
      }

      this.logger.log(`🔄 Syncing voucher payment ${payment.code} to Lark...`);

      const existingRecordId = await this.searchRecordByPaymentId(payment.id);

      const larkData = this.mapPaymentVoucherToLarkBase(payment, order);

      this.logger.debug(`📤 Data being sent to Lark:`);
      this.logger.debug(JSON.stringify(larkData, null, 2));

      const headers = await this.larkAuthService.getVoucherHeaders();

      if (existingRecordId) {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.debug(`📥 Lark API Response (UPDATE):`);
        this.logger.debug(JSON.stringify(response.data, null, 2));

        this.logger.log(`✅ Updated voucher payment ${payment.code} in Lark`);
      } else {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const response = await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.debug(`📥 Lark API Response (CREATE):`);
        this.logger.debug(JSON.stringify(response.data, null, 2));

        if (response.data.code !== 0) {
          this.logger.error(
            `❌ Lark API returned error code: ${response.data.code}`,
          );
          this.logger.error(`❌ Error message: ${response.data.msg}`);
          throw new Error(`Lark API error: ${response.data.msg}`);
        }

        this.logger.log(`✅ Created voucher payment ${payment.code} in Lark`);
      }
    } catch (error) {
      this.logger.error(
        `❌ Sync voucher payment ${payment.code} failed: ${error.message}`,
      );

      if (error.response) {
        this.logger.error(`📥 Error response from Lark:`);
        this.logger.error(JSON.stringify(error.response.data, null, 2));
      }

      throw error;
    }
  }

  private async searchRecordByPaymentId(
    paymentId: number,
  ): Promise<string | null> {
    try {
      const headers = await this.larkAuthService.getVoucherHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            field_names: [LARK_VOUCHER_FIELDS.PAYMENT_ID],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_VOUCHER_FIELDS.PAYMENT_ID,
                  operator: 'is',
                  value: [paymentId.toString()],
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
      this.logger.warn(`Search voucher payment by ID failed: ${error.message}`);
      return null;
    }
  }

  private mapPaymentVoucherToLarkBase(
    payment: any,
    order: any,
  ): Record<string, any> {
    const fields: Record<string, any> = {};

    // Mã Phiếu Thu -> payment.code
    if (payment.code) {
      fields[LARK_VOUCHER_FIELDS.PRIMARY_CODE] = payment.code;
    }

    // Số Tiền -> payment.amount
    if (payment.amount !== null && payment.amount !== undefined) {
      fields[LARK_VOUCHER_FIELDS.AMOUNT] = Number(payment.amount);
    }

    // Phương Thức Thanh Toán -> payment.method
    if (payment.method) {
      fields[LARK_VOUCHER_FIELDS.METHOD] = payment.method;
    }

    // Trạng Thái -> payment.statusValue
    if (payment.statusValue) {
      fields[LARK_VOUCHER_FIELDS.STATUS] = payment.statusValue;
    }

    // Mã Đơn Hàng -> order.code (join từ Order)
    if (order && order.code) {
      fields[LARK_VOUCHER_FIELDS.ORDER_CODE] = order.code;
    }

    // kiotVietId -> payment.kiotVietId
    if (payment.kiotVietId !== null && payment.kiotVietId !== undefined) {
      fields[LARK_VOUCHER_FIELDS.KIOTVIET_ID] = Number(payment.kiotVietId);
    }

    // Ngày Giao Dịch -> payment.transDate (format yyyy/MM/dd HH:mm)
    if (payment.transDate) {
      fields[LARK_VOUCHER_FIELDS.TRANS_DATE] = new Date(
        payment.transDate,
      ).getTime();
    }

    // Ghi Chú -> payment.description
    if (payment.description) {
      fields[LARK_VOUCHER_FIELDS.DESCRIPTION] = payment.description;
    }

    // OrderId -> payment.orderId
    if (payment.orderId !== null && payment.orderId !== undefined) {
      fields[LARK_VOUCHER_FIELDS.ORDER_ID] = Number(payment.orderId);
    }

    // Id Payment -> payment.id
    if (payment.id !== null && payment.id !== undefined) {
      fields[LARK_VOUCHER_FIELDS.PAYMENT_ID] = Number(payment.id);
    }

    // kiotVietId Từ Order -> order.kiotVietId (join từ Order)
    if (order && order.kiotVietId !== null && order.kiotVietId !== undefined) {
      fields[LARK_VOUCHER_FIELDS.ORDER_KIOTVIET_ID] = Number(order.kiotVietId);
    }

    return fields;
  }

  private async testLarkBaseConnection(): Promise<void> {
    try {
      const headers = await this.larkAuthService.getVoucherHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

      await firstValueFrom(
        this.httpService.post(
          url,
          {
            filter: {
              conjunction: 'and',
              conditions: [],
            },
            page_size: 1,
          },
          { headers, timeout: 10000 },
        ),
      );

      this.logger.log('✅ LarkBase voucher connection test successful');
    } catch (error) {
      this.logger.error(`❌ LarkBase connection test failed: ${error.message}`);
      throw new Error('Cannot connect to LarkBase voucher table');
    }
  }
}
