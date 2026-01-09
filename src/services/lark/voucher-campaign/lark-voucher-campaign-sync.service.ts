import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_VOUCHER_CAMPAIGN_FIELDS = {
  PRIMARY_CODE: 'Mã Voucher Campaign',
  NAME: 'Tên Voucher Campaign',
  STATUS: 'Tình Trạng',
  START_DATE: 'Ngày Bắt Đầu',
  END_DATE: 'Ngày Kết Thúc',
  PACKAGE_PRICE: 'Tổng Tiền Hàng',
  VOUCHER_AMOUNT: 'Tổng Số Voucher',
  VOUCHER_PRICE: 'Mệnh Giá Voucher',
  SYSTEM_APPLY: 'Áp Dụng Cho Toàn Hệ Thống',
  CUSTOMER_APPLY: 'Áp Dụng Cho Toàn Bộ Khách',
  USER_APPLY: 'Áp Dụng Cho Toàn Bộ Người Tạo',
};

const VOUCHER_CAMPAIGN_STATUS_OPTION = {
  ACTIVE: 'Hoạt Động',
  DEACTIVE: 'Ngưng Hoạt Động',
};

const SYSTEM_APPLY_OPTION = {
  YES: 'Có',
  NO: 'Không',
};

const CUSTOMER_APPLY_OPTION = {
  YES: 'Có',
  NO: 'Không',
};

const USER_APPLY_OPTION = {
  YES: 'Có',
  NO: 'Không',
};

@Injectable()
export class LarkVoucherCampaignSyncService {
  private readonly logger = new Logger(LarkVoucherCampaignSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_VOUCHER_CAMPAIGN_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_VOUCHER_CAMPAIGN_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase voucher configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncSingleVoucherCampaignDirect(voucher: any): Promise<void> {
    try {
    } catch (error) {}
  }
}
