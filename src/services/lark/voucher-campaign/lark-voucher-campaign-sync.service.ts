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
  KiotVietId: 'kiotVietId',
};

const VOUCHER_CAMPAIGN_STATUS_OPTION = {
  ACTIVE: 'Hoạt Động',
  INACTIVE: 'Ngưng Hoạt Động',
};

const YES_NO_OPTION = {
  YES: 'Có',
  NO: 'Không',
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
      throw new Error('LarkBase voucher campaign configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncVoucherCampaignsToLarkBase(campaigns: any[]): Promise<void> {
    try {
      this.logger.log(
        `🔄 Starting sync ${campaigns.length} voucher campaigns to LarkBase...`,
      );

      let successCount = 0;
      let failCount = 0;

      for (const campaign of campaigns) {
        try {
          await this.syncSingleVoucherCampaignDirect(campaign);
          successCount++;
        } catch (error) {
          this.logger.error(
            `❌ Failed to sync campaign ${campaign.code}: ${error.message}`,
          );
          failCount++;
        }
      }

      this.logger.log(
        `✅ Sync completed: ${successCount} success, ${failCount} failed`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Sync voucher campaigns to LarkBase failed: ${error.message}`,
      );
      throw error;
    }
  }

  async syncSingleVoucherCampaignDirect(campaign: any): Promise<void> {
    try {
      this.logger.log(
        `🔄 Syncing voucher campaign ${campaign.code} to LarkBase...`,
      );

      // Tìm record có sẵn trong LarkBase
      const existingRecordId = await this.searchRecordByCampaignId(
        campaign.kiotVietId,
      );

      // Map data sang format LarkBase
      const larkData = this.mapVoucherCampaignToLarkBase(campaign);

      this.logger.debug(`📤 Data being sent to LarkBase:`);
      this.logger.debug(JSON.stringify(larkData, null, 2));

      // Refresh token trước khi gọi API
      const headers = await this.larkAuthService.getVoucherHeaders();

      if (existingRecordId) {
        // Update record có sẵn
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        const response = await firstValueFrom(
          this.httpService.put(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.debug(`📥 LarkBase API Response (UPDATE):`);
        this.logger.debug(JSON.stringify(response.data, null, 2));

        this.logger.log(
          `✅ Updated voucher campaign ${campaign.code} in LarkBase`,
        );
      } else {
        // Tạo record mới
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        const response = await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.debug(`📥 LarkBase API Response (CREATE):`);
        this.logger.debug(JSON.stringify(response.data, null, 2));

        if (response.data.code !== 0) {
          this.logger.error(
            `❌ LarkBase API returned error code: ${response.data.code}`,
          );
          this.logger.error(`❌ Error message: ${response.data.msg}`);
          throw new Error(`LarkBase API error: ${response.data.msg}`);
        }

        this.logger.log(
          `✅ Created voucher campaign ${campaign.code} in LarkBase`,
        );
      }

      // Update lastSyncedAt
      await this.prismaService.voucherCampaign.update({
        where: { id: campaign.id },
        data: { lastSyncedAt: new Date() },
      });
    } catch (error) {
      this.logger.error(
        `❌ Sync voucher campaign ${campaign.code} failed: ${error.message}`,
      );

      if (error.response) {
        this.logger.error(`📥 Error response from LarkBase:`);
        this.logger.error(JSON.stringify(error.response.data, null, 2));
      }

      throw error;
    }
  }

  private async searchRecordByCampaignId(
    kiotVietId: number,
  ): Promise<string | null> {
    try {
      const headers = await this.larkAuthService.getVoucherHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post<LarkBatchResponse>(
          url,
          {
            field_names: [LARK_VOUCHER_CAMPAIGN_FIELDS.KiotVietId],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_VOUCHER_CAMPAIGN_FIELDS.KiotVietId,
                  operator: 'is',
                  value: [kiotVietId.toString()],
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
      this.logger.warn(
        `⚠️ Search voucher campaign by ID failed: ${error.message}`,
      );
      return null;
    }
  }

  private mapVoucherCampaignToLarkBase(campaign: any): Record<string, any> {
    const fields: Record<string, any> = {};

    if (campaign.code) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.PRIMARY_CODE] = campaign.code;
    }

    if (campaign.kiotVietId !== null && campaign.kiotVietId !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.KiotVietId] = Number(
        campaign.kiotVietId,
      );
    }

    if (campaign.name) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.NAME] = campaign.name;
    }

    // Tình Trạng
    if (campaign.isActive !== null && campaign.isActive !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.STATUS] = campaign.isActive
        ? VOUCHER_CAMPAIGN_STATUS_OPTION.ACTIVE
        : VOUCHER_CAMPAIGN_STATUS_OPTION.INACTIVE;
    }

    // Ngày Bắt Đầu
    if (campaign.startDate) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.START_DATE] = new Date(
        campaign.startDate,
      ).getTime();
    }

    // Ngày Kết Thúc
    if (campaign.endDate) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.END_DATE] = new Date(
        campaign.endDate,
      ).getTime();
    }

    // Tổng Tiền Hàng
    if (campaign.prereqPrice !== null && campaign.prereqPrice !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.PACKAGE_PRICE] = Number(
        campaign.prereqPrice,
      );
    }

    // Tổng Số Voucher
    if (campaign.quantity !== null && campaign.quantity !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.VOUCHER_AMOUNT] = campaign.quantity;
    }

    // Mệnh Giá Voucher
    if (campaign.price !== null && campaign.price !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.VOUCHER_PRICE] = Number(
        campaign.price,
      );
    }

    // Áp Dụng Cho Toàn Hệ Thống
    if (campaign.isGlobal !== null && campaign.isGlobal !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.SYSTEM_APPLY] = campaign.isGlobal
        ? YES_NO_OPTION.YES
        : YES_NO_OPTION.NO;
    }

    // Áp Dụng Cho Toàn Bộ Khách
    if (
      campaign.forAllCusGroup !== null &&
      campaign.forAllCusGroup !== undefined
    ) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.CUSTOMER_APPLY] =
        campaign.forAllCusGroup ? YES_NO_OPTION.YES : YES_NO_OPTION.NO;
    }

    // Áp Dụng Cho Toàn Bộ Người Tạo
    if (campaign.forAllUser !== null && campaign.forAllUser !== undefined) {
      fields[LARK_VOUCHER_CAMPAIGN_FIELDS.USER_APPLY] = campaign.forAllUser
        ? YES_NO_OPTION.YES
        : YES_NO_OPTION.NO;
    }

    return fields;
  }
}
