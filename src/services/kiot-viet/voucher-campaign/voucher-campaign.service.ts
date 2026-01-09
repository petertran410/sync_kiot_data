import { LarkVoucherCampaignSyncService } from './../../lark/voucher-campaign/lark-voucher-campaign-sync.service';
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';

interface VoucherCampaignData {
  id: number;
  code: string;
  name: string;
  isActive: boolean;
  startDate: string;
  endDate: string;
  prereqPrice?: number;
  quantity: number;
  price: number;
  isGlobal: boolean;
  forAllCusGroup: boolean;
  forAllUser: boolean;
}

@Injectable()
export class KiotVietVoucherCampaign {
  private readonly logger = new Logger(KiotVietVoucherCampaign.name);
  private readonly baseUrl: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkVoucherCampaignSyncService: LarkVoucherCampaignSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async syncAllVoucherCampaigns(): Promise<void> {
    try {
      this.logger.log('🔄 Starting voucher campaign sync from KiotViet...');

      const campaigns = await this.fetchAllVoucherCampaigns();

      this.logger.log(
        `📦 Fetched ${campaigns.length} voucher campaigns from KiotViet`,
      );

      for (const campaign of campaigns) {
        await this.upsertVoucherCampaign(campaign);
      }

      this.logger.log('✅ Saved all voucher campaigns to database');

      // Sync lên LarkBase
      const campaignsToSync = await this.prismaService.voucherCampaign.findMany(
        {
          orderBy: { lastSyncedAt: 'asc' },
        },
      );

      this.logger.log(
        `📤 Starting sync ${campaignsToSync.length} campaigns to LarkBase...`,
      );

      await this.larkVoucherCampaignSyncService.syncVoucherCampaignsToLarkBase(
        campaignsToSync,
      );

      this.logger.log('✅ Voucher campaign sync completed successfully');
    } catch (error) {
      this.logger.error(`❌ Voucher campaign sync failed: ${error.message}`);
      throw error;
    }
  }

  private async fetchAllVoucherCampaigns(): Promise<VoucherCampaignData[]> {
    try {
      // Sử dụng getRequestHeaders thay vì getAuthHeaders
      const headers = await this.authService.getRequestHeaders();
      const url = `${this.baseUrl}/vouchercampaign`;

      this.logger.log(`🌐 Fetching voucher campaigns from: ${url}`);

      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers,
          timeout: 30000,
        }),
      );

      const campaigns = response.data?.data || [];

      this.logger.log(`✅ Fetched ${campaigns.length} voucher campaigns`);

      return campaigns;
    } catch (error) {
      this.logger.error(
        `❌ Failed to fetch voucher campaigns: ${error.message}`,
      );
      if (error.response) {
        this.logger.error(
          `Response status: ${error.response.status}, data: ${JSON.stringify(error.response.data)}`,
        );
      }
      throw error;
    }
  }

  private async upsertVoucherCampaign(
    campaignData: VoucherCampaignData,
  ): Promise<void> {
    try {
      const kiotVietId = campaignData.id;

      // Chỉ các field có trong schema
      const campaignPayload = {
        kiotVietId,
        code: campaignData.code,
        name: campaignData.name,
        isActive: campaignData.isActive,
        startDate: new Date(campaignData.startDate),
        endDate: new Date(campaignData.endDate),
        prereqPrice: campaignData.prereqPrice
          ? new Prisma.Decimal(campaignData.prereqPrice)
          : null,
        quantity: campaignData.quantity,
        price: new Prisma.Decimal(campaignData.price),
        isGlobal: campaignData.isGlobal,
        forAllCusGroup: campaignData.forAllCusGroup,
        forAllUser: campaignData.forAllUser,
        lastSyncedAt: new Date(),
      };

      // Upsert campaign
      const savedCampaign = await this.prismaService.voucherCampaign.upsert({
        where: { kiotVietId },
        update: campaignPayload,
        create: campaignPayload,
      });

      this.logger.log(
        `✅ Upserted voucher campaign: ${savedCampaign.code} (${savedCampaign.name})`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to upsert voucher campaign ${campaignData.code}: ${error.message}`,
      );
      throw error;
    }
  }
}
