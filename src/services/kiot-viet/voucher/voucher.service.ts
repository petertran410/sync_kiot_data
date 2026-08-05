import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RetailerContext } from '../shared/retailer-context';
import { mapWithConcurrency } from '../shared/concurrency.util';

const SYNC_NAME = 'voucher_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'code', type: 'text' },
  { name: 'voucherCampaignId', type: 'int' },
  { name: 'releaseDate', type: 'timestamp' },
  { name: 'expireDate', type: 'timestamp' },
  { name: 'usedDate', type: 'timestamp' },
  { name: 'status', type: 'int' },
  { name: 'sellType', type: 'int' },
  { name: 'price', type: 'numeric' },
  { name: 'partnerType', type: 'text' },
  { name: 'partnerId', type: 'bigint' },
  { name: 'partnerName', type: 'text' },
  { name: 'retailerId', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'code',
  'voucherCampaignId',
  'releaseDate',
  'expireDate',
  'usedDate',
  'status',
  'sellType',
  'price',
  'partnerType',
  'partnerId',
  'partnerName',
  'retailerId',
  'lastSyncedAt',
];

/**
 * Sync `GET /voucher` (doc 2.24.2) into the `Voucher` table.
 *
 * The `Voucher` model existed with no service, so only campaigns were synced and the
 * individual vouchers — the rows that record whether a voucher was actually used —
 * were never pulled.
 *
 * `GET /voucher` REQUIRES a `campaignId`; there is no "all vouchers" endpoint. So this
 * iterates the locally-synced `VoucherCampaign` rows, which means
 * `voucher-campaign` must have run first (the orchestrator enforces the ordering).
 *
 * Because it is one request per campaign, and GETs are capped at 5000/hour, campaigns
 * are fetched with bounded concurrency and the rate limiter throttles the rest.
 */
@Injectable()
export class KiotVietVoucherService {
  private readonly logger = new Logger(KiotVietVoucherService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly retailer: RetailerContext,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  /**
   * The endpoint has no `lastModifiedFrom`, so this cannot be a true delta. It is
   * narrowed instead to campaigns that are still active or ended recently — an
   * expired campaign's vouchers can no longer change.
   */
  async syncIncremental() {
    return this.runSync('incremental');
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn('Voucher sync already running, skipping');
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['voucher']);

    let processed = 0;
    let campaignsScanned = 0;

    try {
      await this.prisma.ensureConnected();

      const campaigns = await this.prisma.voucherCampaign.findMany({
        where:
          mode === 'incremental'
            ? {
                // Vouchers in a long-expired campaign are immutable, so skip them.
                OR: [
                  {
                    endDate: {
                      gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
                    },
                  },
                  { isActive: true },
                ],
              }
            : {},
        select: { id: true, kiotVietId: true, code: true },
      });

      if (campaigns.length === 0) {
        this.logger.warn(
          'No voucher campaigns found locally — run the voucher-campaign sync first.',
        );
        await this.syncControl.markCompleted(SYNC_NAME, {
          processedCount: 0,
          expectedTotal: 0,
        });
        return { total: 0, processed: 0 };
      }

      this.logger.log(
        `voucher-${mode}: scanning ${campaigns.length} campaign(s)`,
      );

      const concurrency = Number(process.env.SYNC_FETCH_CONCURRENCY) || 5;

      await mapWithConcurrency(campaigns, concurrency, async (campaign) => {
        const saved = await this.syncCampaign(campaign, mode);
        processed += saved;
        campaignsScanned++;
        if (campaignsScanned % 25 === 0) {
          this.logger.log(
            `voucher-${mode}: ${campaignsScanned}/${campaigns.length} campaigns, ${processed} vouchers`,
          );
        }
      });

      await this.syncControl.markCompleted(SYNC_NAME, {
        processedCount: processed,
        expectedTotal: processed,
        campaigns: campaigns.length,
      });
      this.logger.log(
        `voucher-${mode}: done — ${processed} voucher(s) across ${campaigns.length} campaign(s)`,
      );
      return { total: processed, processed };
    } catch (error) {
      this.logger.error(`voucher-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
      });
      throw error;
    }
  }

  /** Fetch and store every voucher of one campaign. Returns rows saved. */
  private async syncCampaign(
    campaign: { id: number; kiotVietId: number; code: string | null },
    mode: string,
  ): Promise<number> {
    let saved = 0;

    try {
      const { total } = await this.pageFetcher.fetchAll<any>({
        endpoint: '/voucher',
        baseParams: { campaignId: campaign.kiotVietId },
        label: `voucher-${mode}-campaign-${campaign.kiotVietId}`,
        onPage: async (pageData) => {
          const now = new Date();
          const rows = pageData
            .map((v: any) => ({
              kiotVietId: v.id,
              code: v.code ?? null,
              // FK targets the local Voucher_campaign row, not the KiotViet id.
              voucherCampaignId: campaign.id,
              releaseDate: v.releaseDate ? new Date(v.releaseDate) : null,
              expireDate: v.expireDate ? new Date(v.expireDate) : null,
              usedDate: v.usedDate ? new Date(v.usedDate) : null,
              status: v.status ?? null,
              sellType: v.sellType ?? null,
              price: v.price ?? 0,
              partnerType: v.partnerType ?? null,
              partnerId: v.partnerId ? BigInt(v.partnerId) : null,
              partnerName: v.partnerName ?? null,
              retailerId: this.retailer.resolve(v.retailerId),
              lastSyncedAt: now,
            }))
            .filter((r) => r.kiotVietId != null && r.code != null);

          if (rows.length === 0) return;

          await this.bulkUpsert.bulkUpsert({
            table: '"Voucher"',
            columns: COLUMNS,
            rows,
            conflictTarget: '"kiotVietId"',
            updateColumns: UPDATE_COLUMNS,
          });
          saved += rows.length;
        },
      });

      if (total > 0) {
        this.logger.debug(
          `campaign ${campaign.code ?? campaign.kiotVietId}: ${saved}/${total} voucher(s)`,
        );
      }
    } catch (error: any) {
      // One bad campaign must not abort the whole run.
      this.logger.warn(
        `campaign ${campaign.kiotVietId} failed: ${error.message} — continuing`,
      );
    }

    return saved;
  }
}
