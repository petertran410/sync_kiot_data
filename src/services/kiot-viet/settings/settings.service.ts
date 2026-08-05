import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RetailerContext } from '../shared/retailer-context';

const SYNC_NAME = 'settings_historical';

/**
 * Sync `GET /settings` (doc 2.22) into the `Settings` table.
 *
 * Unlike every other endpoint this returns a SINGLE object, not a paged list, so it
 * cannot go through `fetchAll`. It is keyed by `retailerId`.
 *
 * These flags matter for interpreting the rest of the data — e.g.
 * `allowSellWhenOutStock` explains how an invoice can exist for a product whose
 * on-hand went negative, and `managerCustomerByBranch` decides whether a customer
 * code is unique shop-wide or only per branch.
 */
@Injectable()
export class KiotVietSettingsService {
  private readonly logger = new Logger(KiotVietSettingsService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
    private readonly syncControl: SyncControlHelper,
    private readonly retailer: RetailerContext,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  /** The endpoint has no date filter, so this is the same as a full run. */
  async syncIncremental() {
    return this.runSync('incremental');
  }

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn('Settings sync already running, skipping');
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['settings']);

    try {
      await this.prisma.ensureConnected();

      // Single object response. `fetchPage` still gives us auth + rate limiting.
      const resp: any = await this.pageFetcher.fetchPage<any>(
        '/settings',
        { currentItem: 0, pageSize: 1 },
        { label: `settings-${mode}` },
      );

      // Be liberal about the envelope: the doc shows a bare object, but other
      // endpoints wrap payloads in `data`.
      const s =
        resp?.data && !Array.isArray(resp.data)
          ? resp.data
          : Array.isArray(resp?.data)
            ? resp.data[0]
            : resp;

      if (!s || typeof s !== 'object') {
        throw new Error('GET /settings returned no usable object');
      }

      const retailerId = this.retailer.resolve(s.RetailerId ?? s.retailerId);
      const now = new Date();

      const data = {
        managerCustomerByBranch: this.bool(
          s.ManagerCustomerByBranch ?? s.managerCustomerByBranch,
        ),
        allowOrderWhenOutStock: this.bool(
          s.AllowOrderWhenOutStock ?? s.allowOrderWhenOutStock,
        ),
        allowSellWhenOrderOutStock: this.bool(
          s.AllowSellWhenOrderOutStock ?? s.allowSellWhenOrderOutStock,
        ),
        allowSellWhenOutStock: this.bool(
          s.AllowSellWhenOutStock ?? s.allowSellWhenOutStock,
        ),
        lastSyncedAt: now,
      };

      await this.prisma.settings.upsert({
        where: { retailerId },
        update: data,
        create: { retailerId, ...data },
      });

      this.logger.log(
        `settings-${mode}: retailer ${retailerId} — ` +
          `managerCustomerByBranch=${data.managerCustomerByBranch}, ` +
          `allowOrderWhenOutStock=${data.allowOrderWhenOutStock}, ` +
          `allowSellWhenOrderOutStock=${data.allowSellWhenOrderOutStock}, ` +
          `allowSellWhenOutStock=${data.allowSellWhenOutStock}`,
      );

      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: 1, expectedTotal: 1 },
        resp?.timestamp,
      );
      return { total: 1, processed: 1 };
    } catch (error) {
      this.logger.error(`settings-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: 0,
        expectedTotal: 1,
      });
      throw error;
    }
  }

  private bool(v: unknown): boolean {
    if (typeof v === 'boolean') return v;
    if (typeof v === 'string') return v.toLowerCase() === 'true';
    if (typeof v === 'number') return v !== 0;
    return false;
  }
}
