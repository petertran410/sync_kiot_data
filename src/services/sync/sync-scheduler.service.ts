import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression } from '@nestjs/schedule';
import { SyncOrchestratorService } from './sync-orchestrator.service';
import { SyncControlHelper } from '../kiot-viet/shared/sync-control.helper';
import { WebhookRegistryService } from '../webhook/webhook-registry.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';

/**
 * Scheduled background jobs.
 *
 * `@nestjs/schedule` was already a dependency but was never wired up: there was no
 * `ScheduleModule` and no `@Cron` anywhere, so "incremental sync" only ever ran if
 * somebody manually POSTed to the endpoint. In practice that meant the database drifted
 * from KiotViet indefinitely.
 *
 * Webhooks handle most real-time changes, but a periodic incremental sync is still
 * required because:
 *   - several entities have NO webhook at all (order-supplier, purchase-order,
 *     transfer, cashflow, return, supplier, user, voucher, location, settings)
 *   - a webhook delivery that exhausts its retries is dead-lettered, and the periodic
 *     sync is what eventually repairs that record
 *   - KiotViet stops delivering to an endpoint after a single 4xx, so without a
 *     fallback poll an unnoticed outage would silently freeze the dataset
 *
 * Everything is opt-out via config, and each job refuses to start while another is
 * still running so a slow run can never pile up on itself.
 */
@Injectable()
export class SyncSchedulerService {
  private readonly logger = new Logger(SyncSchedulerService.name);
  private readonly jobSyncName = 'full_sync';

  private readonly incrementalEnabled: boolean;
  private readonly reconcileEnabled: boolean;
  private readonly fullSweepEnabled: boolean;
  private readonly targetedNightlyEnabled: boolean;
  private readonly larkCustomerEnabled: boolean;

  /** Guards against overlap when a run takes longer than the cron interval. */
  private incrementalRunning = false;
  private targetedNightlyRunning = false;

  constructor(
    private readonly orchestrator: SyncOrchestratorService,
    private readonly syncControl: SyncControlHelper,
    private readonly registry: WebhookRegistryService,
    private readonly larkCustomerSync: LarkCustomerSyncService,
    config: ConfigService,
  ) {
    this.incrementalEnabled = this.bool(config.get('SYNC_CRON_ENABLED'), false);
    this.reconcileEnabled = this.bool(
      config.get('WEBHOOK_RECONCILE_CRON_ENABLED'),
      true,
    );
    this.fullSweepEnabled = this.bool(
      config.get('SYNC_FULL_SWEEP_CRON_ENABLED'),
      false,
    );
    this.targetedNightlyEnabled = this.bool(
      config.get('SYNC_NIGHTLY_TARGETED_CRON_ENABLED'),
      true,
    );
    this.larkCustomerEnabled = this.bool(
      config.get('LARK_CUSTOMER_SYNC_CRON_ENABLED'),
      false,
    );

    this.logger.log(
      `Scheduler: incremental sync ${this.incrementalEnabled ? 'ENABLED (hourly)' : 'disabled'}, ` +
        `nightly full sweep ${this.fullSweepEnabled ? 'ENABLED (03:00)' : 'disabled'}, ` +
        `targeted sync ${this.targetedNightlyEnabled ? 'ENABLED (23:00 Asia/Ho_Chi_Minh)' : 'disabled'}, ` +
        `Lark customer queue ${this.larkCustomerEnabled ? 'ENABLED (every minute)' : 'disabled'}, ` +
        `webhook reconcile ${this.reconcileEnabled ? 'ENABLED (every 6h)' : 'disabled'}`,
    );
  }

  /**
   * Hourly incremental sync of every entity.
   *
   * Hourly is deliberate: KiotViet caps GETs at 5000/hour (doc §2), and a full
   * incremental pass over all 24 entities costs a few hundred requests on this dataset.
   * Running much more often risks exhausting the budget that webhook detail-fetches
   * and manual syncs also draw from.
   */
  @Cron(CronExpression.EVERY_HOUR, { name: 'incremental-sync' })
  async hourlyIncrementalSync(): Promise<void> {
    if (!this.incrementalEnabled) return;

    if (this.incrementalRunning) {
      this.logger.warn(
        'Skipping scheduled incremental sync: previous run still in progress',
      );
      return;
    }

    // Also respect a run started by hand through the API.
    if (await this.syncControl.isRunning(this.jobSyncName)) {
      this.logger.warn(
        'Skipping scheduled incremental sync: a sync job is already running',
      );
      return;
    }

    this.incrementalRunning = true;
    const started = Date.now();
    try {
      this.logger.log('Starting scheduled incremental sync');
      await this.orchestrator.runIncrementalSyncNow();
      this.logger.log(
        `Scheduled incremental sync finished in ${((Date.now() - started) / 1000).toFixed(0)}s`,
      );
    } catch (error: any) {
      // Never rethrow: an unhandled rejection here would take down the process and
      // stop all future scheduled runs.
      this.logger.error(`Scheduled incremental sync failed: ${error?.message}`);
    } finally {
      this.incrementalRunning = false;
    }
  }

  /**
   * Nightly full sweep, for deletions.
   *
   * KiotViet only reports `removedIds` relative to the `lastModifiedFrom` that was
   * sent, so the hourly incremental run sees deletions within its own window and
   * nothing more. If a run is skipped, fails, or the process is down over a window,
   * the deletions in that gap are never reported again and those rows would stay
   * marked live forever.
   *
   * A full sync sends no cursor, so KiotViet returns the complete deletion list
   * (366 products and 47 categories on this shop) and any missed deletion is
   * repaired. It runs at 03:00 local time, away from the hourly job.
   */
  @Cron('0 3 * * *', { name: 'nightly-full-sweep' })
  async nightlyFullSweep(): Promise<void> {
    if (!this.fullSweepEnabled) return;

    if (this.incrementalRunning) {
      this.logger.warn('Skipping nightly full sweep: incremental sync still running');
      return;
    }
    if (await this.syncControl.isRunning(this.jobSyncName)) {
      this.logger.warn('Skipping nightly full sweep: a sync job is already running');
      return;
    }

    this.incrementalRunning = true;
    const started = Date.now();
    try {
      this.logger.log('Starting nightly full sweep (reconciles deletions)');
      await this.orchestrator.runFullSyncNow();
      this.logger.log(
        `Nightly full sweep finished in ${((Date.now() - started) / 1000).toFixed(0)}s`,
      );
    } catch (error: any) {
      this.logger.error(`Nightly full sweep failed: ${error?.message}`);
    } finally {
      this.incrementalRunning = false;
    }
  }

  /**
   * Daily fallback for the four operational datasets that must remain current.
   * Webhooks provide near-real-time updates; this incremental pass repairs any
   * missed deliveries without running the expensive all-entity full sync.
   */
  @Cron('0 23 * * *', {
    name: 'nightly-targeted-sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async nightlyTargetedSync(): Promise<void> {
    if (!this.targetedNightlyEnabled) return;
    if (this.targetedNightlyRunning || this.incrementalRunning) {
      this.logger.warn('Skipping targeted 23:00 sync: another sync is running');
      return;
    }
    if (await this.syncControl.isRunning(this.jobSyncName)) {
      this.logger.warn('Skipping targeted 23:00 sync: a manual sync is running');
      return;
    }

    this.targetedNightlyRunning = true;
    const started = Date.now();
    const entities = ['customer', 'product-onhand', 'order', 'invoice'] as const;
    const completed: string[] = [];
    const failed: Array<{ entity: string; error: string }> = [];

    try {
      this.logger.log(
        `Starting targeted 23:00 incremental sync: ${entities.join(', ')}`,
      );
      for (const entity of entities) {
        try {
          this.logger.log(`Targeted sync starting: ${entity}`);
          await this.orchestrator.syncSingle(entity, 'incremental');
          completed.push(entity);
          this.logger.log(`Targeted sync completed: ${entity}`);
        } catch (error: any) {
          const message = error?.message ?? String(error);
          failed.push({ entity, error: message });
          this.logger.error(`Targeted sync failed for ${entity}: ${message}`);
        }
      }

      try {
        const result = await this.larkCustomerSync.drainPending();
        this.logger.log(
          `Targeted Customer-to-Lark drain: ${result.synced} synced, ` +
            `${result.deleted} deleted, ${result.failed} failed`,
        );
      } catch (error: any) {
        const message = error?.message ?? String(error);
        failed.push({ entity: 'customer-lark', error: message });
        this.logger.error(`Targeted Customer-to-Lark drain failed: ${message}`);
      }

      this.logger.log(
        `Targeted 23:00 sync finished in ${((Date.now() - started) / 1000).toFixed(0)}s: ` +
          `completed=[${completed.join(', ')}], failed=[${failed.map((item) => item.entity).join(', ')}]`,
      );
    } finally {
      this.targetedNightlyRunning = false;
    }
  }

  @Cron(CronExpression.EVERY_MINUTE, { name: 'lark-customer-sync' })
  async syncLarkCustomers(): Promise<void> {
    if (!this.larkCustomerEnabled) return;
    try {
      const result = await this.larkCustomerSync.syncPending();
      if (result.processed > 0) {
        this.logger.log(
          `Lark customer queue: ${result.synced} synced, ${result.deleted} deleted, ${result.failed} failed`,
        );
      }
    } catch (error: any) {
      this.logger.error(`Lark customer queue failed: ${error?.message}`);
    }
  }


  /**
   * Detect webhook subscription drift.
   *
   * Reports drift only; repair remains an explicit admin action.
   */
  @Cron(CronExpression.EVERY_6_HOURS, { name: 'webhook-reconcile' })
  async webhookReconcile(): Promise<void> {
    if (!this.reconcileEnabled) return;

    try {
      const report = await this.registry.reconcile({ repair: false });
      const drift =
        report.missing.length + report.inactive.length + report.wrongUrl.length;

      if (drift === 0) {
        this.logger.log(
          `Webhook check: ${report.healthy.length}/${report.expected} healthy`,
        );
        return;
      }

      this.logger.error(
        `Webhook drift detected — ${report.healthy.length}/${report.expected} healthy. ` +
          `missing=[${report.missing.join(', ')}] ` +
          `inactive=[${report.inactive.join(', ')}] ` +
          `wrongUrl=[${report.wrongUrl.map((w) => w.type).join(', ')}]. ` +
          `KiotViet is NOT delivering these events. Repair with POST /webhooks/reconcile.`,
      );
    } catch (error: any) {
      this.logger.error(`Webhook reconcile check failed: ${error?.message}`);
    }
  }

  private bool(v: unknown, fallback: boolean): boolean {
    if (v === undefined || v === null || v === '') return fallback;
    if (typeof v === 'boolean') return v;
    return String(v).toLowerCase() !== 'false';
  }
}
