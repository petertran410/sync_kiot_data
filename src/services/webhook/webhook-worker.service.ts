import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../prisma/prisma.service';
import { WebhookService } from './webhook.service';
import { WebhookDeleteHandler } from './webhook-delete.handler';
import { WebhookRefHandler } from './webhook-ref.handler';
import { WebhookEventStatus } from './webhook-event.types';

interface ClaimedEvent {
  id: number;
  type: string;
  entity: string;
  action: string;
  entityId: string | null;
  payload: any;
  attempts: number;
  maxAttempts: number;
}

/**
 * Drains the `WebhookEvent` queue.
 *
 * The receive path (WebhookIngestService) only persists events, so this worker is
 * what actually applies them to the domain tables. Without it, webhooks are
 * recorded and never acted upon.
 *
 * Design points that matter:
 *
 *  - Claiming uses `FOR UPDATE SKIP LOCKED`, so running several app instances is
 *    safe: two workers can never claim the same row.
 *  - A crash mid-processing leaves the row in `processing`. `reclaimStale()`
 *    returns those to `pending` after a timeout, so nothing is stranded.
 *  - Failures retry with exponential backoff via `availableAt`. After
 *    `maxAttempts` the row becomes `dead` — kept forever for inspection, never
 *    deleted.
 *  - Events for the same entity are processed in `receivedAt` order to avoid an
 *    older payload landing on top of a newer one.
 */
@Injectable()
export class WebhookWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WebhookWorkerService.name);

  private readonly enabled: boolean;
  private readonly batchSize: number;
  private readonly pollMs: number;
  private readonly staleMs: number;

  private timer?: NodeJS.Timeout;
  private running = false;
  private stopped = false;
  /** Set while a batch is in flight so shutdown can wait for it. */
  private inFlight?: Promise<void>;

  constructor(
    private readonly prisma: PrismaService,
    private readonly config: ConfigService,
    private readonly webhookService: WebhookService,
    private readonly deleteHandler: WebhookDeleteHandler,
    private readonly refHandler: WebhookRefHandler,
  ) {
    this.enabled =
      String(
        this.config.get('WEBHOOK_WORKER_ENABLED') ?? 'true',
      ).toLowerCase() !== 'false';
    this.batchSize = this.int(this.config.get('WEBHOOK_WORKER_BATCH_SIZE'), 20);
    this.pollMs = this.int(this.config.get('WEBHOOK_WORKER_POLL_MS'), 2000);
    // A processing row older than this is assumed abandoned by a dead process.
    this.staleMs = this.int(
      this.config.get('WEBHOOK_WORKER_STALE_MS'),
      5 * 60 * 1000,
    );
  }

  onModuleInit() {
    if (!this.enabled) {
      this.logger.warn(
        'WEBHOOK_WORKER_ENABLED=false — webhook events will queue up unprocessed',
      );
      return;
    }
    this.logger.log(
      `Webhook worker started (batch=${this.batchSize}, poll=${this.pollMs}ms)`,
    );
    this.schedule(0);
  }

  async onModuleDestroy() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    // Let an in-flight batch finish so we don't strand rows in `processing`.
    if (this.inFlight) {
      await this.inFlight.catch(() => undefined);
    }
  }

  private schedule(delayMs: number) {
    if (this.stopped) return;
    this.timer = setTimeout(() => void this.tick(), delayMs);
  }

  private async tick() {
    if (this.stopped || this.running) return;
    this.running = true;

    let processedCount = 0;
    this.inFlight = (async () => {
      try {
        await this.reclaimStale();
        processedCount = await this.drainBatch();
      } catch (error: any) {
        this.logger.error(`Worker tick failed: ${error?.message}`);
      }
    })();

    try {
      await this.inFlight;
    } finally {
      this.inFlight = undefined;
      this.running = false;
      // If the batch was full there is probably more waiting — poll immediately.
      this.schedule(processedCount >= this.batchSize ? 0 : this.pollMs);
    }
  }

  /** Return rows abandoned by a crashed process to `pending`. */
  private async reclaimStale(): Promise<void> {
    const cutoff = new Date(Date.now() - this.staleMs);
    const reclaimed = await this.prisma.webhookEvent.updateMany({
      where: {
        status: WebhookEventStatus.Processing,
        startedAt: { lt: cutoff },
      },
      data: {
        status: WebhookEventStatus.Pending,
        lastError: 'Reclaimed after worker timeout',
      },
    });
    if (reclaimed.count > 0) {
      this.logger.warn(
        `Reclaimed ${reclaimed.count} stale processing event(s)`,
      );
    }
  }

  /**
   * Claim up to `batchSize` due events and process them.
   * Returns how many were processed.
   */
  private async drainBatch(): Promise<number> {
    await this.prisma.ensureConnected();

    const claimed = await this.claim();
    if (claimed.length === 0) return 0;

    for (const event of claimed) {
      if (this.stopped) {
        // Hand the row back rather than leaving it `processing`.
        await this.release(event);
        continue;
      }
      await this.processOne(event);
    }

    return claimed.length;
  }

  /**
   * Atomically move due rows from `pending`/`failed` to `processing`.
   *
   * `SKIP LOCKED` is what makes this safe under concurrency — a row locked by
   * another worker is skipped instead of blocking.
   */
  private async claim(): Promise<ClaimedEvent[]> {
    const rows = await this.prisma.$queryRaw<any[]>`
      WITH due AS (
        SELECT "id"
        FROM "WebhookEvent"
        WHERE "status" IN (${WebhookEventStatus.Pending}, ${WebhookEventStatus.Failed})
          AND "availableAt" <= NOW()
        ORDER BY "receivedAt" ASC, "id" ASC
        LIMIT ${this.batchSize}
        FOR UPDATE SKIP LOCKED
      )
      UPDATE "WebhookEvent" e
      SET "status" = ${WebhookEventStatus.Processing},
          "startedAt" = NOW()
      FROM due
      WHERE e."id" = due."id"
      RETURNING e."id", e."type", e."entity", e."action", e."entityId",
                e."payload", e."attempts", e."maxAttempts", e."receivedAt"
    `;

    // RETURNING does not honour the CTE's ORDER BY, so re-sort for per-entity ordering.
    return rows
      .sort((a, b) => {
        const t =
          new Date(a.receivedAt).getTime() - new Date(b.receivedAt).getTime();
        return t !== 0 ? t : Number(a.id) - Number(b.id);
      })
      .map((r) => ({
        id: Number(r.id),
        type: String(r.type),
        entity: String(r.entity),
        action: String(r.action),
        entityId: r.entityId ?? null,
        payload: r.payload,
        attempts: Number(r.attempts ?? 0),
        maxAttempts: Number(r.maxAttempts ?? 8),
      }));
  }

  private async release(event: ClaimedEvent): Promise<void> {
    await this.prisma.webhookEvent
      .update({
        where: { id: event.id },
        data: { status: WebhookEventStatus.Pending, startedAt: null },
      })
      .catch((e) =>
        this.logger.warn(`release #${event.id} failed: ${e.message}`),
      );
  }

  private async processOne(event: ClaimedEvent): Promise<void> {
    const attempt = event.attempts + 1;
    const started = Date.now();

    try {
      const summary = await this.dispatch(event);

      await this.prisma.webhookEvent.update({
        where: { id: event.id },
        data: {
          status: WebhookEventStatus.Done,
          attempts: attempt,
          processedAt: new Date(),
          lastError: null,
        },
      });

      this.logger.log(
        `#${event.id} ${event.type} done in ${Date.now() - started}ms` +
          (summary ? ` — ${summary}` : ''),
      );
    } catch (error: any) {
      const message = this.truncate(error?.message ?? String(error), 1000);
      const exhausted = attempt >= event.maxAttempts;

      await this.prisma.webhookEvent.update({
        where: { id: event.id },
        data: {
          status: exhausted
            ? WebhookEventStatus.Dead
            : WebhookEventStatus.Failed,
          attempts: attempt,
          lastError: message,
          availableAt: exhausted
            ? undefined
            : new Date(Date.now() + this.backoffMs(attempt)),
        },
      });

      if (exhausted) {
        this.logger.error(
          `#${event.id} ${event.type} DEAD after ${attempt} attempt(s): ${message}`,
        );
      } else {
        this.logger.warn(
          `#${event.id} ${event.type} failed (attempt ${attempt}/${event.maxAttempts}), ` +
            `retry in ${Math.round(this.backoffMs(attempt) / 1000)}s: ${message}`,
        );
      }

      // Record the failure so there is an audit trail beyond the queue row.
      await this.prisma.syncLog
        .create({
          data: {
            entityType: `webhook:${event.type}`,
            entityId: event.entityId ?? String(event.id),
            operation: event.action,
            status: exhausted ? 'dead' : 'retry',
            errorMessage: message,
          },
        })
        .catch(() => undefined);
    }
  }

  /** Route an event to its handler. Throws on unknown type so it is not silently dropped. */
  private async dispatch(event: ClaimedEvent): Promise<string | void> {
    if (event.action === 'delete') {
      return this.deleteHandler.handle(event.type as any, event.payload);
    }

    switch (event.type) {
      case 'customer.update':
        return this.webhookService.processCustomerWebhook(event.payload);
      case 'product.update':
        return this.webhookService.processProductWebhook(event.payload);
      case 'stock.update':
        return this.webhookService.processStockWebhook(event.payload);
      case 'order.update':
        return this.webhookService.processOrderWebhook(event.payload);
      case 'invoice.update':
        return this.webhookService.processInvoiceWebhook(event.payload);
      case 'pricebook.update':
        return this.webhookService.processPriceBookWebhook(event.payload);
      case 'pricebookdetail.update':
        return this.webhookService.processPriceBookDetailWebhook(event.payload);
      case 'category.update':
        return this.refHandler.handleCategory(event.payload);
      case 'branch.update':
        return this.refHandler.handleBranch(event.payload);
      default:
        throw new Error(
          `No handler registered for webhook type '${event.type}'`,
        );
    }
  }

  /** Exponential backoff with jitter, capped at 30 minutes. */
  private backoffMs(attempt: number): number {
    const base = Math.min(30 * 60 * 1000, 2000 * Math.pow(2, attempt - 1));
    return Math.round(base * (0.75 + Math.random() * 0.5));
  }

  private truncate(s: string, max: number): string {
    return s.length <= max ? s : `${s.slice(0, max)}…`;
  }

  private int(v: unknown, def: number): number {
    if (v === null || v === undefined || v === '') return def;
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.trunc(n) : def;
  }

  // ---------------------------------------------------------------------------
  // Operational helpers, exposed via the webhook controller.
  // ---------------------------------------------------------------------------

  /** Queue depth by status. */
  async stats(): Promise<Record<string, number>> {
    await this.prisma.ensureConnected();
    const grouped = await this.prisma.webhookEvent.groupBy({
      by: ['status'],
      _count: { _all: true },
    });
    const out: Record<string, number> = {};
    for (const g of grouped) out[g.status] = g._count._all;
    return out;
  }

  /**
   * Requeue `dead` events (e.g. after fixing the underlying bug).
   * Resets attempts so they get a full retry budget again.
   */
  async retryDead(limit = 100): Promise<number> {
    await this.prisma.ensureConnected();
    const dead = await this.prisma.webhookEvent.findMany({
      where: { status: WebhookEventStatus.Dead },
      select: { id: true },
      orderBy: { id: 'asc' },
      take: limit,
    });
    if (dead.length === 0) return 0;

    const result = await this.prisma.webhookEvent.updateMany({
      where: { id: { in: dead.map((d) => d.id) } },
      data: {
        status: WebhookEventStatus.Pending,
        attempts: 0,
        availableAt: new Date(),
        lastError: null,
      },
    });
    this.logger.log(`Requeued ${result.count} dead event(s)`);
    return result.count;
  }
}
