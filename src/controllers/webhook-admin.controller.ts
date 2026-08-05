import {
  BadRequestException,
  Controller,
  Delete,
  Get,
  Logger,
  Param,
  ParseIntPipe,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { AdminKeyGuard } from './admin-key.guard';
import { WebhookRegistryService } from '../services/webhook/webhook-registry.service';
import { WebhookWorkerService } from '../services/webhook/webhook-worker.service';
import {
  WEBHOOK_EVENT_TYPES,
  isWebhookEventType,
} from '../services/webhook/webhook-event.types';

/**
 * Webhook administration.
 *
 * Mounted at `/webhooks` (plural) so it never collides with `/webhook/*`, which is
 * where KiotViet delivers events and which is guarded by an HMAC check.
 *
 * Every route here requires `X-Admin-Key`. See AdminKeyGuard.
 *
 *   POST   /webhooks/register           register all 15 event types
 *   POST   /webhooks/register/:type     register one event type
 *   GET    /webhooks                    what KiotViet currently has + our local mirror
 *   GET    /webhooks/reconcile          report drift (read-only)
 *   POST   /webhooks/reconcile          report drift AND repair it
 *   DELETE /webhooks/:id                unregister one subscription
 *   GET    /webhooks/queue              WebhookEvent queue depth by status
 *   POST   /webhooks/queue/retry-dead   requeue dead-lettered events
 */
@Controller('webhooks')
@UseGuards(AdminKeyGuard)
export class WebhookAdminController {
  private readonly logger = new Logger(WebhookAdminController.name);

  constructor(
    private readonly registry: WebhookRegistryService,
    private readonly worker: WebhookWorkerService,
  ) {}

  @Post('register')
  async registerAll() {
    this.logger.log('Registering all webhook types with KiotViet');
    const report = await this.registry.registerAll();
    return {
      success: report.errors.length === 0,
      report,
      timestamp: new Date().toISOString(),
    };
  }

  @Post('register/:type')
  async registerOne(@Param('type') type: string) {
    if (!isWebhookEventType(type)) {
      throw new BadRequestException(
        `Unknown webhook type '${type}'. Expected one of: ${WEBHOOK_EVENT_TYPES.join(', ')}`,
      );
    }
    const webhook = await this.registry.register(type);
    return { success: true, webhook, timestamp: new Date().toISOString() };
  }

  @Get()
  async list() {
    const [remote, local] = await Promise.all([
      this.registry.listRemote(),
      this.registry.listLocal(),
    ]);
    return {
      success: true,
      remote,
      local,
      counts: { remote: remote.length, local: local.length },
      timestamp: new Date().toISOString(),
    };
  }

  /** Read-only drift report. Safe to poll. */
  @Get('reconcile')
  async reconcileReport() {
    const report = await this.registry.reconcile({ repair: false });
    return {
      success: true,
      repaired: false,
      report,
      timestamp: new Date().toISOString(),
    };
  }

  /** Drift report + re-register anything missing, inactive, or pointing elsewhere. */
  @Post('reconcile')
  async reconcileRepair() {
    this.logger.log('Reconciling webhook subscriptions (repair mode)');
    const report = await this.registry.reconcile({ repair: true });
    return {
      success: report.errors.length === 0,
      repaired: true,
      report,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('queue')
  async queueStats() {
    const stats = await this.worker.stats();
    return { success: true, queue: stats, timestamp: new Date().toISOString() };
  }

  @Post('queue/retry-dead')
  async retryDead(@Query('limit') limit?: string) {
    const parsed = Number(limit);
    const n = Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : 100;
    const requeued = await this.worker.retryDead(n);
    return { success: true, requeued, timestamp: new Date().toISOString() };
  }

  @Delete(':id')
  async unregister(@Param('id', ParseIntPipe) id: number) {
    await this.registry.unregister(id);
    return { success: true, id, timestamp: new Date().toISOString() };
  }
}
