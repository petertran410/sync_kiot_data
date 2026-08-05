import {
  Controller,
  Post,
  Body,
  Param,
  Req,
  HttpCode,
  HttpStatus,
  Logger,
  UseGuards,
  BadRequestException,
  InternalServerErrorException,
} from '@nestjs/common';
import { Request } from 'express';
import { WebhookIngestService } from '../services/webhook/webhook-ingest.service';
import { WebhookSignatureGuard } from './webhook-signature.guard';
import {
  WebhookEnvelope,
  WebhookEventType,
  WEBHOOK_EVENT_TYPES,
  isWebhookEventType,
} from '../services/webhook/webhook-event.types';

/**
 * Inbound KiotViet webhook endpoints.
 *
 * Contract with KiotViet (doc 2.11):
 *  - We must answer within 5 seconds, so this controller only persists the raw
 *    envelope and returns. Processing happens asynchronously in the worker.
 *  - A 4xx response (400/401/403/404/405) makes KiotViet PERMANENTLY STOP
 *    delivering to the endpoint. So 4xx is reserved for genuinely invalid input:
 *    an unknown event type, or a failed signature check (401 is mandated by the doc).
 *  - A 5xx response is retried, so any transient failure on our side (e.g. the
 *    database being briefly unavailable) must surface as 5xx and NOT be swallowed.
 *
 * The previous implementation returned 200 on every error, which meant KiotViet
 * never retried and failed events were lost with no record of them.
 */
@Controller('webhook')
@UseGuards(WebhookSignatureGuard)
export class WebhookController {
  private readonly logger = new Logger(WebhookController.name);

  /** Legacy path segment -> canonical event type, for already-registered webhooks. */
  private static readonly LEGACY_ROUTES: Record<string, WebhookEventType> = {
    order: 'order.update',
    invoice: 'invoice.update',
    customer: 'customer.update',
    product: 'product.update',
    stock: 'stock.update',
    pricebook: 'pricebook.update',
    pricebookdetail: 'pricebookdetail.update',
    category: 'category.update',
    branch: 'branch.update',
  };

  constructor(private readonly ingest: WebhookIngestService) {}

  /**
   * Canonical endpoint. One URL per event type, e.g. POST /webhook/kiot/customer.update
   * The type comes from the path because delete envelopes ({ RemoveId: [] }) carry
   * nothing that identifies which entity they refer to.
   */
  @Post('kiot/:type')
  @HttpCode(HttpStatus.OK)
  async handleTyped(
    @Param('type') type: string,
    @Body() payload: WebhookEnvelope,
  ) {
    if (!isWebhookEventType(type)) {
      // 400 stops KiotViet retrying, which is correct: this URL is misconfigured
      // and retrying an unknown type will never succeed.
      throw new BadRequestException(
        `Unknown webhook type '${type}'. Expected one of: ${WEBHOOK_EVENT_TYPES.join(', ')}`,
      );
    }
    return this.accept(type, payload);
  }

  /**
   * Legacy endpoints kept for existing KiotViet registrations. These are explicit
   * paths rather than `:legacy`, otherwise `/webhook/sepay` is captured here and
   * incorrectly verified with KiotViet's X-Hub-Signature guard.
   */
  @Post([
    'order',
    'invoice',
    'customer',
    'product',
    'stock',
    'pricebook',
    'pricebookdetail',
    'category',
    'branch',
  ])
  @HttpCode(HttpStatus.OK)
  async handleLegacy(
    @Req() request: Request,
    @Body() payload: WebhookEnvelope,
  ) {
    const legacy = request.path.split('/').filter(Boolean).at(-1) ?? '';
    const type = WebhookController.LEGACY_ROUTES[legacy?.toLowerCase()];
    if (!type) {
      throw new BadRequestException(
        `Unknown webhook path '/webhook/${legacy}'. Use /webhook/kiot/{type}.`,
      );
    }
    return this.accept(type, payload);
  }

  private async accept(type: WebhookEventType, payload: WebhookEnvelope) {
    if (!payload || typeof payload !== 'object') {
      throw new BadRequestException('Empty or non-object webhook body');
    }

    try {
      const result = await this.ingest.ingest(type, payload);
      return {
        success: true,
        type,
        queued: result.accepted,
        duplicate: result.duplicates,
      };
    } catch (error: any) {
      // Persisting failed. Answer 5xx so KiotViet retries — do not lose the event.
      this.logger.error(
        `Failed to queue ${type} webhook: ${error.message}`,
        error.stack,
      );
      throw new InternalServerErrorException('Failed to queue webhook event');
    }
  }
}
