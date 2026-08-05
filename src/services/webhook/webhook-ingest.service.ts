import { Injectable, Logger } from '@nestjs/common';
import { createHash } from 'crypto';
import { PrismaService } from '../../prisma/prisma.service';
import {
  WebhookEnvelope,
  WebhookEventStatus,
  WebhookEventType,
  extractDataItems,
  extractRemovedIds,
  isDeleteEnvelope,
} from './webhook-event.types';

export interface IngestResult {
  /** Newly queued rows. */
  accepted: number;
  /** Deliveries dropped because we already hold that dedupKey. */
  duplicates: number;
  eventIds: number[];
}

/**
 * The entire hot path of a KiotViet webhook delivery.
 *
 * KiotViet gives us a 5 second budget and permanently disables any endpoint that
 * answers 4xx. So this does exactly one thing: durably record the envelope and
 * return. No detail fetches, no mapping, no child-table writes — all of that is
 * the worker's job (see WebhookWorkerService).
 *
 * The previous implementation instead did the full mapping inline, with a KiotViet
 * detail GET plus per-line database round trips inside nested loops, and then
 * swallowed every error and answered 200 regardless. Failed events were
 * unrecoverable because nothing was ever persisted.
 */
@Injectable()
export class WebhookIngestService {
  private readonly logger = new Logger(WebhookIngestService.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Persist one delivery.
   *
   * Throws on database failure, deliberately: the controller turns that into a 5xx
   * so KiotViet redelivers. Answering 200 on a failed insert would lose the event
   * permanently, which is exactly the bug this replaces.
   */
  async ingest(
    type: WebhookEventType,
    payload: WebhookEnvelope,
  ): Promise<IngestResult> {
    await this.prisma.ensureConnected();

    const [entity, action] = this.splitType(type, payload);
    const dedupKey = this.buildDedupKey(type, payload);
    const entityIds = this.extractEntityIds(payload);

    try {
      const row = await this.prisma.webhookEvent.create({
        data: {
          notificationId: payload?.Id != null ? String(payload.Id) : null,
          type,
          entity,
          action,
          // Indicative only — a single delivery may carry many entities. The
          // worker re-reads the full list from `payload`.
          entityId: entityIds[0] ?? null,
          dedupKey,
          payload: payload as any,
          sourceAttempt: this.toInt(payload?.Attempt),
          status: WebhookEventStatus.Pending,
          availableAt: new Date(),
        },
        select: { id: true },
      });

      this.logger.log(
        `queued ${type} #${row.id} (dedup=${dedupKey}, entities=${entityIds.length})`,
      );
      return { accepted: 1, duplicates: 0, eventIds: [row.id] };
    } catch (error: any) {
      // P2002 = unique violation on dedupKey. KiotViet redelivered something we
      // already hold, so this is success from its point of view.
      if (error?.code === 'P2002') {
        this.logger.log(
          `duplicate ${type} delivery ignored (dedup=${dedupKey})`,
        );
        return { accepted: 0, duplicates: 1, eventIds: [] };
      }
      throw error;
    }
  }

  /**
   * Splits `product.update` into `['product', 'update']`.
   *
   * The registered URL is not authoritative: KiotViet delivers both
   * `product.update` and `product.delete` to the same endpoint. So when the
   * envelope carries `RemoveId`, that wins over whatever the path implied.
   */
  private splitType(
    type: WebhookEventType,
    payload: WebhookEnvelope,
  ): [string, string] {
    const [entity, action = 'update'] = type.split('.');
    if (isDeleteEnvelope(payload)) return [entity, 'delete'];
    return [entity, action];
  }

  /**
   * Idempotency key. Prefers KiotViet's own delivery id; falls back to a content
   * hash because `.delete` envelopes (`{ RemoveId: [...] }`) carry no id at all.
   */
  private buildDedupKey(
    type: WebhookEventType,
    payload: WebhookEnvelope,
  ): string {
    if (payload?.Id != null && String(payload.Id).length > 0) {
      return `${type}:${payload.Id}`;
    }
    const hash = createHash('sha256')
      .update(JSON.stringify(payload ?? {}))
      .digest('hex')
      .slice(0, 32);
    return `${type}:sha:${hash}`;
  }

  /** Ids touched by this delivery, across both envelope shapes. */
  private extractEntityIds(payload: WebhookEnvelope): string[] {
    const ids = new Set<string>();

    for (const id of extractRemovedIds(payload)) {
      ids.add(String(id));
    }

    for (const item of extractDataItems(payload)) {
      // Entities key their id differently: stock.update uses ProductId,
      // pricebookdetail.update uses PriceBookId + ProductId.
      const raw = item?.Id ?? item?.ProductId ?? item?.PriceBookId;
      if (raw != null) ids.add(String(raw));
    }

    return Array.from(ids);
  }

  private toInt(v: unknown): number | null {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
}
