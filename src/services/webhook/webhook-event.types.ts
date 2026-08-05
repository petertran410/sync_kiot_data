/**
 * KiotViet webhook event catalogue and envelope shapes.
 *
 * Source: KiotViet Public API doc v4.7.1, section 2.11.
 *
 * Two distinct envelope shapes exist and they are NOT interchangeable:
 *
 *   *.update  ->  { Id: string, Attempt: int, Notifications: [{ Action, Data: [...] }] }
 *   *.delete  ->  { RemoveId: int[] }                      // no Id, no Notifications
 *
 * The previous implementation only ever read `Notifications[].Data[]`, so every
 * `.delete` event silently fell through as a no-op while still returning HTTP 200.
 */

/** Entity families that KiotViet can notify us about. */
export type WebhookEntity =
  | 'customer'
  | 'product'
  | 'stock'
  | 'order'
  | 'invoice'
  | 'pricebook'
  | 'pricebookdetail'
  | 'category'
  | 'branch';

export type WebhookAction = 'update' | 'delete';

/** Fully-qualified KiotViet event type, e.g. `product.update`. */
export type WebhookEventType = `${WebhookEntity}.${WebhookAction}`;

/**
 * Every event type KiotViet documents. `stock`, `order` and `invoice` have no
 * documented `.delete` variant — order/invoice cancellation arrives as an
 * `.update` carrying a cancelled status instead.
 */
export const WEBHOOK_EVENT_TYPES: readonly WebhookEventType[] = [
  'customer.update',
  'customer.delete',
  'product.update',
  'product.delete',
  'stock.update',
  'order.update',
  'invoice.update',
  'pricebook.update',
  'pricebook.delete',
  'pricebookdetail.update',
  'pricebookdetail.delete',
  'category.update',
  'category.delete',
  'branch.update',
  'branch.delete',
] as const;

const EVENT_TYPE_SET = new Set<string>(WEBHOOK_EVENT_TYPES);

export function isWebhookEventType(value: string): value is WebhookEventType {
  return EVENT_TYPE_SET.has(value);
}

/**
 * Maps an incoming URL path segment to its entity family.
 * Keeps the legacy routes (`/webhook/order`, `/webhook/pricebookdetail`, ...)
 * working, since webhooks may already be registered against them.
 */
export const PATH_TO_ENTITY: Record<string, WebhookEntity> = {
  customer: 'customer',
  product: 'product',
  stock: 'stock',
  order: 'order',
  invoice: 'invoice',
  pricebook: 'pricebook',
  pricebookdetail: 'pricebookdetail',
  category: 'category',
  branch: 'branch',
};

/** Processing status of a persisted event. Mirrors WebhookEvent.status. */
export enum WebhookEventStatus {
  /** Waiting to be claimed by a worker. */
  Pending = 'pending',
  /** Claimed and in flight. */
  Processing = 'processing',
  /** Applied to the database successfully. */
  Done = 'done',
  /** Failed but still has retry attempts left. */
  Failed = 'failed',
  /** Exhausted all retries. Needs manual attention; never auto-discarded. */
  Dead = 'dead',
}

/** `*.update` envelope. */
export interface WebhookUpdateEnvelope {
  Id?: string;
  Attempt?: number;
  Notifications?: Array<{
    Action?: string;
    Data?: any[];
  }>;
}

/** `*.delete` envelope. */
export interface WebhookDeleteEnvelope {
  RemoveId?: Array<number | string>;
}

export type WebhookEnvelope = WebhookUpdateEnvelope & WebhookDeleteEnvelope;

/**
 * A `.delete` envelope is identified purely by the presence of `RemoveId`.
 * We cannot rely on the registered URL, because KiotViet delivers both
 * `product.update` and `product.delete` to the same registered endpoint.
 */
export function isDeleteEnvelope(body: WebhookEnvelope): boolean {
  return Array.isArray(body?.RemoveId);
}

/** Flattens `Notifications[].Data[]` into a single array. */
export function extractDataItems(body: WebhookUpdateEnvelope): any[] {
  const items: any[] = [];
  for (const notification of body?.Notifications ?? []) {
    for (const item of notification?.Data ?? []) {
      if (item) items.push(item);
    }
  }
  return items;
}

/** Normalises `RemoveId` into numbers, dropping anything unparseable. */
export function extractRemovedIds(body: WebhookDeleteEnvelope): number[] {
  return (body?.RemoveId ?? [])
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v));
}
