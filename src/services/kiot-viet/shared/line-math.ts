/**
 * Pure line-item arithmetic shared by the sync services and the webhook path.
 *
 * The `subTotal` fallback formula was copy-pasted in three places — the invoice
 * sync service and both the order/invoice webhook detail loops — and had already
 * begun to drift (differing null-guards on `discount`). Centralising it keeps the
 * two ingestion paths from disagreeing on money math.
 */

/** Coerce a possibly-null/undefined/string numeric into a finite number, else 0. */
export function num(value: unknown): number {
  if (value === null || value === undefined || value === '') return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Line subtotal. Prefers KiotViet's own `subTotal` when present, otherwise
 * derives it as `(price - discount) * quantity`. Discount is treated as an
 * absolute amount per the KiotViet detail payload, defaulting to 0.
 */
export function lineSubTotal(args: {
  subTotal?: unknown;
  price?: unknown;
  discount?: unknown;
  quantity?: unknown;
}): number {
  if (args.subTotal !== null && args.subTotal !== undefined && args.subTotal !== '') {
    const s = Number(args.subTotal);
    if (Number.isFinite(s)) return s;
  }
  return (num(args.price) - num(args.discount)) * num(args.quantity);
}
