import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { WebhookEventType } from './webhook-event.types';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';

/**
 * Handles every KiotViet `*.delete` event.
 *
 * NOTHING IS EVER DELETED. Rows are marked instead:
 *   - parents (Customer/Product/Category/Branch/PriceBook) get `deletedAt` set
 *     and, where the model has it, `isCurrentForCode = false`
 *   - child rows (PriceBookDetail) get `isCurrent = false`
 *
 * This preserves history and keeps foreign keys intact (a hard delete would
 * break Invoice -> Customer), while letting queries filter with
 * `WHERE "deletedAt" IS NULL` / `WHERE "isCurrent"`.
 *
 * Payload shapes are NOT uniform across entities (doc 2.11):
 *
 *   customer.delete        { RemoveId: int[] }
 *   product.delete         { RemoveId: int[] }
 *   category.delete        { RemoveId: int[] }
 *   branch.delete          { RemoveId: int[] }
 *   pricebook.delete       { Notifications: [{ Data: [ <bare long>, ... ] }] }
 *   pricebookdetail.delete { Notifications: [{ Data: [{ PricebookId, ProductIds: [] }] }] }
 *
 * Note the casing trap: `pricebookdetail.delete` spells it `PricebookId`
 * (lowercase b) whereas `pricebook.update` uses `PriceBookId`. Both spellings
 * are accepted below.
 */
@Injectable()
export class WebhookDeleteHandler {
  private readonly logger = new Logger(WebhookDeleteHandler.name);

   constructor(
     private readonly prisma: PrismaService,
     private readonly larkCustomerSync: LarkCustomerSyncService,
   ) {}

  /** Dispatch a delete event. Returns a short human-readable summary. */
  async handle(type: WebhookEventType, payload: any): Promise<string> {
    switch (type) {
      case 'customer.delete':
        return this.markCustomers(this.removeIds(payload));
      case 'product.delete':
        return this.markProducts(this.removeIds(payload));
      case 'category.delete':
        return this.markCategories(this.removeIds(payload));
      case 'branch.delete':
        return this.markBranches(this.removeIds(payload));
      case 'pricebook.delete':
        return this.markPriceBooks(this.priceBookDeleteIds(payload));
      case 'pricebookdetail.delete':
        return this.markPriceBookDetails(payload);
      default:
        throw new Error(`Not a delete event: ${type}`);
    }
  }

  // ---------------------------------------------------------------------------
  // Payload extraction
  // ---------------------------------------------------------------------------

  /** `{ RemoveId: int[] }` shape. */
  private removeIds(payload: any): number[] {
    const raw = payload?.RemoveId ?? payload?.removeId ?? [];
    if (!Array.isArray(raw)) return [];
    return this.uniqueFinite(raw);
  }

  /**
   * `pricebook.delete` puts bare numeric ids directly in `Data[]`
   * (not objects), so `item?.Id` would always be undefined here.
   */
  private priceBookDeleteIds(payload: any): number[] {
    const ids: any[] = [];
    for (const n of payload?.Notifications ?? []) {
      for (const item of n?.Data ?? []) {
        if (item === null || item === undefined) continue;
        // Tolerate both bare longs and `{ Id }` objects.
        ids.push(typeof item === 'object' ? (item.Id ?? item.id) : item);
      }
    }
    // Some tenants have been observed sending RemoveId here too.
    ids.push(...this.removeIds(payload));
    return this.uniqueFinite(ids);
  }

  private uniqueFinite(arr: any[]): number[] {
    const out = new Set<number>();
    for (const v of arr) {
      const n = Number(v);
      if (Number.isFinite(n)) out.add(n);
    }
    return Array.from(out);
  }

  // ---------------------------------------------------------------------------
  // Markers
  // ---------------------------------------------------------------------------

  private async markCustomers(kiotIds: number[]): Promise<string> {
    if (!kiotIds.length) return 'customer.delete: no ids in payload';
    const now = new Date();
     const res = await this.prisma.customer.updateMany({
       where: { kiotVietId: { in: kiotIds.map((n) => BigInt(n)) } },
       data: {
         deletedAt: now,
         isCurrentForCode: false,
         lastSyncedAt: now,
         larkSyncStatus: 'PENDING',
       },
     });
     void this.larkCustomerSync.syncPending().catch((error) =>
       this.logger.error(`Customer delete Lark sync kick failed: ${error.message}`),
     );
    return `customer.delete: marked ${res.count}/${kiotIds.length}`;
  }

  private async markProducts(kiotIds: number[]): Promise<string> {
    if (!kiotIds.length) return 'product.delete: no ids in payload';
    const now = new Date();
    const res = await this.prisma.product.updateMany({
      where: { kiotVietId: { in: kiotIds.map((n) => BigInt(n)) } },
      data: { deletedAt: now, isCurrentForCode: false, lastSyncedAt: now },
    });
    return `product.delete: marked ${res.count}/${kiotIds.length}`;
  }

  private async markCategories(kiotIds: number[]): Promise<string> {
    if (!kiotIds.length) return 'category.delete: no ids in payload';
    const res = await this.prisma.category.updateMany({
      where: { kiotVietId: { in: kiotIds } },
      data: { deletedAt: new Date(), lastSyncedAt: new Date() },
    });
    return `category.delete: marked ${res.count}/${kiotIds.length}`;
  }

  private async markBranches(kiotIds: number[]): Promise<string> {
    if (!kiotIds.length) return 'branch.delete: no ids in payload';
    const res = await this.prisma.branch.updateMany({
      where: { kiotVietId: { in: kiotIds } },
      data: { deletedAt: new Date(), lastSyncedAt: new Date() },
    });
    return `branch.delete: marked ${res.count}/${kiotIds.length}`;
  }

  /**
   * A deleted pricebook also retires its detail rows, otherwise those prices
   * would keep looking authoritative.
   */
  private async markPriceBooks(kiotIds: number[]): Promise<string> {
    if (!kiotIds.length) return 'pricebook.delete: no ids in payload';
    const now = new Date();

    const books = await this.prisma.priceBook.findMany({
      where: { kiotVietId: { in: kiotIds } },
      select: { id: true },
    });
    const localIds = books.map((b) => b.id);

    const [bookRes, detailRes] = await Promise.all([
      this.prisma.priceBook.updateMany({
        where: { kiotVietId: { in: kiotIds } },
        data: { deletedAt: now, isActive: false, lastSyncedAt: now },
      }),
      localIds.length
        ? this.prisma.priceBookDetail.updateMany({
            where: { priceBookId: { in: localIds } },
            data: { isCurrent: false, lastSyncedAt: now },
          })
        : Promise.resolve({ count: 0 }),
    ]);

    return `pricebook.delete: marked ${bookRes.count}/${kiotIds.length} books, ${detailRes.count} detail rows`;
  }

  /**
   * `{ PricebookId, ProductIds: [] }` — retire only the listed products in that
   * one pricebook.
   */
  private async markPriceBookDetails(payload: any): Promise<string> {
    const now = new Date();
    let totalMarked = 0;
    let groups = 0;

    for (const n of payload?.Notifications ?? []) {
      for (const item of n?.Data ?? []) {
        if (!item || typeof item !== 'object') continue;

        // Accept every casing variant the doc uses across sections.
        const bookKiotId = Number(
          item.PricebookId ??
            item.PriceBookId ??
            item.pricebookId ??
            item.priceBookId,
        );
        if (!Number.isFinite(bookKiotId)) continue;

        const rawProductIds =
          item.ProductIds ??
          item.productIds ??
          item.ProductId ??
          item.productId;
        const productKiotIds = this.uniqueFinite(
          Array.isArray(rawProductIds) ? rawProductIds : [rawProductIds],
        );
        if (!productKiotIds.length) continue;

        groups++;

        const [book, products] = await Promise.all([
          this.prisma.priceBook.findUnique({
            where: { kiotVietId: bookKiotId },
            select: { id: true },
          }),
          this.prisma.product.findMany({
            where: { kiotVietId: { in: productKiotIds.map((v) => BigInt(v)) } },
            select: { id: true },
          }),
        ]);

        if (!book) {
          this.logger.warn(
            `pricebookdetail.delete: unknown pricebook kiotVietId=${bookKiotId}, skipping`,
          );
          continue;
        }
        if (!products.length) continue;

        const res = await this.prisma.priceBookDetail.updateMany({
          where: {
            priceBookId: book.id,
            productId: { in: products.map((p) => p.id) },
          },
          data: { isCurrent: false, lastSyncedAt: now },
        });
        totalMarked += res.count;
      }
    }

    if (!groups) return 'pricebookdetail.delete: no usable groups in payload';
    return `pricebookdetail.delete: marked ${totalMarked} detail rows across ${groups} group(s)`;
  }
}
