import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';

/**
 * Applies the `removedIds` list that KiotViet list endpoints return.
 *
 * This is the ONLY way to learn about deletions when webhooks are not registered.
 * `includeRemoveIds` was hardcoded to `false` in the one place it appeared, and no code
 * read the response field, so a product or customer deleted in KiotViet stayed in the
 * database forever, indistinguishable from a live record. On this shop KiotViet reports
 * 366 deleted products and 47 deleted categories.
 *
 * Nothing is ever deleted here — rows are stamped with `deletedAt` (and
 * `isCurrentForCode = false` where that column exists, so code-based lookups stop
 * resolving to a dead row). That keeps history and cannot break foreign keys.
 *
 * Field-name note: the documentation uses four different spellings across sections
 * (`removedIds`, `removeId`, `removeIds`, `removedId`). Verified against the live API,
 * every endpoint actually returns `removedIds`; all spellings are accepted anyway.
 */
@Injectable()
export class RemovedIdsHandler {
  private readonly logger = new Logger(RemovedIdsHandler.name);

  /** Prisma delegate -> whether its `kiotVietId` column is BigInt, and code-flag support. */
  private static readonly MODELS: Record<
    string,
    { bigIntId: boolean; hasCodeFlag: boolean }
  > = {
    product: { bigIntId: true, hasCodeFlag: true },
    customer: { bigIntId: true, hasCodeFlag: true },
    supplier: { bigIntId: true, hasCodeFlag: true },
    user: { bigIntId: true, hasCodeFlag: false },
    category: { bigIntId: false, hasCodeFlag: false },
    branch: { bigIntId: false, hasCodeFlag: false },
    bankAccount: { bigIntId: false, hasCodeFlag: false },
    priceBook: { bigIntId: false, hasCodeFlag: false },
  };

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Read whichever `remove*` field the response carries and normalise to numbers.
   * Returns an empty array when the endpoint reports no deletions.
   */
  extract(response: any): number[] {
    const raw =
      response?.removedIds ??
      response?.removeIds ??
      response?.removedId ??
      response?.removeId ??
      [];
    if (!Array.isArray(raw)) return [];
    const out = new Set<number>();
    for (const v of raw) {
      const n = Number(v);
      if (Number.isFinite(n) && n > 0) out.add(Math.trunc(n));
    }
    return Array.from(out);
  }

  /**
   * Mark the given KiotViet ids as deleted for `model`.
   * Returns how many local rows were actually stamped.
   */
  async apply(model: string, kiotVietIds: number[]): Promise<number> {
    if (kiotVietIds.length === 0) return 0;

    const cfg = RemovedIdsHandler.MODELS[model];
    if (!cfg) {
      this.logger.warn(`No removedIds handling configured for model '${model}'`);
      return 0;
    }

    await this.prisma.ensureConnected();
    const delegate = (this.prisma as any)[model];
    const now = new Date();

    let stamped = 0;
    // Chunk so a large deletion list cannot build an oversized IN clause.
    const CHUNK = 500;
    for (let i = 0; i < kiotVietIds.length; i += CHUNK) {
      const slice = kiotVietIds.slice(i, i + CHUNK);
      const ids = cfg.bigIntId ? slice.map((n) => BigInt(n)) : slice;

      try {
        const result = await delegate.updateMany({
          where: {
            kiotVietId: { in: ids },
            // Only stamp rows not already marked, so `deletedAt` records the first
            // time the deletion was observed rather than the latest sync run.
            deletedAt: null,
          },
           data: {
             deletedAt: now,
             ...(cfg.hasCodeFlag ? { isCurrentForCode: false } : {}),
             ...(model === 'customer' ? { larkSyncStatus: 'PENDING' } : {}),
           },
        });
        stamped += result.count;
      } catch (error: any) {
        this.logger.error(
          `Failed to mark deleted ${model} rows [${i}..${i + slice.length}]: ${error.message}`,
        );
      }
    }

    if (stamped > 0) {
      this.logger.warn(
        `${model}: marked ${stamped} row(s) as deleted upstream ` +
          `(${kiotVietIds.length} id(s) reported by KiotViet). Rows retained with deletedAt set.`,
      );
    } else {
      this.logger.log(
        `${model}: KiotViet reported ${kiotVietIds.length} deleted id(s), none present locally`,
      );
    }

    return stamped;
  }

  /** Convenience: extract from a raw response and apply in one step. */
  async applyFromResponse(model: string, response: any): Promise<number> {
    return this.apply(model, this.extract(response));
  }
}
