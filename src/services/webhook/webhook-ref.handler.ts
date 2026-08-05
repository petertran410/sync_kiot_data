import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';

/**
 * Handlers for `category.update` and `branch.update`.
 *
 * Neither event had a route or a handler before — KiotViet could not notify this
 * system about category or branch changes at all, so those tables only ever
 * refreshed on a full/incremental sync run.
 *
 * Payloads (doc 2.11.9 / 2.11.10):
 *
 *   category.update  Data[]: { Id, Name, ParentId?, IsDeleted, CreatedDate,
 *                              ModifiedDate?, RetailerId, Rank }
 *   branch.update    Data[]: { Id, Name, ContactNumber, SubContactNumber,
 *                              Address, Location, WardName, IsActive, IsLock,
 *                              CreatedDate, ModifiedDate? }
 *
 * `category.update` carries its own `IsDeleted` flag, which is a second delete
 * channel independent of `category.delete`. It is honoured here.
 */
@Injectable()
export class WebhookRefHandler {
  private readonly logger = new Logger(WebhookRefHandler.name);

  constructor(private readonly prisma: PrismaService) {}

  async handleCategory(payload: any): Promise<string> {
    const items = this.dataItems(payload);
    if (!items.length) return 'category.update: no data items';

    let upserted = 0;
    let markedDeleted = 0;

    for (const item of items) {
      const kiotId = this.toInt(item?.Id ?? item?.id);
      if (kiotId === null) {
        this.logger.warn('category.update: item without Id, skipping');
        continue;
      }

      const now = new Date();
      const isDeleted = this.toBool(item?.IsDeleted ?? item?.isDeleted);

      // `parentId` is a self-relation onto Category.id (the local pk), so the
      // KiotViet parent id has to be translated first. An unknown parent is
      // stored as null rather than failing the whole event; the next category
      // sync repairs the link.
      const parentKiotId = this.toInt(item?.ParentId ?? item?.parentId);
      let parentLocalId: number | null = null;
      if (parentKiotId !== null) {
        const parent = await this.prisma.category.findUnique({
          where: { kiotVietId: parentKiotId },
          select: { id: true },
        });
        parentLocalId = parent?.id ?? null;
        if (!parent) {
          this.logger.warn(
            `category.update: parent kiotVietId=${parentKiotId} not found for category ${kiotId}`,
          );
        }
      }

      const name = this.toStr(item?.Name ?? item?.name) ?? `Category ${kiotId}`;
      const rank = this.toInt(item?.Rank ?? item?.rank);
      const retailerId = this.toInt(item?.RetailerId ?? item?.retailerId);
      const createdDate = this.toDate(item?.CreatedDate ?? item?.createdDate);
      const modifiedDate = this.toDate(
        item?.ModifiedDate ?? item?.modifiedDate,
      );

      const writable = {
        name,
        parentId: parentLocalId,
        rank,
        retailerId,
        lastSyncedAt: now,
        // Only ever SET deletedAt. A category that comes back is cleared by the
        // `IsDeleted=false` branch below, never implicitly.
        deletedAt: isDeleted ? now : null,
      };

      await this.prisma.category.upsert({
        where: { kiotVietId: kiotId },
        update: {
          ...writable,
          ...(modifiedDate ? { modifiedDate } : {}),
        },
        create: {
          kiotVietId: kiotId,
          ...writable,
          ...(createdDate ? { createdDate } : {}),
          ...(modifiedDate ? { modifiedDate } : {}),
        },
      });

      upserted++;
      if (isDeleted) markedDeleted++;
    }

    return `category.update: upserted ${upserted}${markedDeleted ? `, ${markedDeleted} flagged IsDeleted` : ''}`;
  }

  async handleBranch(payload: any): Promise<string> {
    const items = this.dataItems(payload);
    if (!items.length) return 'branch.update: no data items';

    let upserted = 0;

    for (const item of items) {
      const kiotId = this.toInt(item?.Id ?? item?.id);
      if (kiotId === null) {
        this.logger.warn('branch.update: item without Id, skipping');
        continue;
      }

      const now = new Date();
      const createdDate = this.toDate(item?.CreatedDate ?? item?.createdDate);
      const modifiedDate = this.toDate(
        item?.ModifiedDate ?? item?.modifiedDate,
      );

      const writable = {
        name: this.toStr(item?.Name ?? item?.name) ?? `Branch ${kiotId}`,
        contactNumber: this.toStr(item?.ContactNumber ?? item?.contactNumber),
        subContactNumber: this.toStr(
          item?.SubContactNumber ?? item?.subContactNumber,
        ),
        address: this.toStr(item?.Address ?? item?.address),
        // Doc calls this field `Location` here but `location` elsewhere.
        location: this.toStr(item?.Location ?? item?.location),
        wardName: this.toStr(item?.WardName ?? item?.wardName),
        email: this.toStr(item?.Email ?? item?.email),
        isActive: this.toBool(item?.IsActive ?? item?.isActive, true),
        isLock: this.toBool(item?.IsLock ?? item?.isLock, false),
        retailerId: this.toInt(item?.RetailerId ?? item?.retailerId),
        lastSyncedAt: now,
      };

      await this.prisma.branch.upsert({
        where: { kiotVietId: kiotId },
        update: {
          ...writable,
          ...(modifiedDate ? { modifiedDate } : {}),
        },
        create: {
          kiotVietId: kiotId,
          ...writable,
          ...(createdDate ? { createdDate } : {}),
          ...(modifiedDate ? { modifiedDate } : {}),
        },
      });

      upserted++;
    }

    return `branch.update: upserted ${upserted}`;
  }

  // ---------------------------------------------------------------------------

  private dataItems(payload: any): any[] {
    const out: any[] = [];
    for (const n of payload?.Notifications ?? []) {
      for (const item of n?.Data ?? []) {
        if (item && typeof item === 'object') out.push(item);
      }
    }
    return out;
  }

  private toInt(v: unknown): number | null {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }

  private toStr(v: unknown): string | null {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s.length ? s : null;
  }

  private toBool(v: unknown, fallback = false): boolean {
    if (v === null || v === undefined || v === '') return fallback;
    if (typeof v === 'boolean') return v;
    const s = String(v).toLowerCase();
    if (s === 'true' || s === '1') return true;
    if (s === 'false' || s === '0') return false;
    return fallback;
  }

  private toDate(v: unknown): Date | null {
    if (!v) return null;
    const d = new Date(v as any);
    return Number.isNaN(d.getTime()) ? null : d;
  }
}
