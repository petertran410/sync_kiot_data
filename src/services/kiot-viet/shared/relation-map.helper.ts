import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';

// Prisma requires `in` values to use the exact scalar type of each model's
// `kiotVietId`. Reference data uses Int; transactional entities use BigInt.
const INT_KIOT_VIET_ID_MODELS = new Set([
  'bankAccount',
  'branch',
  'category',
  'customerGroup',
  'priceBook',
  'saleChannel',
  'surcharge',
  'tradeMark',
  'voucherCampaign',
]);

/**
 * Batch-prefetch relation maps {kiotVietId -> dbId} to eliminate N+1 findFirst lookups.
 * Call once per page (or per batch) with the set of kiotVietIds referenced on that page.
 */
@Injectable()
export class RelationMapHelper {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Build a Map<kiotVietId (number), dbId (number)> for the given model.
   * @param model Prisma model delegate name, e.g. 'branch', 'user', 'product', 'supplier'
   * @param kiotVietIds array of KiotViet ids (numbers or bigints)
   * @param idField the field storing kiotVietId (default 'kiotVietId')
   */
  async buildIdMap(
    model: string,
    kiotVietIds: Array<number | bigint>,
    idField: string = 'kiotVietId',
  ): Promise<Map<number, number>> {
    await this.prisma.ensureConnected();
    const map = new Map<number, number>();
    const uniqueIds = Array.from(
      new Set(
        kiotVietIds
          .filter((id) => id !== null && id !== undefined)
          .map((id) => Number(id)),
      ),
    );
    if (uniqueIds.length === 0) return map;

    const idsForQuery = INT_KIOT_VIET_ID_MODELS.has(model)
      ? uniqueIds
      : uniqueIds.map((id) => BigInt(id));

    const records = await (this.prisma as any)[model].findMany({
      where: { [idField]: { in: idsForQuery } },
      select: { id: true, [idField]: true },
    });

    for (const r of records) {
      const kvId = r[idField];
      map.set(typeof kvId === 'bigint' ? Number(kvId) : kvId, r.id);
    }
    return map;
  }

  /**
   * Build a Map<code (string), dbId (number)> for models keyed by a code field.
   */
  async buildCodeMap(
    model: string,
    codes: string[],
    codeField: string = 'code',
  ): Promise<Map<string, number>> {
    await this.prisma.ensureConnected();
    const map = new Map<string, number>();
    const unique = Array.from(
      new Set(codes.filter((c) => c !== null && c !== undefined)),
    );
    if (unique.length === 0) return map;

    const records = await (this.prisma as any)[model].findMany({
      where: { [codeField]: { in: unique } },
      select: { id: true, [codeField]: true },
    });

    for (const r of records) {
      map.set(r[codeField], r.id);
    }
    return map;
  }
}
