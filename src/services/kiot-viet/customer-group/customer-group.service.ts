import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';

const SYNC_NAME = 'customer_group_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'description', type: 'text' },
  { name: 'discount', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'createdBy', type: 'int' },
  { name: 'lastSyncedAt', type: 'timestamp' },
];

const UPDATE_COLUMNS = [
  'name',
  'description',
  'discount',
  'retailerId',
  'createdDate',
  'createdBy',
  'lastSyncedAt',
];

/**
 * Membership rows from `customerGroupDetails[]`, i.e. which customer belongs to
 * which group. These were parsed off the API response and then discarded, leaving
 * `CustomerGroupRelation` permanently empty (3,631 rows missing on this shop).
 */
const RELATION_COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'bigint' },
  { name: 'customerId', type: 'int' },
  { name: 'customerGroupId', type: 'int' },
];

const RELATION_UPDATE = ['kiotVietId'];

@Injectable()
export class KiotVietCustomerGroupService {
  private readonly logger = new Logger(KiotVietCustomerGroupService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
  ) {}

  async syncFull() {
    return this.runSync('full');
  }

  async syncIncremental() {
    return this.runSync('incremental');
  }

  async syncHistoricalCustomerGroups(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(mode: 'full' | 'incremental') {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`CustomerGroup sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['customer_group']);
    let processed = 0;
    let total = 0;
    let relationsSeen = 0;
    let relationsSaved = 0;
    try {
      const { total: t, serverTimestamp } =
        await this.pageFetcher.fetchAll<any>({
          endpoint: '/customers/group',
          label: `customer-group-${mode}`,
          onPage: async (pageData) => {
            const now = new Date();
            const rows = pageData.map((g: any) => ({
              kiotVietId: Number(g.id),
              name: g.name || '',
              description: g.description || '',
              discount: g.discount || 0,
              retailerId: g.retailerId ? Number(g.retailerId) : null,
              createdDate: g.createdDate ? new Date(g.createdDate) : now,
              createdBy: g.createdBy ?? null,
              lastSyncedAt: now,
            }));
            const affected = await this.bulkUpsert.bulkUpsert({
              table: '"CustomerGroup"',
              columns: COLUMNS,
              rows,
              conflictTarget: '"kiotVietId"',
              updateColumns: UPDATE_COLUMNS,
            });
            processed += rows.length;
            this.logger.log(
              `customer-group-${mode}: saved ${rows.length} (affected ${affected}), total ${processed}`,
            );

            const r = await this.saveRelations(pageData);
            relationsSeen += r.seen;
            relationsSaved += r.saved;
          },
        });
      total = t;
      if (relationsSeen > 0) {
        this.logger.log(
          `customer-group-${mode}: linked ${relationsSaved}/${relationsSeen} customer-group relation(s)`,
        );
      }
      await this.syncControl.markCompleted(
        SYNC_NAME,
        {
          processedCount: processed,
          expectedTotal: total,
          relations: relationsSaved,
        },
        serverTimestamp,
      );
      return { total, processed, relations: relationsSaved };
    } catch (error) {
      this.logger.error(`customer-group-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message, {
        processedCount: processed,
        expectedTotal: total,
      });
      throw error;
    }
  }

  /**
   * Persist `customerGroupDetails[]` into `CustomerGroupRelation`.
   *
   * Both foreign keys point at local primary keys, so the KiotViet ids have to be
   * translated first. A customer that has not been synced yet cannot be linked, so
   * those rows are skipped and counted rather than crashing the group sync — running
   * the customer sync and then this one again picks them up.
   */
  private async saveRelations(
    groups: any[],
  ): Promise<{ seen: number; saved: number }> {
    const details = groups.flatMap((g: any) =>
      (g.customerGroupDetails ?? []).map((d: any) => ({
        kiotVietId: d.id ?? null,
        customerKvId: Number(d.customerId),
        groupKvId: Number(d.groupId ?? g.id),
      })),
    );
    if (details.length === 0) return { seen: 0, saved: 0 };

    const [customerMap, groupMap] = await Promise.all([
      this.relationMap.buildIdMap(
        'customer',
        details.map((d) => d.customerKvId),
      ),
      this.relationMap.buildIdMap(
        'customerGroup',
        details.map((d) => d.groupKvId),
      ),
    ]);

    const rows = details
      .map((d) => {
        const customerId = customerMap.get(d.customerKvId);
        const customerGroupId = groupMap.get(d.groupKvId);
        if (!customerId || !customerGroupId) return null;
        return {
          kiotVietId: d.kiotVietId != null ? BigInt(d.kiotVietId) : null,
          customerId,
          customerGroupId,
        };
      })
      .filter((r): r is NonNullable<typeof r> => r !== null);

    if (rows.length === 0) return { seen: details.length, saved: 0 };

    await this.bulkUpsert.bulkUpsert({
      table: '"CustomerGroupRelation"',
      columns: RELATION_COLUMNS,
      rows,
      // Natural key: a customer can only be in a group once.
      conflictTarget: '("customerId", "customerGroupId")',
      updateColumns: RELATION_UPDATE,
    });

    const skipped = details.length - rows.length;
    if (skipped > 0) {
      this.logger.warn(
        `${skipped}/${details.length} customer-group link(s) skipped: customer not synced yet. ` +
          `Run the customer sync first, then this sync again.`,
      );
    }

    return { seen: details.length, saved: rows.length };
  }
}
