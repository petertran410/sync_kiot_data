import { Injectable, Logger } from '@nestjs/common';
import { KiotPageFetcher } from '../shared/kiot-page-fetcher';
import { BulkUpsertHelper, ColumnSpec } from '../shared/bulk-upsert.helper';
import { RelationMapHelper } from '../shared/relation-map.helper';
import { SyncControlHelper } from '../shared/sync-control.helper';
import { RemovedIdsHandler } from '../shared/removed-ids.handler';

const SYNC_NAME = 'category_historical';

const COLUMNS: ColumnSpec[] = [
  { name: 'kiotVietId', type: 'int' },
  { name: 'name', type: 'text' },
  { name: 'parentId', type: 'int' },
  { name: 'retailerId', type: 'int' },
  { name: 'createdDate', type: 'timestamp' },
  { name: 'modifiedDate', type: 'timestamp' },
  { name: 'lastSyncedAt', type: 'timestamp' },
  { name: 'hasChild', type: 'boolean' },
  { name: 'rank', type: 'int' },
  { name: 'parent_name', type: 'varchar' },
  { name: 'child_name', type: 'varchar' },
  { name: 'branch_name', type: 'varchar' },
];

const UPDATE_COLUMNS = [
  'name',
  'parentId',
  'retailerId',
  'createdDate',
  'modifiedDate',
  'lastSyncedAt',
  'hasChild',
  'rank',
  'parent_name',
  'child_name',
  'branch_name',
];

interface Cat {
  categoryId: number;
  parentId?: number | null;
  categoryName?: string;
  retailerId?: number;
  hasChild?: boolean;
  rank?: number;
  createdDate?: string;
  modifiedDate?: string;
  children?: Cat[];
}

@Injectable()
export class KiotVietCategoryService {
  private readonly logger = new Logger(KiotVietCategoryService.name);

  constructor(
    private readonly pageFetcher: KiotPageFetcher,
    private readonly bulkUpsert: BulkUpsertHelper,
    private readonly relationMap: RelationMapHelper,
    private readonly syncControl: SyncControlHelper,
    private readonly removedIdsHandler: RemovedIdsHandler,
  ) {}

  async syncFull() {
    return this.runSync('full', {});
  }

  async syncIncremental() {
    return this.runSync('incremental', {});
  }

  async syncHistoricalCategories(): Promise<void> {
    await this.syncFull();
  }

  async enableHistoricalSync(): Promise<void> {}

  private async runSync(
    mode: 'full' | 'incremental',
    extra: Record<string, any>,
  ) {
    if (await this.syncControl.isRunning(SYNC_NAME)) {
      this.logger.warn(`Category sync already running, skipping`);
      return { total: 0, processed: 0 };
    }
    await this.syncControl.markRunning(SYNC_NAME, mode, ['category']);
    try {
      // Fetch hierarchical tree (single pass — categories are low volume).
      const resp = await this.pageFetcher.fetchPage<Cat>('/categories', {
        hierachicalData: true,
        includeRemoveIds: true,
        pageSize: 100,
        currentItem: 0,
        orderBy: 'createdDate',
        orderDirection: 'ASC',
        ...extra,
      } as any);
      const tree = resp.data || [];
      const flat = this.flatten(tree);
      const map = new Map<number, Cat>();
      flat.forEach((c) => map.set(c.categoryId, c));
      const now = new Date();

      // Pass 1: upsert all with parentId=null + computed hierarchy fields.
      const rowsPass1 = flat
        .filter((c) => c.categoryId && c.categoryName && c.categoryName.trim())
        .map((c) => {
          const h = this.hierarchy(c, map);
          return {
            kiotVietId: c.categoryId,
            name: c.categoryName!.trim(),
            parentId: null,
            retailerId: c.retailerId ?? null,
            createdDate: c.createdDate ? new Date(c.createdDate) : now,
            modifiedDate: c.modifiedDate ? new Date(c.modifiedDate) : now,
            lastSyncedAt: now,
            hasChild: c.hasChild ?? false,
            rank: c.rank ?? 0,
            parent_name: h.parentName,
            child_name: h.childName,
            branch_name: h.branchName,
          };
        });

      await this.bulkUpsert.bulkUpsert({
        table: '"Category"',
        columns: COLUMNS,
        rows: rowsPass1,
        conflictTarget: '"kiotVietId"',
        updateColumns: UPDATE_COLUMNS,
      });

      // Pass 2: resolve parentId (self-reference) via kiotVietId -> db id map.
      const idMap = await this.relationMap.buildIdMap(
        'category',
        flat.map((c) => c.categoryId),
      );
      const rowsPass2 = flat
        .filter(
          (c) =>
            c.categoryId &&
            c.categoryName &&
            c.categoryName.trim() &&
            c.parentId,
        )
        .map((c) => ({
          kiotVietId: c.categoryId,
          name: c.categoryName!.trim(),
          parentId: idMap.get(Number(c.parentId)) ?? null,
          retailerId: c.retailerId ?? null,
          createdDate: c.createdDate ? new Date(c.createdDate) : now,
          modifiedDate: c.modifiedDate ? new Date(c.modifiedDate) : now,
          lastSyncedAt: now,
          hasChild: c.hasChild ?? false,
          rank: c.rank ?? 0,
          parent_name: null,
          child_name: null,
          branch_name: null,
        }));

      if (rowsPass2.length) {
        // Only update parentId in pass 2 (avoid overwriting hierarchy fields with null).
        await this.bulkUpsert.bulkUpsert({
          table: '"Category"',
          columns: COLUMNS,
          rows: rowsPass2,
          conflictTarget: '"kiotVietId"',
          updateColumns: ['parentId'],
        });
      }

      // KiotViet reports upstream deletions here; nothing read this before.

      const removed = this.removedIdsHandler.extract(resp);

      if (removed.length) {

        await this.removedIdsHandler.apply('category', removed);

      }


      await this.syncControl.markCompleted(
        SYNC_NAME,
        { processedCount: rowsPass1.length, expectedTotal: rowsPass1.length },
        resp.timestamp,
      );
      this.logger.log(
        `category-${mode} completed: ${rowsPass1.length} categories`,
      );
      return { total: rowsPass1.length, processed: rowsPass1.length };
    } catch (error) {
      this.logger.error(`category-${mode} failed: ${error.message}`);
      await this.syncControl.markFailed(SYNC_NAME, error.message);
      throw error;
    }
  }

  private flatten(categories: Cat[]): Cat[] {
    const flat: Cat[] = [];
    const visited = new Set<number>();
    const process = (cat: Cat) => {
      if (visited.has(cat.categoryId)) return;
      visited.add(cat.categoryId);
      flat.push(cat);
      if (cat.children?.length) for (const ch of cat.children) process(ch);
    };
    for (const root of categories.filter((c) => !c.parentId)) process(root);
    for (const child of categories.filter((c) => c.parentId))
      if (!visited.has(child.categoryId)) process(child);
    return flat;
  }

  private hierarchy(
    cat: Cat,
    map: Map<number, Cat>,
  ): {
    parentName: string | null;
    childName: string | null;
    branchName: string | null;
  } {
    if (!cat.parentId)
      return {
        parentName: cat.categoryName ?? null,
        childName: null,
        branchName: null,
      };
    const parent = map.get(cat.parentId);
    if (!parent) return { parentName: null, childName: null, branchName: null };
    if (!parent.parentId)
      return {
        parentName: parent.categoryName ?? null,
        childName: cat.categoryName ?? null,
        branchName: null,
      };
    const grand = map.get(parent.parentId);
    return {
      parentName: grand?.categoryName ?? null,
      childName: parent.categoryName ?? null,
      branchName: cat.categoryName ?? null,
    };
  }
}
