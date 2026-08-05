import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';

export interface SyncControlState {
  name: string;
  entities: string[];
  syncMode: 'full' | 'incremental' | 'historical' | 'recent';
  isRunning: boolean;
  isEnabled: boolean;
  status:
    | 'idle'
    | 'running'
    | 'completed'
    | 'failed'
    | 'in_progress'
    | 'partial';
  startedAt?: Date | null;
  completedAt?: Date | null;
  lastRunAt?: Date | null;
  progress?: any;
  currentEntity?: string | null;
  error?: string | null;
  /** Cursor from KiotViet's own `timestamp` field. See schema comment. */
  lastServerTimestamp?: Date | null;
}

/**
 * Unified SyncControl management. Every sync run updates a row in `syncControl`
 * so the status API can report progress.
 */
@Injectable()
export class SyncControlHelper {
  private readonly logger = new Logger(SyncControlHelper.name);

  constructor(private readonly prisma: PrismaService) {}

  async upsert(name: string, data: Partial<SyncControlState>): Promise<void> {
    try {
      await this.prisma.ensureConnected();
      await this.prisma.syncControl.upsert({
        where: { name },
        create: {
          name,
          entities: data.entities ?? [],
          syncMode: data.syncMode ?? 'full',
          isRunning: data.isRunning ?? false,
          isEnabled: data.isEnabled ?? true,
          status: data.status ?? 'idle',
          startedAt: data.startedAt,
          completedAt: data.completedAt,
          lastRunAt: data.lastRunAt,
          progress: data.progress,
          currentEntity: data.currentEntity,
          error: data.error,
          lastServerTimestamp: data.lastServerTimestamp ?? null,
        },
        update: {
          ...data,
          progress: data.progress,
          lastRunAt:
            data.status === 'completed' || data.status === 'failed'
              ? new Date()
              : data.lastRunAt,
        },
      });
    } catch (error) {
      this.logger.error(
        `Failed to update syncControl '${name}': ${error.message}`,
      );
      throw error;
    }
  }

  /** Mark a sync as started. */
  async markRunning(
    name: string,
    syncMode: SyncControlState['syncMode'],
    entities: string[] = [],
  ): Promise<void> {
    await this.upsert(name, {
      entities,
      syncMode,
      isRunning: true,
      isEnabled: true,
      status: 'running',
      startedAt: new Date(),
      completedAt: null,
      error: null,
      progress: { processedCount: 0, expectedTotal: 0 },
    });
  }

  /**
   * Mark a sync as completed.
   *
   * `serverTimestamp` is the `timestamp` value KiotViet returned on this run. It
   * becomes the cursor for the next incremental run. Pass it whenever the API
   * supplied one — falling back to our own clock is what causes skew-induced
   * record loss.
   */
  async markCompleted(
    name: string,
    progress?: any,
    serverTimestamp?: string | Date | null,
  ): Promise<void> {
    const parsed = this.parseTimestamp(serverTimestamp);
    await this.upsert(name, {
      isRunning: false,
      status: 'completed',
      completedAt: new Date(),
      progress,
      // Only overwrite when we actually received one, so a run against an
      // endpoint that omits `timestamp` does not wipe a good cursor.
      ...(parsed ? { lastServerTimestamp: parsed } : {}),
    });
  }

  private parseTimestamp(v?: string | Date | null): Date | null {
    if (!v) return null;
    const d = v instanceof Date ? v : new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  /** Mark a sync as failed. */
  async markFailed(name: string, error: string, progress?: any): Promise<void> {
    await this.upsert(name, {
      isRunning: false,
      status: 'failed',
      error,
      progress,
    });
  }

  /** Update progress mid-run. */
  async updateProgress(
    name: string,
    progress: any,
    currentEntity?: string,
  ): Promise<void> {
    await this.prisma.ensureConnected();
    await this.prisma.syncControl
      .update({
        where: { name },
        data: {
          progress: progress,
          currentEntity,
          status: 'in_progress',
        },
      })
      .catch((err) =>
        this.logger.warn(`updateProgress '${name}' failed: ${err.message}`),
      );
  }

  /**
   * Cursor for the next incremental run (`lastModifiedFrom`).
   *
   * Prefers `lastServerTimestamp` — KiotViet's own clock — over `completedAt`,
   * which is this host's clock. If the two clocks disagree by more than the sync
   * interval, a `completedAt`-based cursor skips every record modified inside the
   * gap, permanently. Falls back to `completedAt` for rows written before the
   * cursor column existed.
   */
  async getLastCompletedAt(name: string): Promise<Date | null> {
    await this.prisma.ensureConnected();
    const row = await this.prisma.syncControl.findUnique({ where: { name } });
    if (!row) return null;
    return row.lastServerTimestamp ?? row.completedAt ?? null;
  }

  /** Check if a sync is currently running. */
  async isRunning(name: string): Promise<boolean> {
    await this.prisma.ensureConnected();
    const row = await this.prisma.syncControl.findUnique({ where: { name } });
    return row?.isRunning ?? false;
  }

  /** Get all sync control rows (for status API). */
  async getAll(): Promise<any[]> {
    await this.prisma.ensureConnected();
    return this.prisma.syncControl.findMany({ orderBy: { name: 'asc' } });
  }

  async getOne(name: string) {
    await this.prisma.ensureConnected();
    return this.prisma.syncControl.findUnique({ where: { name } });
  }

  /**
   * Reset sync status flags WITHOUT deleting any synced data.
   * Clears stuck `isRunning` flags (e.g. after a crash) so new syncs can start.
   * If `name` is omitted, resets every SyncControl row.
   * Returns the number of rows reset.
   */
  async reset(name?: string): Promise<number> {
    await this.prisma.ensureConnected();
    const result = await this.prisma.syncControl.updateMany({
      where: name ? { name } : {},
      data: {
        isRunning: false,
        status: 'idle',
        currentEntity: null,
        error: null,
      },
    });
    this.logger.log(
      `Reset ${result.count} syncControl row(s)${name ? ` for '${name}'` : ''}`,
    );
    return result.count;
  }
}
