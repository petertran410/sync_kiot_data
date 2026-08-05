import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';

/**
 * PostgreSQL type cast per column. Required so that NULL parameters
 * get a concrete type (Postgres prepared statements cannot infer NULL type),
 * and so BigInt/Decimal/arrays/jsonb serialize correctly.
 */
export type PgType =
  | 'text'
  | 'varchar'
  | 'bigint'
  | 'int'
  | 'numeric'
  | 'boolean'
  | 'timestamp'
  | 'date'
  | 'jsonb'
  | 'text[]'
  | 'int[]'
  | 'real'
  | 'float8'
  | '"LarkSyncStatus"';

export interface ColumnSpec {
  /** Column name as in DB (will be quoted). */
  name: string;
  type: PgType;
}

export interface BulkUpsertSpec {
  /** Quoted table name, e.g. '"User"' or '"OrderDetail"'. */
  table: string;
  columns: ColumnSpec[];
  /** Row objects keyed by column name. Missing keys treated as NULL. */
  rows: Record<string, any>[];
  /**
   * Conflict target SQL fragment, e.g. '"kiotVietId"' or '("orderId", "lineNumber")'.
   */
  conflictTarget: string;
  /** Column names to update on conflict (subset of columns). */
  updateColumns: string[];
  /**
   * Columns used to decide whether an existing row changed. This lets a sync
   * update bookkeeping columns (for example an outbound queue status) only
   * when source columns differ, instead of re-queuing every unchanged row.
   */
  compareColumns?: string[];
  /** If true (default), add `WHERE ... IS DISTINCT FROM EXCLUDED...` to skip no-op updates. */
  skipUnchanged?: boolean;
}

const PG_MAX_PARAMS = 60000; // Postgres hard limit is 65535; keep margin.

/**
 * Convert a JS value to a value Prisma's $executeRawUnsafe can send to pg driver,
 * given the target Postgres type.
 */
function toPgValue(value: any, type: PgType): any {
  if (value === null || value === undefined) return null;
  switch (type) {
    case 'bigint':
      // BigInt or number -> string; ::bigint cast parses it.
      return typeof value === 'bigint' ? value.toString() : String(value);
    case 'numeric':
      // Prisma.Decimal or number -> string.
      if (typeof value === 'object' && 'toString' in value)
        return value.toString();
      return String(value);
    case 'jsonb':
      return JSON.stringify(value);
    case 'text[]':
      return arrayToPgLiteral(value);
    case 'int[]':
      return arrayToPgLiteral(value);
    default:
      return value;
  }
}

/** Build a Postgres array literal string '{a,b,c}' with proper escaping. */
function arrayToPgLiteral(arr: any[]): string {
  if (!Array.isArray(arr) || arr.length === 0) return '{}';
  const items = arr.map((item) => {
    const s = item === null || item === undefined ? '' : String(item);
    // Escape backslash and double-quote, wrap in double-quotes if needed.
    if (/[,{}"\\\s]/.test(s)) {
      return `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
    }
    return s;
  });
  return `{${items.join(',')}}`;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Build the SQL + params for a bulk upsert (INSERT ... ON CONFLICT DO UPDATE).
 * Returns null if rows is empty.
 */
function buildSql(spec: BulkUpsertSpec): { sql: string; params: any[] } | null {
  const {
    table,
    columns,
    rows,
    conflictTarget,
    updateColumns,
    compareColumns = updateColumns,
    skipUnchanged = true,
  } = spec;
  if (rows.length === 0) return null;

  const colNames = columns.map((c) => quoteIdent(c.name));
  const colList = colNames.join(', ');

  // VALUES clause: each row -> ($1::type, $2::type, ...), ...
  const valueRows: string[] = [];
  const params: any[] = [];
  let paramIdx = 1;

  for (const row of rows) {
    const placeholders: string[] = [];
    for (const col of columns) {
      const val = toPgValue(row[col.name], col.type);
      params.push(val);
      placeholders.push(`$${paramIdx}::${col.type}`);
      paramIdx++;
    }
    valueRows.push(`(${placeholders.join(', ')})`);
  }

  const valuesClause = valueRows.join(', ');

  // ON CONFLICT DO UPDATE SET
  const setClauses = updateColumns.map(
    (col) => `${quoteIdent(col)} = EXCLUDED.${quoteIdent(col)}`,
  );
  const setClause = setClauses.join(', ');

  let whereClause = '';
  if (skipUnchanged && compareColumns.length > 0) {
    const distinctChecks = compareColumns.map(
      (col) =>
        `${table}.${quoteIdent(col)} IS DISTINCT FROM EXCLUDED.${quoteIdent(col)}`,
    );
    whereClause = ` WHERE ${distinctChecks.join(' OR ')}`;
  }

  // PostgreSQL requires parentheses around a conflict column list. Services pass
  // composite keys already wrapped; normalize single-column unique keys here.
  const normalizedConflictTarget =
    conflictTarget.startsWith('(') ||
    conflictTarget.toUpperCase().startsWith('ON CONSTRAINT ')
      ? conflictTarget
      : `(${conflictTarget})`;

  const sql = `INSERT INTO ${table} (${colList}) VALUES ${valuesClause} ON CONFLICT ${normalizedConflictTarget} DO UPDATE SET ${setClause}${whereClause}`;

  return { sql, params };
}

/**
 * Bulk upsert via raw SQL `INSERT ... ON CONFLICT DO UPDATE`.
 * Auto-splits into sub-batches to stay under Postgres parameter limit.
 * Returns total number of affected rows (inserts + updates that changed).
 */
@Injectable()
export class BulkUpsertHelper {
  private readonly logger = new Logger(BulkUpsertHelper.name);

  constructor(private readonly prisma: PrismaService) {}

  async bulkUpsert(spec: BulkUpsertSpec): Promise<number> {
    if (spec.rows.length === 0) return 0;

    await this.prisma.ensureConnected();

    const paramsPerRow = spec.columns.length;
    const maxRowsPerBatch = Math.max(
      1,
      Math.floor(PG_MAX_PARAMS / paramsPerRow),
    );
    const batchSize = Math.min(
      maxRowsPerBatch,
      this.parseInt(process.env.SYNC_DB_BATCH_SIZE, 500),
    );

    let totalAffected = 0;
    // Rows lost in the row-by-row fallback. Previously each failure was only logged,
    // so a systematic problem (e.g. a foreign key that rejects ~98% of rows) looked
    // like a successful sync. Now the losses are counted and reported per table.
    let droppedRows = 0;
    const dropReasons = new Map<string, number>();

    for (let i = 0; i < spec.rows.length; i += batchSize) {
      const chunk = spec.rows.slice(i, i + batchSize);
      const built = buildSql({ ...spec, rows: chunk });
      if (!built) continue;
      try {
        const affected = await this.prisma.$executeRawUnsafe(
          built.sql,
          ...built.params,
        );
        totalAffected += affected;
      } catch (error) {
        this.logger.error(
          `bulkUpsert failed on ${spec.table} batch [${i}..${i + chunk.length}]: ${error.message}`,
        );
        // Fallback: try row-by-row so one bad row doesn't kill the whole batch.
        if (chunk.length > 1) {
          this.logger.warn(
            `Falling back to row-by-row for ${spec.table} batch`,
          );
          for (const row of chunk) {
            const one = buildSql({ ...spec, rows: [row] });
            if (!one) continue;
            try {
              totalAffected += await this.prisma.$executeRawUnsafe(
                one.sql,
                ...one.params,
              );
            } catch (err) {
              droppedRows++;
              const reason = this.classify(err.message);
              dropReasons.set(reason, (dropReasons.get(reason) ?? 0) + 1);
              // Log only the first few per batch; the summary below carries the totals.
              if (droppedRows <= 3) {
                this.logger.error(
                  `bulkUpsert single-row failed on ${spec.table}: ${err.message}`,
                );
              }
            }
          }
        } else {
          throw error;
        }
      }
    }

    if (droppedRows > 0) {
      const pct = ((droppedRows / spec.rows.length) * 100).toFixed(1);
      const breakdown = Array.from(dropReasons.entries())
        .map(([reason, count]) => `${reason}=${count}`)
        .join(', ');
      const summary =
        `DATA LOSS on ${spec.table}: ${droppedRows}/${spec.rows.length} row(s) (${pct}%) ` +
        `could not be written [${breakdown}]`;
      // A large systematic loss is not a warning — it means the sync silently
      // produced an incomplete dataset.
      if (droppedRows / spec.rows.length >= 0.1) {
        this.logger.error(summary);
      } else {
        this.logger.warn(summary);
      }
    }

    return totalAffected;
  }

  /** Coarse bucket for a Postgres error, used in the data-loss summary. */
  private classify(message: string): string {
    if (/foreign key constraint/i.test(message)) {
      const m = message.match(/constraint "([^"]+)"/);
      return `fk:${m?.[1] ?? 'unknown'}`;
    }
    if (/unique constraint/i.test(message)) return 'unique_violation';
    if (/not-null|null value in column/i.test(message))
      return 'not_null_violation';
    if (
      /invalid input syntax|numeric field overflow|out of range/i.test(message)
    ) {
      return 'type_error';
    }
    return 'other';
  }

  private parseInt(v: string | undefined, def: number): number {
    if (v === undefined || v === null || v === '') return def;
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? n : def;
  }
}
