import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotPageFetcher } from '../kiot-viet/shared/kiot-page-fetcher';

interface EntityCheck {
  key: string;
  /** Prisma model delegate name. */
  model: string;
  endpoint: string;
  /** Extra query params required for the API `total` to be comparable. */
  params?: Record<string, any>;
  /**
   * Some endpoints are gated behind a store setting and answer HTTP 420 when the
   * feature is off. That is not a failure, so it is reported as `skipped`.
   */
  featureGated?: boolean;
  /** Rows this table legitimately holds even when KiotViet reports none. */
  note?: string;
}

export interface EntityReport {
  entity: string;
  db: number | null;
  api: number | null;
  diff: number | null;
  status: 'ok' | 'gap' | 'extra' | 'skipped' | 'error';
  message?: string;
}

export interface ReconciliationReport {
  checkedAt: string;
  /** Rows in DB vs `total` reported by KiotViet, per entity. */
  entities: EntityReport[];
  childTables: Record<string, number>;
  summary: {
    ok: number;
    gap: number;
    extra: number;
    skipped: number;
    error: number;
    /** True when every comparable entity matched within tolerance. */
    healthy: boolean;
  };
}

/**
 * Compares row counts in the database against the `total` KiotViet reports.
 *
 * This exists because of a real failure this project already hit: a foreign key on
 * `soldById` rejected ~98% of orders and invoices, `BulkUpsertHelper` logged each row
 * and continued, and the sync still reported "processed 4600". Nothing compared the
 * result against the source, so 6,600 missing orders looked like a healthy run.
 *
 * A small difference is expected and tolerated — rows can be created in KiotViet
 * between the DB count and the API call.
 */
@Injectable()
export class ReconciliationService {
  private readonly logger = new Logger(ReconciliationService.name);

  /** Absolute difference treated as "in sync" (accounts for concurrent writes). */
  private static readonly TOLERANCE = 10;

  private readonly checks: EntityCheck[] = [
    { key: 'branch', model: 'branch', endpoint: '/branches' },
    { key: 'user', model: 'user', endpoint: '/users' },
    { key: 'sale-channel', model: 'saleChannel', endpoint: '/salechannel' },
    { key: 'bank-account', model: 'bankAccount', endpoint: '/BankAccounts' },
    { key: 'trademark', model: 'tradeMark', endpoint: '/trademark' },
    { key: 'category', model: 'category', endpoint: '/categories' },
    { key: 'location', model: 'location', endpoint: '/locations' },
    {
      key: 'customer-group',
      model: 'customerGroup',
      endpoint: '/customers/group',
    },
    { key: 'supplier', model: 'supplier', endpoint: '/suppliers' },
    {
      key: 'customer',
      model: 'customer',
      endpoint: '/customers',
      params: { includeTotal: true },
    },
    { key: 'product', model: 'product', endpoint: '/products' },
    { key: 'pricebook', model: 'priceBook', endpoint: '/pricebooks' },
    { key: 'order', model: 'order', endpoint: '/orders' },
    { key: 'invoice', model: 'invoice', endpoint: '/invoices' },
    {
      key: 'purchase-order',
      model: 'purchaseOrder',
      endpoint: '/purchaseorders',
    },
    { key: 'transfer', model: 'transfer', endpoint: '/transfers' },
    { key: 'cashflow', model: 'cashflow', endpoint: '/cashflow' },
    { key: 'return', model: 'return', endpoint: '/returns' },
    {
      key: 'voucher-campaign',
      model: 'voucherCampaign',
      endpoint: '/vouchercampaign',
    },
    // Both of these 420 when the shop has the feature switched off.
    {
      key: 'surcharge',
      model: 'surcharge',
      endpoint: '/surchages',
      featureGated: true,
    },
    {
      key: 'order-supplier',
      model: 'orderSupplier',
      endpoint: '/ordersuppliers',
      featureGated: true,
    },
  ];

  private readonly childTables = [
    'orderDetail',
    'orderDelivery',
    'orderSurcharge',
    'invoiceDetail',
    'invoiceDelivery',
    'invoiceSurcharge',
    'payment',
    'returnDetail',
    'purchaseOrderDetail',
    'productInventory',
    'productImage',
    'productAttribute',
    'productFormula',
    'productUnit',
    'productSerial',
    'productBatchExpire',
    'productWarranty',
    'priceBookDetail',
    'priceBookBranch',
    'priceBookCustomerGroup',
    'priceBookUser',
    'customerGroupRelation',
    'transferDetail',
    'voucher',
    'settings',
    'webhookEvent',
  ];

  constructor(
    private readonly prisma: PrismaService,
    private readonly pageFetcher: KiotPageFetcher,
  ) {}

  async run(): Promise<ReconciliationReport> {
    await this.prisma.ensureConnected();

    const entities: EntityReport[] = [];

    for (const check of this.checks) {
      entities.push(await this.checkOne(check));
    }

    const childTables: Record<string, number> = {};
    for (const table of this.childTables) {
      try {
        childTables[table] = await (this.prisma as any)[table].count();
      } catch {
        // Model absent — skip rather than fail the whole report.
      }
    }

    const summary = {
      ok: entities.filter((e) => e.status === 'ok').length,
      gap: entities.filter((e) => e.status === 'gap').length,
      extra: entities.filter((e) => e.status === 'extra').length,
      skipped: entities.filter((e) => e.status === 'skipped').length,
      error: entities.filter((e) => e.status === 'error').length,
      healthy: false,
    };
    summary.healthy = summary.gap === 0 && summary.error === 0;

    if (!summary.healthy) {
      const problems = entities
        .filter((e) => e.status === 'gap' || e.status === 'error')
        .map((e) => `${e.entity}(db=${e.db} api=${e.api})`)
        .join(', ');
      this.logger.error(`Reconciliation found problems: ${problems}`);
    } else {
      this.logger.log(
        `Reconciliation clean: ${summary.ok} entity/entities in sync, ${summary.skipped} skipped`,
      );
    }

    return {
      checkedAt: new Date().toISOString(),
      entities,
      childTables,
      summary,
    };
  }

  private async checkOne(check: EntityCheck): Promise<EntityReport> {
    let db: number;
    try {
      db = await (this.prisma as any)[check.model].count();
    } catch (error: any) {
      return {
        entity: check.key,
        db: null,
        api: null,
        diff: null,
        status: 'error',
        message: `DB count failed: ${error.message}`,
      };
    }

    try {
      // pageSize=1 is enough: only the `total` field is needed.
      const resp = await this.pageFetcher.fetchPage<any>(
        check.endpoint,
        { currentItem: 0, pageSize: 1, ...(check.params ?? {}) },
        { label: `reconcile-${check.key}`, maxRetries: 2 },
      );

      const api = typeof resp?.total === 'number' ? resp.total : null;
      if (api === null) {
        return {
          entity: check.key,
          db,
          api: null,
          diff: null,
          status: 'error',
          message: 'API response contained no `total`',
        };
      }

      const diff = db - api;
      let status: EntityReport['status'];
      if (Math.abs(diff) <= ReconciliationService.TOLERANCE) status = 'ok';
      else if (diff < 0) status = 'gap';
      else status = 'extra';

      return {
        entity: check.key,
        db,
        api,
        diff,
        status,
        message:
          status === 'gap'
            ? `${Math.abs(diff)} record(s) missing from the database`
            : status === 'extra'
              ? `${diff} more row(s) locally than KiotViet reports ` +
                `(deleted upstream but retained here, which is intended)`
              : undefined,
      };
    } catch (error: any) {
      const status420 = error?.response?.status === 420;
      if (check.featureGated && status420) {
        return {
          entity: check.key,
          db,
          api: null,
          diff: null,
          status: 'skipped',
          message:
            error?.response?.data?.responseStatus?.message ??
            'Feature disabled in KiotViet store settings',
        };
      }
      return {
        entity: check.key,
        db,
        api: null,
        diff: null,
        status: 'error',
        message: error?.message ?? String(error),
      };
    }
  }
}
