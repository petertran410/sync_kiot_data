import {
  Controller,
  Get,
  Post,
  Param,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { SyncOrchestratorService } from '../services/sync/sync-orchestrator.service';
import { SyncControlHelper } from '../services/kiot-viet/shared/sync-control.helper';
import { ReconciliationService } from '../services/sync/reconciliation.service';
import { LarkCustomerSyncService } from '../services/lark/customer/lark-customer-sync.service';

/**
 * Manual sync API. All sync triggers are async — they return immediately with a jobId
 * and run in the background. Poll status via GET /sync/status.
 *
 * Endpoints:
 *   POST /sync/full              — sync all entities (full, no date filter)
 *   POST /sync/incremental       — sync all entities (only changes since last success)
 *   POST /sync/:entity/full      — sync a single entity (full)
 *   POST /sync/:entity/incremental — sync a single entity (incremental)
 *   GET  /sync/status            — status of all entity syncs + the aggregate job
 *   GET  /sync/status/:entity    — status of a single entity
 *   GET  /sync/entities          — list of syncable entity keys
 */
@Controller('sync')
export class SyncController {
  private readonly logger = new Logger(SyncController.name);

  constructor(
    private readonly orchestrator: SyncOrchestratorService,
     private readonly syncControl: SyncControlHelper,
     private readonly reconciliation: ReconciliationService,
     private readonly larkCustomerSync: LarkCustomerSyncService,
  ) {}

   @Post('customer/lark')
   async syncCustomerLark() {
     const result = await this.larkCustomerSync.syncPending(100);
     return { success: true, ...result, timestamp: new Date().toISOString() };
   }

   @Post('customer/lark/drain')
   async drainCustomerLark() {
     const result = await this.larkCustomerSync.drainPending();
     return { success: true, ...result, timestamp: new Date().toISOString() };
   }

   @Get('customer/lark/status')
   async getCustomerLarkStatus() {
     const stats = await this.larkCustomerSync.getStats();
     return { success: true, ...stats, timestamp: new Date().toISOString() };
   }

   @Post('full')
  triggerFullSync() {
    const res = this.orchestrator.startFullSync();
    this.logger.log(`Full sync triggered: ${res.jobId}`);
    return { success: true, ...res, timestamp: new Date().toISOString() };
  }

  @Post('incremental')
  triggerIncrementalSync() {
    const res = this.orchestrator.startIncrementalSync();
    this.logger.log(`Incremental sync triggered: ${res.jobId}`);
    return { success: true, ...res, timestamp: new Date().toISOString() };
  }

  /**
   * Reset sync status flags without deleting any synced data.
   * Clears stuck `isRunning`/`error` (e.g. after a crash) so new syncs can start.
   * POST /sync/reset            — reset all sync rows
   * POST /sync/reset/:name      — reset a single row by its SyncControl name
   *                               (e.g. product_historical, full_sync)
   */
  @Post('reset')
  async resetAll() {
    const count = await this.syncControl.reset();
    this.logger.log(`Reset ${count} sync status row(s)`);
    return { success: true, reset: count, timestamp: new Date().toISOString() };
  }

  @Post('reset/:name')
  async resetOne(@Param('name') name: string) {
    const count = await this.syncControl.reset(name);
    this.logger.log(`Reset sync status for '${name}': ${count} row(s)`);
    return {
      success: true,
      name,
      reset: count,
      timestamp: new Date().toISOString(),
    };
  }

  @Post(':entity/full')
  async triggerEntityFull(@Param('entity') entity: string) {
    return this.runEntity(entity, 'full');
  }

  @Post(':entity/incremental')
  async triggerEntityIncremental(@Param('entity') entity: string) {
    return this.runEntity(entity, 'incremental');
  }

  @Get('status')
  async getStatus() {
    const rows = await this.syncControl.getAll();
    return { success: true, syncs: rows, timestamp: new Date().toISOString() };
  }

  @Get('status/:entity')
  async getEntityStatus(@Param('entity') entity: string) {
    const map: Record<string, string> = {
      branch: 'branch_historical',
      user: 'user_historical',
      'sale-channel': 'salechannel_historical',
      surcharge: 'surcharge_historical',
      'bank-account': 'bankaccount_historical',
      trademark: 'trademark_historical',
      'customer-group': 'customer_group_historical',
      supplier: 'supplier_historical',
      'voucher-campaign': 'voucher_campaign_historical',
      category: 'category_historical',
      customer: 'customer_historical',
      pricebook: 'pricebook_historical',
      product: 'product_historical',
      'product-onhand': 'product_onhand_historical',
      order: 'order_historical',
      'order-supplier': 'order_supplier_historical',
      'purchase-order': 'purchase_order_historical',
      transfer: 'transfer_historical',
      cashflow: 'cashflow_historical',
      invoice: 'invoice_historical',
      return: 'return_historical',
      location: 'location_historical',
      settings: 'settings_historical',
      voucher: 'voucher_historical',
      full_sync: 'full_sync',
      incremental_sync: 'incremental_sync',
    };
    const name = map[entity];
    if (!name) throw new NotFoundException(`Unknown entity: ${entity}`);
    const row = await this.syncControl.getOne(name);
    return {
      success: true,
      entity,
      sync: row,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Compare row counts in the database against the totals KiotViet reports.
   * Read-only. Use this to confirm a sync actually landed the data, rather than
   * trusting the "processed" number a sync run prints.
   */
  @Get('reconcile')
  async reconcile() {
    const report = await this.reconciliation.run();
    return { success: report.summary.healthy, ...report };
  }

  @Get('entities')
  getEntities() {
    return {
      success: true,
      entities: this.orchestrator.getEntityKeys(),
      timestamp: new Date().toISOString(),
    };
  }

  private async runEntity(entity: string, mode: 'full' | 'incremental') {
    try {
      this.logger.log(`Manual ${mode} sync triggered for entity: ${entity}`);
      const result = await this.orchestrator.syncSingle(entity, mode);
      return {
        success: true,
        entity,
        mode,
        result,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      if (error instanceof NotFoundException) throw error;
      this.logger.error(`${mode} sync for ${entity} failed: ${error.message}`);
      return {
        success: false,
        entity,
        mode,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }
}
