import { Injectable, Logger } from '@nestjs/common';
import { SyncControlHelper } from '../kiot-viet/shared/sync-control.helper';
import { KiotVietBranchService } from '../kiot-viet/branch/branch.service';
import { KiotVietUserService } from '../kiot-viet/user/user.service';
import { KiotVietSaleChannelService } from '../kiot-viet/sale-channel/sale-channel.service';
import { KiotVietSurchargeService } from '../kiot-viet/surcharge/surcharge.service';
import { KiotVietBankAccountService } from '../kiot-viet/bank-account/bank-account.service';
import { KiotVietTradeMarkService } from '../kiot-viet/trademark/trademark.service';
import { KiotVietCustomerGroupService } from '../kiot-viet/customer-group/customer-group.service';
import { KiotVietSupplierService } from '../kiot-viet/supplier/supplier.service';
import { KiotVietVoucherCampaign } from '../kiot-viet/voucher-campaign/voucher-campaign.service';
import { KiotVietCategoryService } from '../kiot-viet/category/category.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietPriceBookService } from '../kiot-viet/pricebook/pricebook.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { KiotVietOrderSupplierService } from '../kiot-viet/order-supplier/order-supplier.service';
import { KiotVietPurchaseOrderService } from '../kiot-viet/purchase-order/purchase-order.service';
import { KiotVietTransferService } from '../kiot-viet/transfer/transfer.service';
import { KiotVietCashflowService } from '../kiot-viet/cashflow/cashflow.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { KiotVietReturnService } from '../kiot-viet/returns/return.service';
import { KiotVietProductOnHandService } from '../kiot-viet/product-onhand/product-onhand.service';
import { KiotVietLocationService } from '../kiot-viet/location/location.service';
import { KiotVietSettingsService } from '../kiot-viet/settings/settings.service';
import { KiotVietVoucherService } from '../kiot-viet/voucher/voucher.service';
import { mapAllSettled } from '../kiot-viet/shared/concurrency.util';

export type SyncMode = 'full' | 'incremental';

/** Entity key -> { syncControlName, service with syncFull/syncIncremental } */
interface EntityDef {
  key: string;
  syncName: string;
  sync: (mode: SyncMode) => Promise<any>;
}

interface SyncableService {
  syncFull(): Promise<any>;
  syncIncremental(): Promise<any>;
}

/**
 * Orchestrates full / incremental sync across all KiotViet entities in dependency order.
 * Runs async (fire-and-forget) and tracks progress via SyncControl table.
 *
 * Dependency stages:
 *  1. reference (parallel): branch, user, saleChannel, surcharge, bankAccount, trademark,
 *     customerGroup, supplier, voucherCampaign, category
 *  2. customer, pricebook (parallel, need branch/user/customerGroup)
 *  3. product (needs category, trademark, pricebook)
 *  4. transactional (parallel): order, orderSupplier, purchaseOrder, transfer, cashflow
 *  5. invoice (needs order)
 *  6. returns (needs invoice)
 */
@Injectable()
export class SyncOrchestratorService {
  private readonly logger = new Logger(SyncOrchestratorService.name);
  private readonly jobSyncName = 'full_sync';

  constructor(
    private readonly syncControl: SyncControlHelper,
    private readonly branchService: KiotVietBranchService,
    private readonly userService: KiotVietUserService,
    private readonly saleChannelService: KiotVietSaleChannelService,
    private readonly surchargeService: KiotVietSurchargeService,
    private readonly bankAccountService: KiotVietBankAccountService,
    private readonly tradeMarkService: KiotVietTradeMarkService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly supplierService: KiotVietSupplierService,
    private readonly voucherCampaignService: KiotVietVoucherCampaign,
    private readonly categoryService: KiotVietCategoryService,
    private readonly customerService: KiotVietCustomerService,
    private readonly priceBookService: KiotVietPriceBookService,
    private readonly productService: KiotVietProductService,
    private readonly orderService: KiotVietOrderService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly transferService: KiotVietTransferService,
    private readonly cashflowService: KiotVietCashflowService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly returnService: KiotVietReturnService,
    private readonly productOnHandService: KiotVietProductOnHandService,
    private readonly locationService: KiotVietLocationService,
    private readonly settingsService: KiotVietSettingsService,
    private readonly voucherService: KiotVietVoucherService,
  ) {}

  private getEntities(): EntityDef[] {
    return [
      {
        key: 'branch',
        syncName: 'branch_historical',
        sync: (m) => this.runService(this.branchService, m),
      },
      {
        key: 'user',
        syncName: 'user_historical',
        sync: (m) => this.runService(this.userService, m),
      },
      {
        key: 'sale-channel',
        syncName: 'salechannel_historical',
        sync: (m) => this.runService(this.saleChannelService, m),
      },
      {
        key: 'surcharge',
        syncName: 'surcharge_historical',
        sync: (m) => this.runService(this.surchargeService, m),
      },
      {
        key: 'bank-account',
        syncName: 'bankaccount_historical',
        sync: (m) => this.runService(this.bankAccountService, m),
      },
      {
        key: 'trademark',
        syncName: 'trademark_historical',
        sync: (m) => this.runService(this.tradeMarkService, m),
      },
      {
        key: 'customer-group',
        syncName: 'customer_group_historical',
        sync: (m) => this.runService(this.customerGroupService, m),
      },
      {
        key: 'supplier',
        syncName: 'supplier_historical',
        sync: (m) => this.runService(this.supplierService, m),
      },
      {
        key: 'voucher-campaign',
        syncName: 'voucher_campaign_historical',
        sync: (m) => this.runService(this.voucherCampaignService, m),
      },
      {
        key: 'category',
        syncName: 'category_historical',
        sync: (m) => this.runService(this.categoryService, m),
      },
      {
        key: 'customer',
        syncName: 'customer_historical',
        sync: (m) => this.runService(this.customerService, m),
      },
      {
        key: 'pricebook',
        syncName: 'pricebook_historical',
        sync: (m) => this.runService(this.priceBookService, m),
      },
      {
        key: 'product',
        syncName: 'product_historical',
        sync: (m) => this.runService(this.productService, m),
      },
      {
        key: 'product-onhand',
        syncName: 'product_onhand_historical',
        sync: (m) => this.runService(this.productOnHandService, m),
      },
      {
        key: 'order',
        syncName: 'order_historical',
        sync: (m) => this.runService(this.orderService, m),
      },
      {
        key: 'order-supplier',
        syncName: 'order_supplier_historical',
        sync: (m) => this.runService(this.orderSupplierService, m),
      },
      {
        key: 'purchase-order',
        syncName: 'purchase_order_historical',
        sync: (m) => this.runService(this.purchaseOrderService, m),
      },
      {
        key: 'transfer',
        syncName: 'transfer_historical',
        sync: (m) => this.runService(this.transferService, m),
      },
      {
        key: 'cashflow',
        syncName: 'cashflow_historical',
        sync: (m) => this.runService(this.cashflowService, m),
      },
      {
        key: 'invoice',
        syncName: 'invoice_historical',
        sync: (m) => this.runService(this.invoiceService, m),
      },
      {
        key: 'return',
        syncName: 'return_historical',
        sync: (m) => this.runService(this.returnService, m),
      },
      // Entities that had a Prisma model but no sync service at all.
      {
        key: 'location',
        syncName: 'location_historical',
        sync: (m) => this.runService(this.locationService, m),
      },
      {
        key: 'settings',
        syncName: 'settings_historical',
        sync: (m) => this.runService(this.settingsService, m),
      },
      {
        key: 'voucher',
        syncName: 'voucher_historical',
        sync: (m) => this.runService(this.voucherService, m),
      },
    ];
  }

  private runService(service: SyncableService, mode: SyncMode): Promise<any> {
    return mode === 'full' ? service.syncFull() : service.syncIncremental();
  }

  /** Trigger a full sync across all entities (async, returns immediately). */
  startFullSync(): { jobId: string; status: string } {
    return this.start('full');
  }

  startIncrementalSync(): { jobId: string; status: string } {
    return this.start('incremental');
  }

  /**
   * Run an incremental sync and WAIT for it to finish.
   *
   * `startIncrementalSync()` is fire-and-forget, which suits an HTTP trigger but not the
   * scheduler: without awaiting, the cron job could not tell whether the previous run
   * was still going and overlapping runs would fight over the same SyncControl rows.
   */
  /** Run a FULL sync and wait. Used by the nightly sweep that reconciles deletions. */
  async runFullSyncNow(): Promise<void> {
    const jobId = `full-${Date.now()}`;
    await this.runAll('full', jobId);
  }

  async runIncrementalSyncNow(): Promise<void> {
    const jobId = `incremental-${Date.now()}`;
    await this.runAll('incremental', jobId);
  }

  private start(mode: SyncMode): { jobId: string; status: string } {
    const jobId = `${mode}-${Date.now()}`;
    // Fire-and-forget; errors are captured into the job SyncControl row.
    void this.runAll(mode, jobId).catch((err) =>
      this.logger.error(`Sync job ${jobId} crashed: ${err.message}`),
    );
    return { jobId, status: 'running' };
  }

  private async runAll(mode: SyncMode, jobId: string): Promise<void> {
    const entities = this.getEntities();
    const completed: string[] = [];
    const failed: string[] = [];
    await this.syncControl.markRunning(
      this.jobSyncName,
      mode,
      entities.map((e) => e.key),
    );
    await this.syncControl.updateProgress(
      this.jobSyncName,
      { jobId, mode, completed, failed },
      'starting',
    );

    const byKey = new Map(entities.map((e) => [e.key, e]));

    const runStage = async (keys: string[]) => {
      const defs = keys.map((k) => byKey.get(k)!).filter(Boolean);
      const results = await mapAllSettled(defs, 5, async (e) => {
        await this.syncControl.updateProgress(
          this.jobSyncName,
          { jobId, completed, failed, current: e.key },
          e.key,
        );
        return e.sync(mode);
      });
      results.forEach((r, i) => {
        if (r.ok) {
          completed.push(defs[i].key);
        } else {
          failed.push(defs[i].key);
          this.logger.error(
            `Entity ${defs[i].key} (${mode}) failed: ${r.error?.message}`,
          );
        }
      });
    };

    try {
      // Stage 1: reference data (parallel)
      await runStage([
        'branch',
        'user',
        'sale-channel',
        'surcharge',
        'bank-account',
        'trademark',
        'customer-group',
        'supplier',
        'voucher-campaign',
        'category',
        // location + settings depend on nothing.
        'location',
        'settings',
      ]);
      // Stage 2: customer + pricebook (need branch/user/customerGroup).
      // `voucher` belongs here, not stage 1: GET /voucher requires a campaignId, so it
      // iterates the VoucherCampaign rows that stage 1 just wrote.
      await runStage(['customer', 'pricebook', 'voucher']);
      // Stage 3: product (needs category, trademark, pricebook)
      await runStage(['product']);
      // Stage 3b: product on-hand stock (needs product + branch)
      await runStage(['product-onhand']);
      // Stage 4: transactional (parallel, need reference + product)
      await runStage([
        'order',
        'order-supplier',
        'purchase-order',
        'transfer',
        'cashflow',
      ]);
      // Stage 5: invoice (needs order)
      await runStage(['invoice']);
      // Stage 6: returns (needs invoice)
      await runStage(['return']);

      const status =
        failed.length === 0
          ? 'completed'
          : failed.length === entities.length
            ? 'failed'
            : 'partial';
      await this.syncControl.upsert(this.jobSyncName, {
        entities: entities.map((e) => e.key),
        syncMode: mode,
        isRunning: false,
        isEnabled: true,
        status,
        completedAt: new Date(),
        error: failed.length ? `Failed entities: ${failed.join(', ')}` : null,
        progress: { jobId, mode, completed, failed, total: entities.length },
      });
      this.logger.log(
        `Sync job ${jobId} (${mode}) ${status}: ${completed.length} ok, ${failed.length} failed`,
      );
    } catch (error) {
      await this.syncControl.markFailed(this.jobSyncName, error.message, {
        jobId,
        mode,
        completed,
        failed,
      });
      throw error;
    }
  }

  /** Sync a single entity by key. */
  async syncSingle(key: string, mode: SyncMode): Promise<any> {
    const entity = this.getEntities().find((e) => e.key === key);
    if (!entity) throw new Error(`Unknown entity: ${key}`);
    return entity.sync(mode);
  }

  /** All syncable entity keys. */
  getEntityKeys(): string[] {
    return this.getEntities().map((e) => e.key);
  }
}
