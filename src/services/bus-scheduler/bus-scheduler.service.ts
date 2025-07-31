import { LarkPurchaseOrderSyncService } from 'src/services/lark/purchase-order/lark-purchase-order-sync.service';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { LarkProductSyncService } from '../lark/product/lark-product-sync.service';
import { KiotVietPriceBookService } from '../kiot-viet/pricebook/pricebook.service';
import { KiotVietSupplierService } from '../kiot-viet/supplier/supplier.service';
import { LarkSupplierSyncService } from '../lark/supplier/lark-supplier-sync.service';
import { KiotVietOrderSupplierService } from '../kiot-viet/order-supplier/order-supplier.service';
import { LarkOrderSupplierSyncService } from '../lark/order-supplier/lark-order-supplier-sync.service';
import { KiotVietPurchaseOrderService } from './../kiot-viet/purchase-order/purchase-order.service';

interface DailyEntityConfig {
  name: string;
  syncFunction: () => Promise<void>;
  larkSyncFunction?: () => Promise<void>;
  dependencies?: string[];
  enabled: boolean;
}

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);

  private isMainSchedulerEnabled = true;
  private isDailyProductSchedulerEnabled = true;
  private isDailyProductCompletedToday = false;
  private lastProductSyncDate: string | null = null;

  private isDailyCycleRunning = false;
  private dailyCyclePriorityLevel = 0;
  private mainSchedulerSuspendedForDaily = false;
  private isMainCycleGracefulShutdown = false;
  private mainCycleAbortController: AbortController | null = null;
  private startupAbortController: AbortController | null = null;
  private dailyCycleStartTime: Date | null = null;

  private readonly DAILY_ENTITIES_CONFIG: DailyEntityConfig[] = [
    {
      name: 'pricebook_product_sequence',
      syncFunction: async () => {
        await this.runProductSequenceSync();
      },
      larkSyncFunction: async () => {
        await this.autoTriggerProductLarkSync();
      },
      enabled: true,
    },
    {
      name: 'order_supplier',
      syncFunction: async () => {
        await this.enableAndRunOrderSupplierSync();
      },
      larkSyncFunction: async () => {
        await this.autoTriggerOrderSupplierLarkSync();
      },
      enabled: true,
    },
    {
      name: 'order_supplier_detail',
      syncFunction: async () => {
        await this.enableAndRunOrderSupplierDetailSync();
      },
      larkSyncFunction: async () => {
        await this.autoTriggerOrderSupplierDetailLarkSync();
      },
      dependencies: ['order_supplier'],
      enabled: true,
    },
    {
      name: 'purchase_order',
      syncFunction: async () => {
        await this.enableAndRunPurchaseOrderSync();
      },
      larkSyncFunction: async () => {
        await this.autoTriggerPurchaseOrderLarkSync();
      },
      enabled: true,
    },
    {
      name: 'purchase_order_detail',
      syncFunction: async () => {
        await this.enableAndRunPurchaseOrderDetailSync();
      },
      larkSyncFunction: async () => {
        await this.autoTriggerPurchaseOrderDetailLarkSync();
      },
      dependencies: ['purchase_order'],
      enabled: true,
    },
  ];

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly productService: KiotVietProductService,
    private readonly larkProductSyncService: LarkProductSyncService,
    private readonly priceBookService: KiotVietPriceBookService,
    private readonly supplierService: KiotVietSupplierService,
    private readonly larkSupplierSyncService: LarkSupplierSyncService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierSyncService: LarkOrderSupplierSyncService,
    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
  ) {}

  async onModuleInit() {
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  @Cron('*/4 * * * *', {
    name: 'main_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    if (this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0) {
      if (!this.mainSchedulerSuspendedForDaily) {
        this.logger.log(
          '🛑 SUSPENDING 7-minute cycle - Daily cycle has priority',
        );
        this.mainSchedulerSuspendedForDaily = true;

        if (this.mainCycleAbortController) {
          this.mainCycleAbortController.abort();
          this.logger.log('🚫 FORCE ABORTING ongoing 7-minute cycle');
        }
      }

      this.logger.debug(
        `⏸️ 4-minute cycle suspended (dailyCyclePriorityLevel: ${this.dailyCyclePriorityLevel}, isDailyCycleRunning: ${this.isDailyCycleRunning})`,
      );
      return;
    }

    if (
      this.mainSchedulerSuspendedForDaily &&
      !this.isDailyCycleRunning &&
      this.dailyCyclePriorityLevel === 0
    ) {
      this.logger.log('▶️ RESUMING 4-minute cycle - Daily cycle completed');
      this.mainSchedulerSuspendedForDaily = false;
    }

    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    this.mainCycleAbortController = new AbortController();
    const signal = this.mainCycleAbortController.signal;

    try {
      this.logger.log('🚀 Starting 4-minute parallel sync cycle...');
      const startTime = Date.now();

      if (signal.aborted) {
        this.logger.log('🚫 7-minute cycle aborted before starting');
        return;
      }

      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Parallel sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        return;
      }

      await this.updateCycleTracking('main_cycle', 'running');

      const CYCLE_TIMEOUT_MS = 10 * 60 * 1000;

      try {
        const cyclePromise = this.executeMainCycleWithAbortSignal(signal);
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Cycle timeout after 10 minutes')),
            CYCLE_TIMEOUT_MS,
          ),
        );

        await Promise.race([cyclePromise, timeoutPromise]);
        await this.updateCycleTracking('main_cycle', 'completed');

        const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
        this.logger.log(
          `🎉 4-minute sync cycle completed in ${totalDuration}s`,
        );
      } catch (timeoutError) {
        if (signal.aborted) {
          this.logger.log(
            '🚫 4-minute cycle was aborted by daily cycle priority',
          );
          await this.updateCycleTracking(
            'main_cycle',
            'aborted',
            'Aborted by daily cycle priority',
          );
        } else if (
          timeoutError instanceof Error &&
          timeoutError.message.includes('timeout')
        ) {
          this.logger.error(`⏰ 4-minute cycle timed out after 15 minutes`);
          await this.updateCycleTracking(
            'main_cycle',
            'timeout',
            'Cycle exceeded 15 minute timeout',
          );
        } else {
          throw timeoutError;
        }
      }
    } catch (error) {
      if (signal.aborted) {
        this.logger.log('🚫 4-minute cycle aborted during execution');
        await this.updateCycleTracking(
          'main_cycle',
          'aborted',
          'Aborted by daily cycle priority',
        );
      } else {
        this.logger.error(`❌ 4-minute sync cycle failed: ${error.message}`);
        await this.updateCycleTracking('main_cycle', 'failed', error.message);
      }
    } finally {
      this.mainCycleAbortController = null;
    }
  }

  private async executeMainCycleWithAbortSignal(
    signal: AbortSignal,
  ): Promise<void> {
    const syncPromises: Promise<void>[] = [];

    syncPromises.push(
      this.executeAbortableSync('customer', signal, async () => {
        await this.runCustomerSync();
        await this.runCustomerLarkSync();
      }),
    );

    syncPromises.push(
      this.executeAbortableSync('invoice', signal, async () => {
        await this.runInvoiceSync();
        await this.runInvoiceLarkSync();
      }),
    );

    syncPromises.push(
      this.executeAbortableSync('order', signal, async () => {
        await this.runOrderSync();
        await this.runOrderLarkSync();
      }),
    );

    await Promise.all(syncPromises);
  }

  private async executeAbortableSync(
    syncType: string,
    signal: AbortSignal,
    syncFunction: () => Promise<void>,
  ): Promise<void> {
    if (signal.aborted) {
      this.logger.debug(`🚫 ${syncType} sync aborted before starting`);
      return;
    }

    try {
      await syncFunction();

      if (signal.aborted) {
        this.logger.debug(
          `🚫 ${syncType} sync completed but was marked for abort`,
        );
      }
    } catch (error) {
      if (signal.aborted) {
        this.logger.debug(`🚫 ${syncType} sync aborted during execution`);
      } else {
        throw error;
      }
    }
  }

  @Cron('50 0 * * *', {
    name: 'daily_product_sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyProductSync() {
    if (!this.isDailyProductSchedulerEnabled) {
      this.logger.debug('⏸️ Daily sync cycle disabled');
      return;
    }

    const today = new Date().toISOString().split('T')[0];

    if (
      this.isDailyProductCompletedToday &&
      this.lastProductSyncDate === today
    ) {
      this.logger.log(
        '✅ Daily 23h sync sequence already completed today - skipping',
      );
      return;
    }

    try {
      this.logger.log('🌙 21:30 Daily Sequential Sync triggered');

      this.dailyCyclePriorityLevel = 2;
      this.isDailyCycleRunning = true;
      this.dailyCycleStartTime = new Date();

      this.logger.log(
        '🛑 ACTIVATING daily cycle priority mode - 7-minute cycle STOPPED IMMEDIATELY',
      );

      await this.forceStopMainCycleImmediately();

      const startTime = Date.now();
      this.isDailyProductCompletedToday = false;

      await this.updateCycleTracking('daily_product_cycle', 'running');

      const DAILY_TIMEOUT_MS = 2 * 60 * 60 * 1000;

      try {
        const dailyPromise = this.executeDailySequence();
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Daily sequence timeout after 10 minutes')),
            DAILY_TIMEOUT_MS,
          ),
        );

        await Promise.race([dailyPromise, timeoutPromise]);
        await this.updateCycleTracking('daily_product_cycle', 'completed');

        this.isDailyProductCompletedToday = true;
        this.lastProductSyncDate = today;

        const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
        this.logger.log(
          `🎉 Daily sequential execution completed in ${duration} minutes`,
        );
        this.logger.log(`🔒 Daily sequence locked until tomorrow 23:00`);
      } catch (timeoutError) {
        this.isDailyProductCompletedToday = false;
        if (
          timeoutError instanceof Error &&
          timeoutError.message.includes('timeout')
        ) {
          this.logger.error(`⏰ Daily sequence timed out after 60 minutes`);
          await this.updateCycleTracking(
            'daily_product_cycle',
            'timeout',
            'Daily sequence exceeded 60 minute timeout',
          );
        } else {
          throw timeoutError;
        }
      }
    } catch (error) {
      this.isDailyProductCompletedToday = false;
      this.logger.error(
        `❌ Daily sequential execution failed: ${error.message}`,
      );
      await this.updateCycleTracking(
        'daily_product_cycle',
        'failed',
        error.message,
      );
    } finally {
      this.logger.log('▶️ RESUMING 4-minute cycle - Daily cycle completed');
      this.isDailyCycleRunning = false;
      this.dailyCycleStartTime = null;
      this.mainSchedulerSuspendedForDaily = false;
      this.dailyCyclePriorityLevel = 0;
      this.isMainCycleGracefulShutdown = false;
      this.startupAbortController = null;
    }
  }

  private async forceStopMainCycleImmediately(): Promise<void> {
    this.logger.log('🚫 FORCE STOPPING all ongoing cycles IMMEDIATELY...');

    if (this.mainCycleAbortController) {
      this.mainCycleAbortController.abort();
      this.logger.log('⚡ Abort signal sent to ongoing 7-minute cycle');
    }

    if (this.startupAbortController) {
      this.startupAbortController.abort();
      this.logger.log('⚡ Abort signal sent to ongoing startup syncs');
    }

    this.isMainCycleGracefulShutdown = true;

    await this.forceStopRunningSyncControls();

    this.logger.log(
      '✅ All cycles force stop completed - proceeding immediately',
    );
  }

  private async runCustomerLarkSync(): Promise<void> {
    const customersToSync = await this.prismaService.customer.findMany({
      where: {
        larkSyncStatus: { in: ['PENDING', 'FAILED'] },
      },
      take: 100,
    });

    if (customersToSync.length > 0) {
      await this.larkCustomerSyncService.syncCustomersToLarkBase(
        customersToSync,
      );
    }
  }

  private async runInvoiceLarkSync(): Promise<void> {
    const invoicesToSync = await this.prismaService.invoice.findMany({
      where: {
        larkSyncStatus: { in: ['PENDING', 'FAILED'] },
      },
      take: 100,
    });

    if (invoicesToSync.length > 0) {
      await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoicesToSync);
    }
  }

  private async runOrderLarkSync(): Promise<void> {
    const ordersToSync = await this.prismaService.order.findMany({
      where: {
        larkSyncStatus: { in: ['PENDING', 'FAILED'] },
      },
      take: 100,
    });

    if (ordersToSync.length > 0) {
      await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
    }
  }

  private async executeEntitySequentially(
    entity: DailyEntityConfig,
  ): Promise<void> {
    this.logger.log(`🔄 [${entity.name}] Starting sequential execution...`);
    const startTime = Date.now();

    try {
      this.logger.log(`📥 [${entity.name}] Entity sync starting...`);
      await entity.syncFunction();
      this.logger.log(`✅ [${entity.name}] Entity sync completed`);

      if (entity.larkSyncFunction) {
        this.logger.log(`📤 [${entity.name}] LarkBase sync starting...`);
        await entity.larkSyncFunction();
        this.logger.log(`✅ [${entity.name}] LarkBase sync completed`);
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(
        `🎉 [${entity.name}] Sequential execution completed in ${duration}s`,
      );
    } catch (error) {
      this.logger.error(
        `❌ [${entity.name}] Sequential execution failed: ${error.message}`,
      );
      throw new Error(
        `${entity.name} sequential execution failed: ${error.message}`,
      );
    }
  }

  private async executeDailySequence(): Promise<void> {
    this.logger.log(
      '🌙 Starting Daily Sequential Execution (Entity → LarkBase → Next Entity)...',
    );

    const enabledEntities = this.DAILY_ENTITIES_CONFIG.filter(
      (entity) => entity.enabled,
    );

    this.logger.log(
      `📋 Sequential Flow: ${enabledEntities.map((e) => e.name).join(' → ')}`,
    );

    try {
      for (let i = 0; i < enabledEntities.length; i++) {
        const entity = enabledEntities[i];

        this.logger.log(
          `🔄 [${i + 1}/${enabledEntities.length}] Processing ${entity.name}...`,
        );

        await this.executeEntitySequentially(entity);

        this.logger.log(
          `✅ [${i + 1}/${enabledEntities.length}] ${entity.name} completed`,
        );

        if (i < enabledEntities.length - 1) {
          this.logger.log(`⏳ Brief pause before next entity...`);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      this.logger.log('🎉 Daily Sequential Execution completed successfully');
    } catch (error) {
      this.logger.error(
        `❌ Daily Sequential Execution failed: ${error.message}`,
      );
      throw error;
    }
  }

  async getSchedulerStatus(): Promise<any> {
    const runningSyncs = await this.checkRunningSyncs();
    const productStatus = this.getDailyProductStatus();

    const coreMainCycle = runningSyncs.filter((sync) =>
      [
        'main_cycle',
        'main_sync_cycle',
        'customer_recent',
        'invoice_recent',
        'order_recent',
      ].includes(sync.name),
    );

    const larkBaseSyncs = runningSyncs.filter((sync) =>
      ['customer_lark_sync', 'invoice_lark_sync', 'order_lark_sync'].includes(
        sync.name,
      ),
    );

    const dailyCycleSyncs = runningSyncs.filter((sync) =>
      [
        'pricebook_historical',
        'product_historical',
        'purchase_order_historical',
        'purchase_order_detail_historical',
        'order_supplier_historical',
        'order_supplier_detail_historical',
        'daily_product_cycle',
      ].includes(sync.name),
    );

    const getPriorityStatus = () => {
      if (this.isDailyCycleRunning && this.dailyCyclePriorityLevel === 2) {
        return 'DAILY_CYCLE_ACTIVE';
      } else if (this.dailyCyclePriorityLevel === 1) {
        return 'DAILY_CYCLE_PREPARING';
      } else if (this.mainSchedulerSuspendedForDaily) {
        return 'SUSPENDED_FOR_DAILY';
      } else {
        return 'NORMAL_OPERATION';
      }
    };

    return {
      scheduler: {
        mainScheduler: {
          enabled: this.isMainSchedulerEnabled,
          currentStatus:
            this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0
              ? 'SUSPENDED'
              : 'ACTIVE',
          suspendedForDaily: this.mainSchedulerSuspendedForDaily,
          priorityLevel: this.dailyCyclePriorityLevel,
          abortControllerActive: this.mainCycleAbortController !== null,
          nextRun:
            this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0
              ? `Suspended during daily cycle (Priority Level ${this.dailyCyclePriorityLevel})`
              : '4 minutes interval',
          entities: ['customer', 'invoice', 'order'],
          note: 'Enhanced priority management with force abort capability',
        },
        dailyProductSequenceScheduler: {
          enabled: this.isDailyProductSchedulerEnabled,
          currentStatus: this.isDailyCycleRunning ? 'RUNNING' : 'IDLE',
          priorityLevel: this.dailyCyclePriorityLevel,
          nextRun: 'Daily at 23:00 (Vietnam time)',
          entities: this.DAILY_ENTITIES_CONFIG.map((e) => e.name),
          sequence: 'Sequential: Entity sync → LarkBase sync → Next entity',
          isolation:
            'ABSOLUTE PRIORITY - Force stops 7-minute cycle immediately',
          status: productStatus,
          timeout: '10 minutes total (enhanced abort mechanism)',
          execution: 'True sequential execution - no delays',
          dailyCycleStartTime: this.dailyCycleStartTime?.toISOString() || null,
          entitiesConfig: this.getDailyEntitiesStatus(),
        },
        enhancedPriorityManagement: {
          currentPriorityStatus: getPriorityStatus(),
          dailyCyclePriorityLevel: this.dailyCyclePriorityLevel,
          mainCycleAbortController:
            this.mainCycleAbortController !== null ? 'ACTIVE' : 'INACTIVE',
          isMainCycleGracefulShutdown: this.isMainCycleGracefulShutdown,
          forceStopCapability: 'ENABLED',
          waitTimeOptimization: 'ENABLED (immediate activation)',
        },
        currentActivity: {
          coreMainCycleSyncs: coreMainCycle.map((s) => ({
            name: s.name,
            status: s.status,
          })),
          larkBaseSyncs: larkBaseSyncs.map((s) => ({
            name: s.name,
            status: s.status,
          })),
          dailyCycleSyncs: dailyCycleSyncs.map((s) => ({
            name: s.name,
            status: s.status,
          })),
          totalRunningSyncs: runningSyncs.length,
        },
      },
    };
  }

  @Cron('0 0 * * *', {
    name: 'daily_reset',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyReset() {
    this.isDailyProductCompletedToday = false;
    this.lastProductSyncDate = null;
    this.logger.log(
      '🔄 Daily reset completed - Product sync available for today',
    );
  }

  private async runCustomerSync(): Promise<void> {
    try {
      this.logger.log('👥 [Customer] Starting parallel sync...');
      const startTime = Date.now();

      await this.customerService.checkAndRunAppropriateSync();
      await this.autoTriggerCustomerLarkSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`✅ [Customer] Parallel sync completed in ${duration}s`);

      return Promise.resolve();
    } catch (error) {
      this.logger.error(`❌ [Customer] Parallel sync failed: ${error.message}`);
      throw new Error(`Customer sync failed: ${error.message}`);
    }
  }

  private async runInvoiceSync(): Promise<void> {
    try {
      this.logger.log('🧾 [Invoice] Starting parallel sync...');
      const startTime = Date.now();

      await this.invoiceService.checkAndRunAppropriateSync();
      await this.autoTriggerInvoiceLarkSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`✅ [Invoice] Parallel sync completed in ${duration}s`);

      return Promise.resolve();
    } catch (error) {
      this.logger.error(`❌ [Invoice] Parallel sync failed: ${error.message}`);
      throw new Error(`Invoice sync failed: ${error.message}`);
    }
  }

  private async runOrderSync(): Promise<void> {
    try {
      this.logger.log('📋 [Order] Starting parallel sync...');
      const startTime = Date.now();

      await this.orderService.checkAndRunAppropriateSync();
      await this.autoTriggerOrderLarkSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`✅ [Order] Parallel sync completed in ${duration}s`);

      return Promise.resolve();
    } catch (error) {
      this.logger.error(`❌ [Order] Parallel sync failed: ${error.message}`);
      throw new Error(`Order sync failed: ${error.message}`);
    }
  }

  async cleanupStuckSyncs(): Promise<number> {
    this.logger.log('🧹 Starting stuck sync cleanup...');

    const now = new Date();
    const stuckThresholdMs = 1 * 60 * 60 * 1000;

    const stuckSyncs = await this.prismaService.syncControl.findMany({
      where: {
        isRunning: true,
        startedAt: {
          not: null,
          lt: new Date(now.getTime() - stuckThresholdMs),
        },
      },
    });

    if (stuckSyncs.length === 0) {
      this.logger.log('✅ No stuck syncs found');
      return 0;
    }

    this.logger.warn(`⚠️ Found ${stuckSyncs.length} stuck syncs`);

    let cleanedCount = 0;

    for (const sync of stuckSyncs) {
      if (!sync.startedAt) {
        this.logger.warn(
          `⚠️ Skipping sync ${sync.name} - no startedAt timestamp`,
        );
        continue;
      }

      const stuckDuration = now.getTime() - sync.startedAt.getTime();
      const stuckHours = Math.round(stuckDuration / (60 * 60 * 1000));

      this.logger.warn(
        `🔧 Cleaning stuck sync: ${sync.name} (running ${stuckHours}h)`,
      );

      try {
        await this.prismaService.syncControl.update({
          where: { id: sync.id },
          data: {
            isRunning: false,
            status: 'force_stopped',
            error: `Force stopped after ${stuckHours}h (stuck sync cleanup)`,
            completedAt: now,
            progress: {
              ...((sync.progress as any) || {}),
              forceStoppedAt: now.toISOString(),
              stuckDuration: `${stuckHours}h`,
            },
          },
        });

        cleanedCount++;
      } catch (error) {
        this.logger.error(
          `❌ Failed to cleanup sync ${sync.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(`✅ Cleaned up ${cleanedCount} stuck syncs`);
    return cleanedCount;
  }

  private async runProductSequenceSync(): Promise<void> {
    try {
      this.logger.log(
        '📦 [ProductSequence] Starting sequential dependency sync...',
      );
      const startTime = Date.now();

      this.logger.log(
        '💰 [1/2] Syncing PriceBooks (dependency for Product)...',
      );

      if (!this.priceBookService) {
        throw new Error(
          'PriceBookService not injected - cannot sync Product without PriceBook dependency',
        );
      }

      await this.enableAndRunPriceBookSync();
      await this.waitForSyncCompletion('pricebook_historical', 300);

      this.logger.log(
        '📦 [2/2] Syncing Products (with PriceBook dependency satisfied)...',
      );
      await this.enableAndRunProductSync();
      await this.waitForSyncCompletion('product_historical', 360);

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(
        `✅ [ProductSequence] Sequential sync completed in ${duration}s`,
      );

      return Promise.resolve();
    } catch (error) {
      this.logger.error(
        `❌ [ProductSequence] Sequential sync failed: ${error.message}`,
      );
      throw new Error(`ProductSequence sync failed: ${error.message}`);
    }
  }

  private async autoTriggerCustomerLarkSync(): Promise<void> {
    try {
      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_recent' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_lark_sync' },
      });

      if (
        recentSync?.status === 'completed' &&
        !recentSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'customer_lark_sync' },
          create: {
            name: 'customer_lark_sync',
            entities: ['customer'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const customersToSync = await this.prismaService.customer.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

        if (customersToSync.length > 0) {
          try {
            await this.larkCustomerSyncService.syncCustomersToLarkBase(
              customersToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'customer_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered customer LarkBase sync: ${customersToSync.length} customers`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'customer_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto customer LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'customer_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No customers need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(`❌ Auto customer Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerInvoiceLarkSync(): Promise<void> {
    try {
      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'invoice_recent' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'invoice_lark_sync' },
      });

      if (
        recentSync?.status === 'completed' &&
        !recentSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'invoice_lark_sync' },
          create: {
            name: 'invoice_lark_sync',
            entities: ['invoice'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const invoicesToSync = await this.prismaService.invoice.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

        if (invoicesToSync.length > 0) {
          try {
            await this.larkInvoiceSyncService.syncInvoicesToLarkBase(
              invoicesToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'invoice_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered invoice LarkBase sync: ${invoicesToSync.length} invoices`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'invoice_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto invoice LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'invoice_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No invoices need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(`❌ Auto invoice Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerOrderLarkSync(): Promise<void> {
    try {
      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_recent' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_lark_sync' },
      });

      if (
        recentSync?.status === 'completed' &&
        !recentSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'order_lark_sync' },
          create: {
            name: 'order_lark_sync',
            entities: ['order'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const ordersToSync = await this.prismaService.order.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

        if (ordersToSync.length > 0) {
          try {
            await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);

            await this.prismaService.syncControl.update({
              where: { name: 'order_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered order LarkBase sync: ${ordersToSync.length} orders`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'order_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto order LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'order_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No orders need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(`❌ Auto order Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerProductLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'product_lark_sync' },
          create: {
            name: 'product_lark_sync',
            entities: ['product'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const productsToSync = await this.prismaService.product.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

        if (productsToSync.length > 0) {
          try {
            await this.larkProductSyncService.syncProductsToLarkBase(
              productsToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'product_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered product LarkBase sync: ${productsToSync.length} products`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'product_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto product LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'product_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No products need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(`❌ Auto product Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerSupplierLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'supplier_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'supplier_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'supplier_lark_sync' },
          create: {
            name: 'supplier_lark_sync',
            entities: ['supplier'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const suppliersToSync = await this.prismaService.supplier.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

        if (suppliersToSync.length > 0) {
          try {
            await this.larkSupplierSyncService.syncSuppliersToLarkBase(
              suppliersToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'supplier_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered supplier LarkBase sync: ${suppliersToSync.length} suppliers`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'supplier_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto supplier LarkBase sync failed: ${syncError.message}`,
            );
          }
        }
      } else {
        await this.prismaService.syncControl.update({
          where: { name: 'supplier_lark_sync' },
          data: {
            isRunning: false,
            status: 'completed',
            completedAt: new Date(),
          },
        });

        this.logger.log('📋 No suppliers need LarkBase sync');
      }
    } catch (error) {
      this.logger.error(`❌ Auto supplier Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerOrderSupplierLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_supplier_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_supplier_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'order_supplier_lark_sync' },
          create: {
            name: 'order_supplier_lark_sync',
            entities: ['order_supplier'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const orderSuppliersToSync =
          await this.prismaService.orderSupplier.findMany({
            where: {
              OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
            },
            take: 1000,
          });

        if (orderSuppliersToSync.length > 0) {
          try {
            await this.larkOrderSupplierSyncService.syncOrderSuppliersToLarkBase(
              orderSuppliersToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'order_supplier_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered order_suppliers LarkBase sync: ${orderSuppliersToSync.length} order_suppliers`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'order_supplier_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto order_supplier LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'order_supplier_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No order_supplier need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Auto order_supplier Lark sync failed: ${error.message}`,
      );
    }
  }

  private async autoTriggerOrderSupplierDetailLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_supplier_detail_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'order_supplier_detail_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'order_supplier_detail_lark_sync' },
          create: {
            name: 'order_supplier_detail_lark_sync',
            entities: ['order_supplier_detail'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const orderSuppliersDetailToSync =
          await this.prismaService.orderSupplierDetail.findMany({
            where: {
              OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
            },
            take: 1000,
          });

        if (orderSuppliersDetailToSync.length > 0) {
          try {
            await this.larkOrderSupplierSyncService.syncOrderSupplierDetailsToLarkBase();

            await this.prismaService.syncControl.update({
              where: { name: 'order_supplier_detail_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered order_suppliers_detail LarkBase sync: ${orderSuppliersDetailToSync.length} order_suppliers_detail`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'order_supplier_detail_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto order_supplier_detail LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'order_supplier_detail_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No order_supplier_detail need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Auto order_supplier Lark sync failed: ${error.message}`,
      );
    }
  }

  private async autoTriggerPurchaseOrderLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'purchase_order_lark_sync' },
          create: {
            name: 'purchase_order_lark_sync',
            entities: ['purchase_order'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const purchaseOrdersToSync =
          await this.prismaService.purchaseOrder.findMany({
            where: {
              OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
            },
            take: 1000,
          });

        if (purchaseOrdersToSync.length > 0) {
          try {
            await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
              purchaseOrdersToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'purchase_order_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered purchase_order LarkBase sync: ${purchaseOrdersToSync.length} purchase_orders`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'purchase_order_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto product LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'purchase_order_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No purchase_order need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Auto purchase_order Lark sync failed: ${error.message}`,
      );
    }
  }

  private async autoTriggerPurchaseOrderDetailLarkSync(): Promise<void> {
    try {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_historical' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_detail_lark_sync' },
      });

      if (
        historicalSync?.status === 'completed' &&
        !historicalSync.isRunning &&
        (!larkSync?.isRunning || !larkSync)
      ) {
        await this.prismaService.syncControl.upsert({
          where: { name: 'purchase_order_detail_lark_sync' },
          create: {
            name: 'purchase_order_detail_lark_sync',
            entities: ['purchase_order_detail'],
            syncMode: 'lark_sync',
            isRunning: true,
            isEnabled: true,
            status: 'running',
            startedAt: new Date(),
          },
          update: {
            isRunning: true,
            status: 'running',
            startedAt: new Date(),
            error: null,
          },
        });

        const purchaseOrderDetailsToSync =
          await this.prismaService.purchaseOrderDetail.findMany({
            where: {
              OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
            },
            take: 1000,
          });

        if (purchaseOrderDetailsToSync.length > 0) {
          try {
            await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase(
              purchaseOrderDetailsToSync,
            );

            await this.prismaService.syncControl.update({
              where: { name: 'purchase_order_detail_lark_sync' },
              data: {
                isRunning: false,
                status: 'completed',
                completedAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered purchase_order_detail LarkBase sync: ${purchaseOrderDetailsToSync.length} details`,
            );
          } catch (syncError) {
            await this.prismaService.syncControl.update({
              where: { name: 'purchase_order_detail_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: syncError.message,
                completedAt: new Date(),
              },
            });

            this.logger.error(
              `❌ Auto purchase_order_detail LarkBase sync failed: ${syncError.message}`,
            );
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'purchase_order_detail_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
            },
          });

          this.logger.log('📋 No purchase_order_detail need LarkBase sync');
        }
      }
    } catch (error) {
      this.logger.error(
        `❌ Auto purchase_order_detail Lark sync failed: ${error.message}`,
      );
    }
  }

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Main scheduler (7-minute cycle) enabled');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Main scheduler (7-minute cycle) disabled');
  }

  enableDailyProductScheduler() {
    this.isDailyProductSchedulerEnabled = true;
    this.logger.log('✅ Daily product scheduler (23:00) enabled');
  }

  disableDailyProductScheduler() {
    this.isDailyProductSchedulerEnabled = false;
    this.logger.log('🔇 Daily sync scheduler (23:00) disabled');
  }

  private async runStartupCheck() {
    try {
      this.logger.log('🔍 Running startup check...');

      if (this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0) {
        this.logger.log('⏸️ Skipping startup syncs - Daily cycle has priority');
        return;
      }

      const stuckSyncs = await this.prismaService.syncControl.updateMany({
        where: { isRunning: true },
        data: { isRunning: false, status: 'interrupted' },
      });

      if (stuckSyncs.count > 0) {
        this.logger.log(
          `🔄 Reset ${stuckSyncs.count} stuck sync(s) from previous session`,
        );
      }

      await this.cleanupOldSyncPatterns();

      this.logger.log(
        '📋 Running parallel startup sync checks (7-minute entities only)...',
      );

      this.startupAbortController = new AbortController();
      const signal = this.startupAbortController.signal;

      const startupPromises = [
        this.executeAbortableStartupSync('customer', signal, () =>
          this.customerService.checkAndRunAppropriateSync(),
        ),
        this.executeAbortableStartupSync('invoice', signal, () =>
          this.invoiceService.checkAndRunAppropriateSync(),
        ),
        this.executeAbortableStartupSync('order', signal, () =>
          this.orderService.checkAndRunAppropriateSync(),
        ),
      ];

      await Promise.allSettled(startupPromises);

      this.logger.log('✅ Startup check completed');
    } catch (error) {
      this.logger.error(`❌ Startup check failed: ${error.message}`);
    } finally {
      this.startupAbortController = null;
    }
  }

  private async cleanupOldSyncPatterns(): Promise<number> {
    const now = new Date();
    const STUCK_THRESHOLD_HOURS = 4;

    const stuckSyncs = await this.checkRunningSyncsWithNullSafety();

    let cleanedCount = 0;

    for (const sync of stuckSyncs) {
      if (!sync.startedAt) continue;

      const stuckDuration = now.getTime() - sync.startedAt.getTime();
      const stuckHours = Math.round(stuckDuration / (60 * 60 * 1000));

      this.logger.warn(
        `🔧 Cleaning stuck sync: ${sync.name} (running ${stuckHours}h)`,
      );

      try {
        await this.prismaService.syncControl.update({
          where: { id: sync.id },
          data: {
            isRunning: false,
            status: 'force_stopped',
            error: `Force stopped after ${stuckHours}h (stuck sync cleanup)`,
            completedAt: now,
            progress: {
              ...((sync.progress as any) || {}),
              forceStoppedAt: now.toISOString(),
              stuckDuration: `${stuckHours}h`,
            },
          },
        });

        cleanedCount++;
      } catch (error) {
        this.logger.error(
          `❌ Failed to cleanup sync ${sync.name}: ${error.message}`,
        );
      }
    }

    this.logger.log(`✅ Cleaned up ${cleanedCount} stuck syncs`);
    return cleanedCount;
  }

  private async checkRunningSyncsWithNullSafety(): Promise<any[]> {
    const runningSyncs = await this.prismaService.syncControl.findMany({
      where: {
        isRunning: true,
        startedAt: { not: null },
      },
    });

    return runningSyncs.filter(
      (sync) => sync.startedAt && sync.name && typeof sync.name === 'string',
    );
  }

  private async enableAndRunPriceBookSync(): Promise<void> {
    try {
      this.logger.log('💰 Enabling and running PriceBook sync...');

      await this.priceBookService.enableHistoricalSync();

      await this.priceBookService.syncHistoricalPriceBooks();

      this.logger.log('✅ PriceBook sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ PriceBook sync failed: ${error.message}`);
      throw new Error(`PriceBook sync failed: ${error.message}`);
    }
  }

  private async enableAndRunProductSync(): Promise<void> {
    try {
      this.logger.log('📦 Enabling and running Product sync...');

      await this.productService.enableHistoricalSync();

      await this.productService.syncHistoricalProducts();

      this.logger.log('✅ Product sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ Product sync failed: ${error.message}`);
      throw new Error(`Product sync failed: ${error.message}`);
    }
  }

  private async enableAndRunOrderSupplierSync(): Promise<void> {
    try {
      this.logger.log('📦 Enabling and running OrderSupplier sync...');

      await this.orderSupplierService.enableHistoricalSync();

      await this.orderSupplierService.syncHistoricalOrderSuppliers();

      this.logger.log('✅ OrderSupplier sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ OrderSupplier sync failed: ${error.message}`);
      throw new Error(`OrderSupplier sync failed: ${error.message}`);
    }
  }

  private async enableAndRunOrderSupplierDetailSync(): Promise<void> {
    try {
      this.logger.log('📦 Enabling and running OrderSupplierDetail sync...');

      const orderSupplierDetailSync =
        await this.prismaService.syncControl.findFirst({
          where: { name: 'order_supplier_historical' },
        });

      if (
        !orderSupplierDetailSync ||
        orderSupplierDetailSync.status !== 'completed'
      ) {
        this.logger.warn(
          '⚠️ OrderSupplierDetail sync not completed, skipping OrderSupplierDetail sync',
        );
        return;
      }

      await this.prismaService.syncControl.upsert({
        where: { name: 'order_supplier_detail_historical' },
        create: {
          name: 'order_supplier_detail_historical',
          entities: ['order_supplier_detail'],
          syncMode: 'historical',
          isEnabled: true,
          isRunning: false,
          status: 'idle',
        },
        update: {
          isEnabled: true,
          isRunning: false,
          status: 'idle',
          error: null,
        },
      });
      await this.syncOrderSupplierDetailsFromDatabase();

      this.logger.log('✅ OrderSupplierDetail sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ OrderSupplierDetail sync failed: ${error.message}`);
      throw new Error(`OrderSupplierDetail sync failed: ${error.message}`);
    }
  }

  private async enableAndRunPurchaseOrderSync(): Promise<void> {
    try {
      this.logger.log('📦 Enabling and running Purchase Order sync...');

      await this.purchaseOrderService.enableHistoricalSync();

      await this.purchaseOrderService.syncHistoricalPurchaseOrder();

      this.logger.log('✅ PurchaseOrder sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ PurchaseOrder sync failed: ${error.message}`);
      throw new Error(`PurchaseOrder sync failed: ${error.message}`);
    }
  }

  private async enableAndRunPurchaseOrderDetailSync(): Promise<void> {
    try {
      this.logger.log('📦 Enabling and running Purchase Order Detail sync...');

      const purchaseOrderSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'purchase_order_historical' },
      });

      if (!purchaseOrderSync || purchaseOrderSync.status !== 'completed') {
        this.logger.warn(
          '⚠️ PurchaseOrder sync not completed, skipping PurchaseOrderDetail sync',
        );
        return;
      }

      await this.prismaService.syncControl.upsert({
        where: { name: 'purchase_order_detail_historical' },
        create: {
          name: 'purchase_order_detail_historical',
          entities: ['purchase_order_detail'],
          syncMode: 'historical',
          isEnabled: true,
          isRunning: false,
          status: 'idle',
        },
        update: {
          isEnabled: true,
          isRunning: false,
          status: 'idle',
          error: null,
        },
      });
      await this.syncPurchaseOrderDetailsFromDatabase();

      this.logger.log('✅ PurchaseOrderDetail sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ PurchaseOrderDetail sync failed: ${error.message}`);
      throw new Error(`PurchaseOrderDetail sync failed: ${error.message}`);
    }
  }

  private async syncPurchaseOrderDetailsFromDatabase(): Promise<void> {
    try {
      await this.prismaService.syncControl.update({
        where: { name: 'purchase_order_detail_historical' },
        data: {
          isRunning: true,
          status: 'running',
          startedAt: new Date(),
        },
      });

      this.logger.log('📊 Starting PurchaseOrderDetail sync from database...');

      const purchaseOrdersWithDetails =
        await this.prismaService.purchaseOrder.findMany({
          where: {
            details: {
              some: {
                OR: [
                  { larkSyncStatus: 'PENDING' },
                  { larkSyncStatus: 'FAILED' },
                ],
              },
            },
          },
          include: {
            details: {
              where: {
                OR: [
                  { larkSyncStatus: 'PENDING' },
                  { larkSyncStatus: 'FAILED' },
                ],
              },
            },
          },
          orderBy: { createdDate: 'asc' },
        });

      if (purchaseOrdersWithDetails.length === 0) {
        this.logger.log('📋 No PurchaseOrderDetails need sync');

        await this.prismaService.syncControl.update({
          where: { name: 'purchase_order_detail_historical' },
          data: {
            isRunning: false,
            status: 'completed',
            completedAt: new Date(),
          },
        });
        return;
      }

      const allDetailIds = purchaseOrdersWithDetails
        .flatMap((po) => po.details)
        .map((detail) => detail.id);

      await this.prismaService.purchaseOrderDetail.updateMany({
        where: {
          id: { in: allDetailIds },
          OR: [{ larkSyncStatus: 'FAILED' }],
        },
        data: { larkSyncStatus: 'PENDING' },
      });

      this.logger.log(
        `📊 Found ${purchaseOrdersWithDetails.length} PurchaseOrders with ${allDetailIds.length} details to sync`,
      );

      // Sync the details to LarkBase
      await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase(
        purchaseOrdersWithDetails,
      );

      await this.prismaService.syncControl.update({
        where: { name: 'purchase_order_detail_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
        },
      });

      this.logger.log('✅ PurchaseOrderDetail historical sync completed');
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'purchase_order_detail_historical' },
        data: {
          isRunning: false,
          status: 'failed',
          error: error.message,
          completedAt: new Date(),
        },
      });

      this.logger.error(`❌ PurchaseOrderDetail sync failed: ${error.message}`);
      throw error;
    }
  }

  private async syncOrderSupplierDetailsFromDatabase(): Promise<void> {
    try {
      await this.prismaService.syncControl.update({
        where: { name: 'order_supplier_detail_historical' },
        data: {
          isRunning: true,
          status: 'running',
          startedAt: new Date(),
        },
      });

      const orderSupplierDetailsToSync =
        await this.prismaService.orderSupplierDetail.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      if (orderSupplierDetailsToSync.length === 0) {
        this.logger.log('📋 No OrderSupplierDetails need sync');
        await this.prismaService.syncControl.update({
          where: { name: 'order_supplier_detail_historical' },
          data: {
            isRunning: false,
            status: 'completed',
            completedAt: new Date(),
          },
        });
        return;
      }

      this.logger.log(
        `🔄 Syncing ${orderSupplierDetailsToSync.length} OrderSupplierDetails...`,
      );

      await this.larkOrderSupplierSyncService.syncOrderSupplierDetailsToLarkBase();

      await this.prismaService.syncControl.update({
        where: { name: 'order_supplier_detail_historical' },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
        },
      });
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'order_supplier_detail_historical' },
        data: { isRunning: false, status: 'failed', error: error.message },
      });
      throw error;
    }
  }

  private async waitForSyncCompletion(
    syncName: string,
    maxWaitSeconds: number,
  ): Promise<void> {
    const CHECK_INTERVAL_MS = 5000;
    let waitedSeconds = 0;

    this.logger.log(
      `⏳ Waiting for ${syncName} completion (max ${maxWaitSeconds}s)...`,
    );

    while (waitedSeconds < maxWaitSeconds) {
      const syncControl = await this.prismaService.syncControl.findFirst({
        where: { name: syncName },
      });

      if (!syncControl?.isRunning) {
        if (syncControl?.status === 'completed') {
          this.logger.log(`✅ ${syncName} completed successfully`);
          return;
        } else if (syncControl?.status === 'failed') {
          throw new Error(
            `${syncName} failed: ${syncControl.error || 'Unknown error'}`,
          );
        } else {
          this.logger.log(
            `✅ ${syncName} finished with status: ${syncControl?.status}`,
          );
          return;
        }
      }

      await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL_MS));
      waitedSeconds += CHECK_INTERVAL_MS / 1000;
    }

    throw new Error(`${syncName} timeout after ${maxWaitSeconds} seconds`);
  }

  private async checkRunningSyncs(): Promise<any[]> {
    return await this.prismaService.syncControl.findMany({
      where: { isRunning: true },
    });
  }

  private getDailyProductStatus(): any {
    const today = new Date().toISOString().split('T')[0];
    const isCompletedToday =
      this.isDailyProductCompletedToday && this.lastProductSyncDate === today;

    return {
      completedToday: isCompletedToday,
      lastSyncDate: this.lastProductSyncDate,
      nextAvailableTime: isCompletedToday
        ? 'Tomorrow 23:00'
        : 'Today 23:00 or manual trigger',
      dailyCycleRunning: this.isDailyCycleRunning,
      dailyCycleStartTime: this.dailyCycleStartTime?.toISOString() || null,
    };
  }

  private async updateCycleTracking(
    cycleName: string,
    status: string,
    error?: string,
  ): Promise<void> {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: cycleName },
        create: {
          name: cycleName,
          entities: ['cycle_tracking'],
          syncMode: 'cycle',
          isRunning: status === 'running',
          isEnabled: true,
          status,
          error,
          startedAt: status === 'running' ? new Date() : undefined,
          completedAt: ['completed', 'aborted', 'timeout', 'failed'].includes(
            status,
          )
            ? new Date()
            : undefined,
        },
        update: {
          isRunning: status === 'running',
          status,
          error,
          startedAt: status === 'running' ? new Date() : undefined,
          completedAt: ['completed', 'aborted', 'timeout', 'failed'].includes(
            status,
          )
            ? new Date()
            : undefined,
        },
      });
    } catch (trackingError) {
      this.logger.warn(`⚠️ Cycle tracking failed: ${trackingError.message}`);
    }
  }

  enableDailyEntity(entityName: string): void {
    const entity = this.DAILY_ENTITIES_CONFIG.find(
      (e) => e.name === entityName,
    );
    if (entity) {
      entity.enabled = true;
      this.logger.log(`✅ Daily entity '${entityName}' enabled`);
    } else {
      this.logger.warn(`⚠️ Daily entity '${entityName}' not found`);
    }
  }

  disableDailyEntity(entityName: string): void {
    const entity = this.DAILY_ENTITIES_CONFIG.find(
      (e) => e.name === entityName,
    );
    if (entity) {
      entity.enabled = false;
      this.logger.log(`🔇 Daily entity '${entityName}' disabled`);
    } else {
      this.logger.warn(`⚠️ Daily entity '${entityName}' not found`);
    }
  }

  getDailyEntitiesStatus(): any {
    return this.DAILY_ENTITIES_CONFIG.map((entity) => ({
      name: entity.name,
      enabled: entity.enabled,
      hasLarkSync: !!entity.larkSyncFunction,
      dependencies: entity.dependencies || [],
    }));
  }

  private async executeAbortableStartupSync(
    syncType: string,
    signal: AbortSignal,
    syncFunction: () => Promise<void>,
  ): Promise<void> {
    if (signal.aborted) {
      this.logger.debug(`🚫 ${syncType} startup sync aborted before starting`);
      return;
    }

    try {
      await syncFunction();

      if (signal.aborted) {
        this.logger.debug(
          `🚫 ${syncType} startup sync completed but was marked for abort`,
        );
      }
    } catch (error) {
      if (signal.aborted) {
        this.logger.debug(
          `🚫 ${syncType} startup sync aborted during execution`,
        );
      } else {
        this.logger.error(
          `❌ ${syncType} startup sync failed: ${error.message}`,
        );
      }
    }
  }

  private async forceStopRunningSyncControls(): Promise<void> {
    try {
      const runningSyncs = await this.prismaService.syncControl.findMany({
        where: {
          isRunning: true,
          OR: [
            { name: 'customer_recent' },
            { name: 'invoice_recent' },
            { name: 'order_recent' },
            { name: 'main_cycle' },
            { name: 'main_sync_cycle' },
          ],
        },
      });

      if (runningSyncs.length > 0) {
        await this.prismaService.syncControl.updateMany({
          where: {
            isRunning: true,
            OR: [
              { name: 'customer_recent' },
              { name: 'invoice_recent' },
              { name: 'order_recent' },
              { name: 'main_cycle' },
              { name: 'main_sync_cycle' },
            ],
          },
          data: {
            isRunning: false,
            status: 'aborted',
            error: 'Aborted by daily cycle priority',
            completedAt: new Date(),
          },
        });

        this.logger.log(
          `🛑 Force stopped ${runningSyncs.length} running sync controls`,
        );
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Failed to force stop sync controls: ${error.message}`,
      );
    }
  }

  async resetAllSyncs(): Promise<number> {
    const result = await this.prismaService.syncControl.updateMany({
      data: { isRunning: false, status: 'idle' },
    });
    this.logger.log(`🔄 Reset ${result.count} sync(s) to idle state`);
    return result.count;
  }

  async forceStopAllSyncs(): Promise<number> {
    const result = await this.prismaService.syncControl.updateMany({
      where: { isRunning: true },
      data: { isRunning: false, status: 'stopped' },
    });
    this.logger.log(`🛑 Force stopped ${result.count} running sync(s)`);
    return result.count;
  }
}
