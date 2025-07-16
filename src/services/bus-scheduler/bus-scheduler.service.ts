// src/services/bus-scheduler/bus-scheduler.service.ts
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietCustomerGroupService } from '../kiot-viet/customer-group/customer-group.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { KiotVietPriceBookService } from '../kiot-viet/pricebook/pricebook.service';
import { LarkProductSyncService } from '../lark/product/lark-product-sync.service';

// import { KiotVietCategoryService } from '../kiot-viet/category/category.service';
// import { KiotVietTradeMarkService } from '../kiot-viet/trademark/trademark.service';
// import { KiotVietBranchService } from '../kiot-viet/branch/branch.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isMainSchedulerEnabled = true;
  private isWeeklySchedulerEnabled = true;
  private isDependencySchedulerEnabled = true;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly larkProductSyncService: LarkProductSyncService,
    private readonly productService: KiotVietProductService,
    // private readonly categoryService: KiotVietCategoryService,
    // private readonly tradeMarkService: KiotVietTradeMarkService,
    // private readonly branchService: KiotVietBranchService,
    private readonly priceBookService: KiotVietPriceBookService,
  ) {}

  async onModuleInit() {
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  @Cron('*/7 * * * *', {
    name: 'unified_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    try {
      this.logger.log('🚀 Starting unified parallel sync cycle...');
      const startTime = Date.now();

      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Unified sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        return;
      }

      await this.updateCycleTracking('unified_cycle', 'running');

      const CYCLE_TIMEOUT_MS = 35 * 60 * 1000;

      try {
        const cyclePromise = this.executeHybridParallelSyncs();
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Cycle timeout after 35 minutes')),
            CYCLE_TIMEOUT_MS,
          ),
        );

        await Promise.race([cyclePromise, timeoutPromise]);

        await this.updateCycleTracking('unified_cycle', 'completed');

        const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
        this.logger.log(`🎉 Unified sync cycle completed in ${totalDuration}s`);
      } catch (timeoutError) {
        if (
          timeoutError instanceof Error &&
          timeoutError.message.includes('timeout')
        ) {
          this.logger.error(`⏰ Sync cycle timed out after 35 minutes`);
          await this.updateCycleTracking(
            'unified_cycle',
            'timeout',
            'Cycle exceeded 35 minute timeout',
          );
        } else {
          throw timeoutError;
        }
      }
    } catch (error) {
      this.logger.error(`❌ Unified sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('unified_cycle', 'failed', error.message);
    }
  }

  private async runProductSequenceSync(): Promise<void> {
    try {
      this.logger.log(
        '📦 [ProductSequence] Starting sequential dependency sync...',
      );
      const startTime = Date.now();

      // this.logger.log('🏷️ [1/4] Syncing Categories...');
      // await this.enableAndRunCategorySync();
      // await this.waitForSyncCompletion('category_historical', 300);

      // this.logger.log('🏢 [2/4] Syncing TradeMarks...');
      // await this.enableAndRunTradeMarkSync();
      // await this.waitForSyncCompletion('trademark_historical', 300);

      this.logger.log('💰 [1/2] Syncing PriceBooks...');
      await this.enableAndRunPriceBookSync();
      await this.waitForSyncCompletion('pricebook_historical', 300);

      this.logger.log('📦 [2/2] Syncing Products (with full dependencies)...');
      await this.enableAndRunProductSync();
      await this.waitForSyncCompletion('product_historical', 1800);

      // Auto-trigger Product LarkBase sync
      await this.autoTriggerProductLarkSync();

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

  private async executeHybridParallelSyncs(): Promise<void> {
    this.logger.log(
      '🔀 Starting hybrid parallel execution: Customer ⫸ Invoice ⫸ Order ⫸ ProductSequence',
    );

    const syncPromises = [
      this.runCustomerSync().catch((error) => {
        this.logger.error(
          `❌ [Customer] Parallel sync failed: ${error.message}`,
        );
        return { status: 'rejected', reason: error.message, sync: 'Customer' };
      }),
      this.runInvoiceSync().catch((error) => {
        this.logger.error(
          `❌ [Invoice] Parallel sync failed: ${error.message}`,
        );
        return { status: 'rejected', reason: error.message, sync: 'Invoice' };
      }),
      this.runOrderSync().catch((error) => {
        this.logger.error(`❌ [Order] Parallel sync failed: ${error.message}`);
        return { status: 'rejected', reason: error.message, sync: 'Order' };
      }),
      this.runProductSequenceSync().catch((error) => {
        this.logger.error(
          `❌ [ProductSequence] Parallel sync failed: ${error.message}`,
        );
        return {
          status: 'rejected',
          reason: error.message,
          sync: 'ProductSequence',
        };
      }),
    ];

    const results = await Promise.allSettled(syncPromises);
    await this.executeStaggeredLarkSync(results);
  }

  private async executeStaggeredLarkSync(syncResults: any[]): Promise<void> {
    this.logger.log('🚀 Starting staggered LarkBase sync for all entities...');

    const successfulSyncs = syncResults
      .map((result, index) => ({
        result,
        syncType: ['customer', 'invoice', 'order', 'productsequence'][index],
      }))
      .filter(({ result }) => result.status === 'fulfilled' || !result.reason);

    let delay = 0;

    for (const { syncType } of successfulSyncs) {
      if (delay > 0) {
        this.logger.log(
          `⏳ Waiting ${delay / 1000}s before ${syncType} LarkBase sync...`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      try {
        switch (syncType) {
          case 'customer':
            await this.runCustomerLarkSync();
            break;
          case 'invoice':
            await this.runInvoiceLarkSync();
            break;
          case 'order':
            await this.runOrderLarkSync();
            break;
          case 'productsequence':
            await this.runProductLarkSync();
            break;
        }
        this.logger.log(`✅ ${syncType} LarkBase sync completed`);
      } catch (error) {
        this.logger.error(
          `❌ Auto ${syncType} Lark sync failed: ${error.message}`,
        );
      }

      delay += 30000; // 30-second stagger
    }
  }

  private async runCustomerLarkSync(): Promise<void> {
    const customersToSync = await this.prismaService.customer.findMany({
      where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
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
      where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
      take: 100,
    });

    if (invoicesToSync.length > 0) {
      await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoicesToSync);
    }
  }

  private async runOrderLarkSync(): Promise<void> {
    const ordersToSync = await this.prismaService.order.findMany({
      where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
      take: 100,
    });

    if (ordersToSync.length > 0) {
      await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
    }
  }

  private async runProductLarkSync(): Promise<void> {
    const productsToSync = await this.prismaService.product.findMany({
      where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
      take: 100,
    });

    if (productsToSync.length > 0) {
      await this.larkProductSyncService.syncProductsToLarkBase(productsToSync);
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

  private async checkRunningSyncsWithNullSafety(): Promise<any[]> {
    const runningSyncs = await this.prismaService.syncControl.findMany({
      where: { isRunning: true },
      select: { name: true, startedAt: true },
    });

    return runningSyncs.filter(
      (sync) => sync.startedAt && sync.name && typeof sync.name === 'string',
    );
  }

  // @Cron('0 6 * * 0', {
  //   name: 'weekly_sync_cycle',
  //   timeZone: 'Asia/Ho_Chi_Minh',
  // })
  // async handleWeeklySyncCycle() {
  //   if (!this.isWeeklySchedulerEnabled) {
  //     this.logger.debug('🔇 Weekly scheduler is disabled');
  //     return;
  //   }

  //   try {
  //     this.logger.log('📅 Sunday 6 AM: Weekly sync cycle triggered');

  //     // Check if any sync is running
  //     const runningSyncs = await this.checkRunningSyncs();
  //     if (runningSyncs.length > 0) {
  //       this.logger.log(
  //         `⏸️ Weekly sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
  //       );
  //       this.logger.log('⏸️ Will retry next Sunday');
  //       return;
  //     }

  //     // Update cycle tracking
  //     await this.updateCycleTracking('weekly_cycle', 'running');

  //     // Run CustomerGroup sync
  //     await this.runCustomerGroupSync();

  //     // Complete cycle
  //     await this.updateCycleTracking('weekly_cycle', 'completed');

  //     this.logger.log('✅ Weekly sync cycle completed successfully');
  //   } catch (error) {
  //     this.logger.error(`❌ Weekly sync cycle failed: ${error.message}`);
  //     await this.updateCycleTracking('weekly_cycle', 'failed', error.message);
  //   }
  // }

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

  // private async runCustomerGroupSync(): Promise<void> {
  //   try {
  //     this.logger.log('👥 [CustomerGroup] Starting sync...');

  //     const startTime = Date.now();
  //     await this.customerGroupService.syncCustomerGroups();
  //     const duration = ((Date.now() - startTime) / 1000).toFixed(2);

  //     this.logger.log(`✅ [CustomerGroup] Sync completed in ${duration}s`);
  //   } catch (error) {
  //     this.logger.error(`❌ [CustomerGroup] Sync failed: ${error.message}`);
  //     throw error;
  //   }
  // }

  private async autoTriggerCustomerLarkSync(): Promise<void> {
    try {
      const customersToSync = await this.prismaService.customer.findMany({
        where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
        take: 100,
      });

      if (customersToSync.length > 0) {
        await this.larkCustomerSyncService.syncCustomersToLarkBase(
          customersToSync,
        );
      }
    } catch (error) {
      this.logger.error(`❌ Auto customer Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerInvoiceLarkSync(): Promise<void> {
    try {
      const invoicesToSync = await this.prismaService.invoice.findMany({
        where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
        take: 1000,
      });

      if (invoicesToSync.length > 0) {
        await this.larkInvoiceSyncService.syncInvoicesToLarkBase(
          invoicesToSync,
        );
      }
    } catch (error) {
      this.logger.error(`❌ Auto invoice Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerOrderLarkSync(): Promise<void> {
    try {
      const ordersToSync = await this.prismaService.order.findMany({
        where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
        take: 1000,
      });

      if (ordersToSync.length > 0) {
        await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
      }
    } catch (error) {
      this.logger.error(`❌ Auto order Lark sync failed: ${error.message}`);
    }
  }

  private async autoTriggerProductLarkSync(): Promise<void> {
    try {
      const productsToSync = await this.prismaService.product.findMany({
        where: { larkSyncStatus: { in: ['PENDING', 'FAILED'] } },
        take: 1000,
      });

      if (productsToSync.length > 0) {
        await this.larkProductSyncService.syncProductsToLarkBase(
          productsToSync,
        );
      }
    } catch (error) {
      this.logger.error(`❌ Auto product Lark sync failed: ${error.message}`);
    }
  }

  // private async enableAndRunCategorySync(): Promise<void> {
  //   try {
  //     await this.categoryService.enableHistoricalSync();
  //     await this.categoryService.syncHistoricalCategories();
  //   } catch (error) {
  //     throw new Error(`Category sync failed: ${error.message}`);
  //   }
  // }

  // private async enableAndRunTradeMarkSync(): Promise<void> {
  //   try {
  //     await this.tradeMarkService.enableHistoricalSync();
  //     await this.tradeMarkService.syncHistoricalTradeMarks();
  //   } catch (error) {
  //     throw new Error(`TradeMark sync failed: ${error.message}`);
  //   }
  // }

  private async enableAndRunPriceBookSync(): Promise<void> {
    try {
      await this.priceBookService.enableHistoricalSync();
      await this.priceBookService.syncHistoricalPriceBooks();
    } catch (error) {
      throw new Error(`PriceBook sync failed: ${error.message}`);
    }
  }

  private async enableAndRunProductSync(): Promise<void> {
    try {
      await this.productService.enableHistoricalSync();
      await this.productService.syncHistoricalProducts();
    } catch (error) {
      throw new Error(`Product sync failed: ${error.message}`);
    }
  }

  private async waitForSyncCompletion(
    syncName: string,
    timeoutSeconds: number,
  ): Promise<void> {
    this.logger.log(
      `⏳ Waiting for ${syncName} to complete (timeout: ${timeoutSeconds}s)...`,
    );

    const startTime = Date.now();
    const timeout = timeoutSeconds * 1000;

    while (Date.now() - startTime < timeout) {
      const syncControl = await this.prismaService.syncControl.findFirst({
        where: { name: syncName },
      });

      if (!syncControl?.isRunning) {
        this.logger.log(`✅ ${syncName} completed successfully`);
        return;
      }

      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    throw new Error(`${syncName} timeout after ${timeoutSeconds} seconds`);
  }

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Unified scheduler (7-minute cycle) enabled');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Unified scheduler (7-minute cycle) disabled');
  }

  enableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = true;
    this.logger.log('✅ Weekly scheduler (Sunday 6 AM) enabled');
  }

  disableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = false;
    this.logger.log('🔇 Weekly scheduler (Sunday 6 AM) disabled');
  }

  private async runStartupCheck() {
    try {
      this.logger.log('🔍 Running startup check...');

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

      this.logger.log('✅ Startup check completed successfully');
    } catch (error) {
      this.logger.error(`❌ Startup check failed: ${error.message}`);
    }
  }

  private async cleanupOldSyncPatterns(): Promise<void> {
    try {
      const cleanup = await this.prismaService.syncControl.deleteMany({
        where: {
          name: {
            in: [
              'customer_historical_lark',
              'customer_recent_lark',
              'invoice_historical_lark',
              'invoice_recent_lark',
              'order_historical_lark',
              'order_recent_lark',
              'dependency_cycle', // Remove old dependency cycle
            ],
          },
        },
      });

      if (cleanup.count > 0) {
        this.logger.log(`🧹 Cleaned up ${cleanup.count} old sync pattern(s)`);
      }
    } catch (error) {
      this.logger.warn(`⚠️ Cleanup failed: ${error.message}`);
    }
  }

  async getSchedulerStatus(): Promise<any> {
    const runningSyncs = await this.checkRunningSyncs();

    return {
      scheduler: {
        unifiedScheduler: {
          enabled: this.isMainSchedulerEnabled,
          nextRun: '7 minutes interval',
          entities: ['customer', 'invoice', 'order', 'productsequence'],
          architecture: 'Hybrid Parallel-Sequential',
          productSequence: 'PriceBook → Product',
        },
        // weeklyScheduler: {
        //   enabled: this.isWeeklySchedulerEnabled,
        //   nextRun: 'Sunday 6 AM (Vietnam time)',
        //   entities: ['customergroup'],
        // },
      },
      runningTasks: runningSyncs.length,
      runningSyncs: runningSyncs.map((sync) => ({
        name: sync.name,
        startedAt: sync.startedAt,
      })),
    };
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

  async checkRunningSyncs(): Promise<any[]> {
    return this.checkRunningSyncsWithNullSafety();
  }

  private async updateCycleTracking(
    cycleName: string,
    status: string,
    error?: string,
  ) {
    await this.prismaService.syncControl.upsert({
      where: { name: cycleName },
      create: {
        name: cycleName,
        entities:
          cycleName === 'unified_cycle'
            ? ['customer', 'invoice', 'order', 'pricebook', 'product']
            : [''],
        syncMode: 'unified',
        isRunning: status === 'running',
        status,
        startedAt: status === 'running' ? new Date() : undefined,
        completedAt: status === 'completed' ? new Date() : undefined,
        error: error || null,
      },
      update: {
        isRunning: status === 'running',
        status,
        startedAt: status === 'running' ? new Date() : undefined,
        completedAt: status === 'completed' ? new Date() : undefined,
        error: error || null,
      },
    });
  }
}
