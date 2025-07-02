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
import { error } from 'console';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isMainSchedulerEnabled = true;
  private isWeeklySchedulerEnabled = true;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
  ) {}

  async onModuleInit() {
    this.logger.log('🚀 BusScheduler initialized - Central sync management');
    this.logger.log('📅 Main cycle: Every 10 minutes');
    this.logger.log('📅 Weekly cycle: Sunday 6 AM (Vietnam time)');

    // Startup check after 5 seconds
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  // ============================================================================
  // MAIN 10-MINUTE SCHEDULER (Current: Customer & Invoice)
  // ============================================================================

  @Cron('*/8 * * * *', {
    name: 'main_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    try {
      this.logger.log('🚀 Starting 8-minute parallel sync cycle...');
      const startTime = Date.now();

      // Check if any critical sync is running
      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Parallel sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        return;
      }

      // Update cycle tracking
      await this.updateCycleTracking('main_cycle', 'running');

      // ✅ Enhanced parallel execution with proper error handling
      const syncPromises = [
        // this.runCustomerSync().catch((error) => {
        //   this.logger.error(`Customer sync failed: ${error.message}`);
        //   return { status: 'rejected', reason: error.message };
        // }),
        // this.runInvoiceSync().catch((error) => {
        //   this.logger.error(`Invoice sync failed: ${error.message}`);
        //   return { status: 'rejected', reason: error.message };
        // }),
        this.runOrderSync().catch((error) => {
          this.logger.error(`Order sync failed: ${error.message}`);

          return { status: 'rejected', reason: error.message };
        }),
      ];

      const results = await Promise.allSettled(syncPromises);
      const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);

      // Process results
      let successCount = 0;
      let failureCount = 0;
      const syncResults: string[] = [];

      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const syncName = i === 0 ? 'Customer' : 'Invoice';

        if (result.status === 'fulfilled') {
          successCount++;
          syncResults.push(`✅ ${syncName}: Success`);
          this.logger.log(
            `✅ [${syncName}] Parallel sync completed successfully`,
          );
        } else {
          failureCount++;
          syncResults.push(`❌ ${syncName}: ${result.reason}`);
          this.logger.error(
            `❌ [${syncName}] Parallel sync failed: ${result.reason}`,
          );
        }
      }

      // Complete cycle with comprehensive summary
      const cycleStatus = failureCount === 0 ? 'completed' : 'partial_failure';
      await this.updateCycleTracking(
        'main_cycle',
        cycleStatus,
        failureCount > 0
          ? `${failureCount}/${results.length} syncs failed`
          : undefined,
      );

      this.logger.log(
        `🎉 Parallel sync cycle ${cycleStatus} in ${totalDuration}s: ${successCount} success, ${failureCount} failed`,
      );
      this.logger.log(`📊 Results: ${syncResults.join(' | ')}`);
    } catch (error) {
      this.logger.error(`❌ Main sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('main_cycle', 'failed', error.message);
    }
  }

  // ============================================================================
  // SUNDAY 6 AM SCHEDULER (Weekly entities)
  // ============================================================================

  @Cron('0 6 * * 0', {
    name: 'weekly_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleWeeklySyncCycle() {
    if (!this.isWeeklySchedulerEnabled) {
      this.logger.debug('🔇 Weekly scheduler is disabled');
      return;
    }

    try {
      this.logger.log('📅 Sunday 6 AM: Weekly sync cycle triggered');

      // Check if any sync is running
      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Weekly sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        this.logger.log('⏸️ Will retry next Sunday');
        return;
      }

      // Update cycle tracking
      await this.updateCycleTracking('weekly_cycle', 'running');

      // Run CustomerGroup sync
      await this.runCustomerGroupSync();

      // ===== FUTURE: ADD OTHER WEEKLY ENTITIES =====
      // await this.runCategorySync();
      // await this.runSupplierSync();

      // Complete cycle
      await this.updateCycleTracking('weekly_cycle', 'completed');

      this.logger.log('✅ Weekly sync cycle completed successfully');
    } catch (error) {
      this.logger.error(`❌ Weekly sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('weekly_cycle', 'failed', error.message);
    }
  }

  // ============================================================================
  // ENTITY-SPECIFIC SYNC METHODS (Current & Future-Ready)
  // ============================================================================

  private async runCustomerSync(): Promise<void> {
    try {
      this.logger.log('👥 [Customer] Starting parallel sync...');
      const startTime = Date.now();

      // ✅ Main sync
      await this.customerService.checkAndRunAppropriateSync();

      // ✅ Auto-trigger Lark sync if recent sync completed
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

      // ✅ Main sync
      await this.invoiceService.checkAndRunAppropriateSync();

      // ✅ FIX: Auto-trigger Lark sync if recent sync completed
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
      this.logger.log('🧾 [Order] Starting parallel sync...');
      const startTime = Date.now();

      // ✅ Main sync
      await this.orderService.checkAndRunAppropriateSync();

      // ✅ FIX: Auto-trigger Lark sync if recent sync completed
      await this.autoTriggerOrderLarkSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`✅ [Order] Parallel sync completed in ${duration}s`);

      return Promise.resolve();
    } catch (error) {
      this.logger.error(`❌ [Order] Parallel sync failed: ${error.message}`);
      throw new Error(`Order sync failed: ${error.message}`);
    }
  }

  private async runCustomerGroupSync(): Promise<void> {
    try {
      this.logger.log('👥 [CustomerGroup] Starting sync...');

      const startTime = Date.now();
      await this.customerGroupService.syncCustomerGroups();
      const duration = ((Date.now() - startTime) / 1000).toFixed(2);

      this.logger.log(`✅ [CustomerGroup] Sync completed in ${duration}s`);
    } catch (error) {
      this.logger.error(`❌ [CustomerGroup] Sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // AUTO-TRIGGER LARK SYNC METHODS - NEW
  // ============================================================================

  private async autoTriggerCustomerLarkSync(): Promise<void> {
    try {
      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_recent' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'customer_lark_sync' },
      });

      // ✅ Trigger if recent sync just completed and Lark sync is not running
      if (
        recentSync?.status === 'completed' &&
        recentSync.isRunning === false &&
        (!larkSync?.isRunning || larkSync.isRunning !== true)
      ) {
        this.logger.log('🔄 Auto-triggering customer Lark sync...');

        // Get recent customers that need sync
        const customersToSync = await this.prismaService.customer.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000, // Limit to prevent overload
        });

        if (customersToSync.length > 0) {
          await this.larkCustomerSyncService.syncCustomersToLarkBase(
            customersToSync,
          );
          this.logger.log(
            `✅ Auto-triggered customer Lark sync: ${customersToSync.length} customers`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Auto-trigger customer Lark sync failed: ${error.message}`,
      );
      // Don't throw - this shouldn't block main sync
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

      // ✅ FIX: Trigger if recent sync just completed and Lark sync is not running
      if (
        recentSync?.status === 'completed' &&
        recentSync.isRunning === false &&
        (!larkSync?.isRunning || larkSync.isRunning !== true)
      ) {
        this.logger.log('🔄 Auto-triggering invoice Lark sync...');

        // Get recent invoices that need sync
        const invoicesToSync = await this.prismaService.invoice.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000, // Limit to prevent overload
        });

        if (invoicesToSync.length > 0) {
          await this.larkInvoiceSyncService.syncInvoicesToLarkBase(
            invoicesToSync,
          );
          this.logger.log(
            `✅ Auto-triggered invoice Lark sync: ${invoicesToSync.length} invoices`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Auto-trigger invoice Lark sync failed: ${error.message}`,
      );
      // Don't throw - this shouldn't block main sync
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

      // ✅ FIX: Trigger if recent sync just completed and Lark sync is not running
      if (
        recentSync?.status === 'completed' &&
        recentSync.isRunning === false &&
        (!larkSync?.isRunning || larkSync.isRunning !== true)
      ) {
        this.logger.log('🔄 Auto-triggering order Lark sync...');

        // Get recent invoices that need sync
        const ordersToSync = await this.prismaService.order.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000, // Limit to prevent overload
        });

        if (ordersToSync.length > 0) {
          await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
          this.logger.log(
            `✅ Auto-triggered order Lark sync: ${ordersToSync.length} orders`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `⚠️ Auto-trigger order Lark sync failed: ${error.message}`,
      );
      // Don't throw - this shouldn't block main sync
    }
  }

  // ============================================================================
  // MANUAL SCHEDULER CONTROLS
  // ============================================================================

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Main scheduler (10-minute cycle) enabled');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Main scheduler (10-minute cycle) disabled');
  }

  enableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = true;
    this.logger.log('✅ Weekly scheduler (Sunday 6 AM) enabled');
  }

  disableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = false;
    this.logger.log('🔇 Weekly scheduler (Sunday 6 AM) disabled');
  }

  // ============================================================================
  // STARTUP CHECKS & CLEANUP
  // ============================================================================

  private async runStartupCheck() {
    try {
      this.logger.log('🔍 Running startup check...');

      // Reset any stuck syncs from previous session
      const stuckSyncs = await this.prismaService.syncControl.updateMany({
        where: { isRunning: true },
        data: { isRunning: false, status: 'interrupted' },
      });

      if (stuckSyncs.count > 0) {
        this.logger.log(
          `🔄 Reset ${stuckSyncs.count} stuck sync(s) from previous session`,
        );
      }

      // Cleanup old sync patterns
      await this.cleanupOldSyncPatterns();

      // ✅ PARALLEL startup checks
      this.logger.log('📋 Running parallel startup sync checks...');

      const startupPromises = [
        // this.runCustomerSync().catch((error) => {
        //   this.logger.warn(`Customer startup check failed: ${error.message}`);
        //   return Promise.resolve(); // Don't fail startup
        // }),
        // this.runInvoiceSync().catch((error) => {
        //   this.logger.warn(`Invoice startup check failed: ${error.message}`);
        //   return Promise.resolve(); // Don't fail startup
        // }),
        this.runOrderSync().catch((error) => {
          this.logger.warn(`Order startup check failed: ${error.message}`);
          return Promise.resolve(); // Don't fail startup
        }),
      ];

      await Promise.allSettled(startupPromises);

      this.logger.log('✅ Startup check completed successfully');
    } catch (error) {
      this.logger.error(`❌ Startup check failed: ${error.message}`);
      // Don't throw - let the system continue and retry on next cycle
    }
  }

  private async cleanupOldSyncPatterns(): Promise<void> {
    try {
      // Remove old LarkBase-specific sync controls
      const cleanup = await this.prismaService.syncControl.deleteMany({
        where: {
          name: {
            in: [
              'customer_historical_lark',
              'customer_recent_lark',
              'order_historical_lark',
              'order_recent_lark',
              'invoice_historical_lark',
              'invoice_recent_lark',
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

  // ============================================================================
  // STATUS & MONITORING
  // ============================================================================

  async getSchedulerStatus(): Promise<any> {
    const runningSyncs = await this.checkRunningSyncs();

    return {
      scheduler: {
        mainScheduler: {
          enabled: this.isMainSchedulerEnabled,
          nextRun: '8 minutes interval',
          entities: ['customer', 'invoice', 'order'], // ← CẬP NHẬT
        },
        weeklyScheduler: {
          enabled: this.isWeeklySchedulerEnabled,
          nextRun: 'Sunday 6 AM (Vietnam time)',
          entities: ['customergroup'],
        },
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

  // ============================================================================
  // TRACKING & UTILITY METHODS
  // ============================================================================

  private async checkRunningSyncs() {
    return await this.prismaService.syncControl.findMany({
      where: { isRunning: true },
      select: { name: true, startedAt: true },
    });
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
          cycleName === 'main_cycle'
            ? ['customer', 'invoice', 'order'] // ← CẬP NHẬT
            : ['customergroup'],
        syncMode: 'cycle',
        isRunning: status === 'running',
        isEnabled: true,
        status,
        error,
        startedAt: status === 'running' ? new Date() : undefined,
        completedAt: status !== 'running' ? new Date() : undefined,
        lastRunAt: new Date(),
      },
      update: {
        isRunning: status === 'running',
        status,
        error,
        startedAt: status === 'running' ? new Date() : undefined,
        completedAt: status !== 'running' ? new Date() : undefined,
        lastRunAt: new Date(),
      },
    });
  }
}
