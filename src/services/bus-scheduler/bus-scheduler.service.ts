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
    this.logger.log('📅 Main cycle: Every 8 minutes');
    this.logger.log('📅 Weekly cycle: Sunday 6 AM (Vietnam time)');

    // Startup check after 5 seconds
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  // ============================================================================
  // MAIN 8-MINUTE SCHEDULER - ENHANCED WITH TIMEOUT PROTECTION
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

      // ✅ ENHANCED: Check with auto-cleanup of stuck syncs
      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Parallel sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        return;
      }

      // ✅ ENHANCED: Set cycle with timeout protection
      await this.updateCycleTracking('main_cycle', 'running');

      // ✅ ENHANCED: Add timeout protection for entire cycle
      const CYCLE_TIMEOUT_MS = 10 * 60 * 1000; // 10 minutes max

      try {
        // ✅ CONSOLIDATED: Inline parallel sync execution with timeout
        const cyclePromise = this.executeParallelSyncs();
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Cycle timeout after 10 minutes')),
            CYCLE_TIMEOUT_MS,
          ),
        );

        await Promise.race([cyclePromise, timeoutPromise]);

        // Success - mark as completed
        await this.updateCycleTracking('main_cycle', 'completed');

        const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
        this.logger.log(
          `🎉 Parallel sync cycle completed in ${totalDuration}s`,
        );
      } catch (timeoutError) {
        if (
          timeoutError instanceof Error &&
          timeoutError.message.includes('timeout')
        ) {
          this.logger.error(`⏰ Sync cycle timed out after 10 minutes`);
          await this.updateCycleTracking(
            'main_cycle',
            'timeout',
            'Cycle exceeded 10 minute timeout',
          );
        } else {
          throw timeoutError; // Re-throw non-timeout errors
        }
      }
    } catch (error) {
      this.logger.error(`❌ Main sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('main_cycle', 'failed', error.message);
    }
  }

  // ✅ CONSOLIDATED: Inline parallel sync execution
  private async executeParallelSyncs(): Promise<void> {
    const syncPromises = [
      this.runOrderSync().catch((error) => {
        this.logger.error(`❌ [Order] Parallel sync failed: ${error.message}`);
        return { status: 'rejected', reason: error.message, sync: 'Order' };
      }),
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
    ];

    const results = await Promise.allSettled(syncPromises);

    // 🆕 ADDED: Stagger LarkBase sync to prevent collision
    await this.executeStaggeredLarkSync(results);
  }

  // 🆕 ADD these new methods after executeParallelSyncs:

  private async executeStaggeredLarkSync(syncResults: any[]): Promise<void> {
    this.logger.log('🚀 Starting staggered LarkBase sync...');

    const successfulSyncs = syncResults
      .map((result, index) => ({
        result,
        syncType: ['order', 'customer', 'invoice'][index],
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
        }
        this.logger.log(`✅ ${syncType} LarkBase sync completed`);
      } catch (error) {
        this.logger.error(
          `❌ Auto ${syncType} Lark sync failed: ${error.message}`,
        );
        // Continue with next sync even if one fails
      }

      delay += 15000; // 15 second delay between each LarkBase sync
    }
  }

  private async runCustomerLarkSync(): Promise<void> {
    const customersToSync = await this.prismaService.customer.findMany({
      where: {
        larkSyncStatus: { in: ['PENDING', 'FAILED'] },
      },
      take: 100, // Limit to prevent large batches
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

  // 🆕 ADD this new method for stuck sync cleanup:

  async cleanupStuckSyncs(): Promise<number> {
    this.logger.log('🧹 Starting stuck sync cleanup...');

    const now = new Date();
    const stuckThresholdMs = 2 * 60 * 60 * 1000; // 2 hours

    // Find syncs running longer than threshold
    const stuckSyncs = await this.prismaService.syncControl.findMany({
      where: {
        isRunning: true,
        startedAt: {
          not: null, // 🆕 ADDED: Ensure startedAt is not null
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
      // 🆕 ADDED: Null check for startedAt (extra safety)
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
      where: {
        isRunning: true,
        startedAt: { not: null }, // Ensure startedAt exists
      },
    });

    // Filter out any syncs without proper timing
    return runningSyncs.filter(
      (sync) => sync.startedAt && sync.name && typeof sync.name === 'string',
    );
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

      // Complete cycle
      await this.updateCycleTracking('weekly_cycle', 'completed');

      this.logger.log('✅ Weekly sync cycle completed successfully');
    } catch (error) {
      this.logger.error(`❌ Weekly sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('weekly_cycle', 'failed', error.message);
    }
  }

  // ============================================================================
  // ENTITY-SPECIFIC SYNC METHODS
  // ============================================================================

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
  // AUTO-TRIGGER LARK SYNC METHODS
  // ============================================================================

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
                lastRunAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered customer Lark sync: ${customersToSync.length} customers`,
            );
          } catch (larkError) {
            await this.prismaService.syncControl.update({
              where: { name: 'customer_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: larkError.message,
              },
            });
            throw larkError;
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'customer_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
              lastRunAt: new Date(),
            },
          });
          this.logger.log(
            '📋 No customers need Lark sync - marked as completed',
          );
        }
      } else {
        this.logger.debug(
          `⏸️ Customer Lark sync conditions not met: recent=${recentSync?.status}(${recentSync?.isRunning}), lark=${larkSync?.status}(${larkSync?.isRunning})`,
        );
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
                lastRunAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered invoice Lark sync: ${invoicesToSync.length} invoices`,
            );
          } catch (larkError) {
            await this.prismaService.syncControl.update({
              where: { name: 'invoice_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: larkError.message,
              },
            });
            throw larkError;
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'invoice_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
              lastRunAt: new Date(),
            },
          });
          this.logger.log(
            '📋 No invoices need Lark sync - marked as completed',
          );
        }
      } else {
        this.logger.debug(
          `⏸️ Invoice Lark sync conditions not met: recent=${recentSync?.status}(${recentSync?.isRunning}), lark=${larkSync?.status}(${larkSync?.isRunning})`,
        );
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
                lastRunAt: new Date(),
              },
            });

            this.logger.log(
              `✅ Auto-triggered order Lark sync: ${ordersToSync.length} orders`,
            );
          } catch (larkError) {
            await this.prismaService.syncControl.update({
              where: { name: 'order_lark_sync' },
              data: {
                isRunning: false,
                status: 'failed',
                error: larkError.message,
              },
            });
            throw larkError;
          }
        } else {
          await this.prismaService.syncControl.update({
            where: { name: 'order_lark_sync' },
            data: {
              isRunning: false,
              status: 'completed',
              completedAt: new Date(),
              lastRunAt: new Date(),
            },
          });
          this.logger.log('📋 No orders need Lark sync - marked as completed');
        }
      } else {
        this.logger.debug(
          `⏸️ Order Lark sync conditions not met: recent=${recentSync?.status}(${recentSync?.isRunning}), lark=${larkSync?.status}(${larkSync?.isRunning})`,
        );
      }
    } catch (error) {
      this.logger.error(`❌ Auto order Lark sync failed: ${error.message}`);
    }
  }

  // ============================================================================
  // SCHEDULER CONTROLS
  // ============================================================================

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Main scheduler (8-minute cycle) enabled');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Main scheduler (8-minute cycle) disabled');
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

      this.logger.log('📋 Running parallel startup sync checks...');

      const startupPromises = [
        this.runOrderSync().catch((error) => {
          this.logger.warn(`Order startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
        this.runCustomerSync().catch((error) => {
          this.logger.warn(`Customer startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
        this.runInvoiceSync().catch((error) => {
          this.logger.warn(`Invoice startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
      ];

      await Promise.allSettled(startupPromises);

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
          entities: ['order', 'customer', 'invoice'],
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
  // TRACKING & UTILITY METHODS - ENHANCED
  // ============================================================================

  async checkRunningSyncs(): Promise<any[]> {
    return this.checkRunningSyncsWithNullSafety();
  }

  // ✅ ENHANCED: updateCycleTracking with progress field fix
  private async updateCycleTracking(
    cycleName: string,
    status: string,
    error?: string,
  ) {
    try {
      await this.prismaService.syncControl.upsert({
        where: { name: cycleName },
        create: {
          name: cycleName,
          entities:
            cycleName === 'main_cycle'
              ? ['order', 'customer', 'invoice']
              : ['customergroup'],
          syncMode: 'cycle',
          isRunning: status === 'running',
          isEnabled: true,
          status,
          error,
          startedAt: status === 'running' ? new Date() : undefined,
          completedAt: status !== 'running' ? new Date() : undefined,
          lastRunAt: new Date(),
          // ✅ FIXED: Use progress instead of metadata
          progress:
            status === 'running'
              ? { stage: 'started', timestamp: new Date().toISOString() }
              : { stage: 'completed', timestamp: new Date().toISOString() },
        },
        update: {
          isRunning: status === 'running',
          status,
          error,
          startedAt: status === 'running' ? new Date() : undefined,
          completedAt: status !== 'running' ? new Date() : undefined,
          lastRunAt: new Date(),
          // ✅ FIXED: Use progress instead of metadata
          progress:
            status === 'running'
              ? { stage: 'started', timestamp: new Date().toISOString() }
              : { stage: 'completed', timestamp: new Date().toISOString() },
        },
      });
    } catch (error) {
      this.logger.error(
        `❌ Failed to update cycle tracking for ${cycleName}: ${error.message}`,
      );
      // Don't throw - prevent cascade failures
    }
  }
}
