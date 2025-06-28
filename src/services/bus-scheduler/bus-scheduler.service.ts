// src/services/bus-scheduler/bus-scheduler.service.ts
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietCustomerGroupService } from '../kiot-viet/customer-group/customer-group.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';

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
  // MAIN 10-MINUTE SCHEDULER (Current: Customer | Future: All entities)
  // ============================================================================

  @Cron('*/10 * * * *', {
    name: 'main_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    try {
      this.logger.log('⏰ 10-minute sync cycle triggered');

      // Check if any sync is running (prevent overlap)
      const runningSyncs = await this.checkRunningSyncs();
      if (runningSyncs.length > 0) {
        this.logger.log(
          `⏸️ Sync already running: ${runningSyncs.map((s) => s.name).join(', ')}`,
        );
        this.logger.log('⏸️ Skipping this cycle - will retry in 10 minutes');
        return;
      }

      // Update cycle tracking
      await this.updateCycleTracking('main_cycle', 'running');

      // ===== PHASE 1: CUSTOMER SYNC =====
      await this.runCustomerSync();

      // ===== PHASE 2: FUTURE ENTITIES (Ready for scaling) =====
      // await this.runOrderSync();
      await this.runInvoiceSync();
      // await this.runProductSync();

      // Complete cycle
      await this.updateCycleTracking('main_cycle', 'completed');

      this.logger.log('✅ 10-minute sync cycle completed successfully');
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
      this.logger.log('👥 [Customer] Starting sync...');

      const startTime = Date.now();
      await this.customerService.checkAndRunAppropriateSync();
      const duration = ((Date.now() - startTime) / 1000).toFixed(2);

      this.logger.log(`✅ [Customer] Sync completed in ${duration}s`);
    } catch (error) {
      this.logger.error(`❌ [Customer] Sync failed: ${error.message}`);
      throw error; // Re-throw to fail the cycle
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

  // ===== FUTURE ENTITY METHODS (Templates for scaling) =====

  // private async runOrderSync(): Promise<void> {
  //   try {
  //     this.logger.log('📦 [Order] Starting sync...');
  //
  //     const startTime = Date.now();
  //     await this.orderService.checkAndRunAppropriateSync();
  //     const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  //
  //     this.logger.log(`✅ [Order] Sync completed in ${duration}s`);
  //   } catch (error) {
  //     this.logger.error(`❌ [Order] Sync failed: ${error.message}`);
  //     // Decision: throw vs continue with other entities
  //     throw error;
  //   }
  // }

  private async runInvoiceSync(): Promise<void> {
    try {
      this.logger.log('🧾 [Invoice] Starting sync...');

      const startTime = Date.now();
      await this.invoiceService.checkAndRunAppropriateSync();
      const duration = ((Date.now() - startTime) / 1000).toFixed(2);

      this.logger.log(`✅ [Invoice] Sync completed in ${duration}s`);
    } catch (error) {
      this.logger.error(`❌ [Invoice] Sync failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================================================
  // STARTUP & SYSTEM METHODS
  // ============================================================================

  private async runStartupCheck(): Promise<void> {
    try {
      this.logger.log('📋 Running startup system check...');

      // Reset any stuck "running" syncs from previous restart
      const stuckSyncs = await this.prismaService.syncControl.updateMany({
        where: { isRunning: true },
        data: {
          isRunning: false,
          status: 'failed',
          error: 'System restart detected',
          completedAt: new Date(),
        },
      });

      if (stuckSyncs.count > 0) {
        this.logger.log(
          `🔄 Reset ${stuckSyncs.count} stuck sync(s) from previous session`,
        );
      }

      // Cleanup old sync patterns (from previous implementations)
      await this.cleanupOldSyncPatterns();

      // Run initial customer sync check
      this.logger.log('📋 Running initial customer + invoice sync check...');
      await this.runCustomerSync();
      await this.runInvoiceSync();

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
              // 'order_historical_lark',
              // 'order_recent_lark',
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
            ? ['customer', 'invoice']
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

  // ============================================================================
  // CONTROL & STATUS METHODS
  // ============================================================================

  async getSchedulerStatus() {
    const allSyncs = await this.prismaService.syncControl.findMany({
      orderBy: { lastRunAt: 'desc' },
      take: 20,
    });

    const runningSyncs = allSyncs.filter((s) => s.isRunning);
    const nextMainCycle = this.getNextCycleTime(10); // Next 10-minute cycle
    const nextWeeklyCycle = this.getNextSundayTime();

    return {
      scheduler: {
        mainScheduler: {
          enabled: this.isMainSchedulerEnabled,
          interval: '10 minutes',
          nextRun: nextMainCycle,
          currentlyRunning: runningSyncs.filter((s) => s.name === 'main_cycle'),
        },
        weeklyScheduler: {
          enabled: this.isWeeklySchedulerEnabled,
          schedule: 'Sunday 6:00 AM',
          nextRun: nextWeeklyCycle,
          currentlyRunning: runningSyncs.filter(
            (s) => s.name === 'weekly_cycle',
          ),
        },
        timezone: 'Asia/Ho_Chi_Minh',
      },
      runningTasks: runningSyncs.length,
      runningSyncs: runningSyncs.map((s) => ({
        name: s.name,
        startedAt: s.startedAt,
        duration: s.startedAt
          ? `${((Date.now() - new Date(s.startedAt).getTime()) / 1000).toFixed(0)}s`
          : null,
      })),
      recentSyncs: allSyncs.slice(0, 10),
      timestamp: new Date(),
    };
  }

  private getNextCycleTime(intervalMinutes: number): Date {
    const now = new Date();
    const nextCycle = new Date(now);
    nextCycle.setMinutes(
      Math.ceil(now.getMinutes() / intervalMinutes) * intervalMinutes,
      0,
      0,
    );
    if (nextCycle <= now) {
      nextCycle.setMinutes(nextCycle.getMinutes() + intervalMinutes);
    }
    return nextCycle;
  }

  private getNextSundayTime(): Date {
    const now = new Date();
    const nextSunday = new Date(now);
    const daysUntilSunday = (7 - now.getDay()) % 7 || 7;
    nextSunday.setDate(now.getDate() + daysUntilSunday);
    nextSunday.setHours(6, 0, 0, 0);

    // If it's already past 6 AM this Sunday, move to next Sunday
    if (now.getDay() === 0 && now.getHours() >= 6) {
      nextSunday.setDate(nextSunday.getDate() + 7);
    }

    return nextSunday;
  }

  // ============================================================================
  // SCHEDULER CONTROLS
  // ============================================================================

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Main scheduler (10-minute cycle) ENABLED');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Main scheduler (10-minute cycle) DISABLED');
  }

  enableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = true;
    this.logger.log('✅ Weekly scheduler (Sunday 6 AM) ENABLED');
  }

  disableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = false;
    this.logger.log('🔇 Weekly scheduler (Sunday 6 AM) DISABLED');
  }

  async resetAllSyncs() {
    const resetCount = await this.prismaService.syncControl.updateMany({
      where: {},
      data: {
        isRunning: false,
        isEnabled: true,
        status: 'idle',
        error: null,
        completedAt: new Date(),
      },
    });

    this.logger.log(`🔄 Reset ${resetCount.count} sync(s) to idle state`);
    return resetCount.count;
  }

  async forceStopAllSyncs() {
    const stopCount = await this.prismaService.syncControl.updateMany({
      where: { isRunning: true },
      data: {
        isRunning: false,
        status: 'failed',
        error: 'Force stopped by admin',
        completedAt: new Date(),
      },
    });

    this.logger.log(`🛑 Force stopped ${stopCount.count} running sync(s)`);
    return stopCount.count;
  }
}
