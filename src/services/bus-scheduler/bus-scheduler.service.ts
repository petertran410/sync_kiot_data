import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { LarkProductSyncService } from '../lark/product/lark-product-sync.service';
import { KiotVietPriceBookService } from '../kiot-viet/pricebook/pricebook.service';
import { KiotVietSupplierService } from '../kiot-viet/supplier/supplier.service';
import { LarkSupplierSyncService } from '../lark/supplier/lark-supplier-sync.service';
import { KiotVietOrderSupplierService } from '../kiot-viet/order-supplier/order-supplier.service';
import { LarkOrderSupplierSyncService } from '../lark/order-supplier/lark-order-supplier-sync.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);

  private isMainSchedulerEnabled = true;
  private isDailyProductSchedulerEnabled = true;
  private isDailyProductCompletedToday = false;
  private lastProductSyncDate: string | null = null;

  private isDailyCycleRunning = false;
  private mainSchedulerSuspendedForDaily = false;
  private dailyCycleStartTime: Date | null = null;

  private isMainCycleGracefulShutdown = false;
  private mainCycleAbortController: AbortController | null = null;
  private dailyCyclePriorityLevel = 0;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly productService: KiotVietProductService,
    private readonly larkProductSyncService: LarkProductSyncService,
    private readonly priceBookService: KiotVietPriceBookService,
    private readonly supplierService: KiotVietSupplierService,
    private readonly larkSupplierSyncService: LarkSupplierSyncService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierSyncService: LarkOrderSupplierSyncService,
  ) {}

  async onModuleInit() {
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  @Cron('*/7 * * * *', {
    name: 'main_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    // ✅ ENHANCED: Kiểm tra multiple priority levels
    if (this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0) {
      if (!this.mainSchedulerSuspendedForDaily) {
        this.logger.log(
          '🛑 SUSPENDING 7-minute cycle - Daily cycle has priority',
        );
        this.mainSchedulerSuspendedForDaily = true;

        // ✅ NEW: Force abort nếu đang chạy
        if (this.mainCycleAbortController) {
          this.mainCycleAbortController.abort();
          this.logger.log('🚫 FORCE ABORTING ongoing 7-minute cycle');
        }
      }

      this.logger.debug(
        `⏸️ 7-minute cycle suspended (dailyCyclePriorityLevel: ${this.dailyCyclePriorityLevel}, isDailyCycleRunning: ${this.isDailyCycleRunning})`,
      );
      return;
    }

    // ✅ NEW: Resume mechanism
    if (
      this.mainSchedulerSuspendedForDaily &&
      !this.isDailyCycleRunning &&
      this.dailyCyclePriorityLevel === 0
    ) {
      this.logger.log('▶️ RESUMING 7-minute cycle - Daily cycle completed');
      this.mainSchedulerSuspendedForDaily = false;
    }

    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    // ✅ NEW: Tạo AbortController cho cycle này
    this.mainCycleAbortController = new AbortController();
    const signal = this.mainCycleAbortController.signal;

    try {
      this.logger.log('🚀 Starting 7-minute parallel sync cycle...');
      const startTime = Date.now();

      // ✅ NEW: Kiểm tra abort signal trước khi bắt đầu
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

      const CYCLE_TIMEOUT_MS = 15 * 60 * 1000;

      try {
        // ✅ NEW: Enhanced cycle execution với abort signal
        const cyclePromise = this.executeMainCycleWithAbortSignal(signal);
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Cycle timeout after 15 minutes')),
            CYCLE_TIMEOUT_MS,
          ),
        );

        await Promise.race([cyclePromise, timeoutPromise]);
        await this.updateCycleTracking('main_cycle', 'completed');

        const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
        this.logger.log(
          `🎉 7-minute sync cycle completed in ${totalDuration}s`,
        );
      } catch (timeoutError) {
        if (signal.aborted) {
          this.logger.log(
            '🚫 7-minute cycle was aborted by daily cycle priority',
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
          this.logger.error(`⏰ 7-minute cycle timed out after 15 minutes`);
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
        this.logger.log('🚫 7-minute cycle aborted during execution');
        await this.updateCycleTracking(
          'main_cycle',
          'aborted',
          'Aborted by daily cycle priority',
        );
      } else {
        this.logger.error(`❌ 7-minute sync cycle failed: ${error.message}`);
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

    // Customer sync
    syncPromises.push(
      this.executeAbortableSync('customer', signal, async () => {
        await this.runCustomerSync();
        await this.runCustomerLarkSync();
      }),
    );

    // Invoice sync
    syncPromises.push(
      this.executeAbortableSync('invoice', signal, async () => {
        await this.runInvoiceSync();
        await this.runInvoiceLarkSync();
      }),
    );

    // Order sync
    syncPromises.push(
      this.executeAbortableSync('order', signal, async () => {
        await this.runOrderSync();
        await this.runOrderLarkSync();
      }),
    );

    await Promise.all(syncPromises);
  }

  // ✅ NEW: Helper để execute sync với abort signal
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

      // Kiểm tra abort sau khi hoàn thành
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

  @Cron('0 23 * * *', {
    name: 'daily_product_sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyProductSync() {
    if (!this.isDailyProductSchedulerEnabled) {
      this.logger.debug('🔇 Daily 23h sync scheduler is disabled');
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
      this.logger.log(
        '🌙 23:00 Daily Product Sequence + OrderSupplier Sync triggered',
      );

      this.dailyCyclePriorityLevel = 1;
      this.logger.log('🔄 PREPARING daily cycle priority mode (Level 1)');

      await this.forceStopMainCycleImmediately();

      // ✅ STEP 3: Activate full daily cycle
      this.dailyCyclePriorityLevel = 2; // active
      this.isDailyCycleRunning = true;
      this.dailyCycleStartTime = new Date();

      this.logger.log(
        '🛑 ACTIVATING daily cycle priority mode (Level 2) - 7-minute cycle STOPPED',
      );

      // ✅ STEP 4: Reduced wait time - chỉ còn 30 giây thay vì 3 phút
      await this.quickWaitForMainCycleCompletion();

      const startTime = Date.now();
      this.isDailyProductCompletedToday = false;

      await this.updateCycleTracking('daily_product_cycle', 'running');

      const DAILY_TIMEOUT_MS = 60 * 60 * 1000; // 60 phút

      try {
        const dailyPromise = this.executeDailyProductAndOrderSupplierSequence();
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Daily sequence timeout after 60 minutes')),
            DAILY_TIMEOUT_MS,
          ),
        );

        await Promise.race([dailyPromise, timeoutPromise]);
        await this.updateCycleTracking('daily_product_cycle', 'completed');

        this.isDailyProductCompletedToday = true;
        this.lastProductSyncDate = today;

        const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
        this.logger.log(
          `🎉 Daily product + orderSupplier sequence completed in ${duration} minutes`,
        );
        this.logger.log(`🔒 Product sequence locked until tomorrow 23:00`);
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
        `❌ Daily product + orderSupplier sequence failed: ${error.message}`,
      );
      await this.updateCycleTracking(
        'daily_product_cycle',
        'failed',
        error.message,
      );
    } finally {
      this.logger.log('▶️ RESUMING 7-minute cycle - Daily cycle completed');
      this.isDailyCycleRunning = false;
      this.dailyCycleStartTime = null;
      this.mainSchedulerSuspendedForDaily = false;
      this.dailyCyclePriorityLevel = 0; // back to normal
      this.isMainCycleGracefulShutdown = false;
    }
  }

  private async forceStopMainCycleImmediately(): Promise<void> {
    this.logger.log('🚫 FORCE STOPPING any ongoing 7-minute cycle...');

    // Abort current cycle if running
    if (this.mainCycleAbortController) {
      this.mainCycleAbortController.abort();
      this.logger.log('⚡ Sent abort signal to ongoing 7-minute cycle');
    }

    // Set graceful shutdown flag
    this.isMainCycleGracefulShutdown = true;

    // Quick wait for abort to take effect
    await new Promise((resolve) => setTimeout(resolve, 2000)); // 2 giây thay vì 3 phút

    this.logger.log('✅ Main cycle force stop completed');
  }

  // ✅ NEW: Quick wait thay thế cho waitForMainCycleCompletion
  private async quickWaitForMainCycleCompletion(): Promise<void> {
    const MAX_WAIT_SECONDS = 30; // ✅ GIẢM từ 3 phút xuống 30 giây
    const CHECK_INTERVAL_MS = 2000; // Kiểm tra mỗi 2 giây
    let waitedSeconds = 0;

    this.logger.log('⏳ Quick check for ongoing 7-minute cycle (max 30s)...');

    while (waitedSeconds < MAX_WAIT_SECONDS) {
      const runningSyncs = await this.checkRunningSyncs();
      const coreMainCycleSyncs = runningSyncs.filter(
        (sync) =>
          ['main_cycle', 'main_sync_cycle'].includes(sync.name) ||
          ['customer_recent', 'invoice_recent', 'order_recent'].includes(
            sync.name,
          ),
      );

      if (coreMainCycleSyncs.length === 0) {
        this.logger.log(
          '✅ No ongoing 7-minute cycle detected - Proceeding with daily cycle',
        );
        return;
      }

      this.logger.debug(
        `⏳ Core syncs still running: ${coreMainCycleSyncs.map((s) => s.name).join(', ')} (${waitedSeconds}/${MAX_WAIT_SECONDS}s)`,
      );

      await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL_MS));
      waitedSeconds += CHECK_INTERVAL_MS / 1000;
    }

    this.logger.warn(
      `⚠️ Proceeding after ${MAX_WAIT_SECONDS} seconds - Some syncs may still be running`,
    );
    this.logger.log('🚀 Daily cycle proceeding with ABSOLUTE PRIORITY');
  }

  private async waitForMainCycleCompletion(): Promise<void> {
    const MAX_WAIT_MINUTES = 3; // GIẢM từ 10 → 3 phút
    const CHECK_INTERVAL_MS = 10000; // GIẢM từ 15s → 10s
    let waitedMinutes = 0;

    this.logger.log(
      '⏳ Waiting for ongoing 7-minute cycle to complete gracefully...',
    );

    while (waitedMinutes < MAX_WAIT_MINUTES) {
      const runningSyncs = await this.checkRunningSyncs();
      const coreMainCycleSyncs = runningSyncs.filter(
        (sync) =>
          ['main_cycle', 'main_sync_cycle'].includes(sync.name) ||
          ['customer_recent', 'invoice_recent', 'order_recent'].includes(
            sync.name,
          ),
      );

      const larkBaseSyncs = runningSyncs.filter((sync) =>
        ['customer_lark_sync', 'invoice_lark_sync', 'order_lark_sync'].includes(
          sync.name,
        ),
      );

      if (coreMainCycleSyncs.length === 0) {
        if (larkBaseSyncs.length > 0) {
          this.logger.log(
            `✅ Core 7-minute cycle completed - LarkBase syncs (${larkBaseSyncs.map((s) => s.name).join(', ')}) will continue in background`,
          );
        } else {
          this.logger.log(
            '✅ 7-minute cycle completed gracefully - Proceeding with daily cycle',
          );
        }
        return;
      }

      this.logger.debug(
        `⏳ Core syncs still running: ${coreMainCycleSyncs.map((s) => s.name).join(', ')} (${waitedMinutes.toFixed(1)}/${MAX_WAIT_MINUTES} min)`,
      );

      if (larkBaseSyncs.length > 0) {
        this.logger.debug(
          `📊 LarkBase syncs running in background: ${larkBaseSyncs.map((s) => s.name).join(', ')}`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, CHECK_INTERVAL_MS));
      waitedMinutes += CHECK_INTERVAL_MS / 60000;
    }

    const stillRunningSyncs = await this.checkRunningSyncs();
    this.logger.warn(
      `⚠️ Proceeding after ${MAX_WAIT_MINUTES} minutes - Still running: ${stillRunningSyncs.map((s) => s.name).join(', ')}`,
    );
    this.logger.log(
      '🚀 Daily cycle proceeding - Background syncs will continue independently',
    );
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
        'product_historical',
        'order_supplier_historical',
        'pricebook_historical',
        'daily_product_cycle',
      ].includes(sync.name),
    );

    // ✅ NEW: Enhanced status với priority levels
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
              : '7 minutes interval',
          entities: ['customer', 'invoice', 'order'],
          note: 'Enhanced priority management with force abort capability',
        },
        dailyProductSequenceScheduler: {
          enabled: this.isDailyProductSchedulerEnabled,
          currentStatus: this.isDailyCycleRunning ? 'RUNNING' : 'IDLE',
          priorityLevel: this.dailyCyclePriorityLevel,
          nextRun: 'Daily at 23:00 (Vietnam time)',
          entities: ['pricebook', 'product', 'order_supplier'],
          sequence: 'Sequential: PriceBook → Product | Parallel: OrderSupplier',
          isolation:
            'ABSOLUTE PRIORITY - Force stops 7-minute cycle immediately',
          status: productStatus,
          timeout: '60 minutes total (enhanced abort mechanism)',
          execution: 'Product sequence (Sequential) + OrderSupplier (Parallel)',
          waitTime: 'Reduced from 3 minutes to 30 seconds maximum',
          dailyCycleStartTime: this.dailyCycleStartTime?.toISOString() || null,
        },
        enhancedPriorityManagement: {
          currentPriorityStatus: getPriorityStatus(),
          dailyCyclePriorityLevel: this.dailyCyclePriorityLevel,
          mainCycleAbortController:
            this.mainCycleAbortController !== null ? 'ACTIVE' : 'INACTIVE',
          isMainCycleGracefulShutdown: this.isMainCycleGracefulShutdown,
          forceStopCapability: 'ENABLED',
          waitTimeOptimization: 'ENABLED (30s max vs 3min before)',
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

  private async executeParallelSyncs(): Promise<void> {
    this.logger.log(
      '🔀 Main entities parallel execution: Customer ⫸ Invoice ⫸ Order (Product ISOLATED)',
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
    ];

    const results = await Promise.allSettled(syncPromises);
    await this.executeStaggeredLarkSync(results, false);
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

  private async executeStaggeredLarkSync(
    syncResults: any[],
    includeProduct: boolean = false,
  ): Promise<void> {
    this.logger.log('🚀 Starting staggered LarkBase sync...');

    const entityTypes = ['customer', 'invoice', 'order'];
    if (includeProduct) {
      entityTypes.push('product');
    }

    const successfulSyncs = syncResults
      .map((result, index) => ({
        result,
        syncType: entityTypes[index],
      }))
      .filter(({ result }) => result.status === 'fulfilled' || !result.reason);

    let delay = 0;

    for (const { syncType } of successfulSyncs) {
      if (delay > 0) {
        this.logger.log(
          `⏳ Waiting ${delay}s before ${syncType} LarkBase sync...`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay * 1000));
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
      }

      delay += 30000;
    }
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

  private async runSupplierSync(): Promise<void> {
    try {
      this.logger.log('🏪 [Supplier] Starting supplier sync...');
      const startTime = Date.now();

      await this.supplierService.checkAndRunAppropriateSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(`✅ [Supplier] Supplier sync completed in ${duration}s`);

      return Promise.resolve();
    } catch (error) {
      this.logger.error(`❌ [Supplier] Supplier sync failed: ${error.message}`);
      throw new Error(`Supplier sync failed: ${error.message}`);
    }
  }

  private async runOrderSupplierSync(): Promise<void> {
    try {
      this.logger.log('🏪 [Supplier] Starting order_supplier sync...');
      const startTime = Date.now();

      await this.orderSupplierService.checkAndRunAppropriateSync();

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(
        `✅ [Supplier] OrderSupplier sync completed in ${duration}s`,
      );

      return Promise.resolve();
    } catch (error) {
      this.logger.error(
        `❌ [Supplier] OrderSupplier sync failed: ${error.message}`,
      );
      throw new Error(`OrderSupplier sync failed: ${error.message}`);
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

      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    throw new Error(`${syncName} timeout after ${timeoutSeconds} seconds`);
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

  private async runProductSequenceSync(): Promise<void> {
    try {
      this.logger.log(
        '📦 [ProductSequence] Starting sequential dependency sync...',
      );
      const startTime = Date.now();

      this.logger.log(
        '💰 [1/2] Syncing PriceBooks (dependency for Product)...',
      );

      // Check if PriceBook service exists
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
              `✅ Auto-triggered order_supplier LarkBase sync: ${orderSuppliersToSync.length} order_suppliers`,
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

        this.logger.log('📋 No order_suppliers need LarkBase sync');
      }
    } catch (error) {
      this.logger.error(
        `❌ Auto order_supplier Lark sync failed: ${error.message}`,
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

      const startupPromises = [
        this.runCustomerSync().catch((error) => {
          this.logger.warn(`Customer startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
        this.runInvoiceSync().catch((error) => {
          this.logger.warn(`Invoice startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
        this.runOrderSync().catch((error) => {
          this.logger.warn(`Order startup check failed: ${error.message}`);
          return Promise.resolve();
        }),
      ];

      await Promise.allSettled(startupPromises);

      this.logger.log('✅ Startup check completed successfully');
      this.logger.log(
        '📅 Product & OrderSupplier sync will run daily at 23:00',
      );
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

  getDailyProductStatus(): {
    enabled: boolean;
    completedToday: boolean;
    lastSyncDate: string | null;
    nextAvailable: string;
  } {
    const today = new Date().toISOString().split('T')[0];
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);

    return {
      enabled: this.isDailyProductSchedulerEnabled,
      completedToday:
        this.isDailyProductCompletedToday && this.lastProductSyncDate === today,
      lastSyncDate: this.lastProductSyncDate,
      nextAvailable: this.isDailyProductCompletedToday
        ? `${tomorrow.toISOString().split('T')[0]} 23:00`
        : 'Available now',
    };
  }

  async checkRunningSyncs(): Promise<any[]> {
    return this.checkRunningSyncsWithNullSafety();
  }

  private async updateCycleTracking(
    cycleName: string,
    status: string,
    error?: string,
  ) {
    try {
      const getEntities = (name: string): string[] => {
        switch (name) {
          case 'main_cycle':
            return ['customer', 'invoice', 'order'];
          case 'daily_product_cycle':
            return ['product', 'order_supplier'];
          default:
            return [];
        }
      };

      await this.prismaService.syncControl.upsert({
        where: { name: cycleName },
        create: {
          name: cycleName,
          entities: getEntities(cycleName),
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

  private async executeDailyProductAndOrderSupplierSequence(): Promise<void> {
    this.logger.log(
      '🌙 Starting Daily Product Sequence + OrderSupplier execution...',
    );
    this.logger.log(
      '📋 Flow: Sequential (PriceBook → Product) + Parallel (OrderSupplier)',
    );

    try {
      // ===============================================
      // PHASE 1: Sequential Product Dependencies
      // ===============================================
      this.logger.log(
        '🔄 PHASE 1: Sequential Product Sequence (PriceBook → Product)',
      );

      await this.runProductSequenceSync();

      this.logger.log(
        '✅ PHASE 1 Complete: Product sequence with dependencies finished',
      );

      // ===============================================
      // PHASE 2: Parallel OrderSupplier Sync
      // ===============================================
      this.logger.log('🔄 PHASE 2: Parallel OrderSupplier Sync');

      const orderSupplierPromise = this.runOrderSupplierSync().catch(
        (error) => {
          this.logger.error(`❌ [OrderSupplier] Sync failed: ${error.message}`);
          return {
            status: 'rejected',
            reason: error.message,
            sync: 'OrderSupplier',
          };
        },
      );

      // Wait for OrderSupplier sync completion
      const orderSupplierResult = await orderSupplierPromise;

      if (orderSupplierResult && orderSupplierResult.status === 'rejected') {
        this.logger.warn(
          `⚠️ OrderSupplier sync failed but continuing: ${orderSupplierResult.reason}`,
        );
      } else {
        this.logger.log('✅ PHASE 2 Complete: OrderSupplier sync finished');
      }

      // ===============================================
      // PHASE 3: Staggered LarkBase Sync (Optional)
      // ===============================================
      this.logger.log(
        '🔄 PHASE 3: Auto-trigger LarkBase syncs for completed entities',
      );

      // Auto-trigger Product LarkBase sync if needed
      await this.autoTriggerProductLarkSync();

      // Auto-trigger OrderSupplier LarkBase sync if needed
      await this.autoTriggerOrderSupplierLarkSync();

      this.logger.log('✅ PHASE 3 Complete: LarkBase syncs triggered');

      this.logger.log(
        '🎉 Daily Product Sequence + OrderSupplier execution completed successfully',
      );
    } catch (error) {
      this.logger.error(`❌ Daily sequence execution failed: ${error.message}`);
      throw error;
    }
  }
}
