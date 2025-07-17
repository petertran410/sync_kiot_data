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
import { LarkProductSyncService } from '../lark/product/lark-product-sync.service';
import { KiotVietPriceBookService } from '../kiot-viet/pricebook/pricebook.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isMainSchedulerEnabled = true;
  private isWeeklySchedulerEnabled = true;
  private isDailyProductSchedulerEnabled = true;
  private isDailyProductCompletedToday = false;
  private lastProductSyncDate: string | null = null;

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly productService: KiotVietProductService,
    private readonly larkProductSyncService: LarkProductSyncService,
    private readonly priceBookService: KiotVietPriceBookService,
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

  @Cron('*/7 * * * *', {
    name: 'main_sync_cycle',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleMainSyncCycle() {
    if (!this.isMainSchedulerEnabled) {
      this.logger.debug('🔇 Main scheduler is disabled');
      return;
    }

    try {
      this.logger.log('🚀 Starting 7-minute parallel sync cycle...');
      const startTime = Date.now();
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
        const cyclePromise = this.executeParallelSyncs();
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error('Cycle timeout after 25 minutes')),
            CYCLE_TIMEOUT_MS,
          ),
        );

        await Promise.race([cyclePromise, timeoutPromise]);

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
          this.logger.error(`⏰ Sync cycle timed out after 25 minutes`);
          await this.updateCycleTracking(
            'main_cycle',
            'timeout',
            'Cycle exceeded 25 minute timeout',
          );
        } else {
          throw timeoutError;
        }
      }
    } catch (error) {
      this.logger.error(`❌ Main sync cycle failed: ${error.message}`);
      await this.updateCycleTracking('main_cycle', 'failed', error.message);
    }
  }

  @Cron('0 23 * * *', {
    name: 'daily_product_sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyProductSync() {
    if (!this.isDailyProductSchedulerEnabled) {
      this.logger.debug('🔇 Daily product scheduler is disabled');
      return;
    }

    const today = new Date().toISOString().split('T')[0];

    if (
      this.isDailyProductCompletedToday &&
      this.lastProductSyncDate === today
    ) {
      this.logger.log(
        '✅ Daily product sequence already completed today - skipping',
      );
      return;
    }

    try {
      this.logger.log('🌙 23:00 Daily Product Sequence Sync triggered');
      const startTime = Date.now();

      const runningSyncs = await this.checkRunningSyncs();
      const mainCycleRunning = runningSyncs.some(
        (sync) => sync.name === 'main_cycle' || sync.name === 'main_sync_cycle',
      );

      if (mainCycleRunning) {
        this.logger.log(
          '⏸️ Daily product sequence skipped - main cycle running',
        );
        this.logger.log(
          '🔄 Product sequence will NOT run with main cycle (isolation)',
        );
        return;
      }

      this.isDailyProductCompletedToday = false;
      await this.updateCycleTracking('daily_product_cycle', 'running');

      const PRODUCT_SEQUENCE_TIMEOUT_MS = 60 * 60 * 1000; // 60 minutes

      try {
        // THAY ĐỔI: Gọi sequence thay vì direct product sync
        const sequencePromise = this.runProductSequenceSync(); // ← PriceBook → Product sequence
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(
            () =>
              reject(new Error('Product sequence timeout after 60 minutes')),
            PRODUCT_SEQUENCE_TIMEOUT_MS,
          ),
        );

        await Promise.race([sequencePromise, timeoutPromise]);

        // Auto-trigger Product LarkBase sync
        await this.autoTriggerProductLarkSync();

        this.isDailyProductCompletedToday = true;
        this.lastProductSyncDate = today;

        await this.updateCycleTracking('daily_product_cycle', 'completed');

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        this.logger.log(`🎉 Daily product sequence completed in ${duration}s`);
        this.logger.log(`🔒 Product sequence locked until tomorrow 23:00`);
      } catch (timeoutError) {
        this.isDailyProductCompletedToday = false;

        if (
          timeoutError instanceof Error &&
          timeoutError.message.includes('timeout')
        ) {
          this.logger.error(`⏰ Product sequence timed out after 60 minutes`);
          await this.updateCycleTracking(
            'daily_product_cycle',
            'timeout',
            'Product sequence exceeded 60 minute timeout',
          );
        } else {
          throw timeoutError;
        }
      }
    } catch (error) {
      this.isDailyProductCompletedToday = false;
      this.logger.error(`❌ Daily product sequence failed: ${error.message}`);
      await this.updateCycleTracking(
        'daily_product_cycle',
        'failed',
        error.message,
      );
    }
  }

  private async executeParallelSyncs(): Promise<void> {
    // REMOVE all Product logic from main cycle
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

    // NO Product logic here - completely isolated to daily cron

    const results = await Promise.allSettled(syncPromises);
    await this.executeStaggeredLarkSync(results, false); // false = no product
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

      // Enable historical pricebook sync
      await this.priceBookService.enableHistoricalSync();

      // Run the sync
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

      // Enable historical product sync
      await this.productService.enableHistoricalSync();

      // Run the sync
      await this.productService.syncHistoricalProducts();

      this.logger.log('✅ Product sync initiated successfully');
    } catch (error) {
      this.logger.error(`❌ Product sync failed: ${error.message}`);
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

      // Check progress every 5 seconds
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }

    throw new Error(`${syncName} timeout after ${timeoutSeconds} seconds`);
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

  private async runProductSequenceSync(): Promise<void> {
    try {
      this.logger.log(
        '📦 [ProductSequence] Starting sequential dependency sync...',
      );
      const startTime = Date.now();

      // STEP 1: PriceBook sync TRƯỚC - MANDATORY
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
      await this.waitForSyncCompletion('pricebook_historical', 600); // 10 minutes timeout

      // STEP 2: Product sync SAU - depends on PriceBook
      this.logger.log(
        '📦 [2/2] Syncing Products (with PriceBook dependency satisfied)...',
      );
      await this.enableAndRunProductSync();
      await this.waitForSyncCompletion('product_historical', 2700); // 45 minutes timeout

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

  private async autoTriggerProductLarkSync(): Promise<void> {
    try {
      const recentSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_recent' },
      });

      const larkSync = await this.prismaService.syncControl.findFirst({
        where: { name: 'product_lark_sync' },
      });

      if (
        recentSync?.status === 'completed' &&
        !recentSync.isRunning &&
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

  // ============================================================================
  // SCHEDULER CONTROLS
  // ============================================================================

  enableMainScheduler() {
    this.isMainSchedulerEnabled = true;
    this.logger.log('✅ Main scheduler (7-minute cycle) enabled');
  }

  disableMainScheduler() {
    this.isMainSchedulerEnabled = false;
    this.logger.log('🔇 Main scheduler (7-minute cycle) disabled');
  }

  enableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = true;
    this.logger.log('✅ Weekly scheduler (Sunday 6 AM) enabled');
  }

  disableWeeklyScheduler() {
    this.isWeeklySchedulerEnabled = false;
    this.logger.log('🔇 Weekly scheduler (Sunday 6 AM) disabled');
  }

  enableDailyProductScheduler() {
    this.isDailyProductSchedulerEnabled = true;
    this.logger.log('✅ Daily product scheduler (23:00) enabled');
  }

  disableDailyProductScheduler() {
    this.isDailyProductSchedulerEnabled = false;
    this.logger.log('🔇 Daily product scheduler (23:00) disabled');
  }

  // ============================================================================
  // STARTUP CHECKS & CLEANUP
  // ============================================================================

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

      this.logger.log('📋 Running parallel startup sync checks...');

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

  // ============================================================================
  // STATUS & MONITORING
  // ============================================================================

  async getSchedulerStatus(): Promise<any> {
    const runningSyncs = await this.checkRunningSyncs();
    const productStatus = this.getDailyProductStatus();

    return {
      scheduler: {
        mainScheduler: {
          enabled: this.isMainSchedulerEnabled,
          nextRun: '7 minutes interval',
          entities: ['customer', 'invoice', 'order'],
          note: 'Product completely isolated from main cycle',
        },
        dailyProductSequenceScheduler: {
          enabled: this.isDailyProductSchedulerEnabled,
          nextRun: 'Daily at 23:00 (Vietnam time)',
          entities: ['pricebook', 'product'], // Sequential order
          sequence: 'PriceBook → Product (dependency managed)',
          isolation: 'Complete isolation from main cycle',
          status: productStatus,
          timeout: '60 minutes',
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

  // ============================================================================
  // TRACKING & UTILITY METHODS - ENHANCED
  // ============================================================================

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
            return ['product'];
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
          completedAt: status === 'completed' ? new Date() : undefined,
        },
        update: {
          isRunning: status === 'running',
          status,
          error,
          startedAt: status === 'running' ? new Date() : undefined,
          completedAt: status === 'completed' ? new Date() : undefined,
        },
      });
    } catch (trackingError) {
      this.logger.warn(`⚠️ Cycle tracking failed: ${trackingError.message}`);
    }
  }
}
