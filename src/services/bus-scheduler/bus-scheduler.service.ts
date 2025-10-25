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
import { KiotVietCashflowService } from '../kiot-viet/cashflow/cashflow.service';
import { LarkCashflowSyncService } from '../lark/cashflow/lark-cashflow-sync.service';
import { KiotVietTransferService } from '../kiot-viet/transfer/transfer.service';

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
    // {
    //   name: 'pricebook_product_sequence',
    //   syncFunction: async () => {
    //     await this.runProductSequenceSync();
    //   },
    //   larkSyncFunction: async () => {
    //     await this.autoTriggerProductLarkSync();
    //   },
    //   enabled: true,
    // },
    // {
    //   name: 'order_supplier',
    //   syncFunction: async () => {
    //     await this.enableAndRunOrderSupplierSync();
    //   },
    //   larkSyncFunction: async () => {
    //     await this.autoTriggerOrderSupplierLarkSync();
    //   },
    //   enabled: true,
    // },
    // {
    //   name: 'order_supplier_detail',
    //   syncFunction: async () => {
    //     await this.enableAndRunOrderSupplierDetailSync();
    //   },
    //   larkSyncFunction: async () => {
    //     await this.autoTriggerOrderSupplierDetailLarkSync();
    //   },
    //   dependencies: ['order_supplier'],
    //   enabled: true,
    // },
    // {
    //   name: 'purchase_order',
    //   syncFunction: async () => {
    //     await this.enableAndRunPurchaseOrderSync();
    //   },
    //   larkSyncFunction: async () => {
    //     await this.autoTriggerPurchaseOrderLarkSync();
    //   },
    //   enabled: true,
    // },
    // {
    //   name: 'purchase_order_detail',
    //   syncFunction: async () => {
    //     await this.enableAndRunPurchaseOrderDetailSync();
    //   },
    //   larkSyncFunction: async () => {
    //     await this.autoTriggerPurchaseOrderDetailLarkSync();
    //   },
    //   dependencies: ['purchase_order'],
    //   enabled: true,
    // },
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
    private readonly larkProductSevice: LarkProductSyncService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierService: LarkOrderSupplierSyncService,
    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
    private readonly cashflowService: KiotVietCashflowService,
    private readonly transferService: KiotVietTransferService,
  ) {}

  async onModuleInit() {
    setTimeout(async () => {
      await this.runStartupCheck();
    }, 5000);
  }

  // @Cron('*/1 * * * *', {
  //   name: 'main_sync_cycle',
  //   timeZone: 'Asia/Ho_Chi_Minh',
  // })
  // async handleMainSyncCycle() {
  //   if (this.isDailyCycleRunning || this.dailyCyclePriorityLevel > 0) {
  //     if (!this.mainSchedulerSuspendedForDaily) {
  //       this.logger.log('SUSPENDING 1-minute cycle - Daily cycle has priority');
  //       this.mainSchedulerSuspendedForDaily = true;

  //       if (this.mainCycleAbortController) {
  //         this.mainCycleAbortController.abort();
  //         this.logger.log('FORCE ABORTING ongoing 1-minute cycle');
  //       }
  //     }

  //     this.logger.debug(
  //       `⏸1-minute cycle suspended (dailyCyclePriorityLevel: ${this.dailyCyclePriorityLevel}, isDailyCycleRunning: ${this.isDailyCycleRunning})`,
  //     );
  //     return;
  //   }

  //   if (
  //     this.mainSchedulerSuspendedForDaily &&
  //     !this.isDailyCycleRunning &&
  //     this.dailyCyclePriorityLevel === 0
  //   ) {
  //     this.logger.log('RESUMING 2-minute cycle - Daily cycle completed');
  //     this.mainSchedulerSuspendedForDaily = false;
  //   }

  //   if (!this.isMainSchedulerEnabled) {
  //     this.logger.debug('Main scheduler is disabled');
  //     return;
  //   }

  //   this.mainCycleAbortController = new AbortController();
  //   const signal = this.mainCycleAbortController.signal;

  //   try {
  //     this.logger.log('Starting 1-minute parallel sync cycle...');
  //     const startTime = Date.now();

  //     if (signal.aborted) {
  //       this.logger.log('1-minute cycle aborted before starting');
  //       return;
  //     }

  //     const runningSyncs = await this.checkRunningSyncsWithTimeout();
  //     if (runningSyncs.length > 0) {
  //       const runningTooLong = runningSyncs.some((sync) => {
  //         if (!sync.startedAt) return false;
  //         const runningTime = Date.now() - sync.startedAt.getTime();
  //         return runningTime > 5 * 60 * 1000;
  //       });

  //       if (runningTooLong) {
  //         this.logger.warn('Detected long-running syncs - triggering cleanup');
  //         await this.cleanupStuckSyncs();

  //         this.logger.log('Waiting 10s for processes to terminate...');
  //         await new Promise((resolve) => setTimeout(resolve, 10000));

  //         this.logger.log(
  //           'Skipping this cycle after cleanup - will try next cycle',
  //         );
  //         return;
  //       } else {
  //         this.logger.log(
  //           `Parallel sync skipped - running: ${runningSyncs.map((s) => s.name).join(', ')}`,
  //         );
  //         return;
  //       }
  //     }

  //     await this.updateCycleTracking('main_cycle', 'running');

  //     const CYCLE_TIMEOUT_MS = 8 * 60 * 1000;

  //     try {
  //       const cyclePromise = this.executeMainCycleWithAbortSignal(signal);
  //       const timeoutPromise = new Promise<never>((_, reject) =>
  //         setTimeout(
  //           () => reject(new Error('Cycle timeout after 8 minutes')),
  //           CYCLE_TIMEOUT_MS,
  //         ),
  //       );

  //       await Promise.race([cyclePromise, timeoutPromise]);
  //       await this.updateCycleTracking('main_cycle', 'completed');

  //       const totalDuration = ((Date.now() - startTime) / 1000).toFixed(2);
  //       this.logger.log(`1-minute sync cycle completed in ${totalDuration}s`);
  //     } catch (timeoutError) {
  //       if (signal.aborted) {
  //         this.logger.log('2-minute cycle was aborted by daily cycle priority');
  //         await this.updateCycleTracking(
  //           'main_cycle',
  //           'aborted',
  //           'Aborted by daily cycle priority',
  //         );
  //       } else if (
  //         timeoutError instanceof Error &&
  //         timeoutError.message.includes('timeout')
  //       ) {
  //         this.logger.error(`1-minute cycle timed out after 8 minutes`);
  //         await this.updateCycleTracking(
  //           'main_cycle',
  //           'timeout',
  //           'Cycle exceeded 8 minute timeout',
  //         );
  //       } else {
  //         throw timeoutError;
  //       }
  //     }
  //   } catch (error) {
  //     if (signal.aborted) {
  //       this.logger.log('2-minute cycle aborted during execution');
  //       await this.updateCycleTracking(
  //         'main_cycle',
  //         'aborted',
  //         'Aborted by daily cycle priority',
  //       );
  //     } else {
  //       this.logger.error(`2-minute sync cycle failed: ${error.message}`);
  //       await this.updateCycleTracking('main_cycle', 'failed', error.message);
  //     }
  //   } finally {
  //     this.mainCycleAbortController = null;
  //   }
  // }

  private async checkRunningSyncsWithTimeout(): Promise<any[]> {
    const runningSyncs = await this.prismaService.syncControl.findMany({
      where: { isRunning: true },
    });

    const now = Date.now();
    const cleanupPromises = runningSyncs
      .filter(
        (sync) =>
          sync.startedAt && now - sync.startedAt.getTime() > 10 * 60 * 1000,
      )
      .map((sync) =>
        this.prismaService.syncControl.update({
          where: { id: sync.id },
          data: {
            isRunning: false,
            status: 'auto_cleanup',
            error: 'Auto-cleaned due to timeout',
            completedAt: new Date(),
          },
        }),
      );

    if (cleanupPromises.length > 0) {
      this.logger.warn(
        `🧹 Auto-cleaning ${cleanupPromises.length} stuck syncs`,
      );
      await Promise.all(cleanupPromises);

      return await this.prismaService.syncControl.findMany({
        where: { isRunning: true },
      });
    }

    return runningSyncs;
  }

  private async executeMainCycleWithAbortSignal(
    signal: AbortSignal,
  ): Promise<void> {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();

    const isHistoricalWindow =
      hour >= 22 || hour < 3 || (hour === 3 && minute <= 29);

    if (isHistoricalWindow) {
      this.logger.log('Night window (22:00-03:29) - Running HISTORICAL sync');
      await this.enableHistoricalSyncForMainEntities();
    } else {
      this.logger.log('Day window (03:30-21:59) - Running RECENT sync');

      await this.forceStopHistoricalSyncs();

      await this.disableHistoricalSyncForMainEntities();
    }

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

  private async forceStopHistoricalSyncs(): Promise<void> {
    const historicalSyncs = await this.prismaService.syncControl.findMany({
      where: {
        OR: [
          { name: 'order_historical' },
          { name: 'invoice_historical' },
          { name: 'customer_historical' },
        ],
        isRunning: true,
      },
    });

    if (historicalSyncs.length > 0) {
      this.logger.warn(
        `🛑 Force stopping ${historicalSyncs.length} historical syncs at day window transition`,
      );

      await this.prismaService.syncControl.updateMany({
        where: {
          OR: [
            { name: 'order_historical' },
            { name: 'invoice_historical' },
            { name: 'customer_historical' },
          ],
          isRunning: true,
        },
        data: {
          isRunning: false,
          status: 'stopped',
          error: 'Stopped by day window transition (03:31)',
          completedAt: new Date(),
        },
      });
    }
  }

  private async enableHistoricalSyncForMainEntities(): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: {
        name: {
          in: ['order_historical', 'invoice_historical', 'customer_historical'],
        },
      },
      data: {
        isEnabled: true,
        status: 'idle',
      },
    });

    await this.prismaService.syncControl.updateMany({
      where: {
        name: {
          in: ['order_recent', 'invoice_recent', 'customer_recent'],
        },
      },
      data: {
        isEnabled: false,
      },
    });
  }

  private async disableHistoricalSyncForMainEntities(): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: {
        name: {
          in: ['order_historical', 'invoice_historical', 'customer_historical'],
        },
      },
      data: {
        isEnabled: false,
      },
    });

    await this.prismaService.syncControl.updateMany({
      where: {
        name: {
          in: ['order_recent', 'invoice_recent', 'customer_recent'],
        },
      },
      data: {
        isEnabled: true,
        status: 'idle',
      },
    });
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

  @Cron('30 3 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncCashflowsAM() {
    try {
      this.logger.log('Starting cashflows sync...');

      // await this.cashflowService.enableHistoricalSync();

      await this.cashflowService.syncHistoricalCashflows();

      return {
        success: true,
        message: 'Cashflows sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Cashflows sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('15 12 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncCashflowsMID() {
    try {
      this.logger.log('Starting cashflows sync...');

      // await this.cashflowService.enableHistoricalSync();

      await this.cashflowService.syncRecentCashflows();

      return {
        success: true,
        message: 'Cashflows sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Cashflows sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('30 17 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncCashflowsPM() {
    try {
      this.logger.log('Starting cashflows sync...');

      // await this.cashflowService.enableHistoricalSync();

      await this.cashflowService.syncRecentCashflows();

      return {
        success: true,
        message: 'Cashflows sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Cashflows sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 5 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncTransfersAM() {
    try {
      this.logger.log('Starting transfers sync...');

      await this.transferService.enableHistoricalSync();

      await this.transferService.syncHistoricalTransfers();

      return {
        success: true,
        message: 'Transfers sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Transfers sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 17 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncTransfersPM() {
    try {
      this.logger.log('Starting transfers sync...');

      await this.transferService.enableHistoricalSync();

      await this.transferService.syncHistoricalTransfers();

      return {
        success: true,
        message: 'Transfers sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Transfers sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('30 5 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncProductsAM() {
    try {
      this.logger.log('Starting product sync...');

      await this.productService.enableHistoricalSync();

      await this.productService.syncHistoricalProducts();

      const productsToSync = await this.prismaService.product.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
        take: 1000,
      });

      await this.larkProductSevice.syncProductsToLarkBase(productsToSync);

      return {
        success: true,
        message: 'Product sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Product sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('30 18 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncProductsPM() {
    try {
      this.logger.log('Starting product sync...');

      await this.productService.enableHistoricalSync();

      await this.productService.syncHistoricalProducts();

      const productsToSync = await this.prismaService.product.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
        take: 1000,
      });

      await this.larkProductSevice.syncProductsToLarkBase(productsToSync);

      return {
        success: true,
        message: 'Product sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Product sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 6 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncOrderSuppliersAM() {
    try {
      this.logger.log('Starting order-supplier sync...');

      await this.orderSupplierService.enableHistoricalSync();

      await this.orderSupplierService.syncHistoricalOrderSuppliers();

      const orderSuppliersToSync =
        await this.prismaService.orderSupplier.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

      await this.larkOrderSupplierService.syncOrderSuppliersToLarkBase(
        orderSuppliersToSync,
      );

      await this.larkOrderSupplierService.syncOrderSupplierDetailsToLarkBase();

      return {
        success: true,
        message: 'Order Supplier sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Order Supplier sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 18 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncOrderSuppliersPM() {
    try {
      this.logger.log('Starting order-supplier sync...');

      await this.orderSupplierService.enableHistoricalSync();

      await this.orderSupplierService.syncHistoricalOrderSuppliers();

      const orderSuppliersToSync =
        await this.prismaService.orderSupplier.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

      await this.larkOrderSupplierService.syncOrderSuppliersToLarkBase(
        orderSuppliersToSync,
      );

      await this.larkOrderSupplierService.syncOrderSupplierDetailsToLarkBase();

      return {
        success: true,
        message: 'Order Supplier sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Order Supplier sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('30 6 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncPurchaseOrdersAM() {
    try {
      this.logger.log('Starting purchase-order sync...');

      await this.purchaseOrderService.enableHistoricalSync();

      await this.purchaseOrderService.syncHistoricalPurchaseOrder();

      const purchaseOrdersToSync =
        await this.prismaService.purchaseOrder.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

      await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
        purchaseOrdersToSync,
      );

      await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase();

      return {
        success: true,
        message: 'Purchase Order sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Purchase Order sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('30 18 * * *', {
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async syncPurchaseOrdersPM() {
    try {
      this.logger.log('Starting purchase-order sync...');

      await this.purchaseOrderService.enableHistoricalSync();

      await this.purchaseOrderService.syncHistoricalPurchaseOrder();

      const purchaseOrdersToSync =
        await this.prismaService.purchaseOrder.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
          take: 1000,
        });

      await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
        purchaseOrdersToSync,
      );

      await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase();

      return {
        success: true,
        message: 'Purchase Order sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Purchase Order sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
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

  public async cleanupStuckSyncs(): Promise<number> {
    this.logger.log('🧹 Starting stuck sync cleanup...');

    const now = new Date();
    const stuckThresholdMs = 15 * 60 * 1000;

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
      if (!sync.startedAt) continue;

      const stuckDuration = now.getTime() - sync.startedAt.getTime();
      const stuckMinutes = Math.round(stuckDuration / (60 * 1000));

      this.logger.warn(
        `🔧 Cleaning stuck sync: ${sync.name} (running ${stuckMinutes}min)`,
      );

      try {
        await this.prismaService.syncControl.update({
          where: { id: sync.id },
          data: {
            isRunning: false,
            status: 'force_stopped',
            error: `Force stopped after ${stuckMinutes}min (aggressive cleanup)`,
            completedAt: now,
            progress: {
              ...((sync.progress as any) || {}),
              forceStoppedAt: now.toISOString(),
              stuckDuration: `${stuckMinutes}min`,
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
    this.logger.log('✅ Daily product scheduler (22:30) enabled');
  }

  disableDailyProductScheduler() {
    this.isDailyProductSchedulerEnabled = false;
    this.logger.log('🔇 Daily sync scheduler (22:30) disabled');
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
          `Reset ${stuckSyncs.count} stuck sync(s) from previous session`,
        );
      }
      await this.cleanupOldSyncPatterns();
      const currentlyRunning = await this.prismaService.syncControl.findMany({
        where: { isRunning: true },
      });
      if (currentlyRunning.length > 0) {
        this.logger.log(
          `Found ${currentlyRunning.length} syncs already running: ${currentlyRunning.map((s) => s.name).join(', ')}`,
        );
        this.logger.log('Skipping startup sync checks to avoid conflicts');
        return;
      }
      this.logger.log(
        'Running parallel startup sync checks (7-minute entities only)...',
      );
      this.startupAbortController = new AbortController();
      // const signal = this.startupAbortController.signal;
      // const startupPromises = [
      //   this.executeAbortableStartupSync('customer', signal, () =>
      //     this.customerService.checkAndRunAppropriateSync(),
      //   ),
      //   this.executeAbortableStartupSync('invoice', signal, () =>
      //     this.invoiceService.checkAndRunAppropriateSync(),
      //   ),
      //   this.executeAbortableStartupSync('order', signal, () =>
      //     this.orderService.checkAndRunAppropriateSync(),
      //   ),
      // ];
      // await Promise.allSettled(startupPromises);
      this.logger.log('Startup check completed');
    } catch (error) {
      this.logger.error(`Startup check failed: ${error.message}`);
    } finally {
      this.startupAbortController = null;
    }
  }

  private async cleanupOldSyncPatterns(): Promise<number> {
    const now = new Date();

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
