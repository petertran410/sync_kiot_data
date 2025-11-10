import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { KiotVietReturnService } from '../kiot-viet/returns/return.service';
import { KiotVietOrderSupplierService } from '../kiot-viet/order-supplier/order-supplier.service';
import { LarkOrderSupplierSyncService } from '../lark/order-supplier/lark-order-supplier-sync.service';
import { KiotVietPurchaseOrderService } from '../kiot-viet/purchase-order/purchase-order.service';
import { LarkPurchaseOrderSyncService } from '../lark/purchase-order/lark-purchase-order-sync.service';
import { LarkInvoiceDetailSyncService } from '../lark/invoice-detail/lark-invoice-detail-sync.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly larkInvoiceDetailSyncService: LarkInvoiceDetailSyncService,

    private readonly orderService: KiotVietOrderService,
    private readonly larkOrderSyncService: LarkOrderSyncService,

    private readonly returnService: KiotVietReturnService,

    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierService: LarkOrderSupplierSyncService,

    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
  ) {}

  async onModuleInit() {
    this.logger.log('BusScheduler initialized - Daily sync at 22:00');
  }

  // @Cron('0 22 * * *', {
  //   name: 'daily_full_sync',
  //   timeZone: 'Asia/Ho_Chi_Minh',
  // })
  // async handleDailyFullSync() {
  //   this.logger.log('Starting daily full sync at 22:00 (parallel mode)...');

  //   try {
  //     await this.updateCycleTracking('daily_full_sync', 'running');

  //     const results = await Promise.allSettled([
  //       this.syncDailyOrders(),
  //       this.syncDailyInvoices(),
  //     ]);

  //     const statuses = results.map((result, index) => {
  //       const entityName = ['Customer', 'Order', 'Invoice'][index];
  //       if (result.status === 'fulfilled') {
  //         return `${entityName}: Success`;
  //       } else {
  //         this.logger.error(`❌ ${entityName} failed: ${result.reason}`);
  //         return `❌ ${entityName}: Failed`;
  //       }
  //     });

  //     this.logger.log('Sync results:');
  //     statuses.forEach((status) => this.logger.log(status));

  //     const allSuccess = results.every((r) => r.status === 'fulfilled');

  //     if (allSuccess) {
  //       await this.updateCycleTracking('daily_full_sync', 'completed');
  //       this.logger.log('Daily full sync completed successfully');
  //     } else {
  //       await this.updateCycleTracking(
  //         'daily_full_sync',
  //         'partial',
  //         'Some entities failed - check logs',
  //       );
  //       this.logger.warn('⚠️ Daily full sync completed with errors');
  //     }
  //   } catch (error) {
  //     this.logger.error(`❌ Daily full sync failed: ${error.message}`);
  //     await this.updateCycleTracking(
  //       'daily_full_sync',
  //       'failed',
  //       error.message,
  //     );
  //   }
  // }

  // @Cron('0 7 * * 0', { timeZone: 'Asia/Ho_Chi_Minh' })
  // async syncAllReturns() {
  //   try {
  //     this.logger.log('Starting return sync...');

  //     await this.returnService.enableHistoricalSync();

  //     await this.returnService.syncHistoricalReturns();

  //     return {
  //       success: true,
  //       message: 'Returns sync completed successfully',
  //       timestamp: new Date().toISOString,
  //     };
  //   } catch (error) {
  //     this.logger.error(`❌ Cashflow sync failed: ${error.message}`);
  //     return {
  //       success: false,
  //       error: error.message,
  //       timestamp: new Date().toISOString(),
  //     };
  //   }
  // }

  // @Cron('0 8 * * 0', { timeZone: 'Asia/Ho_Chi_Minh' })
  // async syncOrderSuppliers() {
  //   try {
  //     this.logger.log('Starting order-supplier sync...');

  //     await this.orderSupplierService.enableHistoricalSync();
  //     await this.orderSupplierService.syncHistoricalOrderSuppliers();

  //     const orderSuppliersToSync =
  //       await this.prismaService.orderSupplier.findMany({
  //         where: {
  //           OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
  //         },
  //       });

  //     await this.larkOrderSupplierService.syncOrderSuppliersToLarkBase(
  //       orderSuppliersToSync,
  //     );

  //     await this.larkOrderSupplierService.syncOrderSupplierDetailsToLarkBase();

  //     return {
  //       success: true,
  //       message: 'Order supplier and detail sync completed',
  //       timestamp: new Date().toISOString(),
  //     };
  //   } catch (error) {
  //     this.logger.error(`Order supplier sync failed: ${error.message}`);
  //     return {
  //       success: false,
  //       error: error.message,
  //       timestamp: new Date().toISOString(),
  //     };
  //   }
  // }

  // @Cron('0 9 * * 0', { timeZone: 'Asia/Ho_Chi_Minh' })
  // async syncPurchaseOrders() {
  //   try {
  //     this.logger.log('Starting purchase-order sync...');

  //     await this.purchaseOrderService.enableHistoricalSync();
  //     await this.purchaseOrderService.syncHistoricalPurchaseOrder();

  //     const purchaseOrdersToSync =
  //       await this.prismaService.purchaseOrder.findMany({
  //         where: {
  //           OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
  //         },
  //       });

  //     await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
  //       purchaseOrdersToSync,
  //     );

  //     await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase();

  //     return {
  //       success: true,
  //       message: 'Purchase Order and Purchase Order Detail sync completed',
  //       timestamp: new Date().toISOString(),
  //     };
  //   } catch (error) {
  //     this.logger.error(`Purchase Order sync failed: ${error.message}`);
  //     return {
  //       success: false,
  //       error: error.message,
  //       timestamp: new Date().toISOString(),
  //     };
  //   }
  // }

  private async syncDailyOrders() {
    this.logger.log('Syncing orders...');

    await this.orderService.enableHistoricalSync();
    await this.orderService.syncHistoricalOrders();

    const ordersToSync = await this.prismaService.order.findMany({
      where: {
        OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
      },
    });

    await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);
    this.logger.log(`Synced ${ordersToSync.length} orders to LarkBase`);
  }

  private async syncDailyInvoices() {
    this.logger.log('Syncing invoices...');

    await this.invoiceService.enableHistoricalSync();
    await this.invoiceService.syncHistoricalInvoices();

    const invoicesToSync = await this.prismaService.invoice.findMany({
      where: {
        OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
      },
    });

    await this.larkInvoiceSyncService.syncInvoicesToLarkBase(invoicesToSync);

    await this.larkInvoiceDetailSyncService.syncInvoiceDetailsToLarkBase();
    this.logger.log(`Synced ${invoicesToSync.length} invoices to LarkBase`);
  }

  private async updateCycleTracking(
    name: string,
    status: string,
    error?: string,
  ) {
    await this.prismaService.syncControl.upsert({
      where: { name },
      create: {
        name,
        entities: ['order', 'invoice'],
        syncMode: 'historical',
        isEnabled: false,
        isRunning: status === 'running',
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
  }
}
