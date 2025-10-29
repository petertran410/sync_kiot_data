import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { LarkCustomerSyncService } from '../lark/customer/lark-customer-sync.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
  ) {}

  async onModuleInit() {
    this.logger.log('BusScheduler initialized - Daily sync at 22:00');
  }

  @Cron('15 9 * * *', {
    name: 'daily_full_sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyFullSync() {
    this.logger.log('Starting daily full sync at 22:00 (parallel mode)...');

    try {
      await this.updateCycleTracking('daily_full_sync', 'running');

      const results = await Promise.allSettled([
        this.syncDailyCustomers(),
        this.syncDailyOrders(),
        this.syncDailyInvoices(),
      ]);

      const statuses = results.map((result, index) => {
        const entityName = ['Customer', 'Order', 'Invoice'][index];
        if (result.status === 'fulfilled') {
          return `${entityName}: Success`;
        } else {
          this.logger.error(`❌ ${entityName} failed: ${result.reason}`);
          return `❌ ${entityName}: Failed`;
        }
      });

      this.logger.log('Sync results:');
      statuses.forEach((status) => this.logger.log(status));

      const allSuccess = results.every((r) => r.status === 'fulfilled');

      if (allSuccess) {
        await this.updateCycleTracking('daily_full_sync', 'completed');
        this.logger.log('Daily full sync completed successfully');
      } else {
        await this.updateCycleTracking(
          'daily_full_sync',
          'partial',
          'Some entities failed - check logs',
        );
        this.logger.warn('⚠️ Daily full sync completed with errors');
      }
    } catch (error) {
      this.logger.error(`❌ Daily full sync failed: ${error.message}`);
      await this.updateCycleTracking(
        'daily_full_sync',
        'failed',
        error.message,
      );
    }
  }

  private async syncDailyCustomers() {
    this.logger.log('Syncing customers...');

    await this.customerService.enableHistoricalSync();
    await this.customerService.syncHistoricalCustomers();

    const customersToSync = await this.prismaService.customer.findMany({
      where: {
        OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
      },
    });

    await this.larkCustomerSyncService.syncCustomersToLarkBase(customersToSync);
    this.logger.log(`Synced ${customersToSync.length} customers to LarkBase`);
  }

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
        entities: ['customer', 'order', 'invoice'],
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
