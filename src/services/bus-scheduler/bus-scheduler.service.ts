import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { LarkInvoiceHistoricalSyncService } from '../lark/invoice-historical/lark-invoice-historical-sync.service';
import { KiotVietReturnService } from '../kiot-viet/returns/return.service';
import { KiotVietOrderSupplierService } from '../kiot-viet/order-supplier/order-supplier.service';
import { LarkOrderSupplierSyncService } from '../lark/order-supplier/lark-order-supplier-sync.service';
import { KiotVietPurchaseOrderService } from '../kiot-viet/purchase-order/purchase-order.service';
import { LarkPurchaseOrderSyncService } from '../lark/purchase-order/lark-purchase-order-sync.service';
import { LarkInvoiceDetailSyncService } from '../lark/invoice-detail/lark-invoice-detail-sync.service';
import { KiotVietCashflowService } from '../kiot-viet/cashflow/cashflow.service';
import { LarkCashflowSyncService } from '../lark/cashflow/lark-cashflow-sync.service';
import { KiotVietTransferService } from '../kiot-viet/transfer/transfer.service';
import { LarkTransferSyncService } from '../lark/transfer/lark-transfer-sync.service';
import { KiotVietSupplierService } from '../kiot-viet/supplier/supplier.service';
import { LarkSupplierSyncService } from '../lark/supplier/lark-supplier-sync.service';
import { MisaVoucherService } from '../misa/misa-voucher.service';
import { MisaDictionaryService } from '../misa/misa-dictionary.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly larkInvoiceSyncService: LarkInvoiceHistoricalSyncService,
    private readonly larkInvoiceDetailSyncService: LarkInvoiceDetailSyncService,

    private readonly supplierService: KiotVietSupplierService,
    private readonly larkSupplierSyncService: LarkSupplierSyncService,

    private readonly returnService: KiotVietReturnService,

    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierService: LarkOrderSupplierSyncService,

    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,

    private readonly cashflowService: KiotVietCashflowService,
    private readonly larkCashflowSyncService: LarkCashflowSyncService,

    private readonly transferService: KiotVietTransferService,
    private readonly larkTransferSyncService: LarkTransferSyncService,

    private readonly misaVoucherService: MisaVoucherService,
    private readonly misaDictionaryService: MisaDictionaryService,

    private readonly productService: KiotVietProductService,
  ) {}

  async onModuleInit() {
    this.logger.log('BusScheduler initialized - Daily sync at 22:00');
  }

  @Cron('0 22 * * *', {
    name: 'daily_full_sync',
    timeZone: 'Asia/Ho_Chi_Minh',
  })
  async handleDailyFullSync() {
    this.logger.log('Starting daily full sync at 22:00 (parallel mode)...');

    try {
      await this.updateCycleTracking('daily_full_sync', 'running');

      const results = await Promise.allSettled([
        // this.syncDailyOrders(),
        this.syncDailyInvoices(),
      ]);

      const statuses = results.map((result, index) => {
        // const entityName = ['Customer', 'Order', 'Invoice'][index];
        const entityName = ['Invoice'][index];
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

  @Cron('0 2 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncTransfers() {
    try {
      this.logger.log('Starting transfer sync...');

      await this.transferService.enableHistoricalSync();
      await this.transferService.syncHistoricalTransfers();

      const transfersToSync = await this.prismaService.transfer.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
      });

      await this.larkTransferSyncService.syncTransferToLarkBase(
        transfersToSync,
      );

      await this.larkTransferSyncService.syncTransferDetailsToLarkBase();

      return {
        success: true,
        message: 'Transfers and Transfers Detail sync completed',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Transfers sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 */1 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncCashflowsHistorical() {
    try {
      this.logger.log('Starting cashflow sync...');

      await this.cashflowService.enableHistoricalSync();

      await this.cashflowService.syncHistoricalCashflows();

      return {
        success: true,
        message: 'Cashflow sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Cashflow sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 7 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncAllReturns() {
    try {
      this.logger.log('Starting return sync...');

      await this.returnService.enableHistoricalSync();

      await this.returnService.syncHistoricalReturns();

      return {
        success: true,
        message: 'Returns sync completed successfully',
        timestamp: new Date().toISOString,
      };
    } catch (error) {
      this.logger.error(`❌ Cashflow sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 0 * * SUN', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncSuppliers() {
    try {
      this.logger.log('Starting supplier sync...');

      await this.supplierService.enableHistoricalSync();
      await this.supplierService.syncHistoricalSuppliers();

      const suppliersToSync = await this.prismaService.supplier.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
      });

      await this.larkSupplierSyncService.syncSuppliersToLarkBase(
        suppliersToSync,
      );

      return {
        success: true,
        message: 'Supplier sync completed',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Supplier sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 8 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncOrderSuppliers() {
    try {
      this.logger.log('Starting order-supplier sync...');

      await this.orderSupplierService.enableHistoricalSync();
      await this.orderSupplierService.syncHistoricalOrderSuppliers();

      const orderSuppliersToSync =
        await this.prismaService.orderSupplier.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      await this.larkOrderSupplierService.syncOrderSuppliersToLarkBase(
        orderSuppliersToSync,
      );

      await this.larkOrderSupplierService.syncOrderSupplierDetailsToLarkBase();

      return {
        success: true,
        message: 'Order supplier and detail sync completed',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Order supplier sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 9 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncPurchaseOrders() {
    try {
      this.logger.log('Starting purchase-order sync...');

      await this.purchaseOrderService.enableHistoricalSync();
      await this.purchaseOrderService.syncHistoricalPurchaseOrder();

      const purchaseOrdersToSync =
        await this.prismaService.purchaseOrder.findMany({
          where: {
            OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
          },
        });

      await this.larkPurchaseOrderSyncService.syncPurchaseOrdersToLarkBase(
        purchaseOrdersToSync,
      );

      await this.larkPurchaseOrderSyncService.syncPurchaseOrderDetailsToLarkBase();

      return {
        success: true,
        message: 'Purchase Order and Purchase Order Detail sync completed',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Purchase Order sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Cron('0 * * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncProducts() {
    try {
      this.logger.log('Starting product sync...');

      await this.productService.enableHistoricalSync();
      await this.productService.syncHistoricalProducts();

      this.logger.log('Product sync completed');
    } catch (error) {
      this.logger.error(`Product sync failed: ${error.message}`);
    }
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

  // ========================================
  // MISA BATCH PROCESSING
  // ========================================

  /**
   * Tạo mốc thời gian VN (UTC+7) cho ngày hiện tại hoặc hôm qua
   */
  private getVNTime(hour: number, dayOffset = 0): Date {
    const now = new Date();
    const d = new Date(now);
    d.setDate(d.getDate() + dayOffset);
    // VN = UTC+7 → UTC hour = VN hour - 7
    d.setUTCHours(hour - 7, 0, 0, 0);
    return d;
  }

  /**
   * Đợt 1 - 15h: Đẩy toàn bộ hóa đơn buổi sáng (trước 12h) + tồn đọng từ chiều hôm qua
   */
  @Cron('0 15 * * 1-6', { timeZone: 'Asia/Ho_Chi_Minh' })
  async misaMorningBatch() {
    this.logger.log('🕐 [MISA] Đợt 1 - Đẩy hóa đơn sáng (15h)');

    try {
      const from = this.getVNTime(8);
      const to = this.getVNTime(12);

      const result = await this.misaVoucherService.batchCreateVouchers(
        from,
        to,
        { fromOp: 'gte', toOp: 'lt', preferAdjusted: true },
      );

      this.logger.log(`✅ [MISA] Đợt 1 completed: ${JSON.stringify(result)}`);
    } catch (error) {
      this.logger.error(`❌ [MISA] Đợt 1 failed: ${error.message}`);
    }
  }

  /**
   * Đợt 2 - 19h: Đẩy toàn bộ hóa đơn buổi chiều (sau 12h)
   */
  @Cron('0 19 * * 1-6', { timeZone: 'Asia/Ho_Chi_Minh' })
  async misaAfternoonBatch() {
    this.logger.log('🕖 [MISA] Đợt 2 - Đẩy hóa đơn chiều (19h)');

    try {
      const from = this.getVNTime(12);
      const to = this.getVNTime(19);

      const result = await this.misaVoucherService.batchCreateVouchers(
        from,
        to,
        { fromOp: 'gte', toOp: 'lt', preferAdjusted: true },
      );

      this.logger.log(`✅ [MISA] Đợt 2 completed: ${JSON.stringify(result)}`);
    } catch (error) {
      this.logger.error(`❌ [MISA] Đợt 2 failed: ${error.message}`);
    }
  }

  /**
   * 23h30: Đồng bộ danh mục Misa về database
   */
  @Cron('30 23 * * *', { timeZone: 'Asia/Ho_Chi_Minh' })
  async syncMisaDictionaries() {
    this.logger.log('📦 [MISA] Dictionary sync started (23h30)');

    try {
      const result = await this.misaDictionaryService.syncAllDictionaries();

      this.logger.log(
        `✅ [MISA] Dictionary sync completed: ${JSON.stringify(result)}`,
      );
    } catch (error) {
      this.logger.error(`❌ [MISA] Dictionary sync failed: ${error.message}`);
    }
  }
}
