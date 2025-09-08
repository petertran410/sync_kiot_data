import { LarkPurchaseOrderSyncService } from './../services/lark/purchase-order/lark-purchase-order-sync.service';
import { Controller, Get, Post, Query, Logger } from '@nestjs/common';
import { BusSchedulerService } from '../services/bus-scheduler/bus-scheduler.service';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
import { KiotVietInvoiceService } from '../services/kiot-viet/invoice/invoice.service';
import { KiotVietOrderService } from 'src/services/kiot-viet/order/order.service';
import { KiotVietProductService } from 'src/services/kiot-viet/product/product.service';
import { KiotVietCategoryService } from '../services/kiot-viet/category/category.service';
import { KiotVietCustomerGroupService } from 'src/services/kiot-viet/customer-group/customer-group.service';
import { KiotVietReturnService } from 'src/services/kiot-viet/returns/return.service';
import { KiotVietPriceBookService } from 'src/services/kiot-viet/pricebook/pricebook.service';
import { LarkProductSyncService } from 'src/services/lark/product/lark-product-sync.service';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietOrderSupplierService } from 'src/services/kiot-viet/order-supplier/order-supplier.service';
import { LarkOrderSupplierSyncService } from 'src/services/lark/order-supplier/lark-order-supplier-sync.service';
import { KiotVietPurchaseOrderService } from 'src/services/kiot-viet/purchase-order/purchase-order.service';
import { KiotVietTradeMarkService } from 'src/services/kiot-viet/trademark/trademark.service';
import { KiotVietCashflowService } from 'src/services/kiot-viet/cashflow/cashflow.service';

@Controller('sync')
export class SyncController {
  private readonly logger = new Logger(SyncController.name);

  constructor(
    private readonly busScheduler: BusSchedulerService,
    private readonly customerService: KiotVietCustomerService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
    private readonly productService: KiotVietProductService,
    private readonly categoryService: KiotVietCategoryService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly returnService: KiotVietReturnService,
    private readonly priceBookService: KiotVietPriceBookService,
    private readonly larkProductSevice: LarkProductSyncService,
    private readonly prismaService: PrismaService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierService: LarkOrderSupplierSyncService,
    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
    private readonly trademarkService: KiotVietTradeMarkService,
    private readonly cashflowService: KiotVietCashflowService,
  ) {}

  @Get('status')
  async getStatus() {
    try {
      return {
        success: true,
        data: await this.busScheduler.getSchedulerStatus(),
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('reset')
  async resetAllSyncs() {
    try {
      const count = await this.busScheduler.resetAllSyncs();
      return {
        success: true,
        message: `Reset ${count} sync(s) to idle state`,
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('stop')
  async forceStopAllSyncs() {
    try {
      const count = await this.busScheduler.forceStopAllSyncs();
      return {
        success: true,
        message: `Force stopped ${count} running sync(s)`,
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('cleanup-stuck')
  async cleanupStuckSyncs() {
    try {
      const cleanedCount = await this.busScheduler.cleanupStuckSyncs();
      return {
        success: true,
        message: `Cleaned up ${cleanedCount} stuck sync(s)`,
        cleanedCount,
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('customer/historical')
  async triggerHistoricalCustomer() {
    try {
      this.logger.log('🔧 Manual historical customer sync triggered');
      await this.customerService.enableHistoricalSync();
      return {
        success: true,
        message: 'Historical customer sync enabled and started',
      };
    } catch (error) {
      this.logger.error(`Manual historical sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('customer/recent')
  async triggerRecentCustomer(@Query('days') days?: string) {
    try {
      const syncDays = days ? parseInt(days, 10) : 4;
      this.logger.log(
        `🔧 Manual recent customer sync triggered (${syncDays} days)`,
      );

      await this.customerService.syncRecentCustomers(syncDays);

      return {
        success: true,
        message: `Recent customer sync completed (${syncDays} days)`,
      };
    } catch (error) {
      this.logger.error(`Manual recent customer sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('invoice/historical')
  async triggerHistoricalInvoice() {
    try {
      this.logger.log('🔧 Manual historical invoice sync triggered');
      await this.invoiceService.enableHistoricalSync();
      return {
        success: true,
        message: 'Historical invoice sync enabled and started',
      };
    } catch (error) {
      this.logger.error(
        `Manual historical invoice sync failed: ${error.message}`,
      );
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('invoice/recent')
  async triggerRecentInvoice(@Query('days') days?: string) {
    try {
      const syncDays = days ? parseInt(days, 10) : 7;
      this.logger.log(
        `🔧 Manual recent invoice sync triggered (${syncDays} days)`,
      );

      await this.invoiceService.syncRecentInvoices(syncDays);

      return {
        success: true,
        message: `Recent invoice sync completed (${syncDays} days)`,
      };
    } catch (error) {
      this.logger.error(`Manual recent invoice sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('invoice/test-4days')
  async testInvoiceSync4Days() {
    try {
      this.logger.log('🧪 Testing invoice sync with 4 days...');

      await this.invoiceService.syncRecentInvoices(4);

      return {
        success: true,
        message: 'Invoice sync test (4 days) completed successfully',
        note: 'Check logs for detailed results',
      };
    } catch (error) {
      this.logger.error(`Invoice 4-day test failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('order/historical')
  async triggerHistoricalOrder() {
    try {
      this.logger.log('🔧 Manual historical order sync triggered');
      await this.orderService.enableHistoricalSync();
      return {
        success: true,
        message: 'Historical order sync enabled and started',
      };
    } catch (error) {
      this.logger.error(
        `Manual historical order sync failed: ${error.message}`,
      );
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('order/recent')
  async triggerRecentOrder(@Query('days') days?: string) {
    try {
      const syncDays = days ? parseInt(days, 10) : 7;
      this.logger.log(
        `🔧 Manual recent order sync triggered (${syncDays} days)`,
      );

      await this.orderService.syncRecentOrders(syncDays);

      return {
        success: true,
        message: `Recent order sync completed (${syncDays} days)`,
      };
    } catch (error) {
      this.logger.error(`Manual recent order sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  // @Post('product/historical')
  // async triggerHistoricalProduct() {
  //   try {
  //     this.logger.log('🔧 Manual historical product sync triggered');

  //     await this.productService.enableHistoricalSync();

  //     return {
  //       success: true,
  //       message: 'Historical product sync enabled and started',
  //       estimatedDuration: '15-30 minutes',
  //       phases: [
  //         'Product sync',
  //         'Nested data processing',
  //         'LarkBase integration',
  //       ],
  //     };
  //   } catch (error) {
  //     this.logger.error(
  //       `Manual historical product sync failed: ${error.message}`,
  //     );
  //     return {
  //       success: false,
  //       error: error.message,
  //       stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
  //     };
  //   }
  // }

  @Post('trademarks')
  async syncTrademarks() {
    try {
      this.logger.log('🗂️ Starting trademark sync...');

      await this.trademarkService.enableHistoricalSync();

      await this.trademarkService.syncHistoricalTradeMarks();

      return {
        success: true,
        message: 'Trademark sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Trademark sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('categories')
  async syncCategories() {
    try {
      this.logger.log('🗂️ Starting category sync...');

      await this.categoryService.enableHistoricalSync();

      await this.categoryService.syncHistoricalCategories();

      return {
        success: true,
        message: 'Category sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Category sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('customer-groups')
  async syncCustomerGroups() {
    try {
      this.logger.log('👥 Starting customer group sync...');

      await this.customerGroupService.enableHistoricalSync();

      await this.customerGroupService.syncHistoricalCustomerGroups();

      return {
        success: true,
        message: 'Customer group sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Customer group sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('returns')
  async syncReturns() {
    try {
      this.logger.log('👥 Starting return sync...');

      await this.returnService.enableHistoricalSync();

      await this.returnService.syncHistoricalReturns();

      return {
        success: true,
        message: 'Return sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Return sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('pricebooks')
  async syncPriceBooks() {
    try {
      this.logger.log('Starting pricebook sync...');

      await this.priceBookService.enableHistoricalSync();

      await this.priceBookService.syncHistoricalPriceBooks();

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

  @Post('products')
  async syncProducts() {
    try {
      this.logger.log('Starting product sync...');

      // await this.priceBookService.enableHistoricalSync();

      // await this.priceBookService.syncHistoricalPriceBooks();

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

  @Post('order-supplier')
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

  @Post('purchase-order')
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

  @Post('cashflows')
  async syncCashflows() {
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
}
