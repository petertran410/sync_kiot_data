import { LarkPurchaseOrderSyncService } from './../services/lark/purchase-order/lark-purchase-order-sync.service';
import { Controller, Get, Post, Query, Logger } from '@nestjs/common';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
import { LarkCustomerSyncService } from 'src/services/lark/customer/lark-customer-sync.service';
import { KiotVietInvoiceService } from '../services/kiot-viet/invoice/invoice.service';
import { LarkInvoiceHistoricalSyncService } from 'src/services/lark/invoice-historical/lark-invoice-historical-sync.service';
import { KiotVietOrderService } from 'src/services/kiot-viet/order/order.service';
import { LarkOrderSyncService } from './../services/lark/order/lark-order-sync.service';
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
import { LarkCashflowSyncService } from 'src/services/lark/cashflow/lark-cashflow-sync.service';
import { KiotVietTransferService } from 'src/services/kiot-viet/transfer/transfer.service';
import { LarkDemandSyncService } from 'src/services/lark/demand/lark-demand-sync.service';
import { LarkInvoiceDetailSyncService } from 'src/services/lark/invoice-detail/lark-invoice-detail-sync.service';
import { KiotVietVoucherCampaign } from 'src/services/kiot-viet/voucher-campaign/voucher-campaign.service';
import { LarkVoucherCampaignSyncService } from 'src/services/lark/voucher-campaign/lark-voucher-campaign-sync.service';
import { LarkTransferSyncService } from 'src/services/lark/transfer/lark-transfer-sync.service';
import { KiotVietUserService } from 'src/services/kiot-viet/user/user.service';

@Controller('sync')
export class SyncController {
  private readonly logger = new Logger(SyncController.name);

  constructor(
    private readonly customerService: KiotVietCustomerService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,

    private readonly invoiceService: KiotVietInvoiceService,
    private readonly larkInvoiceSyncService: LarkInvoiceHistoricalSyncService,
    private readonly larkInvoiceDetailSyncService: LarkInvoiceDetailSyncService,
    private readonly orderService: KiotVietOrderService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly productService: KiotVietProductService,
    private readonly categoryService: KiotVietCategoryService,
    private readonly returnService: KiotVietReturnService,
    private readonly prismaService: PrismaService,
    private readonly orderSupplierService: KiotVietOrderSupplierService,
    private readonly larkOrderSupplierService: LarkOrderSupplierSyncService,
    private readonly purchaseOrderService: KiotVietPurchaseOrderService,
    private readonly larkPurchaseOrderSyncService: LarkPurchaseOrderSyncService,
    private readonly cashflowService: KiotVietCashflowService,
    private readonly transferService: KiotVietTransferService,
    private readonly larkTransferSyncService: LarkTransferSyncService,
    private readonly larkDemandSyncService: LarkDemandSyncService,
    private readonly voucherCampaignService: KiotVietVoucherCampaign,
    private readonly userService: KiotVietUserService,
  ) {}

  @Post('customer/historical')
  async triggerHistoricalCustomer() {
    try {
      this.logger.log('Manual historical customer sync triggered');

      await this.customerService.enableHistoricalSync();

      await this.customerService.syncHistoricalCustomers();

      const customersToSync = await this.prismaService.customer.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
        take: 1000,
      });

      await this.larkCustomerSyncService.syncCustomersToLarkBase(
        customersToSync,
      );

      return {
        success: true,
        message: 'Historical customer sync enabled and started',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`Manual historical sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('invoice/historical')
  async triggerHistoricalInvoice() {
    try {
      this.logger.log('Manual historical invoice sync triggered');

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

      return {
        success: true,
        message: 'Historical invoice sync enabled and started',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(
        `Manual historical invoice sync failed: ${error.message}`,
      );
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('order/historical')
  async triggerHistoricalOrder() {
    try {
      this.logger.log('Manual historical order sync triggered');

      await this.orderService.enableHistoricalSync();

      await this.orderService.syncHistoricalOrders();

      const ordersToSync = await this.prismaService.order.findMany({
        where: {
          OR: [{ larkSyncStatus: 'PENDING' }, { larkSyncStatus: 'FAILED' }],
        },
        take: 1000,
      });

      await this.larkOrderSyncService.syncOrdersToLarkBase(ordersToSync);

      return {
        success: true,
        message: 'Historical order sync enabled and started',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(
        `Manual historical order sync failed: ${error.message}`,
      );
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('transfers')
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

  @Post('cashflows-historical')
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

  @Post('products')
  async syncProducts() {
    try {
      this.logger.log('Starting product sync...');

      // await this.priceBookService.enableHistoricalSync();

      // await this.priceBookService.syncHistoricalPriceBooks();

      await this.productService.enableHistoricalSync();

      await this.productService.syncHistoricalProducts();

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

  @Post('return-historical')
  async syncReturnsHistorical() {
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
      this.logger.error(`❌ Return Historical sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Get('demand/from-lark')
  async syncDemandFromLark() {
    try {
      this.logger.log('Starting demand sync from LarkBase...');
      const result = await this.larkDemandSyncService.syncDemandsFromLarkBase();
      return {
        success: true,
        message: 'Demand sync from LarkBase completed',
        data: result,
      };
    } catch (error) {
      this.logger.error(`❌ Demand sync failed: ${error.message}`);
      throw error;
    }
  }

  @Post('voucher-campaign')
  async syncVoucherCampaigns() {
    try {
      this.logger.log('🎫 Starting voucher campaign sync...');

      await this.voucherCampaignService.syncAllVoucherCampaigns();

      return {
        success: true,
        message: 'Voucher campaign sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`❌ Voucher campaign sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  @Post('user')
  async syncUser() {
    try {
      this.logger.log('Starting user sync...');

      await this.userService.syncHistoricalUsers();

      return {
        success: true,
        message: 'User sync completed successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.log(`User sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
        timestampt: new Date().toISOString(),
      };
    }
  }
}
