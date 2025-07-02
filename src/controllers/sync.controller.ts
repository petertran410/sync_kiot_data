// src/controllers/sync.controller.ts
import { Controller, Get, Post, Query, Logger } from '@nestjs/common';
import { BusSchedulerService } from '../services/bus-scheduler/bus-scheduler.service';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
import { KiotVietInvoiceService } from '../services/kiot-viet/invoice/invoice.service';
import { KiotVietOrderService } from 'src/services/kiot-viet/order/order.service';

@Controller('sync')
export class SyncController {
  private readonly logger = new Logger(SyncController.name);

  constructor(
    private readonly busScheduler: BusSchedulerService,
    private readonly customerService: KiotVietCustomerService,
    private readonly invoiceService: KiotVietInvoiceService,
    private readonly orderService: KiotVietOrderService,
  ) {}

  // ============================================================================
  // SCHEDULER STATUS & MONITORING
  // ============================================================================

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

  @Get('status/simple')
  async getSimpleStatus() {
    try {
      const status = await this.busScheduler.getSchedulerStatus();
      return {
        success: true,
        data: {
          mainScheduler: status.scheduler.mainScheduler.enabled
            ? 'ENABLED'
            : 'DISABLED',
          weeklyScheduler: status.scheduler.weeklyScheduler.enabled
            ? 'ENABLED'
            : 'DISABLED',
          runningTasks: status.runningTasks,
          nextMainRun: status.scheduler.mainScheduler.nextRun,
          nextWeeklyRun: status.scheduler.weeklyScheduler.nextRun,
          entities: status.scheduler.mainScheduler.entities,
        },
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
      };
    }
  }

  // ============================================================================
  // SCHEDULER CONTROLS
  // ============================================================================

  @Post('scheduler/enable')
  async enableScheduler() {
    this.busScheduler.enableMainScheduler();
    this.busScheduler.enableWeeklyScheduler();
    return {
      success: true,
      message: 'Both schedulers enabled',
    };
  }

  @Post('scheduler/disable')
  async disableScheduler() {
    this.busScheduler.disableMainScheduler();
    this.busScheduler.disableWeeklyScheduler();
    return {
      success: true,
      message: 'Both schedulers disabled',
    };
  }

  @Post('scheduler/main/enable')
  async enableMainScheduler() {
    this.busScheduler.enableMainScheduler();
    return {
      success: true,
      message: 'Main scheduler (10-minute cycle) enabled',
    };
  }

  @Post('scheduler/main/disable')
  async disableMainScheduler() {
    this.busScheduler.disableMainScheduler();
    return {
      success: true,
      message: 'Main scheduler (10-minute cycle) disabled',
    };
  }

  @Post('scheduler/weekly/enable')
  async enableWeeklyScheduler() {
    this.busScheduler.enableWeeklyScheduler();
    return {
      success: true,
      message: 'Weekly scheduler (Sunday 6 AM) enabled',
    };
  }

  @Post('scheduler/weekly/disable')
  async disableWeeklyScheduler() {
    this.busScheduler.disableWeeklyScheduler();
    return {
      success: true,
      message: 'Weekly scheduler (Sunday 6 AM) disabled',
    };
  }

  // ============================================================================
  // SYNC CONTROLS & CLEANUP
  // ============================================================================

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

  // ============================================================================
  // MANUAL TRIGGERS - CUSTOMER
  // ============================================================================

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

  // ============================================================================
  // MANUAL TRIGGERS - INVOICE ← THÊM TOÀN BỘ SECTION NÀY
  // ============================================================================

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

  @Post('order/test-4days')
  async testOrderSync4Days() {
    try {
      this.logger.log('🧪 Testing order sync with 4 days...');

      await this.orderService.syncRecentOrders(4);

      return {
        success: true,
        message: 'Order sync test (4 days) completed successfully',
        note: 'Check logs for detailed results',
      };
    } catch (error) {
      this.logger.error(`Order 4-day test failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Post('order/lark-sync')
  async triggerOrderLarkSync() {
    try {
      this.logger.log('🔧 Manual order Lark sync triggered');

      // Get LarkOrderSyncService through orderService
      const larkOrderSyncService = this.orderService['larkOrderSyncService'];
      if (!larkOrderSyncService) {
        throw new Error('LarkOrderSyncService not available');
      }

      await larkOrderSyncService.syncOrdersToLark();

      return {
        success: true,
        message: 'Order Lark sync completed successfully',
      };
    } catch (error) {
      this.logger.error(`Manual order Lark sync failed: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }

  @Get('order/stats')
  async getOrderSyncStats() {
    try {
      const larkOrderSyncService = this.orderService['larkOrderSyncService'];
      if (!larkOrderSyncService) {
        throw new Error('LarkOrderSyncService not available');
      }

      const stats = await larkOrderSyncService.getOrderSyncStats();

      return {
        success: true,
        data: stats,
      };
    } catch (error) {
      this.logger.error(`Failed to get order sync stats: ${error.message}`);
      return {
        success: false,
        error: error.message,
      };
    }
  }
}
