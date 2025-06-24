// src/services/bus-scheduler/bus-scheduler.service.ts
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import {
  SYNC_ENTITIES_CONFIG,
  getEntitiesBySchedule,
  SyncEntityConfig,
  ScheduleType,
} from '../../config/sync-schedule.config';

// Import all services
import { KiotVietBranchService } from '../kiot-viet/branch/branch.service';
import { KiotVietCustomerGroupService } from '../kiot-viet/customer-group/customer-group.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { KiotVietUserService } from '../kiot-viet/user/user.service';
import { KiotVietSaleChannelService } from '../kiot-viet/sale-channel/sale-channel.service';
import { KiotVietSurchargeService } from '../kiot-viet/surcharge/surcharge.service';
import { KiotVietBankAccountService } from '../kiot-viet/bank-account/bank-account.service';
import { KiotVietCategoryService } from '../kiot-viet/category/category.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { KiotVietTradeMarkService } from '../kiot-viet/trademark/trademark.service';

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isRunning = false;
  private currentSyncType: string | null = null;
  private currentSyncStartTime: Date | null = null;

  constructor(
    private readonly prismaService: PrismaService,
    // Every 15 minutes services
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly customerService: KiotVietCustomerService,
    private readonly orderService: KiotVietOrderService,
    private readonly invoiceService: KiotVietInvoiceService,
    // Weekend services
    private readonly userService: KiotVietUserService,
    private readonly saleChannelService: KiotVietSaleChannelService,
    private readonly surchargeService: KiotVietSurchargeService,
    private readonly bankAccountService: KiotVietBankAccountService,
    private readonly categoryService: KiotVietCategoryService,
    private readonly productService: KiotVietProductService,
    private readonly tradeMarkService: KiotVietTradeMarkService,
    // Manual service
    private readonly branchService: KiotVietBranchService,
  ) {}

  async onModuleInit() {
    this.logger.log('🚌 Bus Scheduler Service initialized with all entities');

    setTimeout(async () => {
      try {
        await this.checkAndRunStartupSync();
      } catch (error) {
        this.logger.error(`Startup sync check failed: ${error.message}`);
      }
    }, 3000);
  }

  /**
   * Every 15 minutes - High frequency entities
   */
  @Cron('0 */15 * * * *') // Every 15 minutes
  async handleScheduledSync() {
    await this.handleEvery15MinutesSync();
  }

  async handleEvery15MinutesSync() {
    if (this.isRunning) {
      this.logger.log(
        `⏸️  15-minute sync BLOCKED - ${this.currentSyncType} sync is running (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
      return;
    }

    const entities = getEntitiesBySchedule('every_15_minutes');
    await this.runSyncCycle('every_15_minutes', entities);
  }

  /**
   * Weekend sync - Sunday 6 AM
   */
  @Cron('0 0 6 * * 0') // Sunday at 6 AM
  async handleWeekendSync() {
    if (this.isRunning) {
      this.logger.log(
        `⏸️  Weekend sync BLOCKED - ${this.currentSyncType} sync is running (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
      return;
    }

    const entities = getEntitiesBySchedule('weekends');
    await this.runSyncCycle('weekends', entities);
  }

  async getBusStatus(): Promise<any> {
    return this.getEnhancedSyncStatus();
  }

  async stopBusScheduler(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Force stopping bus scheduler...');
      this.isRunning = false;
      this.currentSyncType = null;
      this.currentSyncStartTime = null;
    }
  }

  async forceResetBusScheduler(): Promise<void> {
    // Reset all running sync controls
    await this.prismaService.syncControl.updateMany({
      where: { isRunning: true },
      data: {
        isRunning: false,
        status: 'failed',
        error: 'Force reset by user',
        completedAt: new Date(),
      },
    });

    this.isRunning = false;
    this.currentSyncType = null;
    this.currentSyncStartTime = null;

    this.logger.log('Bus scheduler force reset completed');
  }

  async forceHistoricalSyncEntity(entityName: string): Promise<void> {
    // Reset the entity's historical sync
    await this.prismaService.syncControl.updateMany({
      where: { name: `${entityName}_historical` },
      data: {
        isRunning: false,
        isEnabled: true,
        status: 'idle',
        error: null,
        completedAt: null,
      },
    });

    // Start the sync
    await this.manualSyncEntity(entityName);
  }

  /**
   * Enhanced sync cycle with LarkBase phase
   */
  async runSyncCycle(
    scheduleType: ScheduleType,
    entities: SyncEntityConfig[],
  ): Promise<void> {
    this.isRunning = true;
    this.currentSyncType = scheduleType;
    this.currentSyncStartTime = new Date();

    try {
      this.logger.log(
        `🚀 Starting ${scheduleType} sync cycle with ${entities.length} entities`,
      );

      // SINGLE PHASE: KiotViet → Database → LarkBase (immediate)
      for (const entityConfig of entities) {
        await this.syncSingleEntity(entityConfig);
        // No separate LarkBase phase needed - it's done immediately in each service
      }

      this.logger.log(`✅ ${scheduleType} sync cycle completed successfully`);
    } catch (error) {
      this.logger.error(
        `❌ ${scheduleType} sync cycle failed: ${error.message}`,
      );
      throw error;
    } finally {
      this.isRunning = false;
      this.currentSyncType = null;
      this.currentSyncStartTime = null;
    }
  }

  private async syncSingleEntity(
    entityConfig: SyncEntityConfig,
  ): Promise<void> {
    const startTime = Date.now();

    try {
      this.logger.log(`🔄 Syncing ${entityConfig.name}...`);

      const service = this.getServiceInstance(entityConfig.service);

      if (service && typeof service[entityConfig.syncMethod] === 'function') {
        await service[entityConfig.syncMethod]();

        const duration = Date.now() - startTime;
        this.logger.log(
          `✅ ${entityConfig.name} sync completed (${duration}ms)`,
        );
      } else {
        throw new Error(
          `Service ${entityConfig.service} does not have method ${entityConfig.syncMethod}`,
        );
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logger.error(
        `❌ ${entityConfig.name} sync failed: ${error.message} (${duration}ms)`,
      );
      throw error;
    }
  }

  private getServiceInstance(serviceName: string): any {
    const serviceMap = {
      // Every 15 minutes
      customerGroupService: this.customerGroupService,
      customerService: this.customerService,
      orderService: this.orderService,
      invoiceService: this.invoiceService,
      // Weekends
      userService: this.userService,
      saleChannelService: this.saleChannelService,
      surchargeService: this.surchargeService,
      bankAccountService: this.bankAccountService,
      categoryService: this.categoryService,
      productService: this.productService,
      tradeMarkService: this.tradeMarkService,
      // Manual
      branchService: this.branchService,
    };

    return serviceMap[serviceName];
  }

  /**
   * Manual sync trigger for any schedule type
   */
  async triggerSync(scheduleType: ScheduleType): Promise<any> {
    if (this.isRunning) {
      throw new Error(
        `Cannot trigger ${scheduleType} sync - another sync is already running`,
      );
    }

    const entities = getEntitiesBySchedule(scheduleType);

    if (entities.length === 0) {
      throw new Error(
        `No entities configured for schedule type: ${scheduleType}`,
      );
    }

    await this.runSyncCycle(scheduleType, entities);

    return {
      scheduleType,
      entitiesProcessed: entities.length,
      completedAt: new Date(),
    };
  }

  /**
   * Manual entity sync
   */
  async manualSyncEntity(entityName: string): Promise<void> {
    if (this.isRunning) {
      throw new Error(
        `Cannot sync ${entityName} - another sync is already running`,
      );
    }

    const entityConfig = SYNC_ENTITIES_CONFIG.find(
      (e) => e.name === entityName,
    );

    if (!entityConfig) {
      throw new Error(`Unknown entity: ${entityName}`);
    }

    this.isRunning = true;
    this.currentSyncType = `manual_${entityName}`;

    try {
      // Phase 1: KiotViet → Database
      await this.syncSingleEntity(entityConfig);
    } finally {
      this.isRunning = false;
      this.currentSyncType = null;
      this.currentSyncStartTime = null;
    }
  }

  /**
   * Get current sync status
   */
  async getSyncStatus(): Promise<any> {
    const syncControls = await this.prismaService.syncControl.findMany({
      orderBy: { startedAt: 'desc' },
    });

    return {
      isRunning: this.isRunning,
      currentSyncType: this.currentSyncType,
      currentSyncStartTime: this.currentSyncStartTime,
      entities: SYNC_ENTITIES_CONFIG,
      syncControls,
    };
  }

  private async checkAndRunStartupSync(): Promise<void> {
    this.logger.log('🔍 Checking for startup sync requirements...');

    // Check if any high-priority sync was interrupted
    const interruptedSyncs = await this.prismaService.syncControl.findMany({
      where: {
        isRunning: true,
        status: 'in_progress',
      },
    });

    if (interruptedSyncs.length > 0) {
      this.logger.log(
        `Found ${interruptedSyncs.length} interrupted syncs, resuming...`,
      );

      // Reset interrupted syncs and run recent sync
      await this.prismaService.syncControl.updateMany({
        where: {
          isRunning: true,
          status: 'in_progress',
        },
        data: {
          isRunning: false,
          status: 'failed',
          error: 'System restart detected',
          completedAt: new Date(),
        },
      });

      // Run recent sync for critical entities
      const criticalEntities = getEntitiesBySchedule('every_15_minutes');
      await this.runSyncCycle('every_15_minutes', criticalEntities);
    } else {
      this.logger.log('✅ No interrupted syncs found, system ready');
    }
  }

  /**
   * Enhanced sync status
   */
  async getEnhancedSyncStatus(): Promise<any> {
    const syncControls = await this.prismaService.syncControl.findMany({
      orderBy: { startedAt: 'desc' },
      take: 50,
    });

    const entitiesStatus = SYNC_ENTITIES_CONFIG.map((entity) => {
      const latestSync = syncControls.find((sync) =>
        sync.entities.includes(entity.name),
      );

      return {
        ...entity,
        lastSync: latestSync,
        isHealthy: latestSync?.status === 'completed',
      };
    });

    return {
      systemStatus: {
        isRunning: this.isRunning,
        currentSyncType: this.currentSyncType,
        currentSyncStartTime: this.currentSyncStartTime,
        uptime: process.uptime(),
      },
      configuredEntities: entitiesStatus,
      recentSyncs: syncControls.slice(0, 10),
      statistics: {
        totalEntities: SYNC_ENTITIES_CONFIG.length,
        every15MinEntities: getEntitiesBySchedule('every_15_minutes').length,
        weekendEntities: getEntitiesBySchedule('weekends').length,
        disabledEntities: getEntitiesBySchedule('disabled').length,
        larkBaseEntities: SYNC_ENTITIES_CONFIG.filter((e) => e.hasLarkBaseSync)
          .length,
      },
    };
  }
}
