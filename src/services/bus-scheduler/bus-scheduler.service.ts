// src/services/bus-scheduler/bus-scheduler.service.ts
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
import { ScheduleUtils } from '../../utils/schedule.utils';
import {
  SYNC_ENTITIES_CONFIG,
  getEntitiesBySchedule,
  SyncEntityConfig,
  ScheduleType,
} from '../../config/sync-schedule.config';

// Import all your services
import { KiotVietBranchService } from '../kiot-viet/branch/branch.service';
import { KiotVietCustomerGroupService } from '../kiot-viet/customer-group/customer-group.service';
import { KiotVietTradeMarkService } from '../kiot-viet/trademark/trademark.service';
import { KiotVietUserService } from '../kiot-viet/user/user.service';
import { KiotVietSaleChannelService } from '../kiot-viet/sale-channel/sale-channel.service';
import { KiotVietSurchargeService } from '../kiot-viet/surcharge/surcharge.service';
import { KiotVietBankAccountService } from '../kiot-viet/bank-account/bank-account.service';
import { KiotVietCategoryService } from '../kiot-viet/category/category.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';
import { KiotVietProductService } from '../kiot-viet/product/product.service';
import { KiotVietOrderService } from '../kiot-viet/order/order.service';
import { KiotVietInvoiceService } from '../kiot-viet/invoice/invoice.service';
import { EntityStatus, EntityStatusSummary } from '../../types/sync.types';

// Legacy interfaces for backward compatibility
interface SyncEntity {
  name: string;
  service: any;
  syncMethod: string;
  syncType: 'simple' | 'full';
  dependencies?: string[];
  retryCount?: number;
}

export interface SyncResult {
  entityName: string;
  success: boolean;
  duration: number;
  error?: string;
}

export interface SyncCycleResult {
  schedule: ScheduleType;
  totalEntities: number;
  successful: number;
  failed: number;
  results: SyncResult[];
  duration: number;
  startTime: Date;
  endTime: Date;
}

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isRunning = false;
  private currentSyncType: ScheduleType | 'manual' | null = null;
  private currentSyncStartTime: Date | null = null;
  private isRunning15Min = false;
  private isRunningWeekend = false;

  private readonly MAX_RETRIES = 2;
  private readonly MAX_CONCURRENT = 3;

  private readonly syncEntities: SyncEntity[] = SYNC_ENTITIES_CONFIG.map(
    (config) => ({
      name: config.name,
      service: config.service,
      syncMethod: config.syncMethod,
      syncType: config.syncType,
      dependencies: config.dependencies,
      retryCount: config.retryCount,
    }),
  );

  constructor(
    private readonly prismaService: PrismaService,
    // Inject all services
    private readonly branchService: KiotVietBranchService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
    private readonly tradeMarkService: KiotVietTradeMarkService,
    private readonly userService: KiotVietUserService,
    private readonly saleChannelService: KiotVietSaleChannelService,
    private readonly surchargeService: KiotVietSurchargeService,
    private readonly bankAccountService: KiotVietBankAccountService,
    private readonly categoryService: KiotVietCategoryService,
    private readonly customerService: KiotVietCustomerService,
    private readonly productService: KiotVietProductService,
    private readonly orderService: KiotVietOrderService,
    private readonly invoiceService: KiotVietInvoiceService,
  ) {}

  async onModuleInit() {
    this.logger.log(
      '🚌 Enhanced Bus Scheduler initialized with MUTUAL EXCLUSION!',
    );
    ScheduleUtils.logScheduleStatus(this.logger);

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
    // ===== MUTUAL EXCLUSION CHECK =====
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
   * Weekend sync - Saturday 6 AM and Sunday 6 AM
   */
  @Cron('0 0 6 * * 0') // Sunday at 6 AM
  async handleWeekendSync() {
    // ===== MUTUAL EXCLUSION CHECK =====
    if (this.isRunning) {
      this.logger.log(
        `⏸️  Weekend sync BLOCKED - ${this.currentSyncType} sync is running (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
      return;
    }

    if (!ScheduleUtils.isWeekendVietnamTime()) {
      this.logger.log('Not weekend time in Vietnam, skipping weekend sync');
      return;
    }

    const entities = getEntitiesBySchedule('weekends');
    await this.runSyncCycle('weekends', entities);
  }

  /**
   * Manual trigger for any schedule type
   */
  async triggerSync(scheduleType: ScheduleType): Promise<SyncCycleResult> {
    // ===== MUTUAL EXCLUSION CHECK =====
    if (this.isRunning) {
      throw new Error(
        `Cannot trigger ${scheduleType} sync - ${this.currentSyncType} sync is already running (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
    }

    const entities = getEntitiesBySchedule(scheduleType);
    return await this.runSyncCycle(scheduleType, entities);
  }

  /**
   * Main sync cycle execution (Enhanced version)
   */
  private async runSyncCycle(
    scheduleType: ScheduleType,
    entities: SyncEntityConfig[],
  ): Promise<SyncCycleResult> {
    const startTime = new Date();

    // ===== ACQUIRE GLOBAL LOCK =====
    if (this.isRunning) {
      throw new Error(
        `Sync cycle already running: ${this.currentSyncType} (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
    }

    this.isRunning = true;
    this.currentSyncType = scheduleType;
    this.currentSyncStartTime = startTime;

    try {
      this.logger.log(
        `🔒 ACQUIRED LOCK: Starting ${scheduleType} sync cycle with ${entities.length} entities`,
      );
      this.logger.log(
        `🚫 ALL OTHER SYNCS BLOCKED until ${scheduleType} sync completes`,
      );

      // Create sync control record
      await this.prismaService.syncControl.upsert({
        where: { name: `bus_scheduler_${scheduleType}` },
        create: {
          name: `bus_scheduler_${scheduleType}`,
          entities: entities.map((e) => e.name),
          syncMode: scheduleType,
          isRunning: true,
          status: 'in_progress',
          startedAt: startTime,
          progress: { stage: 'starting', totalEntities: entities.length },
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: startTime,
          progress: { stage: 'restarted', totalEntities: entities.length },
          error: null,
        },
      });

      // Legacy bus_scheduler_cycle for backward compatibility
      await this.prismaService.syncControl.upsert({
        where: { name: 'bus_scheduler_cycle' },
        create: {
          name: 'bus_scheduler_cycle',
          entities: entities.map((e) => e.name),
          syncMode: 'cycle',
          isRunning: true,
          status: 'in_progress',
          startedAt: startTime,
          progress: {
            currentEntity: `${scheduleType}_sync`,
            completed: [],
            failed: [],
            syncType: scheduleType,
          },
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: startTime,
          progress: {
            currentEntity: `${scheduleType}_sync`,
            completed: [],
            failed: [],
            syncType: scheduleType,
          },
          error: null,
        },
      });

      // Process entities with concurrency control
      const results = await this.processEntitiesWithDependencies(
        entities,
        scheduleType,
      );

      const endTime = new Date();
      const duration = endTime.getTime() - startTime.getTime();
      const successful = results.filter((r) => r.success).length;
      const failed = results.filter((r) => !r.success).length;

      // Update final status
      const finalStatus = failed > 0 ? 'completed_with_errors' : 'completed';
      await this.prismaService.syncControl.update({
        where: { name: `bus_scheduler_${scheduleType}` },
        data: {
          isRunning: false,
          status: finalStatus,
          completedAt: endTime,
          progress: {
            stage: 'completed',
            totalEntities: entities.length,
            successful,
            failed,
            duration,
          },
          error: failed > 0 ? `${failed} entities failed to sync` : null,
        },
      });

      // Update legacy bus_scheduler_cycle
      await this.prismaService.syncControl.update({
        where: { name: 'bus_scheduler_cycle' },
        data: {
          isRunning: false,
          status: finalStatus,
          completedAt: endTime,
          progress: {
            currentEntity: 'completed',
            completed: results
              .filter((r) => r.success)
              .map((r) => r.entityName),
            failed: results.filter((r) => !r.success).map((r) => r.entityName),
            syncType: scheduleType,
          },
          error: failed > 0 ? `${failed} entities failed to sync` : null,
        },
      });

      const cycleResult: SyncCycleResult = {
        schedule: scheduleType,
        totalEntities: entities.length,
        successful,
        failed,
        results,
        duration,
        startTime,
        endTime,
      };

      this.logger.log(
        `🏁 ${scheduleType} sync cycle completed: ${successful}/${entities.length} successful (${duration}ms)`,
      );

      return cycleResult;
    } catch (error) {
      this.logger.error(
        `❌ ${scheduleType} sync cycle failed: ${error.message}`,
      );

      await this.prismaService.syncControl.update({
        where: { name: `bus_scheduler_${scheduleType}` },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      await this.prismaService.syncControl.update({
        where: { name: 'bus_scheduler_cycle' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      throw error;
    } finally {
      // ===== RELEASE GLOBAL LOCK =====
      this.isRunning = false;
      this.currentSyncType = null;
      this.currentSyncStartTime = null;
      this.logger.log(
        `🔓 LOCK RELEASED: ${scheduleType} sync completed - other syncs can now proceed`,
      );
    }
  }

  /**
   * Process entities respecting dependencies and concurrency limits
   */
  private async processEntitiesWithDependencies(
    entities: SyncEntityConfig[],
    scheduleType: ScheduleType,
  ): Promise<SyncResult[]> {
    const results: SyncResult[] = [];
    const completed = new Set<string>();
    const pending = [...entities];

    while (pending.length > 0) {
      // Find entities ready to process (dependencies satisfied)
      const ready = pending.filter((entity) => {
        return (
          !entity.dependencies ||
          entity.dependencies.every((dep) => completed.has(dep))
        );
      });

      if (ready.length === 0) {
        // Circular dependency or missing dependency
        const remaining = pending.map((e) => e.name).join(', ');
        this.logger.error(
          `Circular dependency detected or missing dependencies for: ${remaining}`,
        );
        break;
      }

      // Process ready entities with concurrency control
      const batch = ready.slice(0, this.MAX_CONCURRENT);
      const batchPromises = batch.map((entity) =>
        this.syncSingleEntity(entity, scheduleType),
      );

      const batchResults = await Promise.allSettled(batchPromises);

      // Process results
      batchResults.forEach((result, index) => {
        const entity = batch[index];
        if (result.status === 'fulfilled') {
          results.push(result.value);
          if (result.value.success) {
            completed.add(entity.name);
          }
        } else {
          results.push({
            entityName: entity.name,
            success: false,
            duration: 0,
            error: result.reason?.message || 'Unknown error',
          });
        }

        // Remove from pending
        const index2 = pending.findIndex((e) => e.name === entity.name);
        if (index2 !== -1) {
          pending.splice(index2, 1);
        }
      });

      // Small delay between batches
      if (pending.length > 0) {
        await this.delay(1000);
      }
    }

    return results;
  }

  /**
   * Sync a single entity
   */
  private async syncSingleEntity(
    entity: SyncEntityConfig,
    scheduleType: ScheduleType,
  ): Promise<SyncResult> {
    const startTime = Date.now();

    try {
      this.logger.log(`🔄 Starting ${entity.name} sync (${scheduleType})`);

      const service = this[entity.service];
      if (!service) {
        throw new Error(`Service ${entity.service} not found`);
      }

      // Execute sync method
      if (entity.syncType === 'full') {
        const historicalSync = await this.prismaService.syncControl.findFirst({
          where: { name: `${entity.name}_historical`, status: 'completed' },
        });

        if (!historicalSync) {
          this.logger.log(`Running historical sync for ${entity.name}`);
          await service[entity.syncMethod]();
        } else {
          this.logger.log(`Running recent sync for ${entity.name}`);
          const recentMethod = entity.syncMethod.replace(
            'Historical',
            'Recent',
          );
          if (service[recentMethod]) {
            await service[recentMethod](7);
          } else {
            this.logger.warn(
              `Recent method ${recentMethod} not found for ${entity.name}`,
            );
            await service[entity.syncMethod]();
          }
        }
      } else {
        await service[entity.syncMethod]();
      }

      const duration = Date.now() - startTime;
      this.logger.log(`✅ ${entity.name} sync completed (${duration}ms)`);

      return {
        entityName: entity.name,
        success: true,
        duration,
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      this.logger.error(
        `❌ ${entity.name} sync failed: ${error.message} (${duration}ms)`,
      );

      return {
        entityName: entity.name,
        success: false,
        duration,
        error: error.message,
      };
    }
  }

  /**
   * Check and run startup sync
   */
  private async checkAndRunStartupSync(): Promise<void> {
    this.logger.log('Checking startup sync requirements...');

    // Check if any historical syncs are incomplete
    const allEntities = SYNC_ENTITIES_CONFIG.filter(
      (e) => e.schedule !== 'disabled',
    );
    const needsHistorical =
      await this.checkForMissingHistoricalSyncs(allEntities);

    if (needsHistorical.length > 0) {
      this.logger.log(
        `Found entities needing historical sync: ${needsHistorical.join(', ')}`,
      );

      // Trigger appropriate sync cycles
      const every15minEntities = needsHistorical.filter((name) => {
        const entity = SYNC_ENTITIES_CONFIG.find((e) => e.name === name);
        return entity?.schedule === 'every_15_minutes';
      });

      const weekendEntities = needsHistorical.filter((name) => {
        const entity = SYNC_ENTITIES_CONFIG.find((e) => e.name === name);
        return entity?.schedule === 'weekends';
      });

      if (every15minEntities.length > 0) {
        await this.runSyncCycle(
          'every_15_minutes',
          SYNC_ENTITIES_CONFIG.filter((e) =>
            every15minEntities.includes(e.name),
          ),
        );
      }

      if (weekendEntities.length > 0) {
        this.logger.log(
          `Weekend entities need sync but will wait for weekend: ${weekendEntities.join(', ')}`,
        );
      }
    } else {
      this.logger.log('All historical syncs completed, running regular cycle');
      await this.runSyncCycle(
        'every_15_minutes',
        getEntitiesBySchedule('every_15_minutes'),
      );
    }
  }

  private async checkForMissingHistoricalSyncs(
    entities: SyncEntityConfig[],
  ): Promise<string[]> {
    const needsHistorical: string[] = [];

    for (const entity of entities) {
      if (entity.syncType === 'full') {
        const historicalSync = await this.prismaService.syncControl.findFirst({
          where: { name: `${entity.name}_historical`, status: 'completed' },
        });

        if (!historicalSync) {
          needsHistorical.push(entity.name);
        }
      }
    }

    return needsHistorical;
  }

  // ===== LEGACY METHODS FOR BACKWARD COMPATIBILITY =====

  /**
   * Legacy method for backward compatibility
   */
  async getBusStatus(): Promise<{
    busScheduler: any;
    entities: EntityStatusSummary[];
    isRunning: boolean;
  }> {
    const busControl = await this.prismaService.syncControl.findFirst({
      where: { name: 'bus_scheduler_cycle' },
    });

    const entityStatuses: EntityStatusSummary[] = [];

    for (const entity of this.syncEntities) {
      const statuses: EntityStatus[] = [];

      if (entity.syncType === 'full') {
        const historical = await this.prismaService.syncControl.findFirst({
          where: { name: `${entity.name}_historical` },
        });
        const recent = await this.prismaService.syncControl.findFirst({
          where: { name: `${entity.name}_recent` },
        });

        if (historical) {
          statuses.push({
            ...historical,
            syncType: 'historical' as const,
          });
        }
        if (recent) {
          statuses.push({
            ...recent,
            syncType: 'recent' as const,
          });
        }
      } else {
        const simple = await this.prismaService.syncControl.findFirst({
          where: { name: `${entity.name}_sync` },
        });
        if (simple) {
          statuses.push({
            ...simple,
            syncType: 'simple' as const,
          });
        }
      }

      entityStatuses.push({
        entityName: entity.name,
        entitySyncType: entity.syncType,
        statuses,
      });
    }

    return {
      busScheduler: busControl,
      entities: entityStatuses,
      isRunning: this.isRunning15Min || this.isRunningWeekend,
    };
  }

  /**
   * Legacy method for backward compatibility
   */
  async manualSyncEntity(entityName: string): Promise<void> {
    // ===== MUTUAL EXCLUSION CHECK =====
    if (this.isRunning) {
      throw new Error(
        `Cannot manually sync ${entityName} - ${this.currentSyncType} sync is already running (started at ${this.currentSyncStartTime?.toISOString()})`,
      );
    }

    const entity = SYNC_ENTITIES_CONFIG.find((e) => e.name === entityName);
    if (!entity) {
      throw new Error(`Entity ${entityName} not found`);
    }

    if (entity.schedule === 'disabled') {
      throw new Error(`Entity ${entityName} is disabled`);
    }

    // Set lock for manual sync
    this.isRunning = true;
    this.currentSyncType = 'manual';
    this.currentSyncStartTime = new Date();

    try {
      this.logger.log(
        `🔒 Manual sync requested for ${entityName} - blocking other syncs`,
      );
      await this.syncSingleEntity(entity, 'manual' as ScheduleType);
    } finally {
      this.isRunning = false;
      this.currentSyncType = null;
      this.currentSyncStartTime = null;
      this.logger.log(
        `🔓 Manual sync completed for ${entityName} - other syncs can now proceed`,
      );
    }
  }

  /**
   * Legacy method
   */
  async syncEntity(entity: SyncEntity): Promise<void> {
    const config = SYNC_ENTITIES_CONFIG.find((e) => e.name === entity.name);
    if (config) {
      await this.syncSingleEntity(config, 'manual' as ScheduleType);
    } else {
      await this.executeSyncMethod(entity);
    }
  }

  private async executeSyncMethod(entity: SyncEntity): Promise<void> {
    const service = this[entity.service];

    if (!service) {
      throw new Error(
        `Service ${entity.service} not found for entity ${entity.name}`,
      );
    }

    if (entity.syncType === 'full') {
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: { name: `${entity.name}_historical`, status: 'completed' },
      });

      if (!historicalSync) {
        this.logger.log(`Running historical sync for ${entity.name}`);
        await service[entity.syncMethod]();
      } else {
        this.logger.log(`Running recent sync for ${entity.name}`);
        const recentMethod = entity.syncMethod.replace('Historical', 'Recent');
        if (service[recentMethod]) {
          await service[recentMethod](7);
        } else {
          this.logger.warn(
            `Recent sync method ${recentMethod} not found for ${entity.name}`,
          );
        }
      }
    } else {
      this.logger.log(`Running simple sync for ${entity.name}`);
      await service[entity.syncMethod]();
    }
  }

  /**
   * Legacy methods
   */
  async stopBusScheduler(): Promise<void> {
    this.logger.log('Stop requested for bus scheduler');
    this.isRunning = false;
    this.isRunning15Min = false;
    this.isRunningWeekend = false;
  }

  async forceResetBusScheduler(): Promise<void> {
    this.logger.log('Force reset requested for bus scheduler');

    await this.prismaService.syncControl.updateMany({
      where: { isRunning: true },
      data: {
        isRunning: false,
        status: 'failed',
        error: 'Force reset by admin',
      },
    });

    this.isRunning = false;
    this.isRunning15Min = false;
    this.isRunningWeekend = false;
    this.logger.log('Bus scheduler force reset completed');
  }

  async forceHistoricalSyncEntity(entityName: string): Promise<void> {
    const entity = this.syncEntities.find((e) => e.name === entityName);
    if (!entity) {
      throw new Error(`Entity ${entityName} not found`);
    }

    if (entity.syncType !== 'full') {
      throw new Error(`Entity ${entityName} does not support historical sync`);
    }

    this.logger.log(`Force historical sync requested for ${entityName}`);

    const wasRunning = this.isRunning;
    this.isRunning = true;

    try {
      await this.prismaService.syncControl.upsert({
        where: { name: `${entityName}_historical` },
        create: {
          name: `${entityName}_historical`,
          entities: [entityName],
          syncMode: 'historical',
          isEnabled: true,
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          progress: { stage: 'starting' },
        },
        update: {
          status: 'in_progress',
          isEnabled: true,
          isRunning: true,
          startedAt: new Date(),
          progress: { stage: 'restarted' },
          error: null,
          completedAt: null,
        },
      });

      const service = this[entity.service];
      if (!service) {
        throw new Error(
          `Service ${entity.service} not found for entity ${entityName}`,
        );
      }

      await service[entity.syncMethod]();
    } finally {
      this.isRunning = wasRunning;
    }
  }

  // ===== NEW ENHANCED METHODS =====

  /**
   * Get enhanced sync status
   */
  async getEnhancedSyncStatus() {
    const controls = await this.prismaService.syncControl.findMany({
      where: {
        name: {
          in: [
            'bus_scheduler_every_15_minutes',
            'bus_scheduler_weekends',
            'bus_scheduler_cycle',
            ...SYNC_ENTITIES_CONFIG.map((e) => `${e.name}_historical`),
            ...SYNC_ENTITIES_CONFIG.map((e) => `${e.name}_recent`),
          ],
        },
      },
      orderBy: { name: 'asc' },
    });

    return {
      // Lock status
      isLocked: this.isRunning,
      currentSyncType: this.currentSyncType,
      currentSyncStartTime: this.currentSyncStartTime,
      lockDuration: this.currentSyncStartTime
        ? Date.now() - this.currentSyncStartTime.getTime()
        : null,

      // Schedule info
      isWeekend: ScheduleUtils.isWeekendVietnamTime(),
      nextWeekend: ScheduleUtils.getNextWeekendInfo(),

      // Entities and controls
      controls,
      configuredEntities: SYNC_ENTITIES_CONFIG,

      // Next sync predictions
      nextScheduledSyncs: this.getNextScheduledSyncs(),
    };
  }

  private getNextScheduledSyncs() {
    const now = new Date();
    const currentMinutes = now.getMinutes();
    const minutesUntilNext15 = 15 - (currentMinutes % 15);

    const next15MinSync = new Date(
      now.getTime() + minutesUntilNext15 * 60 * 1000,
    );
    const nextWeekendInfo = ScheduleUtils.getNextWeekendInfo();

    return {
      next15MinuteSync: next15MinSync,
      nextWeekendSync: nextWeekendInfo.nextSunday,
      willNext15MinBeBlocked: this.isRunning,
      willNextWeekendBeBlocked: this.isRunning,
    };
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
