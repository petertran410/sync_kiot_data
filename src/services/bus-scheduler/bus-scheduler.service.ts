// src/services/bus-scheduler/bus-scheduler.service.ts
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { PrismaService } from '../../prisma/prisma.service';
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
import {
  EntityStatus,
  EntityStatusSummary,
  BusStatusResponse,
} from '../../types/sync.types';

interface SyncEntity {
  name: string;
  service: any;
  syncMethod: string;
  syncType: 'simple' | 'full';
  dependencies?: string[];
  retryCount?: number;
}

@Injectable()
export class BusSchedulerService implements OnModuleInit {
  private readonly logger = new Logger(BusSchedulerService.name);
  private isRunning = false;
  private readonly MAX_RETRIES = 2;

  // Define sync entities in dependency order
  private readonly syncEntities: SyncEntity[] = [
    // Phase 1 - No Dependencies
    {
      name: 'branch',
      service: 'branchService',
      syncMethod: 'syncBranches',
      syncType: 'simple',
    },
    {
      name: 'customergroup',
      service: 'customerGroupService',
      syncMethod: 'syncCustomerGroups',
      syncType: 'simple',
    },
    {
      name: 'trademark',
      service: 'tradeMarkService',
      syncMethod: 'syncTradeMarks',
      syncType: 'simple',
    },
    {
      name: 'user',
      service: 'userService',
      syncMethod: 'syncHistoricalUsers',
      syncType: 'full',
    },
    {
      name: 'salechannel',
      service: 'saleChannelService',
      syncMethod: 'syncSaleChannels',
      syncType: 'simple',
    },
    {
      name: 'surcharge',
      service: 'surchargeService',
      syncMethod: 'syncSurcharges',
      syncType: 'simple',
    },
    {
      name: 'bankaccount',
      service: 'bankAccountService',
      syncMethod: 'syncBankAccounts',
      syncType: 'simple',
    },

    // Phase 2 - Basic Dependencies
    {
      name: 'category',
      service: 'categoryService',
      syncMethod: 'syncCategories',
      syncType: 'simple',
      dependencies: [],
    },
    {
      name: 'customer',
      service: 'customerService',
      syncMethod: 'syncHistoricalCustomers',
      syncType: 'full',
      dependencies: ['branch', 'customergroup'],
    },

    // Phase 3 - Complex Dependencies
    {
      name: 'product',
      service: 'productService',
      syncMethod: 'syncHistoricalProducts',
      syncType: 'full',
      dependencies: ['category', 'trademark'],
    },

    // Phase 4 - Transaction Dependencies
    {
      name: 'order',
      service: 'orderService',
      syncMethod: 'syncHistoricalOrders',
      syncType: 'full',
      dependencies: ['branch', 'customer', 'product', 'user', 'salechannel'],
    },

    {
      name: 'invoice',
      service: 'invoiceService',
      syncMethod: 'syncHistoricalInvoices',
      syncType: 'full',
      dependencies: [
        'branch',
        'customer',
        'product',
        'user',
        'order',
        'salechannel',
      ],
    },
  ];

  constructor(
    private readonly prismaService: PrismaService,
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
      'BusSchedulerService initialized, checking for startup sync...',
    );

    setTimeout(async () => {
      try {
        await this.checkAndRunStartupSync();
      } catch (error) {
        this.logger.error(`Startup sync check failed: ${error.message}`);
      }
    }, 2000);
  }

  @Cron('0 */15 * * * *')
  async handleScheduledSync() {
    if (this.isRunning) {
      this.logger.log('Bus scheduler already running, skipping scheduled sync');
      return;
    }

    try {
      await this.runSyncCycle();
    } catch (error) {
      this.logger.error(`Scheduled sync cycle failed: ${error.message}`);
    }
  }

  private async checkAndRunStartupSync(): Promise<void> {
    this.logger.log('Checking if startup sync is needed...');

    const needsHistoricalSync = await this.checkForMissingHistoricalSyncs();

    if (needsHistoricalSync.length > 0) {
      this.logger.log(
        `Found entities needing historical sync: ${needsHistoricalSync.join(', ')}`,
      );
      await this.runSyncCycle();
    } else {
      this.logger.log('No historical sync needed, running recent sync cycle');
      await this.runSyncCycle();
    }
  }

  private async checkForMissingHistoricalSyncs(): Promise<string[]> {
    const needsHistorical: string[] = [];

    for (const entity of this.syncEntities) {
      if (entity.syncType === 'full') {
        const historicalSync = await this.prismaService.syncControl.findFirst({
          where: {
            name: `${entity.name}_historical`,
            status: 'completed',
          },
        });

        if (!historicalSync) {
          needsHistorical.push(entity.name);
        }
      }
    }

    return needsHistorical;
  }

  private async runSyncCycle(): Promise<void> {
    if (this.isRunning) {
      this.logger.log('Sync cycle already running, skipping');
      return;
    }

    try {
      this.isRunning = true;

      await this.prismaService.syncControl.upsert({
        where: { name: 'bus_scheduler_cycle' },
        create: {
          name: 'bus_scheduler_cycle',
          entities: this.syncEntities.map((e) => e.name),
          syncMode: 'cycle',
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          progress: { currentEntity: null, completed: [], failed: [] },
        },
        update: {
          isRunning: true,
          status: 'in_progress',
          startedAt: new Date(),
          progress: { currentEntity: null, completed: [], failed: [] },
          error: null,
        },
      });

      this.logger.log('Starting bus scheduler sync cycle...');

      const completed: string[] = [];
      const failed: string[] = [];

      for (const entity of this.syncEntities) {
        await this.updateBusCycleProgress(entity.name, completed, failed);

        try {
          await this.syncEntity(entity);
          completed.push(entity.name);
          this.logger.log(`✅ ${entity.name} sync completed`);
        } catch (error) {
          failed.push(entity.name);
          this.logger.error(`❌ ${entity.name} sync failed: ${error.message}`);
        }
      }

      const finalStatus =
        failed.length > 0 ? 'completed_with_errors' : 'completed';
      const finalError =
        failed.length > 0 ? `Failed entities: ${failed.join(', ')}` : null;

      await this.prismaService.syncControl.update({
        where: { name: 'bus_scheduler_cycle' },
        data: {
          isRunning: false,
          status: finalStatus,
          completedAt: new Date(),
          progress: {
            currentEntity: null,
            completed,
            failed,
            totalEntities: this.syncEntities.length,
            completedCount: completed.length,
            failedCount: failed.length,
          },
          error: finalError,
        },
      });

      this.logger.log(
        `Bus scheduler cycle completed: ${completed.length} successful, ${failed.length} failed`,
      );

      if (failed.length > 0) {
        this.logger.warn(`Failed entities: ${failed.join(', ')}`);
      }
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: 'bus_scheduler_cycle' },
        data: {
          isRunning: false,
          status: 'failed',
          completedAt: new Date(),
          error: error.message,
        },
      });

      this.logger.error(`Bus scheduler cycle failed: ${error.message}`);
      throw error;
    } finally {
      this.isRunning = false;
    }
  }

  private async updateBusCycleProgress(
    currentEntity: string,
    completed: string[],
    failed: string[],
  ) {
    await this.prismaService.syncControl.update({
      where: { name: 'bus_scheduler_cycle' },
      data: {
        progress: {
          currentEntity,
          completed,
          failed,
          totalEntities: this.syncEntities.length,
          completedCount: completed.length,
          failedCount: failed.length,
        },
      },
    });
  }

  private async syncEntity(entity: SyncEntity): Promise<void> {
    this.logger.log(`Starting ${entity.name} sync...`);

    let retryCount = 0;
    let lastError: Error | null = null;

    while (retryCount <= this.MAX_RETRIES) {
      try {
        await this.executeSyncMethod(entity);
        return;
      } catch (error) {
        lastError = error;
        retryCount++;

        if (retryCount <= this.MAX_RETRIES) {
          this.logger.warn(
            `${entity.name} sync failed (attempt ${retryCount}/${this.MAX_RETRIES + 1}), retrying: ${error.message}`,
          );
          await this.delay(1000 * retryCount);
        }
      }
    }

    throw new Error(
      `${entity.name} sync failed after ${this.MAX_RETRIES + 1} attempts. Last error: ${lastError?.message}`,
    );
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
        where: {
          name: `${entity.name}_historical`,
          status: 'completed',
        },
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

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async manualSyncEntity(entityName: string): Promise<void> {
    if (this.isRunning) {
      throw new Error(
        'Bus scheduler is currently running, cannot run manual sync. Use forceHistoricalSyncEntity to override.',
      );
    }

    const entity = this.syncEntities.find((e) => e.name === entityName);
    if (!entity) {
      throw new Error(`Entity ${entityName} not found`);
    }

    this.logger.log(`Manual sync requested for ${entityName}`);
    await this.syncEntity(entity);
  }

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
      isRunning: this.isRunning,
    };
  }

  async stopBusScheduler(): Promise<void> {
    this.logger.log('Stop requested for bus scheduler');
    this.isRunning = false;
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

    // Temporarily override bus scheduler
    const wasRunning = this.isRunning;
    this.isRunning = true; // Prevent other syncs

    try {
      // Reset historical sync status
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

      // Force run historical sync
      const service = this[entity.service];
      if (!service) {
        throw new Error(
          `Service ${entity.service} not found for entity ${entityName}`,
        );
      }

      this.logger.log(`Running forced historical sync for ${entityName}`);
      await service[entity.syncMethod]();

      // Mark as completed
      await this.prismaService.syncControl.update({
        where: { name: `${entityName}_historical` },
        data: {
          status: 'completed',
          isRunning: false,
          completedAt: new Date(),
          progress: { stage: 'completed', forced: true },
        },
      });

      this.logger.log(`✅ Forced historical sync completed for ${entityName}`);
    } catch (error) {
      await this.prismaService.syncControl.update({
        where: { name: `${entityName}_historical` },
        data: {
          status: 'failed',
          isRunning: false,
          error: error.message,
          progress: { stage: 'failed', forced: true },
        },
      });

      this.logger.error(
        `❌ Forced historical sync failed for ${entityName}: ${error.message}`,
      );
      throw error;
    } finally {
      // Restore original running state
      this.isRunning = wasRunning;
    }
  }
}
