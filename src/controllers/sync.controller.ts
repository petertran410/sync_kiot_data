// src/controllers/sync.controller.ts
import { Controller, Post, Get, Body, Param, Query } from '@nestjs/common';
import { BusSchedulerService } from '../services/bus-scheduler/bus-scheduler.service';
import { PrismaService } from '../prisma/prisma.service';
import { BusStatusResponse } from '../types/sync.types';
import { ScheduleType } from '../config/sync-schedule.config';
import { ScheduleUtils } from '../utils/schedule.utils';

@Controller('sync')
export class SyncController {
  constructor(
    private readonly busSchedulerService: BusSchedulerService,
    private readonly prismaService: PrismaService,
  ) {}

  // ===== LEGACY ENDPOINTS (Backward Compatibility) =====

  @Get('status')
  async getSyncStatus(): Promise<BusStatusResponse> {
    return this.busSchedulerService.getBusStatus();
  }

  @Post('start')
  async startSyncCycle() {
    return { message: 'Sync cycle started' };
  }

  @Post('stop')
  async stopSyncCycle() {
    await this.busSchedulerService.stopBusScheduler();
    return { message: 'Bus scheduler stopped' };
  }

  @Post('reset')
  async resetSyncCycle() {
    await this.busSchedulerService.forceResetBusScheduler();
    return { message: 'Bus scheduler reset completed' };
  }

  @Post('entity/:entityName')
  async syncEntity(@Param('entityName') entityName: string) {
    await this.busSchedulerService.manualSyncEntity(entityName);
    return { message: `${entityName} sync completed` };
  }

  @Get('controls')
  async getAllSyncControls() {
    const syncControls = await this.prismaService.syncControl.findMany({
      orderBy: { name: 'asc' },
    });
    return syncControls;
  }

  @Post('entity/:entityName/force-historical')
  async forceHistoricalSync(@Param('entityName') entityName: string) {
    try {
      await this.busSchedulerService.forceHistoricalSyncEntity(entityName);
      return {
        message: `${entityName} historical sync completed successfully`,
        forced: true,
      };
    } catch (error) {
      return {
        message: `${entityName} historical sync failed: ${error.message}`,
        forced: true,
        error: error.message,
      };
    }
  }

  @Post('entity/:entityName/historical')
  async historicalSync(@Param('entityName') entityName: string) {
    await this.prismaService.syncControl.updateMany({
      where: { name: `${entityName}_historical` },
      data: {
        status: 'idle',
        completedAt: null,
        isEnabled: true,
        isRunning: false,
        error: null,
      },
    });

    await this.busSchedulerService.manualSyncEntity(entityName);
    return { message: `${entityName} historical sync started` };
  }

  // ===== NEW ENHANCED ENDPOINTS =====

  @Get('status/enhanced')
  async getEnhancedSyncStatus() {
    return this.busSchedulerService.getEnhancedSyncStatus();
  }

  @Get('schedule-info')
  async getScheduleInfo() {
    return {
      isWeekend: ScheduleUtils.isWeekendVietnamTime(),
      isSunday: ScheduleUtils.isSunday(),
      nextWeekend: ScheduleUtils.getNextWeekendInfo(),
      currentTime: new Date(),
      vietnamTime: new Date().toLocaleString('en-US', {
        timeZone: 'Asia/Ho_Chi_Minh',
      }),
    };
  }

  @Post('trigger/:scheduleType')
  async triggerSync(@Param('scheduleType') scheduleType: ScheduleType) {
    if (!['every_15_minutes', 'weekends'].includes(scheduleType)) {
      throw new Error(
        'Invalid schedule type. Use: every_15_minutes or weekends',
      );
    }

    const result = await this.busSchedulerService.triggerSync(scheduleType);
    return {
      message: `${scheduleType} sync triggered successfully`,
      result,
    };
  }

  @Get('entities')
  async getEntitiesConfig() {
    const { configuredEntities } =
      await this.busSchedulerService.getEnhancedSyncStatus();
    return configuredEntities;
  }

  @Get('entities/by-schedule/:scheduleType')
  async getEntitiesBySchedule(
    @Param('scheduleType') scheduleType: ScheduleType,
  ) {
    const { configuredEntities } =
      await this.busSchedulerService.getEnhancedSyncStatus();
    return configuredEntities.filter(
      (entity) => entity.schedule === scheduleType,
    );
  }

  @Post('weekend/trigger')
  async triggerWeekendSync() {
    try {
      const result = await this.busSchedulerService.triggerSync('weekends');
      return {
        message: 'Weekend sync triggered successfully',
        result,
      };
    } catch (error) {
      return {
        message: `Weekend sync failed: ${error.message}`,
        error: error.message,
      };
    }
  }

  @Post('15min/trigger')
  async trigger15MinSync() {
    try {
      const result =
        await this.busSchedulerService.triggerSync('every_15_minutes');
      return {
        message: '15-minute sync triggered successfully',
        result,
      };
    } catch (error) {
      return {
        message: `15-minute sync failed: ${error.message}`,
        error: error.message,
      };
    }
  }
}
