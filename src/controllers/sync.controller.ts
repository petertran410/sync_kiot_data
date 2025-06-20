// src/controllers/sync.controller.ts
import { Controller, Post, Get, Body, Param, Query } from '@nestjs/common';
import { BusSchedulerService } from '../services/bus-scheduler/bus-scheduler.service';
import { PrismaService } from '../prisma/prisma.service';
import { BusStatusResponse } from '../types/sync.types';

@Controller('sync')
export class SyncController {
  constructor(
    private readonly busSchedulerService: BusSchedulerService,
    private readonly prismaService: PrismaService,
  ) {}

  @Get('status')
  async getSyncStatus(): Promise<BusStatusResponse> {
    return this.busSchedulerService.getBusStatus();
  }

  @Post('start')
  async startSyncCycle() {
    // This will trigger the bus scheduler cycle manually
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

  @Post('entity/:entityName/historical')
  async forceHistoricalSync(@Param('entityName') entityName: string) {
    // Reset the historical sync status first
    await this.prismaService.syncControl.update({
      where: { name: `${entityName}_historical` },
      data: {
        status: 'idle',
        completedAt: null,
        isEnabled: true,
        isRunning: false,
        error: null,
      },
    });

    // Then run the sync
    await this.busSchedulerService.manualSyncEntity(entityName);
    return { message: `${entityName} historical sync started` };
  }
}
