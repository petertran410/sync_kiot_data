// src/services/bus-scheduler/bus-scheduler.module.ts
import { Module } from '@nestjs/common';
import { BusSchedulerService } from './bus-scheduler.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';

@Module({
  imports: [PrismaModule, KiotVietModule],
  providers: [BusSchedulerService],
  exports: [BusSchedulerService],
})
export class BusSchedulerModule {}
