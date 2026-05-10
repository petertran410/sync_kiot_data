import { Module } from '@nestjs/common';
import { BusSchedulerService } from './bus-scheduler.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { LarkModule } from '../lark/lark.module';
import { MisaModule } from '../misa/misa.module';

@Module({
  imports: [PrismaModule, KiotVietModule, LarkModule, MisaModule],
  providers: [BusSchedulerService],
  exports: [BusSchedulerService],
})
export class BusSchedulerModule {}
