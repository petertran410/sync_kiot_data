// src/app.module.ts
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './services/kiot-viet/kiot-viet.module';
import { SyncModule } from './sync/sync.module';
import { BusSchedulerModule } from './services/bus-scheduler/bus-scheduler.module';
import { SyncController } from './controllers/sync.controller';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    SyncModule,
    BusSchedulerModule,
  ],
  controllers: [AppController, SyncController],
  providers: [AppService],
})
export class AppModule {}
