// src/app.module.ts - Update existing file
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './services/kiot-viet/kiot-viet.module';
import { SyncModule } from './sync/sync.module';
import { BusSchedulerModule } from './services/bus-scheduler/bus-scheduler.module'; // ⭐ ADD
import { SyncController } from './controllers/sync.controller'; // ⭐ ADD

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    SyncModule,
    BusSchedulerModule, // ⭐ ADD
  ],
  controllers: [AppController, SyncController], // ⭐ ADD SyncController
  providers: [AppService],
})
export class AppModule {}
