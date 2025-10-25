// src/app.module.ts - Update existing file
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './services/kiot-viet/kiot-viet.module';
import { BusSchedulerModule } from './services/bus-scheduler/bus-scheduler.module';
import { SyncController } from './controllers/sync.controller';
import { LarkModule } from './services/lark/lark.module';
import { HealthController } from './controllers/health.controller';
import { WebhookModule } from './services/webhook/webhook.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    BusSchedulerModule,
    LarkModule,
    WebhookModule,
  ],
  controllers: [AppController, SyncController, HealthController],
  providers: [AppService],
})
export class AppModule {}
