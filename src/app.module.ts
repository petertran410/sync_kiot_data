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
import { QueueModule } from './services/queue/queue.module';
import { WebhookController } from './controllers/webhook.controller';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    BusSchedulerModule,
    LarkModule,
    WebhookModule,
    QueueModule,
  ],
  controllers: [
    AppController,
    SyncController,
    HealthController,
    WebhookController,
  ],
  providers: [AppService],
})
export class AppModule {}
