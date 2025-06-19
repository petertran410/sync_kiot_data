import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './services/kiot-viet/kiot-viet.module';
import { SyncModule } from './sync/sync.module';
import { SyncController } from './controllers/sync.controller';
import { SchedulerService } from './services/scheduler.service';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    SyncModule,
  ],
  controllers: [AppController, SyncController],
  providers: [AppService, SchedulerService],
})
export class AppModule {}
