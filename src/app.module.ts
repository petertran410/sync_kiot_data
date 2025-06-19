// src/app.module.ts
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './kiot-viet/kiot-viet.module';
import { SyncModule } from './sync/sync.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    SyncModule,
    ScheduleModule.forRoot(), // If using scheduler
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
