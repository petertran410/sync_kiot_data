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
import { WebhookModule } from './services/webhook/webhook.module';
import { WebhookController } from './controllers/webhook.controller';
import { MisaModule } from './services/misa/misa.module';
import { MisaCallbackController } from './controllers/misa-callback.controller';
import { MeInvoiceModule } from './services/meinvoice/meinvoice.module';
import { MeInvoiceController } from './controllers/meinvoice.controller';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    BusSchedulerModule,
    LarkModule,
    WebhookModule,
    MisaModule,
    MeInvoiceModule,
  ],
  controllers: [
    AppController,
    SyncController,
    WebhookController,
    MisaCallbackController,
    MeInvoiceController,
  ],
  providers: [AppService],
})
export class AppModule {}
