import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { HttpModule } from '@nestjs/axios';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { LarkModule } from '../lark/lark.module';
import { MisaModule } from '../misa/misa.module';
import { HisweetieModule } from '../hisweetie/hisweetie.module';

@Module({
  imports: [
    PrismaModule,
    HttpModule,
    KiotVietModule,
    LarkModule,
    MisaModule,
    HisweetieModule,
  ],
  providers: [WebhookService],
  exports: [WebhookService],
})
export class WebhookModule {}
