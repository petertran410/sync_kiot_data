import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { HttpModule } from '@nestjs/axios';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { LarkModule } from '../lark/lark.module';

@Module({
  imports: [PrismaModule, HttpModule, KiotVietModule, LarkModule],
  providers: [WebhookService],
  exports: [WebhookService],
})
export class WebhookModule {}
