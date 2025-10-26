import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { HttpModule } from '@nestjs/axios';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';

@Module({
  imports: [PrismaModule, HttpModule, KiotVietModule],
  providers: [WebhookService],
  exports: [WebhookService],
})
export class WebhookModule {}
