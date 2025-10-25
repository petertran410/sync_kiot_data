import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { HttpModule } from '@nestjs/axios';

@Module({
  imports: [PrismaModule, HttpModule],
  providers: [WebhookService],
  exports: [WebhookService],
})
export class WebhookModule {}
