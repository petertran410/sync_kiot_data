import { Module } from '@nestjs/common';
import { WebhookController } from '../../controllers/webhook.controller';
import { WebhookService } from './webhook.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { LarkModule } from '../lark/lark.module';

@Module({
  imports: [PrismaModule, LarkModule],
  controllers: [WebhookController],
  providers: [WebhookService],
  exports: [WebhookService],
})
export class WebhookModule {}
