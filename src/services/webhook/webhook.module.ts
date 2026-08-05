import { Module } from '@nestjs/common';
import { WebhookService } from './webhook.service';
import { WebhookIngestService } from './webhook-ingest.service';
import { WebhookDeleteHandler } from './webhook-delete.handler';
import { WebhookRefHandler } from './webhook-ref.handler';
import { WebhookWorkerService } from './webhook-worker.service';
import { WebhookRegistryService } from './webhook-registry.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { HttpModule } from '@nestjs/axios';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { LarkModule } from '../lark/lark.module';
import { WebhookSignatureGuard } from '../../controllers/webhook-signature.guard';
import { AdminKeyGuard } from '../../controllers/admin-key.guard';

@Module({
   imports: [PrismaModule, HttpModule, KiotVietModule, LarkModule],
  providers: [
    WebhookService,
    WebhookIngestService,
    WebhookDeleteHandler,
    WebhookRefHandler,
    WebhookWorkerService,
    WebhookRegistryService,
    WebhookSignatureGuard,
    AdminKeyGuard,
  ],
  exports: [
    WebhookService,
    WebhookIngestService,
    WebhookDeleteHandler,
    WebhookRefHandler,
    WebhookWorkerService,
    WebhookRegistryService,
    WebhookSignatureGuard,
    AdminKeyGuard,
  ],
})
export class WebhookModule {}
