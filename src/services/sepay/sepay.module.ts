import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { WebhookModule } from '../webhook/webhook.module';
import { SePayController } from './sepay.controller';
import { SePayIngestService } from './sepay-ingest.service';
import { SePayPaymentService } from './sepay-payment.service';
import { SePaySignatureGuard } from './sepay-signature.guard';
import { SePayWorkerService } from './sepay-worker.service';
import { SePaySyncService } from './sepay-sync.service';
import { SePayAdminController } from './sepay-admin.controller';

@Module({
  imports: [HttpModule, PrismaModule, KiotVietModule, WebhookModule],
  controllers: [SePayController, SePayAdminController],
  providers: [
    SePayIngestService,
    SePayPaymentService,
    SePaySignatureGuard,
    SePayWorkerService,
    SePaySyncService,
  ],
  exports: [SePaySyncService],
})
export class SePayModule {}
