import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietModule } from '../kiot-viet/kiot-viet.module';
import { WebhookModule } from '../webhook/webhook.module';
import { LarkModule } from '../lark/lark.module';
import { SyncOrchestratorService } from './sync-orchestrator.service';
import { SyncSchedulerService } from './sync-scheduler.service';
import { ReconciliationService } from './reconciliation.service';

@Module({
  // WebhookModule is needed for the scheduled webhook-drift check.
  // PrismaModule is needed by ReconciliationService for its row counts.
   imports: [PrismaModule, KiotVietModule, WebhookModule, LarkModule],
  providers: [
    SyncOrchestratorService,
    SyncSchedulerService,
    ReconciliationService,
  ],
  exports: [
    SyncOrchestratorService,
    SyncSchedulerService,
    ReconciliationService,
  ],
})
export class SyncModule {}
