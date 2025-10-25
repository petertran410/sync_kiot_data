import { Module } from '@nestjs/common';
import { JobQueueService } from './job-queue.service';
import { LarkSyncWorkerService } from './lark-sync-worker.service';
import { PrismaModule } from '../../prisma/prisma.module';
import { LarkModule } from '../lark/lark.module';

@Module({
  imports: [PrismaModule, LarkModule],
  providers: [JobQueueService, LarkSyncWorkerService],
  exports: [JobQueueService, LarkSyncWorkerService],
})
export class QueueModule {}
