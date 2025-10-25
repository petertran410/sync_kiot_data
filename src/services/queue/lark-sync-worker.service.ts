import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { JobQueueService, JobStatus } from './job-queue.service';
import { PrismaService } from '../../prisma/prisma.service';
import { LarkOrderSyncService } from '../lark/order/lark-order-sync.service';
import { LarkInvoiceSyncService } from '../lark/invoice/lark-invoice-sync.service';

@Injectable()
export class LarkSyncWorkerService {
  private readonly logger = new Logger(LarkSyncWorkerService.name);
  private isProcessing = false;
  private readonly BATCH_SIZE = 10;

  constructor(
    private readonly jobQueueService: JobQueueService,
    private readonly prismaService: PrismaService,
    private readonly larkOrderSyncService: LarkOrderSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
  ) {}

  @Cron(CronExpression.EVERY_10_SECONDS)
  async processJobs(): Promise<void> {
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;

    try {
      const jobs = await this.jobQueueService.getNextJobs(this.BATCH_SIZE);

      if (jobs.length === 0) {
        return;
      }

      this.logger.log(`📦 Processing ${jobs.length} jobs...`);

      for (const job of jobs) {
        await this.processJob(job);
      }

      this.logger.log(`✅ Batch completed: ${jobs.length} jobs processed`);
    } catch (error) {
      this.logger.error(`❌ Worker error: ${error.message}`);
    } finally {
      this.isProcessing = false;
    }
  }

  private async processJob(job: any): Promise<void> {
    try {
      await this.jobQueueService.markJobProcessing(job.id);

      this.logger.log(
        `🔄 Processing job ${job.id}: ${job.entityType} - ${job.kiotVietId}`,
      );

      switch (job.entityType) {
        case 'order':
          await this.syncOrder(job.entityId);
          break;
        case 'invoice':
          await this.syncInvoice(job.entityId);
          break;
        default:
          throw new Error(`Unknown entity type: ${job.entityType}`);
      }

      await this.jobQueueService.markJobCompleted(job.id);
      this.logger.log(`✅ Job ${job.id} completed`);
    } catch (error) {
      this.logger.error(`❌ Job ${job.id} failed: ${error.message}`);

      await this.jobQueueService.markJobFailed(
        job.id,
        error.message,
        job.retryCount,
        job.maxRetries,
      );
    }
  }

  private async syncOrder(entityId: number): Promise<void> {
    const order = await this.prismaService.order.findUnique({
      where: { id: entityId },
    });

    if (!order) {
      throw new Error(`Order not found: ${entityId}`);
    }

    await this.larkOrderSyncService.syncOrdersToLarkBase([order]);
  }

  private async syncInvoice(entityId: number): Promise<void> {
    const invoice = await this.prismaService.invoice.findUnique({
      where: { id: entityId },
    });

    if (!invoice) {
      throw new Error(`Invoice not found: ${entityId}`);
    }

    await this.larkInvoiceSyncService.syncInvoicesToLarkBase([invoice]);
  }

  @Cron(CronExpression.EVERY_DAY_AT_2AM)
  async cleanupOldJobs(): Promise<void> {
    this.logger.log('🧹 Running daily cleanup...');
    await this.jobQueueService.cleanupOldJobs(7);
  }

  async getStats(): Promise<any> {
    return this.jobQueueService.getQueueStats();
  }
}
