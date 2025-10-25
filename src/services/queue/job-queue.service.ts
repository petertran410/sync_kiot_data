import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';

export enum JobStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Injectable()
export class JobQueueService {
  private readonly logger = new Logger(JobQueueService.name);

  constructor(private readonly prismaService: PrismaService) {}

  async addJob(
    entityType: string,
    entityId: number,
    kiotVietId: bigint,
  ): Promise<void> {
    try {
      await this.prismaService.jobQueue.upsert({
        where: {
          entityType_kiotVietId: {
            entityType,
            kiotVietId,
          },
        },
        create: {
          entityType,
          entityId,
          kiotVietId,
          status: JobStatus.PENDING,
        },
        update: {
          entityId,
          status: JobStatus.PENDING,
          retryCount: 0,
          error: null,
          processedAt: null,
        },
      });

      this.logger.log(
        `✅ Job added: ${entityType} - kiotVietId: ${kiotVietId}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Failed to add job: ${entityType} - ${kiotVietId}: ${error.message}`,
      );
      throw error;
    }
  }

  async getNextJobs(limit: number = 10): Promise<any[]> {
    return this.prismaService.jobQueue.findMany({
      where: {
        status: JobStatus.PENDING,
      },
      orderBy: {
        createdAt: 'asc',
      },
      take: limit,
    });
  }

  async markJobProcessing(jobId: number): Promise<void> {
    await this.prismaService.jobQueue.update({
      where: { id: jobId },
      data: {
        status: JobStatus.PROCESSING,
      },
    });
  }

  async markJobCompleted(jobId: number): Promise<void> {
    await this.prismaService.jobQueue.update({
      where: { id: jobId },
      data: {
        status: JobStatus.COMPLETED,
        processedAt: new Date(),
      },
    });
  }

  async markJobFailed(
    jobId: number,
    error: string,
    retryCount: number,
    maxRetries: number,
  ): Promise<void> {
    const shouldRetry = retryCount < maxRetries;

    await this.prismaService.jobQueue.update({
      where: { id: jobId },
      data: {
        status: shouldRetry ? JobStatus.PENDING : JobStatus.FAILED,
        retryCount: retryCount + 1,
        error,
        processedAt: shouldRetry ? null : new Date(),
      },
    });

    if (shouldRetry) {
      this.logger.warn(
        `⚠️ Job ${jobId} failed, will retry (${retryCount + 1}/${maxRetries})`,
      );
    } else {
      this.logger.error(
        `❌ Job ${jobId} permanently failed after ${maxRetries} retries`,
      );
    }
  }

  async cleanupOldJobs(daysOld: number = 7): Promise<number> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysOld);

    const result = await this.prismaService.jobQueue.deleteMany({
      where: {
        status: {
          in: [JobStatus.COMPLETED, JobStatus.FAILED],
        },
        processedAt: {
          lt: cutoffDate,
        },
      },
    });

    this.logger.log(`🧹 Cleaned up ${result.count} old jobs`);
    return result.count;
  }

  async getQueueStats(): Promise<any> {
    const stats = await this.prismaService.jobQueue.groupBy({
      by: ['status', 'entityType'],
      _count: true,
    });

    return stats.reduce((acc, stat) => {
      if (!acc[stat.entityType]) {
        acc[stat.entityType] = {};
      }
      acc[stat.entityType][stat.status] = stat._count;
      return acc;
    }, {});
  }
}
