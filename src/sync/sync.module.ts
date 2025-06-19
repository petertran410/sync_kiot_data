// src/sync/sync.scheduler.ts
import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { SyncService } from '../sync/sync.service';

@Injectable()
export class SyncScheduler {
  private readonly logger = new Logger(SyncScheduler.name);

  constructor(private readonly syncService: SyncService) {}

  @Cron('0 */1 * * *') // Run every hour
  async handleRecurrentSync() {
    try {
      // Check if any historical sync is running
      const historicalStatus =
        await this.syncService.getSyncStatus('historical_multi');
      if (historicalStatus?.isRunning) {
        this.logger.log('Historical sync is running. Skipping recurrent sync.');
        return;
      }

      // Start recent sync for customers
      await this.syncService.startMultiEntitySync({
        entities: ['customer'],
        syncMode: 'recent',
      });
    } catch (error) {
      this.logger.error(`Scheduled sync failed: ${error.message}`);
    }
  }
}
