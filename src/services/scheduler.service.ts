// src/services/scheduler.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { KiotVietCustomerService } from './kiot-viet/customer/customer.service';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class SchedulerService {
  private readonly logger = new Logger(SchedulerService.name);

  constructor(
    private readonly customerService: KiotVietCustomerService,
    private readonly prismaService: PrismaService,
  ) {}

  @Cron('0 */1 * * *') // Run every hour
  async handleCustomerSync() {
    try {
      // Check if historical sync is running
      const historicalSync = await this.prismaService.syncControl.findFirst({
        where: {
          syncType: 'customer_historical',
          isRunning: true,
        },
      });

      if (historicalSync) {
        this.logger.log(
          'Historical sync is running. Skipping scheduled recent sync.',
        );
        return;
      }

      // Run recent customer sync (last 24 hours)
      await this.customerService.syncRecentCustomers(1);

      // Remove any duplicates
      await this.customerService.removeDuplicateCustomers();
    } catch (error) {
      this.logger.error(`Scheduled customer sync failed: ${error.message}`);
    }
  }
}
