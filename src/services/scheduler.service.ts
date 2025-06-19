import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { KiotVietCustomerService } from './kiot-viet/customer/customer.service';

@Injectable()
export class SchedulerService implements OnModuleInit {
  private readonly logger = new Logger(SchedulerService.name);

  constructor(private readonly customerService: KiotVietCustomerService) {}

  // Handle system startup
  async onModuleInit() {
    this.logger.log('Application started, checking sync status...');
    try {
      await this.customerService.checkAndRunAppropriateSync();
    } catch (error) {
      this.logger.error(`Startup sync check failed: ${error.message}`);
    }
  }

  @Cron('0 */1 * * *') // Run every hour
  async handleScheduledSync() {
    try {
      await this.customerService.syncRecentCustomers(1); // Last 24 hours
    } catch (error) {
      this.logger.error(`Scheduled sync failed: ${error.message}`);
    }
  }
}
