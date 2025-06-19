import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { KiotVietCustomerService } from './kiot-viet/customer/customer.service';

@Injectable()
export class SchedulerService implements OnModuleInit {
  private readonly logger = new Logger(SchedulerService.name);

  constructor(private readonly customerService: KiotVietCustomerService) {
    this.logger.debug(`CustomerService injected: ${!!this.customerService}`);
  }

  async onModuleInit() {
    this.logger.log('Application started, checking sync status...');
    try {
      if (!this.customerService) {
        throw new Error('CustomerService is not available');
      }
      await this.customerService.checkAndRunAppropriateSync();
    } catch (error) {
      this.logger.error(`Startup sync check failed: ${error.message}`);
      this.logger.error(`Error stack: ${error.stack}`);
    }
  }

  @Cron('0 */1 * * *')
  async handleScheduledSync() {
    try {
      await this.customerService.syncRecentCustomers(1);
    } catch (error) {
      this.logger.error(`Scheduled sync failed: ${error.message}`);
    }
  }
}
