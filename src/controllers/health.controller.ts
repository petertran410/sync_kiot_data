// src/controllers/health.controller.ts
import { Controller, Get, Post } from '@nestjs/common';
import { LarkCustomerSyncService } from '../services/lark/customer/lark-customer-sync.service';

@Controller('health')
export class HealthController {
  constructor(
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
  ) {}

  @Get('sync')
  async checkSyncHealth() {
    const healthCheck = await this.larkCustomerSyncService.performHealthCheck();

    // Convert BigInt to string/number to avoid serialization error
    return JSON.parse(
      JSON.stringify(healthCheck, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync/progress')
  async getSyncProgress() {
    const progress = await this.larkCustomerSyncService.getSyncProgress();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(progress, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync/failed')
  async getFailedReport() {
    const report =
      await this.larkCustomerSyncService.getFailedCustomersReport();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(report, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync/reset-failed')
  async resetFailed() {
    const result = await this.larkCustomerSyncService.resetFailedCustomers();
    return result;
  }

  @Get('sync/analyze-missing')
  async analyzeMissing() {
    const analysis = await this.larkCustomerSyncService.analyzeMissingData();

    return JSON.parse(
      JSON.stringify(analysis, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Post('sync/missing-only')
  async syncMissingOnly() {
    const result = await this.larkCustomerSyncService.syncMissingDataOnly();

    return JSON.parse(
      JSON.stringify(result, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync/verify')
  async verifySync() {
    const verification =
      await this.larkCustomerSyncService.verifySyncCompleteness();

    return JSON.parse(
      JSON.stringify(verification, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }
}
