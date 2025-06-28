// src/controllers/health.controller.ts
import { Controller, Get, Post } from '@nestjs/common';
import { LarkCustomerSyncService } from '../services/lark/customer/lark-customer-sync.service';
import { LarkInvoiceSyncService } from 'src/services/lark/invoice/lark-invoice-sync.service';

@Controller('health')
export class HealthController {
  constructor(
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
    private readonly larkInvoiceSyncService: LarkInvoiceSyncService,
  ) {}

  @Get('sync-customer')
  async checkCustomerSyncHealth() {
    const healthCheck = await this.larkCustomerSyncService.performHealthCheck();

    // Convert BigInt to string/number to avoid serialization error
    return JSON.parse(
      JSON.stringify(healthCheck, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-customer/progress')
  async getCustomerSyncProgress() {
    const progress = await this.larkCustomerSyncService.getSyncProgress();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(progress, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-customer/failed')
  async getCustomerFailedReport() {
    const report =
      await this.larkCustomerSyncService.getFailedCustomersReport();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(report, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-customer/reset-failed')
  async resetCustomerFailed() {
    const result = await this.larkCustomerSyncService.resetFailedCustomers();
    return result;
  }

  @Get('sync-customer/analyze-missing')
  async analyzeCustomerMissing() {
    const analysis = await this.larkCustomerSyncService.analyzeMissingData();

    return JSON.parse(
      JSON.stringify(analysis, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Post('sync-customer/missing-only')
  async syncCustomerMissingOnly() {
    const result = await this.larkCustomerSyncService.syncMissingDataOnly();

    return JSON.parse(
      JSON.stringify(result, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-customer/verify')
  async verifyCustomerSync() {
    const verification =
      await this.larkCustomerSyncService.verifySyncCompleteness();

    return JSON.parse(
      JSON.stringify(verification, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-invoice')
  async checkInvoiceSyncHealth() {
    const healthCheck = await this.larkInvoiceSyncService.performHealthCheck();

    // Convert BigInt to string/number to avoid serialization error
    return JSON.parse(
      JSON.stringify(healthCheck, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-invoice/progress')
  async getInvoiceSyncProgress() {
    const progress = await this.larkInvoiceSyncService.getSyncProgress();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(progress, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-invoice/failed')
  async getInvoiceFailedReport() {
    const report = await this.larkInvoiceSyncService.getFailedInvoicesReport();

    // Convert BigInt to string/number
    return JSON.parse(
      JSON.stringify(report, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-invoice/reset-failed')
  async resetInvoiceFailed() {
    const result = await this.larkInvoiceSyncService.resetFailedInvoices();
    return result;
  }

  @Get('sync-invoice/analyze-missing')
  async analyzeInvoiceMissing() {
    const analysis = await this.larkInvoiceSyncService.analyzeMissingData();

    return JSON.parse(
      JSON.stringify(analysis, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Post('sync-invoice/missing-only')
  async syncInvoiceMissingOnly() {
    const result = await this.larkInvoiceSyncService.syncMissingDataOnly();

    return JSON.parse(
      JSON.stringify(result, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }

  @Get('sync-invoice/verify')
  async verifyInvoiceSync() {
    const verification =
      await this.larkInvoiceSyncService.verifySyncCompleteness();

    return JSON.parse(
      JSON.stringify(verification, (key, value) =>
        typeof value === 'bigint' ? Number(value) : value,
      ),
    );
  }
}
