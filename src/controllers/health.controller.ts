// src/controllers/health.controller.ts
import { Controller, Get, Post } from '@nestjs/common';
import { LarkCustomerSyncService } from '../services/lark/customer/lark-customer-sync.service';

@Controller('health')
export class HealthController {
  constructor(
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
  ) {}

  @Get('sync')
  async getSyncHealth() {
    return await this.larkCustomerSyncService.performSyncHealthCheck();
  }

  @Get('reconcile')
  async getDataReconciliation() {
    return await this.larkCustomerSyncService.reconcileDataMismatch();
  }

  @Get('connectivity')
  async getLarkBaseConnectivity() {
    return await this.larkCustomerSyncService.testLarkBaseConnectivity();
  }

  @Get('data-quality')
  async getDataQuality() {
    return await this.larkCustomerSyncService.checkCustomerDataQuality();
  }

  @Get('sync-control')
  async getSyncControlHealth() {
    return await this.larkCustomerSyncService.checkSyncControlHealth();
  }

  @Get('kiotviet-id-debug')
  async getKiotVietIdDebug() {
    return await this.larkCustomerSyncService.debugKiotVietIdDataTypes();
  }
}
