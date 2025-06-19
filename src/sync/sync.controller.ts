import { Controller, Post, Get, Body } from '@nestjs/common';
import { SyncService } from '../sync/sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Get('status')
  async getSyncStatus() {
    return this.syncService.getSyncStatus();
  }

  @Post('customers/recent')
  async syncRecentCustomers(@Body() body: { days?: number }) {
    const days = body.days || 7;
    await this.syncService.startRecentSync(days);
    return { message: `Started recent customer sync for last ${days} days` };
  }

  @Post('historical/enable')
  async enableHistoricalSync() {
    await this.syncService.enableHistoricalSync();
    return { message: 'Historical sync enabled and started' };
  }

  @Post('historical/disable')
  async disableHistoricalSync() {
    await this.syncService.disableHistoricalSync();
    return { message: 'Historical sync disabled' };
  }
}
