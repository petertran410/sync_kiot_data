import { Controller, Get, Post, Query, UseGuards } from '@nestjs/common';
import { AdminKeyGuard } from '../../controllers/admin-key.guard';
import { SePaySyncService } from './sepay-sync.service';

@Controller('sepay')
@UseGuards(AdminKeyGuard)
export class SePayAdminController {
  constructor(private readonly syncService: SePaySyncService) {}

  @Post('sync/full')
  async syncFull(@Query('from') from?: string, @Query('to') to?: string) {
    const result = await this.syncService.syncAll(
      from ? this.parseDate(from) : undefined,
      to ? this.parseDate(to) : undefined,
    );
    return { success: true, result };
  }

  @Get('sync/status')
  async status() {
    return { success: true, transactions: await this.syncService.stats() };
  }

  private parseDate(value: string): Date {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) throw new Error(`Invalid date: ${value}`);
    return date;
  }
}
