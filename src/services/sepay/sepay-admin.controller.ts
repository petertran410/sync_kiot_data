import {
  BadRequestException,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { AdminKeyGuard } from '../../controllers/admin-key.guard';
import { SePaySyncService } from './sepay-sync.service';
import { SePayWorkerService } from './sepay-worker.service';

@Controller('sepay')
@UseGuards(AdminKeyGuard)
export class SePayAdminController {
  constructor(
    private readonly syncService: SePaySyncService,
    private readonly worker: SePayWorkerService,
  ) {}

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

  @Post('transactions/:id/retry')
  async retry(@Param('id', ParseIntPipe) id: number) {
    const requeued = await this.worker.retryFailed(id);
    if (!requeued) {
      throw new BadRequestException(
        `SePay transaction ${id} was not found or is not FAILED`,
      );
    }
    return { success: true, id, status: 'RECEIVED' };
  }

  private parseDate(value: string): Date {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) throw new Error(`Invalid date: ${value}`);
    return date;
  }
}
