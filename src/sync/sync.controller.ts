// src/sync/sync.controller.ts
import { Controller, Post, Get, Body, Param, Query } from '@nestjs/common';
import { SyncService } from './sync.service';

@Controller('sync')
export class SyncController {
  constructor(private readonly syncService: SyncService) {}

  @Get('status')
  async getSyncStatus(@Query('name') name?: string) {
    return this.syncService.getSyncStatus(name);
  }

  @Post('start')
  async startSync(
    @Body()
    options: {
      entities?: string[];
      syncMode: string;
      sequential?: boolean;
    },
  ) {
    return this.syncService.startMultiEntitySync(options);
  }

  @Post('toggle')
  async toggleSync(@Body() body: { name: string; enabled: boolean }) {
    return this.syncService.toggleSync(body.name, body.enabled);
  }
}
