import { Controller, Post, Get, Body, Param, Query } from '@nestjs/common';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
import { PrismaService } from '../prisma/prisma.service';

@Controller('sync')
export class SyncController {
  constructor(
    private readonly customerService: KiotVietCustomerService,
    private readonly prismaService: PrismaService,
  ) {}

  @Post('customers/recent')
  async syncRecentCustomers(@Body() body: { days?: number }) {
    const days = body.days || 7;
    await this.customerService.syncRecentCustomers(days);
    return { message: `Started syncing customers from the last ${days} days` };
  }

  @Post('customers/historical')
  async syncHistoricalCustomers() {
    await this.customerService.syncHistoricalCustomers();
    return { message: 'Started historical customer sync' };
  }

  @Get('status')
  async getSyncStatus() {
    const syncControls = await this.prismaService.syncControl.findMany();
    return syncControls;
  }

  @Post('customers/remove-duplicates')
  async removeDuplicateCustomers() {
    const removed = await this.customerService.removeDuplicateCustomers();
    return { message: `Removed ${removed} duplicate customer records` };
  }

  @Post('customers/toggle-historical')
  async toggleHistoricalSync(@Body() body: { enabled: boolean }) {
    await this.prismaService.syncControl.upsert({
      where: { name: 'customer_historical' },
      create: {
        name: 'customer_historical',
        entities: ['customer'],
        syncMode: 'historical',
        isRunning: body.enabled,
        isEnabled: body.enabled,
        status: body.enabled ? 'in_progress' : 'stopped',
      },
      update: {
        isRunning: body.enabled,
        isEnabled: body.enabled,
        status: body.enabled ? 'in_progress' : 'stopped',
      },
    });

    if (body.enabled) {
      await this.customerService.syncHistoricalCustomers();
    }

    return {
      message: body.enabled
        ? 'Historical sync enabled and started'
        : 'Historical sync disabled',
    };
  }
}
