import { Controller, Post, Get, Body, Param, Query } from '@nestjs/common';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
import { PrismaService } from '../prisma/prisma.service';
import { KiotVietBranchService } from 'src/services/kiot-viet/branch/branch.service';
import { KiotVietCustomerGroupService } from 'src/services/kiot-viet/customer-group/customer-group.service';

@Controller('sync')
export class SyncController {
  constructor(
    private readonly customerService: KiotVietCustomerService,
    private readonly prismaService: PrismaService,
    private readonly branchService: KiotVietBranchService,
    private readonly customerGroupService: KiotVietCustomerGroupService,
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

  @Post('customers/force-historical')
  async forceHistoricalSync() {
    await this.prismaService.syncControl.deleteMany({
      where: { name: 'customer_historical' },
    });

    // Start fresh historical sync
    await this.customerService.syncHistoricalCustomers();
    return {
      message: 'Started complete historical customer sync from scratch',
    };
  }

  @Post('branches')
  async syncBranches() {
    await this.branchService.syncBranches();
    return { message: 'Branch sync completed' };
  }

  @Post('customer-groups')
  async syncCustomerGroups() {
    await this.customerGroupService.syncCustomerGroups();
    return { message: 'Customer groups sync completed' };
  }
}
