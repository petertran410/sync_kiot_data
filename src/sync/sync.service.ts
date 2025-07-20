import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
  ) {}

  async getSyncStatus(name?: string) {
    if (name) {
      return this.prismaService.syncControl.findFirst({ where: { name } });
    }
    return this.prismaService.syncControl.findMany();
  }

  async enableHistoricalSync(): Promise<void> {
    await this.customerService.enableHistoricalSync();
  }

  async disableHistoricalSync(): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: 'customer_historical' },
      data: { isEnabled: false, isRunning: false },
    });
  }

  async startRecentSync(days?: number): Promise<void> {
    await this.customerService.syncRecentCustomers(days || 4);
  }
}
