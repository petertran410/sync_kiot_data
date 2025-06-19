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
    await this.prismaService.syncControl.upsert({
      where: { name: 'customer_historical' },
      create: {
        name: 'customer_historical',
        entities: ['customer'],
        syncMode: 'historical',
        isEnabled: true,
        isRunning: false,
        status: 'idle',
      },
      update: {
        isEnabled: true,
        status: 'idle',
      },
    });

    // Start historical sync
    await this.customerService.syncHistoricalCustomers();
  }

  async disableHistoricalSync(): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: 'customer_historical' },
      data: { isEnabled: false, isRunning: false },
    });
  }

  async startRecentSync(days?: number): Promise<void> {
    await this.customerService.syncRecentCustomers(days);
  }
}
