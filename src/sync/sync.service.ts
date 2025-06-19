// src/sync/sync.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { KiotVietCustomerService } from '../services/kiot-viet/customer/customer.service';
// Import other entity services as needed

@Injectable()
export class SyncService {
  private readonly logger = new Logger(SyncService.name);
  private readonly availableEntities = [
    'customer',
    // Add other entities as you implement them
  ];

  constructor(
    private readonly prismaService: PrismaService,
    private readonly customerService: KiotVietCustomerService,
    // Inject other entity services as needed
  ) {}

  // Place all the SyncControl management functions here

  async startMultiEntitySync(options: {
    entities?: string[];
    syncMode: string;
    sequential?: boolean;
  }) {
    // Implementation from previous response
  }

  private async runSequentialSync(
    syncControlId: number,
    entities: string[],
    syncMode: string,
  ) {
    // Implementation from previous response
  }

  private async runConcurrentSync(
    syncControlId: number,
    entities: string[],
    syncMode: string,
  ) {
    // Implement concurrent sync logic here
  }

  async getSyncStatus(name?: string) {
    // Get sync status from database
  }

  async toggleSync(name: string, enabled: boolean) {
    // Toggle sync on/off
  }

  // Helper method to get the appropriate service for an entity
  private getServiceForEntity(entity: string) {
    switch (entity) {
      case 'customer':
        return this.customerService;
      // Add other cases as you implement more entity services
      default:
        throw new Error(`Unknown entity type: ${entity}`);
    }
  }
}
