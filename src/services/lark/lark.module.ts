import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { LarkBaseClient } from './lark-base.client';
import { LarkCustomerSyncService } from './customer/lark-customer-sync.service';

@Module({
  imports: [PrismaModule],
  providers: [LarkBaseClient, LarkCustomerSyncService],
  exports: [LarkCustomerSyncService],
})
export class LarkModule {}
