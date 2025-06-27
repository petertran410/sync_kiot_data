// src/services/lark/lark.module.ts
import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { LarkAuthService } from './auth/lark-auth.service';
import { LarkCustomerSyncService } from './customer/lark-customer-sync.service';
import { LarkInvoiceSyncService } from './invoice/lark-invoice-sync.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 30000,
      maxRedirects: 3,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [LarkAuthService, LarkCustomerSyncService, LarkInvoiceSyncService],
  exports: [LarkAuthService, LarkCustomerSyncService, LarkInvoiceSyncService],
})
export class LarkModule {}
