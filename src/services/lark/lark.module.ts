import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { LarkAuthService } from './auth/lark-auth.service';
import { LarkCustomerSyncService } from './customer/lark-customer-sync.service';
import { LarkInvoiceSyncService } from './invoice/lark-invoice-sync.service';
import { LarkOrderSyncService } from './order/lark-order-sync.service';
import { LarkProductSyncService } from './product/lark-product-sync.service';
import { LarkSupplierSyncService } from './supplier/lark-supplier-sync.service';
import { LarkOrderSupplierSyncService } from './order-supplier/lark-order-supplier-sync.service';
import { LarkPurchaseOrderSyncService } from './purchase-order/lark-purchase-order-sync.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 30000,
      maxRedirects: 20,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [
    LarkAuthService,
    LarkCustomerSyncService,
    LarkInvoiceSyncService,
    LarkOrderSyncService,
    LarkProductSyncService,
    LarkSupplierSyncService,
    LarkOrderSupplierSyncService,
    LarkPurchaseOrderSyncService,
  ],
  exports: [
    LarkAuthService,
    LarkCustomerSyncService,
    LarkInvoiceSyncService,
    LarkOrderSyncService,
    LarkProductSyncService,
    LarkSupplierSyncService,
    LarkOrderSupplierSyncService,
    LarkPurchaseOrderSyncService,
  ],
})
export class LarkModule {}
