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
import { LarkDemandSyncService } from './demand/lark-demand-sync.service';
import { LarkCashflowSyncService } from './cashflow/lark-cashflow-sync.service';
import { LarkInvoiceDetailSyncService } from './invoice-detail/lark-invoice-detail-sync.service';
import { LarkInvoiceHistoricalSyncService } from './invoice-historical/lark-invoice-historical-sync.service';
import { LarkPaymentVoucherSyncService } from './payment-voucher/lark-payment-voucher-sync.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 60000,
      maxRedirects: 20,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [
    LarkAuthService,
    LarkCustomerSyncService,
    LarkInvoiceSyncService,
    LarkInvoiceHistoricalSyncService,
    LarkInvoiceDetailSyncService,
    LarkOrderSyncService,
    LarkProductSyncService,
    LarkSupplierSyncService,
    LarkOrderSupplierSyncService,
    LarkPurchaseOrderSyncService,
    LarkDemandSyncService,
    LarkCashflowSyncService,
    LarkPaymentVoucherSyncService,
  ],
  exports: [
    LarkAuthService,
    LarkCustomerSyncService,
    LarkInvoiceSyncService,
    LarkInvoiceHistoricalSyncService,
    LarkInvoiceDetailSyncService,
    LarkOrderSyncService,
    LarkProductSyncService,
    LarkSupplierSyncService,
    LarkOrderSupplierSyncService,
    LarkPurchaseOrderSyncService,
    LarkDemandSyncService,
    LarkCashflowSyncService,
    LarkPaymentVoucherSyncService,
  ],
})
export class LarkModule {}
