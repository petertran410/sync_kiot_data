// src/services/kiot-viet/kiot-viet.module.ts
import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietAuthService } from './auth.service';
import { KiotVietCustomerService } from './customer/customer.service';
// import { KiotVietCustomerGroupService } from './customer-group/customer-group.service';
import { KiotVietUserService } from './user/user.service';
import { KiotVietSaleChannelService } from './sale-channel/sale-channel.service';
import { KiotVietSurchargeService } from './surcharge/surcharge.service';
import { KiotVietBankAccountService } from './bank-account/bank-account.service';
import { LarkModule } from '../lark/lark.module';
import { KiotVietInvoiceService } from './invoice/invoice.service';
import { KiotVietOrderService } from './order/order.service';
import { KiotVietPriceBookService } from './pricebook/pricebook.service';
import { KiotVietProductService } from './product/product.service';
import { KiotVietSupplierService } from './supplier/supplier.service';
import { KiotVietOrderSupplierService } from './order-supplier/order-supplier.service';
import { KiotVietPurchaseOrderService } from './purchase-order/purchase-order.service';
// import { KiotVietBranchService } from './branch/branch.service';
// import { KiotVietTradeMarkService } from './trademark/trademark.service';
// import { KiotVietCategoryService } from './category/category.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 60000,
      maxRedirects: 5,
    }),
    ConfigModule,
    PrismaModule,
    LarkModule,
  ],
  providers: [
    KiotVietAuthService,
    KiotVietCustomerService,
    // KiotVietBranchService,
    // KiotVietCustomerGroupService,
    // KiotVietTradeMarkService,
    KiotVietPriceBookService,
    KiotVietUserService,
    KiotVietSaleChannelService,
    KiotVietSurchargeService,
    KiotVietBankAccountService,
    // KiotVietCategoryService,
    KiotVietProductService,
    KiotVietInvoiceService,
    KiotVietOrderService,
    KiotVietSupplierService,
    KiotVietOrderSupplierService,
    KiotVietPurchaseOrderService,
  ],
  exports: [
    KiotVietAuthService,
    KiotVietCustomerService,
    // KiotVietBranchService,
    // KiotVietCustomerGroupService,
    // KiotVietTradeMarkService,
    KiotVietPriceBookService,
    KiotVietUserService,
    KiotVietSaleChannelService,
    KiotVietSurchargeService,
    KiotVietBankAccountService,
    // KiotVietCategoryService,
    KiotVietProductService,
    KiotVietInvoiceService,
    KiotVietOrderService,
    KiotVietSupplierService,
    KiotVietOrderSupplierService,
    KiotVietPurchaseOrderService,
  ],
})
export class KiotVietModule {}
