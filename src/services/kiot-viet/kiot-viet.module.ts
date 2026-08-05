import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietSharedModule } from './shared/kiot-viet-shared.module';
import { KiotVietAuthService } from './auth.service';
import { KiotVietCustomerService } from './customer/customer.service';
import { KiotVietCustomerGroupService } from './customer-group/customer-group.service';
import { KiotVietUserService } from './user/user.service';
import { KiotVietSaleChannelService } from './sale-channel/sale-channel.service';
import { KiotVietSurchargeService } from './surcharge/surcharge.service';
import { KiotVietBankAccountService } from './bank-account/bank-account.service';
import { KiotVietBranchService } from './branch/branch.service';
import { KiotVietInvoiceService } from './invoice/invoice.service';
import { KiotVietOrderService } from './order/order.service';
import { KiotVietPriceBookService } from './pricebook/pricebook.service';
import { KiotVietProductService } from './product/product.service';
import { KiotVietSupplierService } from './supplier/supplier.service';
import { KiotVietOrderSupplierService } from './order-supplier/order-supplier.service';
import { KiotVietPurchaseOrderService } from './purchase-order/purchase-order.service';
import { KiotVietTradeMarkService } from './trademark/trademark.service';
import { KiotVietCategoryService } from './category/category.service';
import { KiotVietReturnService } from './returns/return.service';
import { KiotVietCashflowService } from './cashflow/cashflow.service';
import { KiotVietTransferService } from './transfer/transfer.service';
import { KiotVietVoucherCampaign } from './voucher-campaign/voucher-campaign.service';
import { KiotVietProductOnHandService } from './product-onhand/product-onhand.service';
import { KiotVietLocationService } from './location/location.service';
import { KiotVietSettingsService } from './settings/settings.service';
import { KiotVietVoucherService } from './voucher/voucher.service';

@Module({
  imports: [HttpModule, ConfigModule, PrismaModule, KiotVietSharedModule],
  providers: [
    KiotVietCustomerService,
    KiotVietCustomerGroupService,
    KiotVietBranchService,
    KiotVietTradeMarkService,
    KiotVietPriceBookService,
    KiotVietUserService,
    KiotVietSaleChannelService,
    KiotVietSurchargeService,
    KiotVietBankAccountService,
    KiotVietCategoryService,
    KiotVietProductService,
    KiotVietInvoiceService,
    KiotVietOrderService,
    KiotVietSupplierService,
    KiotVietOrderSupplierService,
    KiotVietPurchaseOrderService,
    KiotVietReturnService,
    KiotVietCashflowService,
    KiotVietTransferService,
    KiotVietVoucherCampaign,
    KiotVietProductOnHandService,
    KiotVietLocationService,
    KiotVietSettingsService,
    KiotVietVoucherService,
  ],
  exports: [
    KiotVietSharedModule,
    KiotVietCustomerService,
    KiotVietCustomerGroupService,
    KiotVietBranchService,
    KiotVietTradeMarkService,
    KiotVietPriceBookService,
    KiotVietUserService,
    KiotVietSaleChannelService,
    KiotVietSurchargeService,
    KiotVietBankAccountService,
    KiotVietCategoryService,
    KiotVietProductService,
    KiotVietInvoiceService,
    KiotVietOrderService,
    KiotVietSupplierService,
    KiotVietOrderSupplierService,
    KiotVietPurchaseOrderService,
    KiotVietReturnService,
    KiotVietCashflowService,
    KiotVietTransferService,
    KiotVietVoucherCampaign,
    KiotVietProductOnHandService,
    KiotVietLocationService,
    KiotVietSettingsService,
    KiotVietVoucherService,
  ],
})
export class KiotVietModule {}
