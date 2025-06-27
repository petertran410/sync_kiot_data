import { LarkCustomerSyncService } from './../../lark/customer/lark-customer-sync.service';
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { firstValueFrom } from 'rxjs';
import { Prisma } from '@prisma/client';

interface KiotVietInvoice {
  id: number;
  code: string;
  purchaseDate?: string;
  branchId?: number;
  branchName?: string;
  soldById?: number;
  soldByName?: string;
  customerId?: number;
  customerCode?: string;
  customerName?: string;
  total?: number;
  totalPayment?: number;
  status?: number;
  statusValue?: string;
  usingCod?: boolean;
  createdDate?: string;
  modifiedDate?: string;
  payment?: [
    {
      id?: number;
      code?: string;
      amount?: number;
      method?: string;
      status?: number;
      statusValue?: string;
      transDate?: string;
      bankAccount?: string;
      accountId?: number;
    },
  ];
  invoiceOrderSurcharges?: [
    {
      id?: number;
      invoiceId?: number;
      surchargeId?: number;
      surchargeName?: string;
      surValue?: number;
      price?: number;
      createdDate?: string;
    },
  ];
  invoiceDetails?: [
    {
      productId?: number;
      productCode?: string;
      productName?: string;
      quantity?: number;
      price?: number;
      discountRatio?: number;
      discount?: number;
      note?: string;
      serialNumbers?: string;
      productBatchExpire?: {
        id?: number;
        productId?: number;
        batchName?: string;
        fullNameVirgule?: string;
        createdDate?: string;
        expireDate?: string;
      };
    },
  ];
  invoiceDelivery?: {
    deliveryCode?: string;
    type?: number;
    status?: number;
    statusValue?: string;
    price?: number;
    receiver?: string;
    contactNumber?: string;
    address?: string;
    locationId?: number;
    locationName?: string;
    usingPriceCod?: boolean;
    priceCodPayment?: number;
    weight?: number;
    length?: number;
    width?: number;
    height?: number;
    partnerDeliveryId?: number;
    partnerDelivery?: {
      code?: string;
      name?: string;
      address?: string;
      contactNumber?: string;
      email?: string;
    };
    SaleChannel?: {
      IsNotDelete?: boolean;
      RetailedId?: number;
      Position?: number;
      IsActivate?: boolean;
      CreatedBy?: number;
      CreatedDate?: string;
      Id?: number;
      Name?: string;
    };
  };
}

@Injectable()
export class KiotVietInvoiceService {
  private readonly logger = new Logger(KiotVietInvoiceService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
    private readonly larkCustomerSyncService: LarkCustomerSyncService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }

    this.baseUrl = baseUrl;
  }
}
