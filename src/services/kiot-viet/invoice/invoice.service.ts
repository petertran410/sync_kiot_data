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
  SaleChannel?: {
    isNotDelete?: boolean;
    RetailedId?: number;
    Position?: number;
    IsActive?: boolean;
    CreatedBy?: number;
    CreatedDate?: string;
    Id?: number;
    Name?: string;
  };
  invoiceDelivery?: {
    type?: number;
    status?: number;
    statusValue?: string;
    price?: number;
    receiver?: string;
    contactNumber?: string;
  };
}
