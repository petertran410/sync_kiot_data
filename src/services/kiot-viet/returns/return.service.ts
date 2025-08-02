import { Inject, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { async, firstValueFrom } from 'rxjs';
import { PrismaService } from '../../../prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';
import { Prisma } from '@prisma/client';

interface KiotVietReturn {
  id: number;
  code: string;
  invoiceId: number;
  returnDate: string;
  branchId: number;
  branchName: string;
  receivedById: Number;
  soldByName: string;
  customerId?: number;
  customerCode: string;
  customerName: string;
  returnTotal: number;
  totalPayment: number;
  status: number;
  statusValue: string;
  createdDate: string;
  modifiedDate: string;
  payments: Array<{
    id: number;
    code: string;
    amount: number;
    method: string;
    status?: number;
    statusValue: string;
    transDate: string;
    bankAccount: string;
    accountId?: number;
    description: string;
  }>;
  returnDetails: Array<{
    returnId: number;
    lineNumber: number;
    productId: number;
    productCode: string;
    productName: string;
    quantity: number;
    price: number;
    note: string;
    usePoint: boolean;
    subTotal: number;
  }>;
}

@Injectable()
export class KiotVietReturnService {
  private readonly logger = new Logger(KiotVietReturnService.name);
  private readonly baseUrl: string;
  private readonly PAGE_SIZE = 100;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly authService: KiotVietAuthService,
  ) {
    const baseUrl = this.configService.get<string>('KIOT_BASE_URL');
    if (!baseUrl) {
      throw new Error('KIOT_BASE_URL environment variable is not configured');
    }
    this.baseUrl = baseUrl;
  }

  async syncHistoricalReturns(): Promise<void> {
    const syncName = 'return_historical';

    let currentItem = 0;
    let processedCount = 0;
    let totalReturns = 0;
    let consecutiveEmptyPages = 0;
    let consecutiveErrorPages = 0;
    let processedCategoryIds = new Set<number>();
  }
}
