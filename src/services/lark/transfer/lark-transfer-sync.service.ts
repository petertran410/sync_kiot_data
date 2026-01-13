import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_TRANSFER_FIELDS = {
  KIOTVIET_ID: 'kiotVietId',
  TRANSFER_CODE: 'Mã Chuyển Hàng',
  STATUS: 'Trạng Thái',
  RECEIVE_DATE: 'Ngày Nhận',
  BRANCH_SEND: 'Chi Nhánh Chuyển',
  BRANCH_RECEIVE: 'Chi Nhánh Nhận',
  SEND_DATE: 'Ngày Gửi Đi',
};

const LARK_TRANSFER_DETAIL_FIELDS = {
  ID_TRANSFER: 'Id Chuyển Hàng',
  ID_PRODUCT: 'Id Sản Phẩm',
  RECEIVE_QUANTITY: 'Số Lượng Sản Phẩm Nhận',
  PRICE: 'Đơn Giá Sản Phẩm',
  TOTAL_PRICE_SEND: 'Tổng Tiền Chuyển',
  TOTAL_PRICE_RECEIVE: 'Tổng Tiền Nhận',
  PRODUCT_CODE: 'Mã Sản Phẩm',
  PRODUCT_NAME: 'Tên Sản Phẩm',
  SEND_QUANTITY: 'Số Lượng Gửi Đi',
  UNIQUE_KEY: 'uniqueKey',
};

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    items?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

interface BatchResult {
  successRecords: any[];
  failedRecords: any[];
}

interface BatchDetailResult {
  successDetailsRecords: any[];
  failedDetailsRecords: any[];
}

@Injectable()
export class LarkTransferSyncService {
  private readonly logger = new Logger(LarkTransferSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;
  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;
  private readonly batchSize = 100;
  private readonly pendingCreation = new Set<number>();
  private readonly pendingDetailCreation = new Set<number>();

  private readonly MAX_AUTH_RETRIES = 3;
  private readonly AUTH_ERROR_CODES = [99991663, 99991664, 99991665];

  private existingRecordsCach = new Map<number, string>();
  private transferCodeCache = new Map<string, string>();

  private existingDetailRecordsCach = new Map<number, string>();
  private transferDetailCodeCache = new Map<string, string>();

  private cacheLoaded = false;
  private cacheDetailLoaded = false;

  private lastCacheLoadTime: Date | null = null;
  private lastDetailCacheLoadTime: Date | null = null;

  private readonly CACHE_VALIDITY_MINUTES = 600;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_TRANSFER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_TRANSFER_SYNC_TABLE_ID',
    );

    const baseTokenDetail = this.configService.get<string>(
      'LARK_TRANSFER_DETAIL_SYNC_BASE_TOKEN',
    );
    const tableIdDetail = this.configService.get<string>(
      'LARK_TRANSFER_DETAIL_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId || !baseTokenDetail || !tableIdDetail) {
      throw new Error('LarkBase transfer configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
    this.baseTokenDetail = baseTokenDetail;
    this.tableIdDetail = tableIdDetail;
  }

  async syncTransferToLarkBase(transfers: any[]): Promise<void> {}
}
