import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

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
export class LarkReturnSyncService {
  private readonly logger = new Logger(LarkReturnSyncService.name);

  private readonly baseToken: string;
  private readonly tableId: string;

  private readonly baseTokenDetail: string;
  private readonly tableIdDetail: string;

  private readonly batchSize = 100;

  private existingRecordsCache = new Map<number, string>();
  private returnCodeCache = new Map<string, string>();

  private existingDetailRecordsCache = new Map<number, string>();
  private;
}
