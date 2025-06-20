import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from 'src/prisma/prisma.service';
import { KiotVietAuthService } from '../auth.service';

// Create src/services/kiot-viet/cashflow/cashflow.service.ts
@Injectable()
export class KiotVietCashflowService {
  private readonly logger = new Logger(KiotVietCashflowService.name);
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

  async fetchCashflow(params: {
    lastModifiedFrom?: string;
    currentItem?: number;
    pageSize?: number;
  }) {
    try {
      const headers = await this.authService.getRequestHeaders();
      const { data } = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/cashflow`, {
          headers,
          params: {
            ...params,
            includeAccount: true,
            includeBranch: true,
            includeUser: true,
            orderBy: 'transDate',
            orderDirection: 'DESC',
          },
        }),
      );
      return data;
    } catch (error) {
      this.logger.error(`Failed to fetch cashflow: ${error.message}`);
      throw error;
    }
  }

  async syncCashflow(): Promise<void> {
    // Implementation similar to other sync methods
    // Will sync all cashflow records (receipts and disbursements)
  }
}
