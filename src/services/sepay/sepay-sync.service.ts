import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { Prisma } from '@prisma/client';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../prisma/prisma.service';

interface SePayV1Transaction {
  id: string | number;
  bank_brand_name?: string;
  account_number: string;
  sub_account?: string;
  transaction_date: string;
  amount_in?: string | number;
  amount_out?: string | number;
  accumulated?: string | number;
  code?: string | null;
  transaction_content?: string;
  reference_number?: string;
  bank_account_id?: string | number;
}

interface DateRange {
  from: Date;
  to: Date;
}

export interface SePaySyncResult {
  expected: number;
  fetched: number;
  inserted: number;
  duplicates: number;
  ranges: number;
  startedAt: string;
  completedAt: string;
}

@Injectable()
export class SePaySyncService {
  private readonly logger = new Logger(SePaySyncService.name);
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly defaultFrom: Date;
  private readonly requestDelayMs = 350;
  private readonly pageLimit = 5000;
  private running = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly http: HttpService,
    config: ConfigService,
  ) {
    this.baseUrl = (config.get<string>('SEPAY_API_BASE_URL') ?? '').replace(
      /\/$/,
      '',
    );
    this.token = config.get<string>('SEPAY_API_TOKEN') ?? '';
    this.defaultFrom = this.parseDate(
      config.get<string>('SEPAY_SYNC_FROM_DATE') ?? '2000-01-01 00:00:00',
    );
  }

  async syncAll(
    from = this.defaultFrom,
    to = new Date(),
  ): Promise<SePaySyncResult> {
    if (this.running) throw new Error('A SePay full sync is already running');
    if (!this.token) throw new Error('SEPAY_API_TOKEN is not configured');
    if (from >= to)
      throw new Error('SePay sync start date must be before end date');

    this.running = true;
    const startedAt = new Date();
    try {
      await this.prisma.ensureConnected();
      const expected = await this.count(from, to);
      const ranges = await this.splitRanges({ from, to }, expected);
      let fetched = 0;
      let inserted = 0;

      this.logger.log(
        `SePay full sync: ${expected} transaction(s), ${ranges.length} range(s)`,
      );
      for (const [index, range] of ranges.entries()) {
        const transactions = await this.list(range.from, range.to);
        fetched += transactions.length;
        inserted += await this.insert(transactions);
        this.logger.log(
          `SePay range ${index + 1}/${ranges.length}: fetched ${transactions.length}, inserted ${inserted}/${fetched}`,
        );
      }

      const completedAt = new Date();
      return {
        expected,
        fetched,
        inserted,
        duplicates: fetched - inserted,
        ranges: ranges.length,
        startedAt: startedAt.toISOString(),
        completedAt: completedAt.toISOString(),
      };
    } finally {
      this.running = false;
    }
  }

  async stats(): Promise<Record<string, number>> {
    const grouped = await this.prisma.sePayTransaction.groupBy({
      by: ['status'],
      _count: { _all: true },
    });
    return Object.fromEntries(
      grouped.map((item) => [item.status, item._count._all]),
    );
  }

  private async splitRanges(
    range: DateRange,
    knownCount?: number,
  ): Promise<DateRange[]> {
    const count = knownCount ?? (await this.count(range.from, range.to));
    if (count === 0) return [];
    if (count <= this.pageLimit) return [range];

    const fromMs = range.from.getTime();
    const toMs = range.to.getTime();
    if (toMs - fromMs <= 1000) {
      throw new Error(
        `SePay has more than ${this.pageLimit} transactions in one second`,
      );
    }
    const midpoint = new Date(Math.floor((fromMs + toMs) / 2000) * 1000);
    const left = { from: range.from, to: midpoint };
    const right = { from: new Date(midpoint.getTime() + 1000), to: range.to };
    // Resolve sequentially to stay below SePay's 3 requests/second limit.
    const leftRanges = await this.splitRanges(left);
    const rightRanges = await this.splitRanges(right);
    return [...leftRanges, ...rightRanges];
  }

  private async count(from: Date, to: Date): Promise<number> {
    const data = await this.get('/transactions/count', {
      transaction_date_min: this.formatDate(from),
      transaction_date_max: this.formatDate(to),
    });
    const count = Number(data?.count_transactions ?? 0);
    if (!Number.isFinite(count) || count < 0) {
      throw new Error('Invalid SePay transaction count response');
    }
    return count;
  }

  private async list(from: Date, to: Date): Promise<SePayV1Transaction[]> {
    const data = await this.get('/transactions/list', {
      transaction_date_min: this.formatDate(from),
      transaction_date_max: this.formatDate(to),
      limit: this.pageLimit,
    });
    if (!Array.isArray(data?.transactions)) {
      throw new Error('Invalid SePay transaction list response');
    }
    return data.transactions;
  }

  private async insert(transactions: SePayV1Transaction[]): Promise<number> {
    let inserted = 0;
    for (let start = 0; start < transactions.length; start += 500) {
      const batch = transactions.slice(start, start + 500);
      const result = await this.prisma.sePayTransaction.createMany({
        data: batch.map((transaction) => this.toRow(transaction)),
        skipDuplicates: true,
      });
      inserted += result.count;
    }
    return inserted;
  }

  private toRow(transaction: SePayV1Transaction) {
    const amountIn = Number(transaction.amount_in ?? 0);
    const amountOut = Number(transaction.amount_out ?? 0);
    const incoming = amountIn > 0;
    const amount = incoming ? amountIn : amountOut;
    if (!transaction.id || !transaction.account_number || amount <= 0) {
      throw new Error(
        `Invalid SePay transaction ${transaction.id ?? 'without id'}`,
      );
    }

    return {
      sepayTransactionId: String(transaction.id),
      gateway: transaction.bank_brand_name ?? null,
      transactionDate: this.parseDate(transaction.transaction_date),
      accountNumber: transaction.account_number,
      subAccount: transaction.sub_account || null,
      code: transaction.code || null,
      content: transaction.transaction_content ?? '',
      transferType: incoming ? 'in' : 'out',
      transferAmount: new Prisma.Decimal(amount),
      accumulated:
        transaction.accumulated == null
          ? null
          : new Prisma.Decimal(transaction.accumulated),
      description: null,
      referenceCode: transaction.reference_number || null,
      rawPayload: transaction as any,
      status: 'IMPORTED',
      processedAt: new Date(),
    };
  }

  private async get(
    path: string,
    params: Record<string, unknown>,
  ): Promise<any> {
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        const response = await firstValueFrom(
          this.http.get(`${this.baseUrl}${path}`, {
            params,
            headers: {
              Authorization: `Bearer ${this.token}`,
              'Content-Type': 'application/json',
            },
            timeout: 30000,
          }),
        );
        await this.sleep(this.requestDelayMs);
        if (response.data?.error) {
          throw new Error(String(response.data.error));
        }
        return response.data;
      } catch (error: any) {
        const status = error?.response?.status;
        if (attempt === 4 || (status && status !== 429 && status < 500)) {
          throw error;
        }
        const retrySeconds = Number(
          error?.response?.headers?.['x-sepay-userapi-retry-after'] ?? attempt,
        );
        await this.sleep(Math.max(retrySeconds, 1) * 1000);
      }
    }
  }

  private parseDate(value: string): Date {
    const date = new Date(
      value.includes('T') ? value : `${value.replace(' ', 'T')}+07:00`,
    );
    if (Number.isNaN(date.getTime())) throw new Error(`Invalid date: ${value}`);
    return date;
  }

  private formatDate(value: Date): string {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Ho_Chi_Minh',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).formatToParts(value);
    const part = (type: string) =>
      parts.find((item) => item.type === type)?.value ?? '';
    return `${part('year')}-${part('month')}-${part('day')} ${part('hour')}:${part('minute')}:${part('second')}`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
