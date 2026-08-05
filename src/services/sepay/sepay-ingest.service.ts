import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import { SEPAY_STATUS, SePayWebhookPayload } from './sepay.types';

@Injectable()
export class SePayIngestService {
  private readonly logger = new Logger(SePayIngestService.name);

  constructor(private readonly prisma: PrismaService) {}

  async ingest(payload: SePayWebhookPayload): Promise<{ duplicate: boolean }> {
    this.validate(payload);
    await this.prisma.ensureConnected();

    try {
      await this.prisma.sePayTransaction.create({
        data: {
          sepayTransactionId: String(payload.id),
          gateway: payload.gateway ?? null,
          transactionDate: this.parseDate(payload.transactionDate),
          accountNumber: payload.accountNumber,
          subAccount: payload.subAccount || null,
          code: payload.code || null,
          content: payload.content,
          transferType: payload.transferType.toLowerCase(),
          transferAmount: new Prisma.Decimal(payload.transferAmount),
          accumulated:
            payload.accumulated == null
              ? null
              : new Prisma.Decimal(payload.accumulated),
          description: payload.description || null,
          referenceCode: payload.referenceCode || null,
          rawPayload: payload as any,
          status: SEPAY_STATUS.Received,
        },
      });
      this.logger.log(`Stored SePay transaction ${payload.id}`);
      return { duplicate: false };
    } catch (error: any) {
      if (error?.code === 'P2002') {
        // A reconciliation import can win the race against the live webhook.
        // Promote only historical IMPORTED rows so the live transaction is still
        // processed; replays of RECEIVED/PROCESSED rows remain no-ops.
        await this.prisma.sePayTransaction.updateMany({
          where: {
            sepayTransactionId: String(payload.id),
            status: 'IMPORTED',
          },
          data: {
            gateway: payload.gateway ?? null,
            transactionDate: this.parseDate(payload.transactionDate),
            accountNumber: payload.accountNumber,
            subAccount: payload.subAccount || null,
            code: payload.code || null,
            content: payload.content,
            transferType: payload.transferType.toLowerCase(),
            transferAmount: new Prisma.Decimal(payload.transferAmount),
            accumulated:
              payload.accumulated == null
                ? null
                : new Prisma.Decimal(payload.accumulated),
            description: payload.description || null,
            referenceCode: payload.referenceCode || null,
            rawPayload: payload as any,
            status: SEPAY_STATUS.Received,
            processedAt: null,
            availableAt: new Date(),
          },
        });
        return { duplicate: true };
      }
      throw error;
    }
  }

  private validate(payload: SePayWebhookPayload): void {
    if (
      payload?.id === null ||
      payload?.id === undefined ||
      payload?.id === ''
    ) {
      throw new BadRequestException('SePay transaction id is required');
    }
    if (
      !payload.transactionDate ||
      !payload.accountNumber ||
      !payload.content
    ) {
      throw new BadRequestException('Invalid SePay transaction payload');
    }
    if (!['in', 'out'].includes(String(payload.transferType).toLowerCase())) {
      throw new BadRequestException('Invalid SePay transferType');
    }
    if (
      !Number.isFinite(Number(payload.transferAmount)) ||
      Number(payload.transferAmount) <= 0
    ) {
      throw new BadRequestException('Invalid SePay transferAmount');
    }
  }

  private parseDate(value: string): Date {
    const date = new Date(
      value.includes('T') ? value : `${value.replace(' ', 'T')}+07:00`,
    );
    if (Number.isNaN(date.getTime())) {
      throw new BadRequestException('Invalid SePay transactionDate');
    }
    return date;
  }
}
