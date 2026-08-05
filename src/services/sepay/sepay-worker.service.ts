import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../prisma/prisma.service';
import { SePayPaymentService } from './sepay-payment.service';
import { extractDocumentCode, SEPAY_STATUS } from './sepay.types';

interface ClaimedTransaction {
  id: number;
  accountNumber: string;
  subAccount: string | null;
  transferType: string;
  transferAmount: unknown;
  content: string;
}

@Injectable()
export class SePayWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SePayWorkerService.name);
  private readonly enabled: boolean;
  private readonly pollMs: number;
  private timer?: NodeJS.Timeout;
  private stopped = false;
  private running = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly paymentService: SePayPaymentService,
    config: ConfigService,
  ) {
    this.enabled =
      String(config.get('SEPAY_WORKER_ENABLED') ?? 'true') !== 'false';
    this.pollMs = Number(config.get('SEPAY_WORKER_POLL_MS') ?? 2000);
  }

  onModuleInit(): void {
    if (!this.enabled) {
      this.logger.warn(
        'SEPAY_WORKER_ENABLED=false - SePay transactions will remain unprocessed',
      );
      return;
    }
    this.logger.log(`SePay worker started (poll=${this.pollMs}ms)`);
    this.schedule(0);
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }

  private schedule(delay: number): void {
    if (!this.stopped) this.timer = setTimeout(() => void this.tick(), delay);
  }

  private async tick(): Promise<void> {
    if (this.stopped || this.running) return;
    this.running = true;
    let processed = false;
    try {
      await this.reclaimStale();
      const transaction = await this.claim();
      if (transaction) {
        processed = true;
        await this.process(transaction);
      }
    } catch (error: any) {
      this.logger.error(`SePay worker failed: ${error?.message}`);
    } finally {
      this.running = false;
      this.schedule(processed ? 0 : this.pollMs);
    }
  }

  private async reclaimStale(): Promise<void> {
    const cutoff = new Date(Date.now() - 5 * 60 * 1000);
    await this.prisma.sePayTransaction.updateMany({
      where: {
        status: SEPAY_STATUS.Processing,
        startedAt: { lt: cutoff },
        writeStartedAt: null,
      },
      data: {
        status: SEPAY_STATUS.Received,
        startedAt: null,
        errorMessage: 'Requeued after worker restart before KiotViet write',
      },
    });
    await this.prisma.sePayTransaction.updateMany({
      where: {
        status: SEPAY_STATUS.Processing,
        startedAt: { lt: cutoff },
        writeStartedAt: { not: null },
      },
      data: {
        status: SEPAY_STATUS.Failed,
        processedAt: new Date(),
        errorMessage:
          'Worker stopped after starting the KiotViet write; verify manually before retrying',
      },
    });
  }

  private async claim(): Promise<ClaimedTransaction | null> {
    await this.prisma.ensureConnected();
    const rows = await this.prisma.$queryRaw<any[]>`
      WITH due AS (
        SELECT "id"
        FROM "SePayTransaction"
        WHERE "status" = ${SEPAY_STATUS.Received}
          AND "availableAt" <= NOW()
        ORDER BY "receivedAt" ASC, "id" ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE "SePayTransaction" t
      SET "status" = ${SEPAY_STATUS.Processing},
          "startedAt" = NOW(),
          "attempts" = t."attempts" + 1
      FROM due
      WHERE t."id" = due."id"
      RETURNING t."id", t."accountNumber", t."subAccount",
                t."transferType", t."transferAmount", t."content"
    `;
    return rows[0] ?? null;
  }

  private async process(transaction: ClaimedTransaction): Promise<void> {
    try {
      if (transaction.transferType.toLowerCase() !== 'in') {
        await this.finish(transaction.id, SEPAY_STATUS.Ignored, {
          errorMessage: 'Outgoing transaction',
        });
        return;
      }

      const parsedDocument = extractDocumentCode(transaction.content);
      if (!parsedDocument) {
        await this.finish(transaction.id, SEPAY_STATUS.Ignored, {
          errorMessage: 'No DH/HD document code in transfer content',
        });
        return;
      }
      const document = {
        ...parsedDocument,
        code: await this.resolveDocumentCode(
          parsedDocument.type,
          parsedDocument.code,
        ),
      };

      await this.prisma.sePayTransaction.update({
        where: { id: transaction.id },
        data: { documentType: document.type, documentCode: document.code },
      });

      const beforeWrite = async () => {
        await this.prisma.sePayTransaction.update({
          where: { id: transaction.id },
          data: { writeStartedAt: new Date() },
        });
      };
      const afterWrite = async (result: {
        documentId: bigint;
        customerId: bigint | null;
        branchId: number;
        accountId: number;
        paymentId?: bigint;
        paymentCode?: string;
      }) => {
        await this.prisma.sePayTransaction.update({
          where: { id: transaction.id },
          data: {
            kiotDocumentId: result.documentId,
            kiotCustomerId: result.customerId,
            kiotBranchId: result.branchId,
            kiotAccountId: result.accountId,
            kiotPaymentId: result.paymentId ?? null,
            kiotPaymentCode: result.paymentCode ?? null,
          },
        });
      };
      const amount = Number(transaction.transferAmount);
      const result =
        document.type === 'INVOICE'
          ? await this.paymentService.processInvoice(
              document.code,
              amount,
              transaction.accountNumber,
              transaction.subAccount,
              beforeWrite,
              afterWrite,
            )
          : await this.paymentService.processOrder(
              document.code,
              amount,
              transaction.accountNumber,
              transaction.subAccount,
              beforeWrite,
              afterWrite,
            );

      await this.finish(
        transaction.id,
        result.dryRun ? SEPAY_STATUS.DryRun : SEPAY_STATUS.Processed,
        {
          kiotDocumentId: result.documentId,
          kiotCustomerId: result.customerId,
          kiotBranchId: result.branchId,
          kiotAccountId: result.accountId,
          kiotPaymentId: result.paymentId ?? null,
          kiotPaymentCode: result.paymentCode ?? null,
          errorMessage: null,
        },
      );
      this.logger.log(
        `SePay transaction #${transaction.id} ${document.code} ${result.dryRun ? 'dry-run' : 'processed'}`,
      );
    } catch (error: any) {
      await this.finish(transaction.id, SEPAY_STATUS.Failed, {
        errorMessage: String(error?.message ?? error).slice(0, 1000),
      });
      this.logger.warn(
        `SePay transaction #${transaction.id} failed: ${error?.message}`,
      );
    }
  }

  private async finish(
    id: number,
    status: string,
    data: Record<string, any>,
  ): Promise<void> {
    await this.prisma.sePayTransaction.update({
      where: { id },
      data: { ...data, status, processedAt: new Date() },
    });
  }

  private async resolveDocumentCode(
    type: 'ORDER' | 'INVOICE',
    rawCode: string,
  ): Promise<string> {
    const normalized = this.normalizeDocumentCode(rawCode);
    const rows =
      type === 'INVOICE'
        ? await this.prisma.$queryRaw<Array<{ code: string }>>`
            SELECT "code"
            FROM "Invoice"
            WHERE UPPER(REGEXP_REPLACE("code", '[^A-Za-z0-9]', '', 'g')) = ${normalized}
            LIMIT 2
          `
        : await this.prisma.$queryRaw<Array<{ code: string }>>`
            SELECT "code"
            FROM "Order"
            WHERE UPPER(REGEXP_REPLACE("code", '[^A-Za-z0-9]', '', 'g')) = ${normalized}
            LIMIT 2
          `;

    if (rows.length === 0) return rawCode;
    if (rows.length > 1) {
      throw new Error(
        `Ambiguous ${type.toLowerCase()} code ${rawCode}: ${rows
          .map((row) => row.code)
          .join(', ')}`,
      );
    }
    return rows[0].code;
  }

  private normalizeDocumentCode(code: string): string {
    return code.replace(/[^A-Za-z0-9]/g, '').toUpperCase();
  }

  async retryFailed(id: number): Promise<boolean> {
    const result = await this.prisma.sePayTransaction.updateMany({
      where: { id, status: SEPAY_STATUS.Failed },
      data: {
        status: SEPAY_STATUS.Received,
        errorMessage: null,
        availableAt: new Date(),
        startedAt: null,
        writeStartedAt: null,
        processedAt: null,
      },
    });
    return result.count === 1;
  }
}
