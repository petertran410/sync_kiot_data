import { HttpService } from '@nestjs/axios';
import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import {
  DeliveryReportPaymentMethod,
  DeliveryReportWebhookStatus,
  Prisma,
} from '@prisma/client';
import { mkdir, readFile, unlink, writeFile } from 'fs/promises';
import { randomUUID } from 'crypto';
import { basename, extname, join } from 'path';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../prisma/prisma.service';
import {
  assertDeliveryReportPassword,
  createDeliveryReportToken,
} from './delivery-report-auth';

type UploadedImage = {
  buffer: Buffer;
  mimetype: string;
  originalname: string;
  size: number;
};

type DeliveryReportInput = {
  invoiceIds: unknown;
  packageCount: unknown;
  paymentMethod: unknown;
  cashAmount: unknown;
  note: unknown;
};

type StoredImage = {
  fileName: string;
  relativePath: string;
  mimeType: string;
  fileSize: number;
};

const MAX_INVOICES = 100;
const MAX_IMAGES = 10;
const MAX_IMAGE_SIZE = 10 * 1024 * 1024;
const MAX_WEBHOOK_ATTEMPTS = 8;

@Injectable()
export class DeliveryReportService {
  private readonly logger = new Logger(DeliveryReportService.name);
  private readonly uploadDir: string;
  private readonly formPassword: string;
  private readonly tokenSecret: string;
  private readonly webhookUrl: string;
  private readonly webhookTimeoutMs: number;
  private readonly sending = new Set<number>();

  constructor(
    private readonly prisma: PrismaService,
    private readonly http: HttpService,
    config: ConfigService,
  ) {
    this.formPassword = config.get<string>('PACKING_FORM_PASSWORD') ?? '';
    this.tokenSecret = config.get<string>('PACKING_FORM_TOKEN_SECRET') ?? '';
    this.uploadDir =
      config.get<string>('PACKING_UPLOADS_DIR') ?? '/app/uploads/packing';
    this.webhookUrl = config.get<string>('PACKING_WEBHOOK_URL') ?? '';
    this.webhookTimeoutMs = Number(
      config.get<string>('PACKING_WEBHOOK_TIMEOUT_MS') ?? 10000,
    );
  }

  login(password: string): string {
    assertDeliveryReportPassword(password, this.formPassword);

    if (!this.tokenSecret) {
      throw new BadRequestException(
        'PACKING_FORM_TOKEN_SECRET is not configured',
      );
    }
    return createDeliveryReportToken(this.tokenSecret);
  }

  async searchInvoices(search: string) {
    const normalized = search.trim();
    if (!normalized) return [];

    const invoices = await this.prisma.invoice.findMany({
      where: {
        saleChannelId: 1,
        code: { contains: normalized, mode: 'insensitive' },
      },
      orderBy: [{ purchaseDate: 'desc' }, { id: 'desc' }],
      take: 30,
      select: {
        id: true,
        code: true,
        customerName: true,
        soldById: true,
        soldByName: true,
        soldBy: {
          select: {
            kiotVietId: true,
            givenName: true,
            userName: true,
          },
        },
        purchaseDate: true,
        total: true,
        totalPayment: true,
        saleChannelId: true,
      },
    });

    return invoices.map((invoice) => this.invoiceSummary(invoice));
  }

  async create(
    input: DeliveryReportInput,
    files: UploadedImage[],
  ): Promise<Record<string, unknown>> {
    const invoiceIds = this.parseInvoiceIds(input.invoiceIds);
    const packageCount = this.parsePositiveInteger(
      input.packageCount,
      'packageCount',
    );
    const paymentMethod = this.parsePaymentMethod(input.paymentMethod);
    const cashAmount = this.parseCashAmount(
      input.cashAmount,
      paymentMethod === DeliveryReportPaymentMethod.CASH,
    );
    const note = this.parseNote(input.note);

    if (files.length > MAX_IMAGES) {
      throw new BadRequestException(`Maximum ${MAX_IMAGES} images are allowed`);
    }
    for (const file of files) {
      if (!file.mimetype.startsWith('image/')) {
        throw new BadRequestException('Only image files are allowed');
      }
      if (file.size > MAX_IMAGE_SIZE) {
        throw new BadRequestException('Each image must be 10 MB or smaller');
      }
    }

    const invoices = await this.prisma.invoice.findMany({
      where: { id: { in: invoiceIds }, saleChannelId: 1 },
      select: {
        id: true,
        code: true,
        customerName: true,
        soldById: true,
        soldByName: true,
        soldBy: {
          select: {
            kiotVietId: true,
            givenName: true,
            userName: true,
          },
        },
      },
    });
    if (invoices.length !== invoiceIds.length) {
      throw new BadRequestException(
        'One or more selected invoices are missing or are not saleChannelId=1',
      );
    }

    const storedImages = await this.storeImages(files);
    let report: any;
    try {
      report = await this.prisma.$transaction(async (tx) => {
        const created = await tx.deliveryReport.create({
          data: {
            code: `TMP-${randomUUID()}`,
            packageCount,
            paymentMethod,
            cashAmount,
            note,
            invoices: {
              create: invoiceIds.map((invoiceId) => ({ invoiceId })),
            },
            images: {
              create: storedImages,
            },
          },
          include: { invoices: true, images: true },
        });

        const code = `BD${created.id.toString().padStart(6, '0')}`;
        const payload = this.buildPayload({
          id: created.id,
          code,
          createdAt: created.createdAt,
          packageCount,
          paymentMethod,
          cashAmount,
          note,
          invoices,
        });

        return tx.deliveryReport.update({
          where: { id: created.id },
          data: {
            code,
            webhookPayload: payload as Prisma.InputJsonValue,
          },
          include: { images: true },
        });
      });

    } catch (error) {
      await this.removeStoredImages(storedImages);
      throw error;
    }

    await this.sendWebhook(report.id);
    const finalReport = await this.getReport(report.id);
    return this.serializeReport(finalReport);
  }

  async retryWebhook(id: number) {
    await this.getReport(id);
    await this.sendWebhook(id);
    return this.serializeReport(await this.getReport(id));
  }

  async getImage(id: number): Promise<{
    buffer: Buffer;
    mimeType: string;
    fileName: string;
  }> {
    const image = await this.prisma.deliveryReportImage.findUnique({
      where: { id },
    });
    if (!image) throw new NotFoundException('Delivery report image not found');

    const filePath = join(this.uploadDir, image.relativePath);
    try {
      return {
        buffer: await readFile(filePath),
        mimeType: image.mimeType,
        fileName: image.fileName,
      };
    } catch {
      throw new NotFoundException('Delivery report image file not found');
    }
  }

  async retryPendingReports(): Promise<void> {
    const reports = await this.prisma.deliveryReport.findMany({
      where: {
        webhookStatus: {
          in: [
            DeliveryReportWebhookStatus.PENDING,
            DeliveryReportWebhookStatus.FAILED,
          ],
        },
        webhookAttempts: { lt: MAX_WEBHOOK_ATTEMPTS },
      },
      orderBy: { updatedAt: 'asc' },
      take: 20,
      select: { id: true },
    });

    for (const report of reports) {
      await this.sendWebhook(report.id);
    }
  }

  @Cron('*/5 * * * *', { name: 'delivery-report-webhook-retry' })
  async scheduledWebhookRetry(): Promise<void> {
    await this.retryPendingReports();
  }

  private async sendWebhook(id: number): Promise<void> {
    if (this.sending.has(id)) return;
    this.sending.add(id);

    try {
      const report = await this.getReport(id);
      if (report.webhookStatus === DeliveryReportWebhookStatus.SENT) return;

      const attempt = report.webhookAttempts + 1;
      await this.prisma.deliveryReport.update({
        where: { id },
        data: {
          webhookStatus: DeliveryReportWebhookStatus.PENDING,
          webhookAttempts: attempt,
        },
      });

      if (!this.webhookUrl) {
        throw new Error('PACKING_WEBHOOK_URL is not configured');
      }

      await firstValueFrom(
        this.http.post(this.webhookUrl, report.webhookPayload, {
          timeout: this.webhookTimeoutMs,
          headers: {
            'Content-Type': 'application/json',
            'X-Idempotency-Key': String(id),
          },
        }),
      );

      await this.prisma.deliveryReport.update({
        where: { id },
        data: {
          webhookStatus: DeliveryReportWebhookStatus.SENT,
          webhookLastError: null,
          webhookSentAt: new Date(),
        },
      });
    } catch (error: any) {
      const message = String(error?.message ?? error).slice(0, 2000);
      this.logger.error(`Delivery report webhook ${id} failed: ${message}`);
      await this.prisma.deliveryReport.update({
        where: { id },
        data: {
          webhookStatus: DeliveryReportWebhookStatus.FAILED,
          webhookLastError: message,
        },
      });
    } finally {
      this.sending.delete(id);
    }
  }

  private async getReport(id: number) {
    const report = await this.prisma.deliveryReport.findUnique({
      where: { id },
      include: {
        invoices: {
          include: {
            invoice: {
              select: {
                id: true,
                code: true,
                customerName: true,
                soldById: true,
                soldByName: true,
                soldBy: {
                  select: {
                    kiotVietId: true,
                    givenName: true,
                    userName: true,
                  },
                },
              },
            },
          },
        },
        images: true,
      },
    });
    if (!report) {
      throw new NotFoundException('Delivery report not found');
    }
    return report;
  }

  private serializeReport(report: any): Record<string, unknown> {
    return {
      id: report.id,
      code: report.code,
      packageCount: report.packageCount,
      paymentMethod: report.paymentMethod,
      cashAmount: report.cashAmount == null ? null : Number(report.cashAmount),
      note: report.note,
      createdAt: report.createdAt,
      webhookStatus: report.webhookStatus,
      webhookAttempts: report.webhookAttempts,
      webhookLastError: report.webhookLastError,
      webhookSentAt: report.webhookSentAt,
      invoices: report.invoices?.map((item: any) => ({
        invoiceId: item.invoice.id,
        invoiceCode: item.invoice.code,
      })),
      images: report.images?.map((image: any) => ({
        id: image.id,
        url: `/packing/images/${image.id}`,
        fileName: image.fileName,
      })),
    };
  }

  private buildPayload(input: {
    id: number;
    code: string;
    createdAt: Date;
    packageCount: number;
    paymentMethod: DeliveryReportPaymentMethod;
    cashAmount: Prisma.Decimal | null;
    note: string | null;
    invoices: any[];
  }) {
    return {
      eventId: String(input.id),
      deliveryReportCode: input.code,
      createdAt: input.createdAt.toISOString(),
      packageCount: input.packageCount,
      paymentMethod: input.paymentMethod,
      ...(input.paymentMethod === DeliveryReportPaymentMethod.CASH
        ? { cashAmount: input.cashAmount == null ? 0 : Number(input.cashAmount) }
        : {}),
      note: input.note,
      invoices: input.invoices.map((invoice) => ({
        invoiceId: invoice.id,
        invoiceCode: invoice.code,
        customerName: invoice.customerName ?? null,
        soldById: invoice.soldById == null ? null : String(invoice.soldById),
        sellerName:
          invoice.soldBy?.givenName?.trim() ||
          invoice.soldBy?.userName?.trim() ||
          invoice.soldByName ||
          null,
      })),
    };
  }

  private invoiceSummary(invoice: any) {
    return {
      id: invoice.id,
      code: invoice.code,
      customerName: invoice.customerName ?? null,
      soldById: invoice.soldById == null ? null : String(invoice.soldById),
      sellerName:
        invoice.soldBy?.givenName?.trim() ||
        invoice.soldBy?.userName?.trim() ||
        invoice.soldByName ||
        null,
      purchaseDate: invoice.purchaseDate,
      total: Number(invoice.total),
      totalPayment: Number(invoice.totalPayment),
      saleChannelId: invoice.saleChannelId,
    };
  }

  private parseInvoiceIds(value: unknown): number[] {
    let raw: unknown = value;
    if (typeof raw === 'string') {
      const text = raw;
      try {
        raw = JSON.parse(text);
      } catch {
        raw = text.split(',').map((item) => item.trim());
      }
    }
    if (!Array.isArray(raw)) {
      throw new BadRequestException('invoiceIds must be an array');
    }

    const ids = [...new Set(raw.map((item) => Number(item)))];
    if (
      ids.length === 0 ||
      ids.length > MAX_INVOICES ||
      ids.some((id) => !Number.isSafeInteger(id) || id <= 0)
    ) {
      throw new BadRequestException(
        `invoiceIds must contain 1-${MAX_INVOICES} valid ids`,
      );
    }
    return ids;
  }

  private parsePositiveInteger(value: unknown, field: string): number {
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      throw new BadRequestException(`${field} must be a positive integer`);
    }
    return parsed;
  }

  private parsePaymentMethod(value: unknown): DeliveryReportPaymentMethod {
    if (
      value !== DeliveryReportPaymentMethod.CASH &&
      value !== DeliveryReportPaymentMethod.TRANSFER
    ) {
      throw new BadRequestException('paymentMethod must be CASH or TRANSFER');
    }
    return value;
  }

  private parseCashAmount(
    value: unknown,
    required: boolean,
  ): Prisma.Decimal | null {
    if (!required && (value == null || value === '')) return null;
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) {
      throw new BadRequestException(
        'cashAmount must be a non-negative number when payment is CASH',
      );
    }
    if (!required && parsed !== 0) {
      throw new BadRequestException(
        'cashAmount must be empty for TRANSFER payments',
      );
    }
    return new Prisma.Decimal(parsed);
  }

  private parseNote(value: unknown): string | null {
    if (value == null) return null;
    const note = String(value).trim();
    return note ? note.slice(0, 5000) : null;
  }

  private async storeImages(files: UploadedImage[]): Promise<StoredImage[]> {
    if (!files.length) return [];
    await mkdir(this.uploadDir, { recursive: true });
    const stored: StoredImage[] = [];

    try {
      for (const file of files) {
        const extension = this.imageExtension(file.mimetype, file.originalname);
        const fileName = `${randomUUID()}${extension}`;
        await writeFile(join(this.uploadDir, fileName), file.buffer, {
          flag: 'wx',
        });
        stored.push({
          fileName: basename(file.originalname).slice(0, 255),
          relativePath: fileName,
          mimeType: file.mimetype,
          fileSize: file.size,
        });
      }
      return stored;
    } catch (error) {
      await this.removeStoredImages(stored);
      throw error;
    }
  }

  private async removeStoredImages(files: StoredImage[]): Promise<void> {
    await Promise.all(
      files.map((file) =>
        unlink(join(this.uploadDir, file.relativePath)).catch(() => undefined),
      ),
    );
  }

  private imageExtension(mimeType: string, originalName: string): string {
    const known: Record<string, string> = {
      'image/jpeg': '.jpg',
      'image/png': '.png',
      'image/webp': '.webp',
      'image/gif': '.gif',
      'image/heic': '.heic',
      'image/heif': '.heif',
    };
    return known[mimeType] ?? extname(originalName).toLowerCase().slice(0, 10);
  }
}
