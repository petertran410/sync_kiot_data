import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { of } from 'rxjs';
import { PrismaService } from '../../prisma/prisma.service';
import {
  DeliveryReportPaymentMethod,
  DeliveryReportWebhookStatus,
  Prisma,
} from '@prisma/client';
import { verifyDeliveryReportImage } from './delivery-report-auth';
import { DeliveryReportService } from './delivery-report.service';

describe('DeliveryReportService webhook images', () => {
  type StatusUpdate = {
    data: {
      webhookStatus: DeliveryReportWebhookStatus;
      webhookLastError?: string;
    };
  };

  function setup(imageIds: number[], publicBaseUrl = 'https://example.com') {
    const webhookPayload = { eventId: '6', deliveryReportCode: 'BD000006' };
    const report = {
      id: 6,
      code: 'BD000006',
      webhookStatus: DeliveryReportWebhookStatus.PENDING,
      webhookAttempts: 0,
      webhookPayload,
      images: imageIds.map((id) => ({
        id,
        fileName: `image-${id}.webp`,
      })),
      invoices: [],
    };
    const prisma = {
      deliveryReport: {
        findUnique: jest.fn().mockResolvedValue(report),
        findMany: jest.fn().mockResolvedValue([{ id: 6 }]),
        update: jest
          .fn<Promise<typeof report>, [StatusUpdate]>()
          .mockResolvedValue(report),
      },
    };
    const http = { post: jest.fn().mockReturnValue(of({ data: {} })) };
    const values: Record<string, string> = {
      PACKING_FORM_TOKEN_SECRET: 'test-secret',
      PACKING_WEBHOOK_URL: 'https://n8n.example.com/webhook/packing',
      WEBHOOK_PUBLIC_BASE_URL: publicBaseUrl,
    };
    const config = {
      get: (key: string) => values[key],
    };
    const service = new DeliveryReportService(
      prisma as unknown as PrismaService,
      http as unknown as HttpService,
      config as ConfigService,
    );
    return { service, http, prisma, webhookPayload };
  }

  it('sends signed image URLs without changing the saved webhook payload', async () => {
    const { service, http, prisma, webhookPayload } = setup([7, 8]);

    await service.retryWebhook(6);

    expect(http.post).toHaveBeenCalledTimes(1);
    const [endpoint, body] = http.post.mock.calls[0] as [
      string,
      { eventId: string; imageUrls: string[] },
    ];
    expect(endpoint).toBe('https://n8n.example.com/webhook/packing');
    expect(body.eventId).toBe('6');
    expect(body.imageUrls).toHaveLength(2);
    for (const [index, rawUrl] of body.imageUrls.entries()) {
      const url = new URL(rawUrl);
      const id = index + 7;
      expect(url.origin).toBe('https://example.com');
      expect(url.pathname).toBe(`/packing/shared-images/${id}`);
      expect(
        verifyDeliveryReportImage(
          id,
          url.searchParams.get('expires') ?? '',
          url.searchParams.get('signature') ?? '',
          'test-secret',
        ),
      ).toBe(true);
    }
    expect(webhookPayload).not.toHaveProperty('imageUrls');
    const sentUpdate = prisma.deliveryReport.update.mock.calls.at(-1)?.[0];
    expect(sentUpdate?.data.webhookStatus).toBe(
      DeliveryReportWebhookStatus.SENT,
    );
  });

  it('sends an empty image list when a report has no images', async () => {
    const { service, http } = setup([], '');

    await service.retryWebhook(6);

    const [, body] = http.post.mock.calls[0] as [
      string,
      { imageUrls: string[] },
    ];
    expect(body.imageUrls).toEqual([]);
  });

  it('does not send an image webhook without a public image URL', async () => {
    const { service, http, prisma } = setup([7], '');

    await service.retryWebhook(6);

    expect(http.post).not.toHaveBeenCalled();
    const failedUpdate = prisma.deliveryReport.update.mock.calls.at(-1)?.[0];
    expect(failedUpdate?.data.webhookStatus).toBe(
      DeliveryReportWebhookStatus.FAILED,
    );
    expect(failedUpdate?.data.webhookLastError).toContain(
      'WEBHOOK_PUBLIC_BASE_URL',
    );
  });

  it('picks up a persisted pending report on the retry sweep', async () => {
    const { service, http, prisma } = setup([7]);

    await service.retryPendingReports();

    expect(prisma.deliveryReport.findMany).toHaveBeenCalledTimes(1);
    expect(http.post).toHaveBeenCalledTimes(1);
  });
});

describe('DeliveryReportService creation', () => {
  it('returns the saved report before webhook delivery finishes', async () => {
    const invoice = { id: 101, code: 'HD000101', customerName: 'Customer' };
    const createdAt = new Date('2026-09-28T10:00:00.000Z');
    const created = {
      id: 6,
      code: 'TMP-temporary',
      packageCount: 2,
      paymentMethod: DeliveryReportPaymentMethod.CASH,
      cashAmount: new Prisma.Decimal(1234567),
      note: null,
      createdAt,
      webhookStatus: DeliveryReportWebhookStatus.PENDING,
      webhookAttempts: 0,
      webhookLastError: null,
      webhookSentAt: null,
      images: [],
    };
    const saved = { ...created, code: 'BD000006' };
    const transaction = {
      deliveryReport: {
        create: jest.fn().mockResolvedValue(created),
        update: jest.fn().mockResolvedValue(saved),
      },
    };
    const prisma = {
      invoice: { findMany: jest.fn().mockResolvedValue([invoice]) },
      $transaction: jest.fn(
        async (callback: (tx: typeof transaction) => Promise<typeof saved>) =>
          callback(transaction),
      ),
      deliveryReport: { findUnique: jest.fn() },
    };
    const service = new DeliveryReportService(
      prisma as unknown as PrismaService,
      {} as HttpService,
      { get: () => undefined } as unknown as ConfigService,
    );
    const pendingDelivery = new Promise<void>(() => undefined);
    const dispatch = jest
      .spyOn(
        service as unknown as { sendWebhook: (id: number) => Promise<void> },
        'sendWebhook',
      )
      .mockReturnValue(pendingDelivery);

    const response = await service.create(
      {
        invoiceIds: [101],
        packageCount: 2,
        paymentMethod: DeliveryReportPaymentMethod.CASH,
        cashAmount: '1234567',
        note: '',
      },
      [],
    );

    expect(response).toEqual(
      expect.objectContaining({
        id: 6,
        code: 'BD000006',
        cashAmount: 1234567,
        webhookStatus: DeliveryReportWebhookStatus.PENDING,
        invoices: [{ invoiceId: 101, invoiceCode: 'HD000101' }],
        images: [],
      }),
    );
    expect(prisma.deliveryReport.findUnique).not.toHaveBeenCalled();
    expect(dispatch).not.toHaveBeenCalled();
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(dispatch).toHaveBeenCalledWith(6);
  });
});
