import { INestApplication } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { Server } from 'http';
import * as request from 'supertest';
import {
  DeliveryReportAuthGuard,
  signDeliveryReportImage,
  signSharedDeliveryReportImage,
} from './delivery-report-auth';
import { DeliveryReportController } from './delivery-report.controller';
import { DeliveryReportService } from './delivery-report.service';

describe('DeliveryReportController shared image', () => {
  let app: INestApplication;
  const getImage = jest.fn();

  beforeAll(async () => {
    const module = await Test.createTestingModule({
      controllers: [DeliveryReportController],
      providers: [
        DeliveryReportAuthGuard,
        { provide: DeliveryReportService, useValue: { getImage } },
        {
          provide: ConfigService,
          useValue: {
            get: (key: string) =>
              key === 'PACKING_FORM_TOKEN_SECRET' ? 'test-secret' : undefined,
          },
        },
      ],
    }).compile();
    app = module.createNestApplication();
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(() => {
    getImage.mockReset();
    getImage.mockResolvedValue({
      buffer: Buffer.from('image-bytes'),
      mimeType: 'image/webp',
      fileName: 'image.webp',
      relativePath: 'stored-image.webp',
    });
  });

  it('serves a valid signed URL without a form cookie', async () => {
    const expires = Math.floor(Date.now() / 1000) + 300;
    const signature = signDeliveryReportImage(42, expires, 'test-secret');

    const response = await request(app.getHttpServer() as Server)
      .get(
        `/packing/shared-images/42?expires=${expires}&signature=${signature}`,
      )
      .expect(200);

    expect(response.headers['content-type']).toMatch(/^image\/webp/);
    expect(response.headers['cache-control']).toBe('private, no-store');
    expect(response.body).toEqual(Buffer.from('image-bytes'));
    expect(getImage).toHaveBeenCalledWith(42);
  });

  it('serves a stable signed image URL and rejects another filename', async () => {
    const signature = signSharedDeliveryReportImage(
      42,
      'stored-image.webp',
      'test-secret',
    );

    const response = await request(app.getHttpServer() as Server)
      .get(`/packing/shared-images/42/stored-image.webp?signature=${signature}`)
      .expect(200);
    expect(response.headers['content-type']).toMatch(/^image\/webp/);

    await request(app.getHttpServer() as Server)
      .get(`/packing/shared-images/42/wrong.jpg?signature=${signature}`)
      .expect(401);
  });

  it('keeps previously issued expiring image URLs valid until their expiry', async () => {
    const expires = Math.floor(Date.now() / 1000) + 300;
    const signature = signDeliveryReportImage(42, expires, 'test-secret');

    await request(app.getHttpServer() as Server)
      .get(
        `/packing/shared-images/42/stored-image.webp?expires=${expires}&signature=${signature}`,
      )
      .expect(200);
    await request(app.getHttpServer() as Server)
      .get(
        `/packing/shared-images/42/wrong.jpg?expires=${expires}&signature=${signature}`,
      )
      .expect(404);
  });

  it('rejects unsigned and expired URLs while keeping the original route private', async () => {
    const expired = Math.floor(Date.now() / 1000) - 1;
    const signature = signDeliveryReportImage(42, expired, 'test-secret');

    await request(app.getHttpServer() as Server)
      .get('/packing/shared-images/42')
      .expect(401);
    await request(app.getHttpServer() as Server)
      .get(
        `/packing/shared-images/42?expires=${expired}&signature=${signature}`,
      )
      .expect(401);
    await request(app.getHttpServer() as Server)
      .get('/packing/images/42')
      .expect(401);
    expect(getImage).not.toHaveBeenCalled();
  });
});
