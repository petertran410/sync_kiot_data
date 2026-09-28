import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import { join } from 'path';

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    // rawBody exposes req.rawBody (Buffer) so the webhook signature guard can
    // compute HMAC-SHA-256 over the exact bytes KiotViet sent.
    rawBody: true,
  });
  app.enableShutdownHooks();
  app
    .getHttpAdapter()
    .getInstance()
    .use(
      '/packing',
      express.static(join(process.cwd(), 'public', 'packing'), {
        index: 'index.html',
        setHeaders: (response, filePath) => {
          if (filePath.endsWith('index.html') || filePath.endsWith('app.js')) {
            response.setHeader('Cache-Control', 'no-store');
          }
        },
      }),
    );
  await app.listen(process.env.PORT ?? 8083);
}
bootstrap();
