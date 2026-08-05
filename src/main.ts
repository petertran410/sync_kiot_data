import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

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
  await app.listen(process.env.PORT ?? 8083);
}
bootstrap();
