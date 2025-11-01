import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { NumberFormatInterceptor } from './interceptors/number-format.interceptor';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalInterceptors(new NumberFormatInterceptor());
  await app.listen(process.env.PORT ?? 8083);
}
bootstrap();
