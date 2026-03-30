import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { MeInvoiceAuthService } from './meinvoice-auth.service';
import { MeInvoiceInvoiceService } from './meinvoice-invoice.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 60000,
      maxRedirects: 5,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [MeInvoiceAuthService, MeInvoiceInvoiceService],
  exports: [MeInvoiceAuthService, MeInvoiceInvoiceService],
})
export class MeInvoiceModule {}
