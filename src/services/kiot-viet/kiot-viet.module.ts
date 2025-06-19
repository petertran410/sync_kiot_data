import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietAuthService } from './auth.service';
import { KiotVietCustomerService } from './customer/customer.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 60000,
      maxRedirects: 5,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [KiotVietAuthService, KiotVietCustomerService],
  exports: [KiotVietAuthService, KiotVietCustomerService],
})
export class KiotVietModule {}
