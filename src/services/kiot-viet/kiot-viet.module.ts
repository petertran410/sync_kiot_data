import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../prisma/prisma.module';
import { KiotVietAuthService } from './auth.service';
import { KiotVietCustomerService } from './customer/customer.service';
import { KiotVietBranchService } from './branch/branch.service';

@Module({
  imports: [
    HttpModule.register({
      timeout: 60000,
      maxRedirects: 5,
    }),
    ConfigModule,
    PrismaModule,
  ],
  providers: [
    KiotVietAuthService,
    KiotVietCustomerService,
    KiotVietBranchService,
  ],
  exports: [
    KiotVietAuthService,
    KiotVietCustomerService,
    KiotVietBranchService,
  ],
})
export class KiotVietModule {}
