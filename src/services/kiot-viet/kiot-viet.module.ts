import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../prisma/prisma.module';
import { KiotVietAuthService } from '../kiot-viet/auth.service';
import { KiotVietCustomerService } from '../kiot-viet/customer/customer.service';

@Module({
  imports: [HttpModule, ConfigModule, PrismaModule],
  providers: [KiotVietAuthService, KiotVietCustomerService],
  exports: [KiotVietAuthService, KiotVietCustomerService],
})
export class KiotVietModule {}
