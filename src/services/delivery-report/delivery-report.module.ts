import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { PrismaModule } from '../../prisma/prisma.module';
import { DeliveryReportController } from './delivery-report.controller';
import { DeliveryReportAuthGuard } from './delivery-report-auth';
import { DeliveryReportService } from './delivery-report.service';

@Module({
  imports: [HttpModule, PrismaModule],
  controllers: [DeliveryReportController],
  providers: [DeliveryReportService, DeliveryReportAuthGuard],
  exports: [DeliveryReportService],
})
export class DeliveryReportModule {}
