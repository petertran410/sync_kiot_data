import {
  Body,
  Controller,
  Get,
  HttpCode,
  Param,
  ParseIntPipe,
  Post,
  Query,
  Res,
  UploadedFiles,
  UseGuards,
  UseInterceptors,
} from '@nestjs/common';
import { FileFieldsInterceptor } from '@nestjs/platform-express';
import { ConfigService } from '@nestjs/config';
import { Response } from 'express';
import {
  DELIVERY_REPORT_COOKIE,
  DeliveryReportAuthGuard,
} from './delivery-report-auth';
import { DeliveryReportService } from './delivery-report.service';

@Controller('packing')
export class DeliveryReportController {
  constructor(
    private readonly service: DeliveryReportService,
    private readonly config: ConfigService,
  ) {}

  @Post('auth')
  @HttpCode(200)
  login(
    @Body() body: { password?: string },
    @Res({ passthrough: true }) response: Response,
  ) {
    const token = this.service.login(body?.password ?? '');
    const secure = this.config.get<string>('NODE_ENV') === 'production';
    response.setHeader(
      'Set-Cookie',
      [
        `${DELIVERY_REPORT_COOKIE}=${encodeURIComponent(token)}`,
        'Path=/',
        'HttpOnly',
        'SameSite=Lax',
        `Max-Age=${7 * 24 * 60 * 60}`,
        secure ? 'Secure' : '',
      ]
        .filter(Boolean)
        .join('; '),
    );
    return { success: true, expiresInDays: 7 };
  }

  @Post('logout')
  @HttpCode(200)
  logout(@Res({ passthrough: true }) response: Response) {
    response.setHeader(
      'Set-Cookie',
      `${DELIVERY_REPORT_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`,
    );
    return { success: true };
  }

  @Get('invoices')
  @UseGuards(DeliveryReportAuthGuard)
  searchInvoices(@Query('search') search = '') {
    return this.service.searchInvoices(search);
  }

  @Post('delivery-reports')
  @UseGuards(DeliveryReportAuthGuard)
  @UseInterceptors(
    FileFieldsInterceptor(
      [{ name: 'images', maxCount: 10 }],
      {
        limits: { files: 10, fileSize: 10 * 1024 * 1024 },
        fileFilter: (_request, file, callback) => {
          if (!file.mimetype.startsWith('image/')) {
            callback(new Error('Only image files are allowed'), false);
            return;
          }
          callback(null, true);
        },
      },
    ),
  )
  create(
    @Body() body: any,
    @UploadedFiles() files: Record<string, any[]>,
  ) {
    return this.service.create(body, files?.images ?? []);
  }

  @Get('images/:id')
  @UseGuards(DeliveryReportAuthGuard)
  async image(
    @Param('id', ParseIntPipe) id: number,
    @Res() response: Response,
  ) {
    const image = await this.service.getImage(id);
    response.setHeader('Content-Type', image.mimeType);
    response.setHeader(
      'Content-Disposition',
      `inline; filename="${image.fileName.replace(/"/g, '')}"`,
    );
    response.send(image.buffer);
  }

  @Post('delivery-reports/:id/retry-webhook')
  @UseGuards(DeliveryReportAuthGuard)
  retryWebhook(@Param('id', ParseIntPipe) id: number) {
    return this.service.retryWebhook(id);
  }
}
