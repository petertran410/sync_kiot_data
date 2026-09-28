import {
  Body,
  Controller,
  Get,
  HttpCode,
  Param,
  ParseIntPipe,
  Post,
  Query,
  Req,
  Res,
  UnauthorizedException,
  UploadedFiles,
  UseGuards,
  UseInterceptors,
} from '@nestjs/common';
import { FileFieldsInterceptor } from '@nestjs/platform-express';
import { ConfigService } from '@nestjs/config';
import { Request, Response } from 'express';
import {
  DELIVERY_REPORT_COOKIE,
  DeliveryReportAuthGuard,
  verifyDeliveryReportImage,
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
    @Req() request: Request,
    @Res({ passthrough: true }) response: Response,
  ) {
    const token = this.service.login(body?.password ?? '');
    const forwardedProto = request.headers['x-forwarded-proto'];
    const secure =
      this.config.get<string>('NODE_ENV') === 'production' ||
      request.protocol === 'https' ||
      (typeof forwardedProto === 'string' &&
        forwardedProto.split(',')[0].trim() === 'https');
    response.cookie(DELIVERY_REPORT_COOKIE, token, {
      httpOnly: true,
      sameSite: 'lax',
      secure,
      maxAge: 7 * 24 * 60 * 60 * 1000,
      path: '/',
    });
    response.setHeader('Cache-Control', 'no-store');
    return { success: true, expiresInDays: 7 };
  }

  @Post('logout')
  @HttpCode(200)
  logout(@Res({ passthrough: true }) response: Response) {
    response.clearCookie(DELIVERY_REPORT_COOKIE, {
      httpOnly: true,
      sameSite: 'lax',
      path: '/',
    });
    response.setHeader('Cache-Control', 'no-store');
    return { success: true };
  }

  @Get('session')
  @UseGuards(DeliveryReportAuthGuard)
  session() {
    return { authenticated: true, expiresInDays: 7 };
  }

  @Get('invoices')
  @UseGuards(DeliveryReportAuthGuard)
  searchInvoices(@Query('search') search = '') {
    return this.service.searchInvoices(search);
  }

  @Post('delivery-reports')
  @UseGuards(DeliveryReportAuthGuard)
  @UseInterceptors(
    FileFieldsInterceptor([{ name: 'images', maxCount: 10 }], {
      limits: { files: 10, fileSize: 10 * 1024 * 1024 },
      fileFilter: (_request, file, callback) => {
        if (!file.mimetype.startsWith('image/')) {
          callback(new Error('Only image files are allowed'), false);
          return;
        }
        callback(null, true);
      },
    }),
  )
  create(@Body() body: any, @UploadedFiles() files: Record<string, any[]>) {
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

  @Get('shared-images/:id')
  async sharedImage(
    @Param('id', ParseIntPipe) id: number,
    @Query('expires') expires: string,
    @Query('signature') signature: string,
    @Res() response: Response,
  ) {
    const secret = this.config.get<string>('PACKING_FORM_TOKEN_SECRET') ?? '';
    if (!verifyDeliveryReportImage(id, expires, signature, secret)) {
      throw new UnauthorizedException('Invalid or expired image URL');
    }

    const image = await this.service.getImage(id);
    response.setHeader('Content-Type', image.mimeType);
    response.setHeader('Content-Disposition', 'inline');
    response.setHeader('Cache-Control', 'private, no-store');
    response.setHeader('X-Content-Type-Options', 'nosniff');
    response.send(image.buffer);
  }

  @Post('delivery-reports/:id/retry-webhook')
  @UseGuards(DeliveryReportAuthGuard)
  retryWebhook(@Param('id', ParseIntPipe) id: number) {
    return this.service.retryWebhook(id);
  }
}
