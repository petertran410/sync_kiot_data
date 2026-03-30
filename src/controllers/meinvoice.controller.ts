import { Controller, Post, Get, Param, Logger, HttpCode } from '@nestjs/common';
import { MeInvoiceInvoiceService } from '../services/meinvoice/meinvoice-invoice.service';

@Controller('meinvoice')
export class MeInvoiceController {
  private readonly logger = new Logger(MeInvoiceController.name);

  constructor(
    private readonly meInvoiceInvoiceService: MeInvoiceInvoiceService,
  ) {}

  /**
   * Đẩy hóa đơn nháp lên MeInvoice Web
   * POST /meinvoice/push/:invoiceCode
   */
  @Post('push/:invoiceCode')
  @HttpCode(200)
  async pushDraftInvoice(@Param('invoiceCode') invoiceCode: string): Promise<{
    success: boolean;
    refId: string | null;
    message: string;
  }> {
    this.logger.log(`📄 Push draft invoice: ${invoiceCode}`);

    try {
      return await this.meInvoiceInvoiceService.pushDraftInvoice(invoiceCode);
    } catch (error) {
      return { success: false, refId: null, message: error.message };
    }
  }

  /**
   * Query hóa đơn trên MeInvoice theo RefID
   * GET /meinvoice/invoice/:refId
   */
  @Get('invoice/:refId')
  async getInvoice(@Param('refId') refId: string): Promise<{
    success: boolean;
    data: any;
    message: string;
  }> {
    this.logger.log(`🔍 Query MeInvoice invoice: ${refId}`);

    try {
      return await this.meInvoiceInvoiceService.getInvoiceByRefId(refId);
    } catch (error) {
      return { success: false, data: null, message: error.message };
    }
  }
}
