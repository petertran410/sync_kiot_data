import { Controller, Post, Param, Logger, HttpCode } from '@nestjs/common';
import { MeInvoiceInvoiceService } from '../services/meinvoice/meinvoice-invoice.service';

@Controller('meinvoice')
export class MeInvoiceController {
  private readonly logger = new Logger(MeInvoiceController.name);

  constructor(
    private readonly meInvoiceInvoiceService: MeInvoiceInvoiceService,
  ) {}

  /**
   * Tạo hóa đơn nháp trên MeInvoice từ Invoice Code
   * URL: POST /meinvoice/create/:invoiceCode
   */
  @Post('create/:invoiceCode')
  @HttpCode(200)
  async createDraftInvoice(@Param('invoiceCode') invoiceCode: string): Promise<{
    success: boolean;
    refId: string | null;
    transactionId: string | null;
    invNo: string | null;
    message: string;
  }> {
    this.logger.log(
      `📄 Manual create MeInvoice draft for invoice code: ${invoiceCode}`,
    );

    try {
      const result =
        await this.meInvoiceInvoiceService.createDraftInvoice(invoiceCode);

      return result;
    } catch (error) {
      this.logger.error(
        `❌ Create MeInvoice draft failed for invoice ${invoiceCode}: ${error.message}`,
      );
      return {
        success: false,
        refId: null,
        transactionId: null,
        invNo: null,
        message: error.message,
      };
    }
  }
}
