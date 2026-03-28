import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  Logger,
  HttpCode,
} from '@nestjs/common';
import { MisaVoucherService } from '../services/misa/misa-voucher.service';
import { MisaDictionaryService } from '../services/misa/misa-dictionary.service';
import { MisaCallbackRequestDto } from '../services/misa/dto';

@Controller('misa')
export class MisaCallbackController {
  private readonly logger = new Logger(MisaCallbackController.name);

  constructor(
    private readonly misaVoucherService: MisaVoucherService,
    private readonly misaDictionaryService: MisaDictionaryService,
  ) {}

  // ========================================
  // DICTIONARY SYNC ENDPOINTS
  // ========================================

  /**
   * Sync tất cả danh mục từ Misa về database
   * URL: POST /misa/dictionary/sync
   */
  @Post('dictionary/sync')
  @HttpCode(200)
  async syncAllDictionaries(): Promise<{
    success: boolean;
    message: string;
    data?: {
      inventoryItems: number;
      stocks: number;
      accountObjects: number;
      organizationUnits: number;
    };
  }> {
    this.logger.log('📦 Manual Misa dictionary sync triggered');

    try {
      const result = await this.misaDictionaryService.syncAllDictionaries();

      return {
        success: true,
        message: 'Dictionary sync completed',
        data: result,
      };
    } catch (error) {
      this.logger.error(`❌ Dictionary sync failed: ${error.message}`);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  // ========================================
  // VOUCHER ENDPOINTS
  // ========================================

  /**
   * Tạo chứng từ bán hàng Misa từ Invoice Code
   * URL: POST /misa/voucher/create/:invoiceCode
   */
  @Post('voucher/create/:invoiceCode')
  @HttpCode(200)
  async createVoucherFromInvoice(
    @Param('invoiceCode') invoiceCode: string,
  ): Promise<{
    success: boolean;
    orgRefId: string | null;
    message: string;
  }> {
    this.logger.log(
      `🧾 Manual create Misa voucher for invoice code: ${invoiceCode}`,
    );

    try {
      const result =
        await this.misaVoucherService.createSaleVoucherFromInvoice(invoiceCode);

      return result;
    } catch (error) {
      this.logger.error(
        `❌ Create voucher failed for invoice ${invoiceCode}: ${error.message}`,
      );
      return {
        success: false,
        orgRefId: null,
        message: error.message,
      };
    }
  }

  /**
   * Retry các invoice FAILED
   * URL: POST /misa/voucher/retry
   */
  @Post('voucher/retry')
  @HttpCode(200)
  async retryFailedVouchers(): Promise<{
    success: boolean;
    message: string;
    retriedCount?: number;
  }> {
    this.logger.log('🔄 Retry failed Misa vouchers triggered');

    try {
      const successCount =
        await this.misaVoucherService.retryFailedInvoices(10);

      return {
        success: true,
        message: `Retried failed invoices`,
        retriedCount: successCount,
      };
    } catch (error) {
      this.logger.error(`❌ Retry failed: ${error.message}`);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  // ========================================
  // CALLBACK ENDPOINTS
  // ========================================

  /**
   * Endpoint nhận callback từ Misa sau khi xử lý voucher
   * URL: POST /misa/callback
   */
  @Post('callback')
  @HttpCode(200)
  async handleCallback(
    @Body() body: MisaCallbackRequestDto,
  ): Promise<{ success: boolean; message: string }> {
    this.logger.log(`📩 Received Misa callback: ${JSON.stringify(body)}`);

    try {
      if (!body.data || !Array.isArray(body.data)) {
        this.logger.warn('⚠️ Invalid callback data format');
        return {
          success: false,
          message: 'Invalid data format',
        };
      }

      for (const item of body.data) {
        await this.misaVoucherService.handleMisaCallback(
          item.org_refid,
          item.status,
          item.voucher_id,
          item.voucher_no,
          item.error_code,
          item.error_message,
        );
      }

      return {
        success: true,
        message: `Processed ${body.data.length} callback(s)`,
      };
    } catch (error) {
      this.logger.error(`❌ Error processing Misa callback: ${error.message}`);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Health check endpoint cho Misa
   * URL: GET /misa/health
   */
  @Get('health')
  @HttpCode(200)
  healthCheck(): { status: string; timestamp: string } {
    return {
      status: 'ok',
      timestamp: new Date().toISOString(),
    };
  }
}
