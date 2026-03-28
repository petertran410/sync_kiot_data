import { Controller, Post, Body, Logger, HttpCode } from '@nestjs/common';
import { MisaVoucherService } from '../services/misa/misa-voucher.service';
import { MisaCallbackRequestDto } from '../services/misa/dto';

@Controller('misa')
export class MisaCallbackController {
  private readonly logger = new Logger(MisaCallbackController.name);

  constructor(private readonly misaVoucherService: MisaVoucherService) {}

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
  @Post('health')
  @HttpCode(200)
  healthCheck(): { status: string } {
    return { status: 'ok' };
  }
}
