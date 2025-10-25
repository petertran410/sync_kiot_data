import {
  Controller,
  Post,
  Body,
  HttpCode,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { WebhookService } from '../services/webhook/webhook.service';

@Controller('webhook')
export class WebhookController {
  private readonly logger = new Logger(WebhookController.name);

  constructor(private readonly webhookService: WebhookService) {}

  @Post('order')
  @HttpCode(HttpStatus.OK)
  async handleOrderWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet order webhook');
      await this.webhookService.processOrderWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ Webhook error: ${error.message}`);
      return { success: true };
    }
  }
}
