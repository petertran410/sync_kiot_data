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

  @Post('invoice')
  @HttpCode(HttpStatus.OK)
  async handleInvoiceWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet invoice webhook');
      await this.webhookService.processInvoiceWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ Invoice webhook error: ${error.message}`);
      return { success: true };
    }
  }

  @Post('customer')
  @HttpCode(HttpStatus.OK)
  async handleCustomerWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet customer webhook');
      await this.webhookService.processCustomerWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ Customer webhook error: ${error.message}`);
      return { success: true };
    }
  }

  @Post('product')
  @HttpCode(HttpStatus.OK)
  async handleProductWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet product webhook');
      await this.webhookService.processProductWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ Product webhook error: ${error.message}`);
      return { success: true };
    }
  }

  @Post('stock')
  @HttpCode(HttpStatus.OK)
  async handleStockWebhook(@Body() webhookData: any) {
    try {
      await this.webhookService.processStockWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ Stock webhook error: ${error.message}`);
      return { success: true };
    }
  }

  @Post('pricebook')
  @HttpCode(HttpStatus.OK)
  async handlePriceBookWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet pricebook webhook');
      await this.webhookService.processPriceBookWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ PriceBook webhook error: ${error.message}`);
      return { success: true };
    }
  }

  @Post('pricebookdetail')
  @HttpCode(HttpStatus.OK)
  async handlePriceBookDetailWebhook(@Body() webhookData: any) {
    try {
      this.logger.log('📨 Received KiotViet pricebook detail webhook');
      await this.webhookService.processPriceBookDetailWebhook(webhookData);
      return { success: true };
    } catch (error) {
      this.logger.error(`❌ PriceBook detail webhook error: ${error.message}`);
      return { success: true };
    }
  }
}
