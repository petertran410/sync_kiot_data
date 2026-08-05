import { Body, Controller, Post, UseGuards } from '@nestjs/common';
import { SePayIngestService } from './sepay-ingest.service';
import { SePaySignatureGuard } from './sepay-signature.guard';
import { SePayWebhookPayload } from './sepay.types';

@Controller('webhook/sepay')
export class SePayController {
  constructor(private readonly ingestService: SePayIngestService) {}

  @Post()
  @UseGuards(SePaySignatureGuard)
  async receive(@Body() payload: SePayWebhookPayload) {
    await this.ingestService.ingest(payload);
    return { success: true };
  }
}
