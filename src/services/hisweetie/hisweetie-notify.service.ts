import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class HisweetieNotifyService {
  private readonly logger = new Logger(HisweetieNotifyService.name);
  private readonly hisweetieUrl: string;
  private readonly apiKey: string;
  private readonly enabled: boolean;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.hisweetieUrl = this.configService.get<string>('HISWEETIE_API_URL', '');
    this.apiKey = this.configService.get<string>('HISWEETIE_API_KEY', '');
    this.enabled = !!this.hisweetieUrl;

    if (this.enabled) {
      this.logger.log(`✅ HiSweetie notify enabled → ${this.hisweetieUrl}`);
    } else {
      this.logger.warn(
        '⚠️ HiSweetie notify disabled (HISWEETIE_API_URL not set)',
      );
    }
  }

  /**
   * Fire-and-forget: gửi notify sang hisweetie, không throw nếu fail
   */
  async notify(
    entityType: string,
    code: string,
    action: string = 'upsert',
  ): Promise<void> {
    if (!this.enabled) return;

    try {
      await firstValueFrom(
        this.httpService.post(
          `${this.hisweetieUrl}/sync-kiot/webhook`,
          { entityType, code, action },
          {
            headers: {
              'Content-Type': 'application/json',
              ...(this.apiKey ? { 'x-api-key': this.apiKey } : {}),
            },
            timeout: 10000,
          },
        ),
      );

      this.logger.log(
        `📤 Notified hisweetie: ${entityType}/${code} (${action})`,
      );
    } catch (error) {
      // Fire-and-forget: log warning, không throw
      this.logger.warn(
        `⚠️ Failed to notify hisweetie: ${entityType}/${code} - ${error.message}`,
      );
    }
  }
}
