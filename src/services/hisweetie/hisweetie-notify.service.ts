import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

interface HisweetieTarget {
  url: string;
  apiKey: string;
  label: string;
}

@Injectable()
export class HisweetieNotifyService {
  private readonly logger = new Logger(HisweetieNotifyService.name);
  private readonly targets: HisweetieTarget[];

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    const targets: HisweetieTarget[] = [];

    const prodUrl = this.configService.get<string>('HISWEETIE_API_URL', '');
    if (prodUrl) {
      targets.push({
        url: prodUrl,
        apiKey: this.configService.get<string>('HISWEETIE_API_KEY', ''),
        label: 'production',
      });
    }

    const sandboxUrl = this.configService.get<string>(
      'HISWEETIE_SANDBOX_API_URL',
      '',
    );
    if (sandboxUrl) {
      targets.push({
        url: sandboxUrl,
        apiKey: this.configService.get<string>('HISWEETIE_SANDBOX_API_KEY', ''),
        label: 'sandbox',
      });
    }

    this.targets = targets;

    if (this.targets.length > 0) {
      this.targets.forEach((t) =>
        this.logger.log(`✅ HiSweetie notify enabled [${t.label}] → ${t.url}`),
      );
    } else {
      this.logger.warn(
        '⚠️ HiSweetie notify disabled (no HISWEETIE_API_URL configured)',
      );
    }
  }

  async notify(
    entityType: string,
    code: string,
    action: string = 'upsert',
  ): Promise<void> {
    // disabled: dừng hẳn việc gọi notify tới hisweetie (route /api/sync-kiot/webhook đã bỏ)
    return;

    // eslint-disable-next-line no-unreachable
    if (this.targets.length === 0) return;

    await Promise.allSettled(
      this.targets.map(async (target) => {
        try {
          await firstValueFrom(
            this.httpService.post(
              `${target.url}/api/sync-kiot/webhook`,
              { entityType, code, action },
              {
                headers: {
                  'Content-Type': 'application/json',
                  ...(target.apiKey ? { 'x-api-key': target.apiKey } : {}),
                },
                timeout: 10000,
              },
            ),
          );
          this.logger.log(
            `📤 Notified hisweetie [${target.label}]: ${entityType}/${code} (${action})`,
          );
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to notify hisweetie [${target.label}]: ${entityType}/${code} - ${error.message}`,
          );
        }
      }),
    );
  }
}
