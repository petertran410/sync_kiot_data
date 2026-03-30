import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { MeInvoiceCachedTokenDto } from './dto';

@Injectable()
export class MeInvoiceAuthService {
  private readonly logger = new Logger(MeInvoiceAuthService.name);
  private cachedToken: MeInvoiceCachedTokenDto | null = null;

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
  ) {}

  async getAccessToken(): Promise<string> {
    if (this.isTokenValid()) {
      return this.cachedToken!.token;
    }

    return this.requestNewToken();
  }

  private isTokenValid(): boolean {
    if (!this.cachedToken) return false;

    const now = new Date();
    const bufferMs = 5 * 60 * 1000; // 5 phút buffer
    return now < new Date(this.cachedToken.expiresAt.getTime() - bufferMs);
  }

  private async requestNewToken(): Promise<string> {
    const baseUrl = this.configService.get<string>('MEINVOICE_WEBAPP_BASE_URL');
    const taxCode = this.configService.get<string>('MEINVOICE_TAX_CODE') || '';
    const username = this.configService.get<string>('MEINVOICE_USERNAME') || '';
    const password = this.configService.get<string>('MEINVOICE_PASSWORD') || '';

    const url = `${baseUrl}/oauth`;
    const body = `grant_type=password&username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}`;

    this.logger.log('🔐 Requesting MeInvoice Web API v2 token...');

    try {
      const response = await firstValueFrom(
        this.httpService.post(url, body, {
          headers: {
            'Content-Type': 'text/plain',
            taxcode: taxCode,
          },
        }),
      );

      const data = response.data;

      if (!data?.access_token) {
        throw new Error('MeInvoice OAuth response missing access_token');
      }

      const expiresInMs = (data.expires_in || 86400) * 1000;
      const expiresAt = new Date(Date.now() + expiresInMs);

      this.cachedToken = {
        token: data.access_token,
        expiresAt,
      };

      this.logger.log(
        `✅ MeInvoice token obtained, expires at: ${expiresAt.toISOString()}`,
      );

      return this.cachedToken.token;
    } catch (error) {
      if (error.response) {
        this.logger.error(
          `❌ MeInvoice OAuth error: ${JSON.stringify(error.response.data)}`,
        );
      }
      this.logger.error(`❌ Failed to get MeInvoice token: ${error.message}`);
      throw error;
    }
  }

  clearToken(): void {
    this.cachedToken = null;
    this.logger.log('🗑️ MeInvoice cached token cleared');
  }

  getTokenInfo(): { hasToken: boolean; expiresAt: Date | null } {
    return {
      hasToken: !!this.cachedToken,
      expiresAt: this.cachedToken?.expiresAt || null,
    };
  }
}
