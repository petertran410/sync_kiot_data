import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { MeInvoiceCachedTokenDto } from './dto';

@Injectable()
export class MeInvoiceAuthService {
  private readonly logger = new Logger(MeInvoiceAuthService.name);
  private cachedToken: MeInvoiceCachedTokenDto | null = null;
  private cachedItgToken: MeInvoiceCachedTokenDto | null = null;

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
  ) {}

  /**
   * Lấy token cho ITG API v3
   */
  async getItgAccessToken(): Promise<string> {
    if (this.isItgTokenValid()) {
      return this.cachedItgToken!.token;
    }

    return this.requestItgToken();
  }

  private isItgTokenValid(): boolean {
    if (!this.cachedItgToken) return false;

    const now = new Date();
    const bufferMs = 60 * 60 * 1000; // 1 giờ buffer
    return now < new Date(this.cachedItgToken.expiresAt.getTime() - bufferMs);
  }

  private async requestItgToken(): Promise<string> {
    const baseUrl = this.configService.get<string>('MEINVOICE_ITG_BASE_URL');
    const url = `${baseUrl}/auth/token`;

    const requestBody = {
      appid: this.configService.get<string>('MEINVOICE_APP_ID') || '',
      taxcode: this.configService.get<string>('MEINVOICE_TAX_CODE') || '',
      username: this.configService.get<string>('MEINVOICE_USERNAME') || '',
      password: this.configService.get<string>('MEINVOICE_PASSWORD') || '',
    };

    this.logger.log('🔐 Requesting MeInvoice ITG v3 token...');

    try {
      const response = await firstValueFrom(
        this.httpService.post(url, requestBody, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );

      const data = response.data;

      let tokenData = data;
      if (typeof data === 'string') {
        try {
          tokenData = JSON.parse(data);
        } catch {
          /* ignore */
        }
      }

      if (!tokenData?.Success || !tokenData?.Data) {
        throw new Error(
          `ITG auth failed: ${tokenData?.ErrorCode || 'Unknown'}`,
        );
      }

      const expiresAt = new Date();
      expiresAt.setDate(expiresAt.getDate() + 15);

      this.cachedItgToken = {
        token: tokenData.Data,
        expiresAt,
      };

      this.logger.log('✅ MeInvoice ITG v3 token obtained');
      return this.cachedItgToken.token;
    } catch (error) {
      if (error.response) {
        this.logger.error(
          `❌ ITG auth error: ${JSON.stringify(error.response.data)}`,
        );
      }
      this.logger.error(`❌ Failed to get ITG token: ${error.message}`);
      throw error;
    }
  }

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
    this.logger.debug(`Request URL: ${url}`);
    this.logger.debug(`Request taxcode header: ${taxCode}`);

    try {
      const response = await firstValueFrom(
        this.httpService.post(url, body, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            taxcode: taxCode,
          },
        }),
      );

      const data = response.data;

      // Debug: log raw response để xác minh structure
      this.logger.debug(`Raw OAuth response: ${JSON.stringify(data)}`);

      // Response có thể là string JSON cần parse
      let tokenData = data;
      if (typeof data === 'string') {
        try {
          tokenData = JSON.parse(data);
        } catch {
          this.logger.error(`❌ Response is not valid JSON: ${data}`);
          throw new Error('MeInvoice OAuth response is not valid JSON');
        }
      }

      if (!tokenData?.access_token) {
        this.logger.error(
          `❌ Token data structure: ${JSON.stringify(tokenData)}`,
        );
        throw new Error('MeInvoice OAuth response missing access_token');
      }

      const expiresInMs = (tokenData.expires_in || 86400) * 1000;
      const expiresAt = new Date(Date.now() + expiresInMs);

      this.cachedToken = {
        token: tokenData.access_token,
        expiresAt,
      };

      this.logger.log(
        `✅ MeInvoice token obtained, expires at: ${expiresAt.toISOString()}`,
      );

      return this.cachedToken.token;
    } catch (error) {
      if (error.response) {
        this.logger.error(
          `❌ MeInvoice OAuth error response: ${JSON.stringify(error.response.data)}`,
        );
        this.logger.error(
          `❌ MeInvoice OAuth status: ${error.response.status}`,
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
