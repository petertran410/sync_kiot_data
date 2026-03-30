import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import {
  MeInvoiceTokenRequestDto,
  MeInvoiceTokenResponseDto,
  MeInvoiceCachedTokenDto,
} from './dto';

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
    if (!this.cachedToken) {
      return false;
    }

    const now = new Date();
    const bufferMs = 60 * 60 * 1000; // 1 giờ buffer
    const expiresAt = new Date(this.cachedToken.expiresAt.getTime() - bufferMs);

    return now < expiresAt;
  }

  private async requestNewToken(): Promise<string> {
    const baseUrl = this.configService.get<string>('MEINVOICE_BASE_URL');
    const url = `${baseUrl}/auth/token`;

    const requestBody: MeInvoiceTokenRequestDto = {
      appid: this.configService.get<string>('MEINVOICE_APP_ID') || '',
      taxcode: this.configService.get<string>('MEINVOICE_TAX_CODE') || '',
      username: this.configService.get<string>('MEINVOICE_USERNAME') || '',
      password: this.configService.get<string>('MEINVOICE_PASSWORD') || '',
    };

    this.logger.log('🔐 Requesting new MeInvoice access token...');

    try {
      const response = await firstValueFrom(
        this.httpService.post<MeInvoiceTokenResponseDto>(url, requestBody, {
          headers: { 'Content-Type': 'application/json' },
        }),
      );

      const data = response.data;

      if (!data.Success || !data.Data) {
        const errorMsg = `MeInvoice auth failed: ${data.ErrorCode}`;
        this.logger.error(`❌ ${errorMsg}`);
        throw new Error(errorMsg);
      }

      // Token hạn 15 ngày
      const expiresAt = new Date();
      expiresAt.setDate(expiresAt.getDate() + 15);

      this.cachedToken = {
        token: data.Data,
        expiresAt: expiresAt,
      };

      this.logger.log(
        `✅ MeInvoice access token obtained, expires at: ${expiresAt.toISOString()}`,
      );

      return this.cachedToken.token;
    } catch (error) {
      if (error.response) {
        this.logger.error(
          `❌ MeInvoice API error: ${JSON.stringify(error.response.data)}`,
        );
      }
      this.logger.error(
        `❌ Failed to get MeInvoice access token: ${error.message}`,
      );
      throw error;
    }
  }

  async refreshToken(): Promise<string> {
    if (!this.cachedToken) {
      return this.requestNewToken();
    }

    const baseUrl = this.configService.get<string>('MEINVOICE_BASE_URL');
    const url = `${baseUrl}/auth/refreshtoken`;

    this.logger.log('🔄 Refreshing MeInvoice token...');

    try {
      const response = await firstValueFrom(
        this.httpService.post<MeInvoiceTokenResponseDto>(
          url,
          this.cachedToken.token,
          {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${this.cachedToken.token}`,
              CompanyTaxCode:
                this.configService.get<string>('MEINVOICE_TAX_CODE') || '',
            },
          },
        ),
      );

      const data = response.data;

      if (!data.Success || !data.Data) {
        this.logger.warn('⚠️ Refresh failed, requesting new token...');
        this.cachedToken = null;
        return this.requestNewToken();
      }

      const expiresAt = new Date();
      expiresAt.setDate(expiresAt.getDate() + 15);

      this.cachedToken = {
        token: data.Data,
        expiresAt: expiresAt,
      };

      this.logger.log('✅ MeInvoice token refreshed');

      return this.cachedToken.token;
    } catch (error) {
      this.logger.warn(
        `⚠️ Refresh token error: ${error.message}, requesting new token...`,
      );
      this.cachedToken = null;
      return this.requestNewToken();
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
