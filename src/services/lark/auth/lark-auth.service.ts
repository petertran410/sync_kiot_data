// src/services/lark/auth/lark-auth.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class LarkAuthService {
  private readonly logger = new Logger(LarkAuthService.name);
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  async getAccessToken(appId: string, appSecret: string): Promise<string> {
    try {
      // Check if current token is still valid
      if (
        this.accessToken &&
        this.tokenExpiry &&
        new Date() < this.tokenExpiry
      ) {
        return this.accessToken;
      }

      // Get new token
      const response = await firstValueFrom(
        this.httpService.post(
          'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal',
          {
            app_id: appId,
            app_secret: appSecret,
          },
          {
            headers: {
              'Content-Type': 'application/json',
            },
          },
        ),
      );

      if (response.data.code === 0) {
        this.accessToken = response.data.tenant_access_token;
        this.tokenExpiry = new Date(Date.now() + 110 * 60 * 1000);

        if (!this.accessToken) {
          throw new Error('Access token is null after successful response');
        }

        return this.accessToken;
      } else {
        throw new Error(`Failed to get access token: ${response.data.msg}`);
      }
    } catch (error) {
      this.logger.error(`LarkBase auth failed: ${error.message}`);
      throw error;
    }
  }

  async getCustomerHeaders(): Promise<Record<string, string>> {
    const appId = this.configService.get<string>('LARK_CUSTOMER_SYNC_APP_ID');
    const appSecret = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_APP_SECRET',
    );

    if (!appId || !appSecret) {
      throw new Error('LarkBase customer credentials not configured');
    }

    const accessToken = await this.getAccessToken(appId, appSecret);

    return {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    };
  }

  async getInvoiceHeaders(): Promise<Record<string, string>> {
    const appId = this.configService.get<string>('LARK_INVOICE_SYNC_APP_ID');
    const appSecret = this.configService.get<string>(
      'LARK_INVOICE_SYNC_APP_SECRET',
    );

    if (!appId || !appSecret) {
      throw new Error('LarkBase invoice credentials not configured');
    }

    const accessToken = await this.getAccessToken(appId, appSecret);

    return {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    };
  }
}
