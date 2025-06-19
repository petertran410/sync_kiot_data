import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class KiotVietAuthService {
  private readonly logger = new Logger(KiotVietAuthService.name);
  private accessToken: string;
  private tokenExpiry: Date;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  async getAccessToken(): Promise<string> {
    if (
      this.accessToken &&
      this.tokenExpiry &&
      this.tokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.accessToken;
    }

    try {
      const clientId = this.configService.get<string>('KIOT_CLIEND_ID');
      const clientSecret = this.configService.get<string>('KIOT_SECRET_KEY');
      const tokenUrl = this.configService.get<string>('KIOT_TOKEN');

      if (!clientId || !clientSecret || !tokenUrl) {
        throw new Error('Missing KiotViet API credentials in configuration');
      }

      const params = new URLSearchParams();
      params.append('scopes', 'PublicApi.Access');
      params.append('grant_type', 'client_credentials');
      params.append('client_id', clientId);
      params.append('client_secret', clientSecret);

      const { data } = await firstValueFrom(
        this.httpService.post(tokenUrl, params, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
        }),
      );

      this.accessToken = data.access_token;
      this.tokenExpiry = new Date(Date.now() + data.expires_in * 1000);
      this.logger.log('KiotViet access token obtained successfully');

      return this.accessToken;
    } catch (error) {
      this.logger.error(
        `Failed to get KiotViet access token: ${error.message}`,
      );
      throw error;
    }
  }

  async getRequestHeaders() {
    const shopName = this.configService.get<string>('KIOT_SHOP_NAME');

    if (!shopName) {
      throw new Error('Missing KiotViet shop name in configuration');
    }

    const token = await this.getAccessToken();

    return {
      Retailer: shopName,
      Authorization: `Bearer ${token}`,
    };
  }
}
