// src/services/lark/auth/lark-auth.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';

interface TenantAccessTokenResponse {
  code: number;
  msg: string;
  tenant_access_token: string;
  expire: number;
}

@Injectable()
export class LarkAuthService {
  private readonly logger = new Logger(LarkAuthService.name);

  // Separate tokens for different services
  private customerAccessToken: string | null = null;
  private customerTokenExpiry: Date | null = null;

  private invoiceAccessToken: string | null = null;
  private invoiceTokenExpiry: Date | null = null;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  // ============================================================================
  // CUSTOMER TOKEN MANAGEMENT
  // ============================================================================

  async getCustomerHeaders(): Promise<Record<string, string>> {
    const token = await this.getCustomerAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getCustomerAccessToken(): Promise<string> {
    // Check if token is still valid (with 5 minute buffer)
    if (
      this.customerAccessToken &&
      this.customerTokenExpiry &&
      this.customerTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.customerAccessToken;
    }

    // Get new token
    return await this.refreshCustomerToken();
  }

  private async refreshCustomerToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase customer access token...');

      const appId = this.configService.get<string>('LARK_CUSTOMER_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_CUSTOMER_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase customer credentials not configured');
      }

      const url =
        'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal';

      const response = await firstValueFrom(
        this.httpService.post<TenantAccessTokenResponse>(
          url,
          {
            app_id: appId,
            app_secret: appSecret,
          },
          {
            headers: {
              'Content-Type': 'application/json',
            },
            timeout: 10000,
          },
        ),
      );

      if (response.data.code !== 0) {
        throw new Error(
          `LarkBase auth failed: ${response.data.msg} (code: ${response.data.code})`,
        );
      }

      this.customerAccessToken = response.data.tenant_access_token;
      // Token expires in seconds, convert to milliseconds
      this.customerTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase customer token refreshed successfully');
      this.logger.debug(
        `Token expires at: ${this.customerTokenExpiry.toISOString()}`,
      );

      return this.customerAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh customer token: ${error.message}`,
      );

      // Reset tokens on error
      this.customerAccessToken = null;
      this.customerTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  // Force refresh (for error recovery)
  async forceRefreshCustomerToken(): Promise<void> {
    this.customerAccessToken = null;
    this.customerTokenExpiry = null;
    await this.refreshCustomerToken();
  }

  // ============================================================================
  // INVOICE TOKEN MANAGEMENT
  // ============================================================================

  async getInvoiceHeaders(): Promise<Record<string, string>> {
    const token = await this.getInvoiceAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getInvoiceAccessToken(): Promise<string> {
    // Check if token is still valid (with 5 minute buffer)
    if (
      this.invoiceAccessToken &&
      this.invoiceTokenExpiry &&
      this.invoiceTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.invoiceAccessToken;
    }

    // Get new token
    return await this.refreshInvoiceToken();
  }

  private async refreshInvoiceToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase invoice access token...');

      const appId = this.configService.get<string>('LARK_INVOICE_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_INVOICE_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase invoice credentials not configured');
      }

      const url =
        'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal';

      const response = await firstValueFrom(
        this.httpService.post<TenantAccessTokenResponse>(
          url,
          {
            app_id: appId,
            app_secret: appSecret,
          },
          {
            headers: {
              'Content-Type': 'application/json',
            },
            timeout: 10000,
          },
        ),
      );

      if (response.data.code !== 0) {
        throw new Error(
          `LarkBase auth failed: ${response.data.msg} (code: ${response.data.code})`,
        );
      }

      this.invoiceAccessToken = response.data.tenant_access_token;
      this.invoiceTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase invoice token refreshed successfully');
      this.logger.debug(
        `Token expires at: ${this.invoiceTokenExpiry.toISOString()}`,
      );

      return this.invoiceAccessToken;
    } catch (error) {
      this.logger.error(`❌ Failed to refresh invoice token: ${error.message}`);

      // Reset tokens on error
      this.invoiceAccessToken = null;
      this.invoiceTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  // Force refresh (for error recovery)
  async forceRefreshInvoiceToken(): Promise<void> {
    this.invoiceAccessToken = null;
    this.invoiceTokenExpiry = null;
    await this.refreshInvoiceToken();
  }
}
