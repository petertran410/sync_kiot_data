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

  private customerAccessToken: string | null = null;
  private customerTokenExpiry: Date | null = null;

  private invoiceAccessToken: string | null = null;
  private invoiceTokenExpiry: Date | null = null;

  private orderAccessToken: string | null = null;
  private orderTokenExpiry: Date | null = null;

  private productAccessToken: string | null = null;
  private productTokenExpiry: Date | null = null;

  private supplierAccessToken: string | null = null;
  private supplierTokenExpiry: Date | null = null;

  private orderSupplierAccessToken: string | null = null;
  private orderSupplierTokenExpiry: Date | null = null;

  private orderSupplierDetailAccessToken: string | null = null;
  private orderSupplierDetailTokenExpiry: Date | null = null;

  private purchaseOrderAccessToken: string | null = null;
  private purchaseOrderTokenExpiry: Date | null = null;

  private purchaseOrderDetailAccessToken: string | null = null;
  private purchaseOrderDetailTokenExpiry: Date | null = null;

  private demandAccessToken: string | null = null;
  private demandTokenExpiry: Date | null = null;

  private cashflowAccessToken: string | null = null;
  private cashflowTokenExpiry: Date | null = null;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {}

  async getAccessToken(
    service:
      | 'customer'
      | 'invoice'
      | 'order'
      | 'product'
      | 'supplier'
      | 'orderSupplier'
      | 'orderSupplierDetail'
      | 'purchaseOrder'
      | 'purchaseOrderDetail'
      | 'demand'
      | 'cashflow',
  ): Promise<string> {
    switch (service) {
      case 'customer':
        return await this.getCustomerAccessToken();
      case 'invoice':
        return await this.getInvoiceAccessToken();
      case 'order':
        return await this.getOrderAccessToken();
      case 'product':
        return await this.getProductAccessToken();
      case 'supplier':
        return await this.getSupplierAccessToken();
      case 'orderSupplier':
        return await this.getOrderSupplierAccessToken();
      case 'orderSupplierDetail':
        return await this.getOrderSupplierDetailAccessToken();
      case 'purchaseOrder':
        return await this.getPurchaseOrderAccessToken();
      case 'purchaseOrderDetail':
        return await this.getPurchaseOrderDetailAccessToken();
      case 'demand':
        return await this.getDemandAccessToken();
      case 'cashflow':
        return await this.getCashflowAccessToken();
      default:
        throw new Error(`Unknown service: ${service}`);
    }
  }

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
      this.customerTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase customer token refreshed successfully');
      return this.customerAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh customer token: ${error.message}`,
      );

      this.customerAccessToken = null;
      this.customerTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

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
    if (
      this.invoiceAccessToken &&
      this.invoiceTokenExpiry &&
      this.invoiceTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.invoiceAccessToken;
    }

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
      return this.invoiceAccessToken;
    } catch (error) {
      this.logger.error(`❌ Failed to refresh invoice token: ${error.message}`);

      this.invoiceAccessToken = null;
      this.invoiceTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshInvoiceToken(): Promise<void> {
    this.invoiceAccessToken = null;
    this.invoiceTokenExpiry = null;
    await this.refreshInvoiceToken();
  }

  // ============================================================================
  // ORDER TOKEN MANAGEMENT
  // ============================================================================

  async getOrderHeaders(): Promise<Record<string, string>> {
    const token = await this.getOrderAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getOrderAccessToken(): Promise<string> {
    if (
      this.orderAccessToken &&
      this.orderTokenExpiry &&
      this.orderTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.orderAccessToken;
    }

    return await this.refreshOrderToken();
  }

  private async refreshOrderToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase order access token...');

      const appId = this.configService.get<string>('LARK_ORDER_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_ORDER_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase order credentials not configured');
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

      this.orderAccessToken = response.data.tenant_access_token;
      this.orderTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase order token refreshed successfully');
      return this.orderAccessToken;
    } catch (error) {
      this.logger.error(`❌ Failed to refresh order token: ${error.message}`);

      this.orderAccessToken = null;
      this.orderTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshOrderToken(): Promise<void> {
    this.orderAccessToken = null;
    this.orderTokenExpiry = null;
    await this.refreshOrderToken();
  }

  // ============================================================================
  // PRODUCT TOKEN MANAGEMENT
  // ============================================================================

  async getProductHeaders(): Promise<Record<string, string>> {
    const token = await this.getProductAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getProductAccessToken(): Promise<string> {
    // Check if token is still valid (with 5 minute buffer)
    if (
      this.productAccessToken &&
      this.productTokenExpiry &&
      this.productTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.productAccessToken;
    }

    // Get new token
    return await this.refreshProductToken();
  }

  private async refreshProductToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase product access token...');

      const appId = this.configService.get<string>('LARK_PRODUCT_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_PRODUCT_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase product credentials not configured');
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

      this.productAccessToken = response.data.tenant_access_token;
      this.productTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase product token refreshed successfully');
      this.logger.debug(
        `Token expires at: ${this.productTokenExpiry.toISOString()}`,
      );

      return this.productAccessToken;
    } catch (error) {
      this.logger.error(`❌ Failed to refresh product token: ${error.message}`);

      this.productAccessToken = null;
      this.productTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  // Force refresh (for error recovery)
  async forceRefreshProductToken(): Promise<void> {
    this.productAccessToken = null;
    this.productTokenExpiry = null;
    await this.refreshProductToken();
  }

  // ============================================================================
  // SUPPLIER TOKEN MANAGEMENT
  // ============================================================================

  async getSupplierHeaders(): Promise<Record<string, string>> {
    const token = await this.getSupplierAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getSupplierAccessToken(): Promise<string> {
    if (
      this.supplierAccessToken &&
      this.supplierTokenExpiry &&
      this.supplierTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.supplierAccessToken;
    }

    return await this.refreshSupplierToken();
  }

  private async refreshSupplierToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase supplier access token...');

      const appId = this.configService.get<string>('LARK_SUPPLIER_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_SUPPLIER_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase supplier credentials not configured');
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

      this.supplierAccessToken = response.data.tenant_access_token;
      this.supplierTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase supplier token refreshed successfully');
      this.logger.debug(
        `Token expires at: ${this.supplierTokenExpiry.toISOString()}`,
      );

      return this.supplierAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh supplier token: ${error.message}`,
      );

      this.supplierAccessToken = null;
      this.supplierTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshSupplierToken(): Promise<void> {
    this.supplierAccessToken = null;
    this.supplierTokenExpiry = null;
    await this.refreshSupplierToken();
  }

  // ============================================================================
  // ORDER_SUPPLIER TOKEN MANAGEMENT
  // ============================================================================

  async getOrderSupplierHeaders(): Promise<Record<string, string>> {
    const token = await this.getOrderSupplierAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getOrderSupplierAccessToken(): Promise<string> {
    if (
      this.orderSupplierAccessToken &&
      this.orderSupplierTokenExpiry &&
      this.orderSupplierTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.orderSupplierAccessToken;
    }

    return await this.refreshOrderSupplierToken();
  }

  private async refreshOrderSupplierToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase order_supplier access token...');

      const appId = this.configService.get<string>(
        'LARK_ORDER_SUPPLIER_SYNC_APP_ID',
      );
      const appSecret = this.configService.get<string>(
        'LARK_ORDER_SUPPLIER_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase order_supplier credentials not configured');
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

      this.orderSupplierAccessToken = response.data.tenant_access_token;
      this.orderSupplierTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase orderSupplier token refreshed successfully');
      return this.orderSupplierAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh orderSupplier token: ${error.message}`,
      );

      this.orderSupplierAccessToken = null;
      this.orderSupplierTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshOrderSupplierToken(): Promise<void> {
    this.orderSupplierAccessToken = null;
    this.orderSupplierTokenExpiry = null;
    await this.refreshOrderSupplierToken();
  }

  // ============================================================================
  // ORDER_SUPPLIER_DETAIL TOKEN MANAGEMENT
  // ============================================================================

  async getOrderSupplierDetailHeaders(): Promise<Record<string, string>> {
    const token = await this.getOrderSupplierDetailAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getOrderSupplierDetailAccessToken(): Promise<string> {
    if (
      this.orderSupplierDetailAccessToken &&
      this.orderSupplierDetailTokenExpiry &&
      this.orderSupplierDetailTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.orderSupplierDetailAccessToken;
    }

    return await this.refreshOrderSupplierDetailToken();
  }

  private async refreshOrderSupplierDetailToken(): Promise<string> {
    try {
      this.logger.log(
        '🔄 Refreshing LarkBase order_supplier_detail access token...',
      );

      const appId = this.configService.get<string>(
        'LARK_ORDER_SUPPLIER_DETAIL_SYNC_APP_ID',
      );
      const appSecret = this.configService.get<string>(
        'LARK_ORDER_SUPPLIER_DETAIL_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error(
          'LarkBase order_supplier_detail credentials not configured',
        );
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

      this.orderSupplierDetailAccessToken = response.data.tenant_access_token;
      this.orderSupplierDetailTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log(
        '✅ LarkBase orderSupplierDetail token refreshed successfully',
      );
      return this.orderSupplierDetailAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh orderSupplierDetail token: ${error.message}`,
      );

      this.orderSupplierDetailAccessToken = null;
      this.orderSupplierDetailTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshOrderSupplierDetailToken(): Promise<void> {
    this.orderSupplierDetailAccessToken = null;
    this.orderSupplierDetailTokenExpiry = null;
    await this.refreshOrderSupplierDetailToken();
  }

  // ============================================================================
  // PURCHASE_ORDER TOKEN MANAGEMENT
  // ============================================================================

  async getPurchaseOrderHeaders(): Promise<Record<string, string>> {
    const token = await this.getPurchaseOrderAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getPurchaseOrderAccessToken(): Promise<string> {
    if (
      this.purchaseOrderAccessToken &&
      this.purchaseOrderTokenExpiry &&
      this.purchaseOrderTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.purchaseOrderAccessToken;
    }

    return await this.refreshPurchaseOrderToken();
  }

  private async refreshPurchaseOrderToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase purchase_order access token...');

      const appId = this.configService.get<string>(
        'LARK_PURCHASE_ORDER_SYNC_APP_ID',
      );
      const appSecret = this.configService.get<string>(
        'LARK_PURCHASE_ORDER_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase purchase_order credentials not configured');
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

      this.purchaseOrderAccessToken = response.data.tenant_access_token;
      this.purchaseOrderTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log(
        '✅ LarkBase purchase_order token refreshed successfully',
      );
      return this.purchaseOrderAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh purchase_order token: ${error.message}`,
      );

      this.purchaseOrderAccessToken = null;
      this.purchaseOrderTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshPurchaseOrderToken(): Promise<void> {
    this.purchaseOrderAccessToken = null;
    this.purchaseOrderTokenExpiry = null;
    await this.forceRefreshPurchaseOrderToken();
  }

  // ============================================================================
  // PURCHASE_ORDER_DETAIL TOKEN MANAGEMENT
  // ============================================================================

  async getPurchaseOrderDetailHeaders(): Promise<Record<string, string>> {
    const token = await this.getPurchaseOrderDetailAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getPurchaseOrderDetailAccessToken(): Promise<string> {
    if (
      this.purchaseOrderDetailAccessToken &&
      this.purchaseOrderDetailTokenExpiry &&
      this.purchaseOrderDetailTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.purchaseOrderDetailAccessToken;
    }

    return await this.refreshPurchaseOrderDetailToken();
  }

  private async refreshPurchaseOrderDetailToken(): Promise<string> {
    try {
      this.logger.log(
        '🔄 Refreshing LarkBase purchase_order_detail access token...',
      );

      const appId = this.configService.get<string>(
        'LARK_PURCHASE_ORDER_DETAIL_SYNC_APP_ID',
      );
      const appSecret = this.configService.get<string>(
        'LARK_PURCHASE_ORDER_DETAIL_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error(
          'LarkBase purchase_order_detail credentials not configured',
        );
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

      this.purchaseOrderDetailAccessToken = response.data.tenant_access_token;
      this.purchaseOrderDetailTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log(
        '✅ LarkBase purchase_order_detail token refreshed successfully',
      );
      return this.purchaseOrderDetailAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh purchase_order_detail token: ${error.message}`,
      );

      this.purchaseOrderDetailAccessToken = null;
      this.purchaseOrderDetailTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshPurchaseOrderDetailToken(): Promise<void> {
    this.purchaseOrderDetailAccessToken = null;
    this.purchaseOrderDetailTokenExpiry = null;
    await this.forceRefreshPurchaseOrderDetailToken();
  }

  // ============================================================================
  // DEMAND TOKEN MANAGEMENT
  // ============================================================================

  async getDemandHeaders(): Promise<Record<string, string>> {
    const token = await this.getDemandAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getDemandAccessToken(): Promise<string> {
    if (
      this.demandAccessToken &&
      this.demandTokenExpiry &&
      this.demandTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.demandAccessToken;
    }

    return await this.refreshDemandToken();
  }

  private async refreshDemandToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase demand access token...');

      const appId = this.configService.get<string>('LARK_DEMAND_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_DEMAND_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase demand credentials not configured');
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

      this.demandAccessToken = response.data.tenant_access_token;
      this.demandTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase demand token refreshed successfully');
      return this.demandAccessToken;
    } catch (error) {
      this.logger.error(`❌ Failed to refresh demand token: ${error.message}`);

      this.demandAccessToken = null;
      this.demandTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshDemandToken(): Promise<void> {
    this.demandAccessToken = null;
    this.demandTokenExpiry = null;
    await this.forceRefreshDemandToken();
  }

  // ============================================================================
  // CASHFLOW TOKEN MANAGEMENT
  // ============================================================================

  async getCashflowHeaders(): Promise<Record<string, string>> {
    const token = await this.getCashflowAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };
  }

  private async getCashflowAccessToken(): Promise<string> {
    if (
      this.cashflowAccessToken &&
      this.cashflowTokenExpiry &&
      this.cashflowTokenExpiry > new Date(Date.now() + 5 * 60 * 1000)
    ) {
      return this.cashflowAccessToken;
    }

    return await this.refreshCashflowToken();
  }

  private async refreshCashflowToken(): Promise<string> {
    try {
      this.logger.log('🔄 Refreshing LarkBase cashflow access token...');

      const appId = this.configService.get<string>('LARK_CASHFLOW_SYNC_APP_ID');
      const appSecret = this.configService.get<string>(
        'LARK_CASHFLOW_SYNC_APP_SECRET',
      );

      if (!appId || !appSecret) {
        throw new Error('LarkBase cashflow credentials not configured');
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

      this.cashflowAccessToken = response.data.tenant_access_token;
      this.cashflowTokenExpiry = new Date(
        Date.now() + response.data.expire * 1000,
      );

      this.logger.log('✅ LarkBase cashflow token refreshed successfully');
      return this.cashflowAccessToken;
    } catch (error) {
      this.logger.error(
        `❌ Failed to refresh cashflow token: ${error.message}`,
      );

      this.cashflowAccessToken = null;
      this.cashflowTokenExpiry = null;

      throw new Error(`Token refresh failed: ${error.message}`);
    }
  }

  async forceRefreshCashflowToken(): Promise<void> {
    this.cashflowAccessToken = null;
    this.cashflowTokenExpiry = null;
    await this.forceRefreshCashflowToken();
  }
}
