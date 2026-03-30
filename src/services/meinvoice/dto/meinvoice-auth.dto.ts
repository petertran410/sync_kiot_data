/**
 * Response từ Web API v2 OAuth
 */
export interface MeInvoiceOAuthResponseDto {
  access_token: string;
  token_type: string;
  expires_in: number;
}

/**
 * Cached token
 */
export interface MeInvoiceCachedTokenDto {
  token: string;
  expiresAt: Date;
}
