export interface MeInvoiceTokenRequestDto {
  appid: string;
  taxcode: string;
  username: string;
  password: string;
}

export interface MeInvoiceTokenResponseDto {
  Success: boolean;
  ErrorCode?: string | null;
  Errors?: string[];
  Data?: string | null;
  CustomData?: string | null;
}

export interface MeInvoiceCachedTokenDto {
  token: string;
  expiresAt: Date;
}
