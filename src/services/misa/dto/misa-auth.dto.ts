/**
 * Request body để lấy access token từ Misa
 */
export interface MisaConnectRequestDto {
  app_id: string;
  access_code: string;
  org_company_code: string;
}

/**
 * Response từ API connect
 */
export interface MisaConnectResponseDto {
  Success: boolean;
  ErrorCode?: string;
  ErrorMessage?: string;
  Data?: MisaTokenDataDto;
}

/**
 * Token data trả về từ Misa
 */
export interface MisaTokenDataDto {
  access_token: string;
  expired_time?: string;
  token_type?: string;
}

/**
 * Cached token với thời gian hết hạn
 */
export interface MisaCachedTokenDto {
  accessToken: string;
  expiresAt: Date;
}
