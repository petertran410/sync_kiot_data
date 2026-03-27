/**
 * Request body để tạo chứng từ bán hàng
 */
export interface MisaSaveVoucherRequestDto {
  app_id: string;
  org_company_code: string;
  voucher: MisaSaInvoiceDto[];
}

/**
 * Chứng từ hóa đơn bán hàng (voucher_type = 11, reftype = 3560)
 */
export interface MisaSaInvoiceDto {
  // Required fields
  voucher_type: number;
  org_refid: string;
  org_refno: string;
  branch_id: string;
  reftype: number;

  // Invoice info
  org_reftype?: number | null;
  org_reftype_name?: string;
  inv_date?: string;
  is_posted?: boolean;

  // Customer info
  account_object_id?: string;
  account_object_code?: string;
  account_object_name?: string;
  account_object_address?: string;
  account_object_tax_code?: string;
  account_object_bank_account?: string;

  // Employee info
  employee_id?: string;
  employee_code?: string;
  employee_name?: string;

  // Discount info
  discount_type?: number;
  discount_rate_voucher?: number;

  // Other info
  container_no?: string;
  exchange_rate?: number;
  currency_id?: string;
  payment_method?: string;
  buyer?: string;
  is_created_savoucher?: number;
  invoice_type?: number;
  include_invoice?: number;

  // Audit fields
  created_date?: string;
  created_by?: string;
  modified_date?: string;
  modified_by?: string;

  // Details
  detail: MisaSaInvoiceDetailDto[];
}

/**
 * Chi tiết hóa đơn bán hàng
 */
export interface MisaSaInvoiceDetailDto {
  // Product info
  inventory_item_id?: string;
  inventory_item_code: string;
  inventory_item_name: string;
  inventory_item_type: number;
  description?: string;

  // Unit info
  unit_id?: string;
  unit_name: string;
  main_unit_id?: string;
  main_unit_name: string;

  // Quantity
  quantity: number;
  main_quantity: number;
  main_convert_rate: number;

  // Price
  unit_price?: number;
  unit_price_after_tax?: number;
  main_unit_price: number;
  amount_oc: number;
  amount: number;

  // Discount
  discount_rate?: number;
  discount_amount_oc?: number;
  discount_amount?: number;

  // VAT
  vat_rate?: number;
  other_vat_rate?: number;
  vat_amount_oc?: number;
  vat_amount?: number;

  // Stock info
  stock_id?: string;
  stock_code?: string;
  stock_name?: string;

  // Account object (customer) info in detail
  account_object_id?: string;
  account_object_code?: string;
  account_object_name?: string;

  // Other
  sort_order: number;
  is_promotion?: boolean;
  not_in_vat_declaration?: boolean;
  exchange_rate_operator?: string;
}

/**
 * Response từ API save voucher
 */
export interface MisaSaveVoucherResponseDto {
  Success: boolean;
  ErrorCode?: string;
  ErrorMessage?: string;
  Data?: string;
}

/**
 * Callback data từ Misa sau khi xử lý voucher
 */
export interface MisaCallbackDataDto {
  org_refid: string;
  status: 'success' | 'failed';
  voucher_id?: string;
  voucher_no?: string;
  error_code?: string;
  error_message?: string;
  created_date?: string;
}

/**
 * Request body callback từ Misa
 */
export interface MisaCallbackRequestDto {
  app_id: string;
  data: MisaCallbackDataDto[];
}
