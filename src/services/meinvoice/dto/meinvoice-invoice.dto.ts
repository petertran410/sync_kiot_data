/**
 * Request body — mảng OriginalInvoiceData
 */
export interface MeInvoiceOriginalInvoiceDataDto {
  RefID: string;
  InvSeries: string;
  InvoiceName: string;
  InvDate: string;
  CurrencyCode: string;
  ExchangeRate: number;
  PaymentMethodName: string;

  // Buyer info
  BuyerLegalName?: string;
  BuyerTaxCode?: string;
  BuyerAddress?: string;
  BuyerCode?: string;
  BuyerPhoneNumber?: string;
  BuyerEmail?: string;
  BuyerFullName?: string;
  BuyerBankAccount?: string;
  BuyerBankName?: string;

  // Totals
  TotalSaleAmountOC: number;
  TotalSaleAmount: number;
  TotalAmountWithoutVATOC: number;
  TotalAmountWithoutVAT: number;
  TotalVATAmountOC: number;
  TotalVATAmount: number;
  TotalDiscountAmountOC: number;
  TotalDiscountAmount: number;
  TotalAmountOC: number;
  TotalAmount: number;
  TotalAmountInWords?: string;

  // Details
  OriginalInvoiceDetail: MeInvoiceOriginalDetailDto[];

  // Tax rate breakdown
  TaxRateInfo: MeInvoiceTaxRateInfoDto[];

  // Display options
  OptionUserDefined?: MeInvoiceOptionUserDefinedDto;

  // Optional
  InvoiceTemplateID?: string;

  // Thay thế / điều chỉnh (null cho hóa đơn gốc)
  ReferenceType?: number | null;
  OrgInvoiceType?: number | null;
  OrgInvTemplateNo?: string | null;
  OrgInvSeries?: string | null;
  OrgInvNo?: string | null;
  OrgInvDate?: string | null;
}

export interface MeInvoiceOriginalDetailDto {
  ItemType: number; // 1: HHDV, 2: khuyến mại, 3: chiết khấu, 4: ghi chú
  LineNumber: number;
  SortOrder: number;
  ItemCode: string;
  ItemName: string;
  UnitName: string;
  Quantity: number;
  UnitPrice: number;
  DiscountRate?: number;
  DiscountAmountOC?: number;
  DiscountAmount?: number;
  AmountOC: number;
  Amount: number;
  AmountWithoutVATOC: number;
  AmountWithoutVAT: number;
  VATRateName: string;
  VATAmountOC: number;
  VATAmount: number;
  UnitPriceAfterTax?: number;
  AmountAfterTax?: number;
}

export interface MeInvoiceTaxRateInfoDto {
  VATRateName: string;
  AmountWithoutVATOC: number;
  VATAmountOC: number;
}

export interface MeInvoiceOptionUserDefinedDto {
  MainCurrency: string;
  AmountDecimalDigits: string;
  AmountOCDecimalDigits: string;
  UnitPriceOCDecimalDigits: string;
  UnitPriceDecimalDigits: string;
  QuantityDecimalDigits: string;
  CoefficientDecimalDigits: string;
  ExchangRateDecimalDigits: string;
}

/**
 * Response từ createinvoice
 */
export interface MeInvoiceCreateResponseDto {
  Success: boolean;
  ErrorCode?: string | null;
  Errors?: string[];
  Data?: string | null;
  CustomData?: string | null;
}

/**
 * Parsed item trong Data (JSON string → array)
 */
export interface MeInvoiceCreateResultItemDto {
  RefID: string;
  TransactionID: string;
  InvNo: string;
  InvCode?: string | null;
  InvDate: string;
  InvoiceData?: string;
  ErrorCode?: string | null;
  ErrorData?: string | null;
  TokenCallback?: string | null;
  CallbackUrl?: string | null;
}
