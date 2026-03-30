/**
 * Request body cho /SAInvoice/Insert
 * Lưu ý: data và detail là JSON STRING, không phải object
 */
export interface MeInvoiceInsertRequestDto {
  data: string;
  detail: string;
}

/**
 * Master hóa đơn (serialize thành JSON string trong field "data")
 */
export interface MeInvoiceMasterDto {
  RefID: string;
  RefType: number;
  AccountObjectID?: string | null;
  AccountObjectName?: string;
  AccountObjectAddress?: string;
  AccountObjectTaxCode?: string;
  AccountObjectBankAccount?: string;
  AccountObjectBankName?: string;
  PaymentMethod: string;
  ContactName?: string;
  ReceiverEmail?: string;
  ReceiverMobile?: string;
  InvTypeCode: string;
  InvTemplateNo: string;
  InvSeries: string;
  InvNo: string;
  InvDate: string;
  CurrencyCode: string;
  ExchangeRate: number;
  VATRate: number;
  TotalSaleAmountOC: number;
  TotalSaleAmount: number;
  TotalDiscountAmountOC: number;
  TotalDiscountAmount: number;
  TotalVATAmountOC: number;
  TotalVATAmount: number;
  TotalAmountOC: number;
  TotalAmount: number;
  TotalAmountWithVAT: number;
  TransactionID?: string | null;
  PublishStatus: number;
  IsInvoiceDeleted: boolean;
  EInvoiceStatus: number;
  CompanyID: number;
  CompanyName: string;
  CompanyTaxCode: string;
  CreatedDate: string;
  CreatedBy?: string | null;
  ModifiedDate: string;
  ModifiedBy?: string | null;
  EditVersion: number;
  EntityState: number;
  InvTemplateNoSeries?: string;
  TypeChangeInvoice?: number;
  SendInvoiceStatus?: number;
  IsMoreVATRate?: boolean;
  BusinessArea?: number;
}

/**
 * Detail hóa đơn (serialize thành JSON string array trong field "detail")
 */
export interface MeInvoiceDetailDto {
  RefDetailID: string;
  RefID: string;
  InventoryItemID: string;
  InventoryItemCode?: string | null;
  InventoryItemName: string;
  Description: string;
  UnitName: string;
  Quantity: number;
  UnitPrice: number;
  AmountOC: number;
  Amount: number;
  DiscountRate: number;
  DiscountAmountOC: number;
  DiscountAmount: number;
  VATRate: number;
  VATAmountOC: number;
  VATAmount: number;
  SortOrder: number;
  IsPromotion: boolean;
  CompanyID: number;
  InventoryItemType: number;
  SortOrderView: number;
  EntityState: number;
  IsDescription?: string;
}

/**
 * Response chung từ Web API v2
 */
export interface MeInvoiceApiResponseDto {
  success: boolean;
  data?: any;
  errorCode?: string[];
  error?: string;
  recordsTotal?: number;
}
