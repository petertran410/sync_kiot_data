import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { PrismaService } from '../../prisma/prisma.service';
import { firstValueFrom } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';
import { MeInvoiceAuthService } from './meinvoice-auth.service';
import {
  MeInvoiceOriginalInvoiceDataDto,
  MeInvoiceOriginalDetailDto,
  MeInvoiceCreateResponseDto,
  MeInvoiceCreateResultItemDto,
} from './dto';

@Injectable()
export class MeInvoiceInvoiceService {
  private readonly logger = new Logger(MeInvoiceInvoiceService.name);

  private readonly VAT_RATE = 8;
  private readonly VAT_RATE_NAME = '8%';

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prismaService: PrismaService,
    private readonly meInvoiceAuthService: MeInvoiceAuthService,
  ) {}

  /**
   * Tạo hóa đơn nháp trên MeInvoice từ Invoice Code
   */
  async createDraftInvoice(invoiceCode: string): Promise<{
    success: boolean;
    refId: string | null;
    transactionId: string | null;
    invNo: string | null;
    message: string;
  }> {
    this.logger.log(
      `📄 Creating MeInvoice draft for invoice code: ${invoiceCode}`,
    );

    try {
      // 1. Lấy Invoice với đầy đủ thông tin
      const invoice = await this.prismaService.invoice.findUnique({
        where: { code: invoiceCode },
        include: {
          invoiceDetails: {
            include: {
              product: {
                select: {
                  id: true,
                  code: true,
                  name: true,
                  misa_code: true,
                  misa_name: true,
                  misa_unit: true,
                  unit: true,
                },
              },
            },
          },
          customer: {
            select: {
              id: true,
              code: true,
              name: true,
              address: true,
              taxCode: true,
              contactNumber: true,
              email: true,
            },
          },
        },
      });

      if (!invoice) {
        return {
          success: false,
          refId: null,
          transactionId: null,
          invNo: null,
          message: `Invoice not found: ${invoiceCode}`,
        };
      }

      // 2. Kiểm tra đã sync MeInvoice chưa
      if (invoice.meinvoiceSyncStatus === 'SYNCED') {
        return {
          success: false,
          refId: invoice.meinvoiceRefId,
          transactionId: invoice.meinvoiceTransactionId,
          invNo: invoice.meinvoiceInvNo,
          message: `Invoice already synced to MeInvoice: ${invoice.code}`,
        };
      }

      // 3. Kiểm tra sản phẩm có misa_code không (dùng chung mapping)
      const productsWithoutCode = invoice.invoiceDetails.filter(
        (detail) =>
          !detail.product.misa_code || detail.product.misa_code.trim() === '',
      );

      if (productsWithoutCode.length > 0) {
        const productCodes = productsWithoutCode
          .map((d) => d.product.code)
          .join(', ');

        this.logger.warn(
          `⚠️ Invoice ${invoice.code} has products without misa_code: ${productCodes}. Skipping...`,
        );

        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'SKIP',
            meinvoiceErrorMessage: `Products without misa_code: ${productCodes}`,
          },
        });

        return {
          success: false,
          refId: null,
          transactionId: null,
          invNo: null,
          message: `Invoice ${invoice.code} skipped: products without misa_code`,
        };
      }

      // 4. Build payload
      const refId = invoice.meinvoiceRefId || uuidv4();
      const payload = this.buildInvoicePayload(invoice, refId);

      if (!payload) {
        return {
          success: false,
          refId: null,
          transactionId: null,
          invNo: null,
          message: `Failed to build MeInvoice payload for invoice: ${invoice.code}`,
        };
      }

      // 5. Gửi API tạo hóa đơn
      const result = await this.sendCreateInvoice([payload]);

      // 6. Cập nhật trạng thái
      if (result.success && result.data) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'SYNCED',
            meinvoiceRefId: result.data.RefID,
            meinvoiceTransactionId: result.data.TransactionID,
            meinvoiceInvNo: result.data.InvNo,
            meinvoiceSyncedAt: new Date(),
            meinvoiceErrorMessage: null,
          },
        });

        this.logger.log(
          `✅ MeInvoice draft created for invoice ${invoice.code}, InvNo: ${result.data.InvNo}, TransactionID: ${result.data.TransactionID}`,
        );

        return {
          success: true,
          refId: result.data.RefID,
          transactionId: result.data.TransactionID,
          invNo: result.data.InvNo,
          message: `Draft invoice created successfully`,
        };
      } else {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'FAILED',
            meinvoiceRefId: refId,
            meinvoiceErrorMessage: result.message,
          },
        });

        this.logger.error(
          `❌ Failed to create MeInvoice draft for invoice ${invoice.code}: ${result.message}`,
        );

        return {
          success: false,
          refId: refId,
          transactionId: null,
          invNo: null,
          message: result.message,
        };
      }
    } catch (error) {
      this.logger.error(
        `❌ Error creating MeInvoice draft for invoice ${invoiceCode}: ${error.message}`,
      );

      const invoice = await this.prismaService.invoice.findUnique({
        where: { code: invoiceCode },
        select: { id: true },
      });

      if (invoice) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'FAILED',
            meinvoiceErrorMessage: error.message,
          },
        });
      }

      return {
        success: false,
        refId: null,
        transactionId: null,
        invNo: null,
        message: error.message,
      };
    }
  }

  /**
   * Build payload cho MeInvoice từ Invoice data
   */
  private buildInvoicePayload(
    invoice: any,
    refId: string,
  ): MeInvoiceOriginalInvoiceDataDto | null {
    const invSeries =
      this.configService.get<string>('MEINVOICE_INV_SERIES') || '';
    const invoiceTemplateId = this.configService.get<string>(
      'MEINVOICE_INVOICE_TEMPLATE_ID',
    );

    // Build details và tính totals
    const details: MeInvoiceOriginalDetailDto[] = [];
    let totalSaleAmount = 0;
    let totalDiscountAmount = 0;
    let totalAmountWithoutVAT = 0;
    let totalVATAmount = 0;
    let totalAmount = 0;

    for (let i = 0; i < invoice.invoiceDetails.length; i++) {
      const detail = invoice.invoiceDetails[i];
      const product = detail.product;

      const quantity = detail.quantity;
      const originalPrice = Number(detail.price); // đơn giá bao gồm VAT
      const discountPerUnit = Number(detail.discount || 0); // CK per-unit (trên giá bao gồm VAT)

      // Đơn giá chưa thuế, chưa CK
      const unitPrice =
        Math.round((originalPrice / (1 + this.VAT_RATE / 100)) * 100) / 100;

      // Thành tiền (UnitPrice * SL) — trước CK
      const amountOC = Math.round(unitPrice * quantity * 100) / 100;

      // Chiết khấu (quy về trước thuế)
      const discountPerUnitBeforeTax =
        Math.round((discountPerUnit / (1 + this.VAT_RATE / 100)) * 100) / 100;
      const discountAmountOC =
        Math.round(discountPerUnitBeforeTax * quantity * 100) / 100;
      const discountRate = detail.discountRatio || 0;

      // Thành tiền sau CK, chưa thuế
      const amountWithoutVAT = amountOC - discountAmountOC;

      // Tiền thuế
      const vatAmount = Math.round((amountWithoutVAT * this.VAT_RATE) / 100);

      // Accumulate totals
      totalSaleAmount += amountOC;
      totalDiscountAmount += discountAmountOC;
      totalAmountWithoutVAT += amountWithoutVAT;
      totalVATAmount += vatAmount;
      totalAmount += amountWithoutVAT + vatAmount;

      details.push({
        ItemType: 1,
        LineNumber: i + 1,
        SortOrder: i + 1,
        ItemCode: product.misa_code || product.code,
        ItemName: product.misa_name || product.name,
        UnitName: product.misa_unit || product.unit || '',
        Quantity: quantity,
        UnitPrice: unitPrice,
        DiscountRate: discountRate,
        DiscountAmountOC: discountAmountOC,
        DiscountAmount: discountAmountOC,
        AmountOC: amountOC,
        Amount: amountOC,
        AmountWithoutVATOC: amountWithoutVAT,
        AmountWithoutVAT: amountWithoutVAT,
        VATRateName: this.VAT_RATE_NAME,
        VATAmountOC: vatAmount,
        VATAmount: vatAmount,
      });
    }

    if (details.length === 0) {
      this.logger.error('❌ No valid details for MeInvoice');
      return null;
    }

    const payload: MeInvoiceOriginalInvoiceDataDto = {
      RefID: refId,
      InvSeries: invSeries,
      InvoiceName: 'Hóa đơn giá trị gia tăng',
      InvDate: this.formatDateForMeInvoice(invoice.purchaseDate),
      CurrencyCode: 'VND',
      ExchangeRate: 1.0,
      PaymentMethodName: 'TM/CK',

      // Buyer
      BuyerLegalName:
        invoice.customer?.name || invoice.customerName || 'Khách lẻ',
      BuyerTaxCode: invoice.customer?.taxCode || '',
      BuyerAddress: invoice.customer?.address || '',
      BuyerCode: invoice.customer?.code || invoice.customerCode || '',
      BuyerPhoneNumber: invoice.customer?.contactNumber || '',
      BuyerEmail: invoice.customer?.email || '',
      BuyerFullName: invoice.customerName || invoice.customer?.name || '',

      // Totals
      TotalSaleAmountOC: totalSaleAmount,
      TotalSaleAmount: totalSaleAmount,
      TotalAmountWithoutVATOC: totalAmountWithoutVAT,
      TotalAmountWithoutVAT: totalAmountWithoutVAT,
      TotalVATAmountOC: totalVATAmount,
      TotalVATAmount: totalVATAmount,
      TotalDiscountAmountOC: totalDiscountAmount,
      TotalDiscountAmount: totalDiscountAmount,
      TotalAmountOC: totalAmount,
      TotalAmount: totalAmount,
      TotalAmountInWords: this.numberToVietnameseWords(totalAmount),

      // Details
      OriginalInvoiceDetail: details,

      // Tax rate info
      TaxRateInfo: [
        {
          VATRateName: this.VAT_RATE_NAME,
          AmountWithoutVATOC: totalAmountWithoutVAT,
          VATAmountOC: totalVATAmount,
        },
      ],

      // Display options
      OptionUserDefined: {
        MainCurrency: 'VND',
        AmountDecimalDigits: '0',
        AmountOCDecimalDigits: '0',
        UnitPriceOCDecimalDigits: '0',
        UnitPriceDecimalDigits: '0',
        QuantityDecimalDigits: '0',
        CoefficientDecimalDigits: '2',
        ExchangRateDecimalDigits: '0',
      },

      // Hóa đơn gốc (không thay thế / điều chỉnh)
      ReferenceType: null,
      OrgInvoiceType: null,
      OrgInvTemplateNo: null,
      OrgInvSeries: null,
      OrgInvNo: null,
      OrgInvDate: null,
    };

    if (invoiceTemplateId) {
      payload.InvoiceTemplateID = invoiceTemplateId;
    }

    return payload;
  }

  /**
   * Gửi request tạo hóa đơn lên MeInvoice API
   */
  private async sendCreateInvoice(
    payload: MeInvoiceOriginalInvoiceDataDto[],
  ): Promise<{
    success: boolean;
    data: MeInvoiceCreateResultItemDto | null;
    message: string;
  }> {
    const baseUrl = this.configService.get<string>('MEINVOICE_BASE_URL');
    const taxCode = this.configService.get<string>('MEINVOICE_TAX_CODE') || '';
    const accessToken = await this.meInvoiceAuthService.getAccessToken();

    // Có mã → /code/itg/invoicepublishing/createinvoice
    const url = `${baseUrl}/code/itg/invoicepublishing/createinvoice`;

    try {
      const response = await firstValueFrom(
        this.httpService.post<MeInvoiceCreateResponseDto>(url, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${accessToken}`,
            CompanyTaxCode: taxCode,
          },
        }),
      );

      const data = response.data;

      if (!data.Success) {
        // Kiểm tra token hết hạn → retry 1 lần
        if (data.ErrorCode === 'TokenExpiredCode') {
          this.logger.warn('⚠️ MeInvoice token expired, refreshing...');
          const newToken = await this.meInvoiceAuthService.refreshToken();

          const retryResponse = await firstValueFrom(
            this.httpService.post<MeInvoiceCreateResponseDto>(url, payload, {
              headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${newToken}`,
                CompanyTaxCode: taxCode,
              },
            }),
          );

          const retryData = retryResponse.data;

          if (!retryData.Success) {
            return {
              success: false,
              data: null,
              message: `${retryData.ErrorCode}: ${retryData.Errors?.join(', ') || ''}`,
            };
          }

          return this.parseCreateResponse(retryData);
        }

        return {
          success: false,
          data: null,
          message: `${data.ErrorCode}: ${data.Errors?.join(', ') || ''}`,
        };
      }

      return this.parseCreateResponse(data);
    } catch (error) {
      return {
        success: false,
        data: null,
        message: error.message,
      };
    }
  }

  /**
   * Parse response từ createinvoice API
   */
  private parseCreateResponse(data: MeInvoiceCreateResponseDto): {
    success: boolean;
    data: MeInvoiceCreateResultItemDto | null;
    message: string;
  } {
    if (!data.Data) {
      return {
        success: false,
        data: null,
        message: 'Response Data is empty',
      };
    }

    try {
      const items: MeInvoiceCreateResultItemDto[] = JSON.parse(data.Data);

      if (!items || items.length === 0) {
        return {
          success: false,
          data: null,
          message: 'No items in response Data',
        };
      }

      const item = items[0];

      // Kiểm tra ErrorCode bên trong item
      if (item.ErrorCode) {
        return {
          success: false,
          data: item,
          message: `Item error: ${item.ErrorCode}`,
        };
      }

      return {
        success: true,
        data: item,
        message: 'Invoice created successfully',
      };
    } catch (parseError) {
      this.logger.error(`❌ Failed to parse response Data: ${data.Data}`);
      return {
        success: false,
        data: null,
        message: `Failed to parse response: ${parseError.message}`,
      };
    }
  }

  /**
   * Format date cho MeInvoice API (ISO 8601)
   */
  private formatDateForMeInvoice(date: Date): string {
    return new Date(date).toISOString();
  }

  /**
   * Chuyển số thành chữ tiếng Việt (đơn giản)
   */
  private numberToVietnameseWords(amount: number): string {
    const rounded = Math.round(amount);
    if (rounded === 0) return 'Không đồng.';

    const ones = [
      '',
      'một',
      'hai',
      'ba',
      'bốn',
      'năm',
      'sáu',
      'bảy',
      'tám',
      'chín',
    ];
    const groups = ['', 'nghìn', 'triệu', 'tỷ', 'nghìn tỷ', 'triệu tỷ'];

    const readThreeDigits = (n: number, showZeroHundred: boolean): string => {
      const h = Math.floor(n / 100);
      const t = Math.floor((n % 100) / 10);
      const o = n % 10;
      let result = '';

      if (h > 0) {
        result += ones[h] + ' trăm';
      } else if (showZeroHundred) {
        result += 'không trăm';
      }

      if (t > 1) {
        result += ' ' + ones[t] + ' mươi';
        if (o === 1) result += ' mốt';
        else if (o === 5) result += ' lăm';
        else if (o > 0) result += ' ' + ones[o];
      } else if (t === 1) {
        result += ' mười';
        if (o === 5) result += ' lăm';
        else if (o > 0) result += ' ' + ones[o];
      } else if (o > 0) {
        if (h > 0 || showZeroHundred) result += ' lẻ';
        result += ' ' + ones[o];
      }

      return result.trim();
    };

    const chunks: number[] = [];
    let temp = rounded;
    while (temp > 0) {
      chunks.push(temp % 1000);
      temp = Math.floor(temp / 1000);
    }

    let result = '';
    for (let i = chunks.length - 1; i >= 0; i--) {
      if (chunks[i] === 0) continue;
      const showZero = i < chunks.length - 1;
      result += readThreeDigits(chunks[i], showZero) + ' ' + groups[i] + ' ';
    }

    result = result.trim();
    result = result.charAt(0).toUpperCase() + result.slice(1) + ' đồng.';

    return result;
  }
}
