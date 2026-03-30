import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { PrismaService } from '../../prisma/prisma.service';
import { firstValueFrom } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';
import { MeInvoiceAuthService } from './meinvoice-auth.service';
import {
  MeInvoiceInsertRequestDto,
  MeInvoiceMasterDto,
  MeInvoiceDetailDto,
} from './dto';

@Injectable()
export class MeInvoiceInvoiceService {
  private readonly logger = new Logger(MeInvoiceInvoiceService.name);

  private readonly VAT_RATE = 8;

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prismaService: PrismaService,
    private readonly meInvoiceAuthService: MeInvoiceAuthService,
  ) {}

  /**
   * Đẩy hóa đơn nháp lên MeInvoice Web
   */
  async pushDraftInvoice(invoiceCode: string): Promise<{
    success: boolean;
    refId: string | null;
    message: string;
  }> {
    this.logger.log(
      `📄 Pushing draft invoice to MeInvoice for: ${invoiceCode}`,
    );

    try {
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
          message: `Invoice not found: ${invoiceCode}`,
        };
      }

      if (invoice.meinvoiceSyncStatus === 'SYNCED') {
        return {
          success: false,
          refId: invoice.meinvoiceRefId,
          message: `Invoice already pushed to MeInvoice: ${invoice.code}`,
        };
      }

      // Kiểm tra misa_code
      const productsWithoutCode = invoice.invoiceDetails.filter(
        (d) => !d.product.misa_code || d.product.misa_code.trim() === '',
      );

      if (productsWithoutCode.length > 0) {
        const codes = productsWithoutCode.map((d) => d.product.code).join(', ');

        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'SKIP',
            meinvoiceErrorMessage: `Products without misa_code: ${codes}`,
          },
        });

        return {
          success: false,
          refId: null,
          message: `Invoice ${invoice.code} skipped: products without misa_code`,
        };
      }

      // Build payload
      const refId = invoice.meinvoiceRefId || uuidv4();
      const payload = this.buildInsertPayload(invoice, refId);

      if (!payload) {
        return {
          success: false,
          refId: null,
          message: 'Failed to build payload',
        };
      }

      // Gửi API
      const result = await this.sendInsert(payload);

      if (result.success) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'SYNCED',
            meinvoiceRefId: refId,
            meinvoiceSyncedAt: new Date(),
            meinvoiceErrorMessage: null,
          },
        });

        this.logger.log(
          `✅ Draft invoice pushed to MeInvoice for ${invoice.code}, RefID: ${refId}`,
        );
      } else {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            meinvoiceSyncStatus: 'FAILED',
            meinvoiceRefId: refId,
            meinvoiceErrorMessage: result.message,
          },
        });
      }

      return { success: result.success, refId, message: result.message };
    } catch (error) {
      this.logger.error(
        `❌ Error pushing draft invoice ${invoiceCode}: ${error.message}`,
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

      return { success: false, refId: null, message: error.message };
    }
  }

  /**
   * Query hóa đơn trên MeInvoice theo RefID
   */
  async getInvoiceByRefId(refId: string): Promise<{
    success: boolean;
    data: any;
    message: string;
  }> {
    const baseUrl = this.configService.get<string>('MEINVOICE_WEBAPP_BASE_URL');
    const taxCode = this.configService.get<string>('MEINVOICE_TAX_CODE') || '';
    const accessToken = await this.meInvoiceAuthService.getAccessToken();

    const url = `${baseUrl}/SAInvoice/Get/${refId}`;

    try {
      const response = await firstValueFrom(
        this.httpService.get(url, {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            taxcode: taxCode,
          },
        }),
      );

      return { success: true, data: response.data, message: 'OK' };
    } catch (error) {
      return {
        success: false,
        data: null,
        message: error.response?.data?.error || error.message,
      };
    }
  }

  /**
   * Build payload cho /SAInvoice/Insert
   */
  private buildInsertPayload(
    invoice: any,
    refId: string,
  ): MeInvoiceInsertRequestDto | null {
    const companyId = Number(
      this.configService.get<string>('MEINVOICE_COMPANY_ID') || '0',
    );
    const companyName =
      this.configService.get<string>('MEINVOICE_COMPANY_NAME') || '';
    const companyTaxCode =
      this.configService.get<string>('MEINVOICE_COMPANY_TAX_CODE') || '';
    const invTemplateNo =
      this.configService.get<string>('MEINVOICE_INV_TEMPLATE_NO') || '';
    const invTypeCode =
      this.configService.get<string>('MEINVOICE_INV_TYPE_CODE') || '';
    const invSeries =
      this.configService.get<string>('MEINVOICE_INV_SERIES') || '';

    // Build details
    const details: MeInvoiceDetailDto[] = [];
    let totalSaleAmount = 0;
    let totalDiscountAmount = 0;
    let totalVATAmount = 0;
    let totalAmount = 0;

    for (let i = 0; i < invoice.invoiceDetails.length; i++) {
      const detail = invoice.invoiceDetails[i];
      const product = detail.product;

      const quantity = detail.quantity;
      const originalPrice = Number(detail.price);
      const discountPerUnit = Number(detail.discount || 0);

      // Đơn giá chưa thuế, chưa CK
      const unitPrice =
        Math.round((originalPrice / (1 + this.VAT_RATE / 100)) * 100) / 100;

      // Thành tiền trước CK
      const amountOC = Math.round(unitPrice * quantity * 100) / 100;

      // Chiết khấu (quy về trước thuế)
      const discountPerUnitBeforeTax =
        Math.round((discountPerUnit / (1 + this.VAT_RATE / 100)) * 100) / 100;
      const discountAmountOC =
        Math.round(discountPerUnitBeforeTax * quantity * 100) / 100;
      const discountRate = detail.discountRatio || 0;

      // Thành tiền sau CK, chưa thuế
      const amountAfterDiscount = amountOC - discountAmountOC;

      // Tiền thuế
      const vatAmount = Math.round((amountAfterDiscount * this.VAT_RATE) / 100);

      totalSaleAmount += amountOC;
      totalDiscountAmount += discountAmountOC;
      totalVATAmount += vatAmount;
      totalAmount += amountAfterDiscount + vatAmount;

      const refDetailId = uuidv4();

      details.push({
        RefDetailID: refDetailId,
        RefID: refId,
        InventoryItemID: product.misa_code || product.code,
        InventoryItemCode: product.misa_code || product.code,
        InventoryItemName: product.misa_name || product.name,
        Description: product.misa_name || product.name,
        UnitName: product.misa_unit || product.unit || '',
        Quantity: quantity,
        UnitPrice: unitPrice,
        AmountOC: amountOC,
        Amount: amountOC,
        DiscountRate: discountRate,
        DiscountAmountOC: discountAmountOC,
        DiscountAmount: discountAmountOC,
        VATRate: 10, // MeInvoice yêu cầu thuế suất gốc, tự giảm về 8%
        VATAmountOC: vatAmount,
        VATAmount: vatAmount,
        SortOrder: i + 1,
        IsPromotion: false,
        CompanyID: companyId,
        InventoryItemType: 0,
        SortOrderView: i + 1,
        EntityState: 1,
      });
    }

    if (details.length === 0) {
      this.logger.error('❌ No valid details');
      return null;
    }

    const totalAmountWithoutVAT = totalSaleAmount - totalDiscountAmount;
    const invDate = this.formatDate(invoice.purchaseDate);
    const now = this.formatDate(new Date());

    const master: MeInvoiceMasterDto = {
      RefID: refId,
      RefType: 0, // Hóa đơn GTGT
      AccountObjectID: null,
      AccountObjectName:
        invoice.customer?.name || invoice.customerName || 'Khách lẻ',
      AccountObjectAddress: invoice.customer?.address || '',
      AccountObjectTaxCode: invoice.customer?.taxCode || '',
      AccountObjectBankAccount: '',
      AccountObjectBankName: '',
      PaymentMethod: 'TM/CK',
      ContactName: invoice.customerName || invoice.customer?.name || '',
      ReceiverEmail: invoice.customer?.email || '',
      ReceiverMobile: invoice.customer?.contactNumber || '',
      InvTypeCode: invTypeCode,
      InvTemplateNo: invTemplateNo,
      InvSeries: invSeries,
      InvNo: '<Chưa cấp số>',
      InvDate: invDate,
      CurrencyCode: 'VND',
      ExchangeRate: 1,
      VATRate: 10, // Max VAT rate gốc
      TotalSaleAmountOC: totalSaleAmount,
      TotalSaleAmount: totalSaleAmount,
      TotalDiscountAmountOC: totalDiscountAmount,
      TotalDiscountAmount: totalDiscountAmount,
      TotalVATAmountOC: totalVATAmount,
      TotalVATAmount: totalVATAmount,
      TotalAmountOC: totalAmount,
      TotalAmount: totalAmount,
      TotalAmountWithVAT: totalAmount,
      TransactionID: null,
      PublishStatus: 0, // Chưa phát hành
      IsInvoiceDeleted: false,
      EInvoiceStatus: 1, // Hóa đơn gốc
      CompanyID: companyId,
      CompanyName: companyName,
      CompanyTaxCode: companyTaxCode,
      CreatedDate: now,
      CreatedBy: null,
      ModifiedDate: now,
      ModifiedBy: null,
      EditVersion: 0,
      EntityState: 1,
      InvTemplateNoSeries: `${invTemplateNo} - ${invSeries}`,
      TypeChangeInvoice: 0,
      SendInvoiceStatus: 0,
    };

    return {
      data: JSON.stringify(master),
      detail: JSON.stringify(details),
    };
  }

  /**
   * Gửi request Insert lên MeInvoice Web API v2
   */
  private async sendInsert(
    payload: MeInvoiceInsertRequestDto,
  ): Promise<{ success: boolean; message: string }> {
    const baseUrl = this.configService.get<string>('MEINVOICE_WEBAPP_BASE_URL');
    const taxCode = this.configService.get<string>('MEINVOICE_TAX_CODE') || '';
    const accessToken = await this.meInvoiceAuthService.getAccessToken();

    const url = `${baseUrl}/SAInvoice/Insert`;

    this.logger.debug(`Insert URL: ${url}`);
    this.logger.debug(
      `Insert payload: ${JSON.stringify(payload).substring(0, 500)}...`,
    );

    try {
      const response = await firstValueFrom(
        this.httpService.post(url, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${accessToken}`,
            taxcode: taxCode,
          },
        }),
      );

      const data = response.data;

      this.logger.debug(`Insert response: ${JSON.stringify(data)}`);

      if (data?.success === false) {
        const errorMsg =
          data.error || data.errorCode?.join(', ') || 'Unknown error';
        return { success: false, message: errorMsg };
      }

      return { success: true, message: 'Draft invoice pushed successfully' };
    } catch (error) {
      const errorDetail = error.response?.data
        ? JSON.stringify(error.response.data)
        : error.message;
      this.logger.error(`❌ Insert error: ${errorDetail}`);
      return { success: false, message: errorDetail };
    }
  }

  /**
   * Lấy link preview PDF hóa đơn chưa phát hành
   */
  async getPreviewLink(invoiceCode: string): Promise<{
    success: boolean;
    link: string | null;
    message: string;
  }> {
    this.logger.log(`🔗 Getting preview link for invoice: ${invoiceCode}`);

    try {
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
          link: null,
          message: `Invoice not found: ${invoiceCode}`,
        };
      }

      // Build ITG v3 OriginalInvoiceData payload
      const payload = this.buildPreviewPayload(invoice);

      if (!payload) {
        return {
          success: false,
          link: null,
          message: 'Failed to build preview payload',
        };
      }

      // Gọi ITG API v3
      const itgBaseUrl = this.configService.get<string>(
        'MEINVOICE_ITG_BASE_URL',
      );
      const taxCode =
        this.configService.get<string>('MEINVOICE_TAX_CODE') || '';
      const itgToken = await this.meInvoiceAuthService.getItgAccessToken();

      const url = `${itgBaseUrl}/code/itg/invoicepublishing/invoicelinkview?type=1`;

      const response = await firstValueFrom(
        this.httpService.post(url, payload, {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${itgToken}`,
            CompanyTaxCode: taxCode,
          },
        }),
      );

      const data = response.data;

      this.logger.debug(`Preview response: ${JSON.stringify(data)}`);

      if (!data?.Success || !data?.Data) {
        return {
          success: false,
          link: null,
          message: `${data?.ErrorCode || 'Unknown error'}`,
        };
      }

      this.logger.log(`✅ Preview link obtained for ${invoiceCode}`);

      return {
        success: true,
        link: data.Data,
        message: 'Preview link generated',
      };
    } catch (error) {
      const errorDetail = error.response?.data
        ? JSON.stringify(error.response.data)
        : error.message;
      this.logger.error(`❌ Preview link error: ${errorDetail}`);
      return { success: false, link: null, message: errorDetail };
    }
  }

  /**
   * Build OriginalInvoiceData cho ITG v3 preview API
   */
  private buildPreviewPayload(invoice: any): any {
    const invSeries =
      this.configService.get<string>('MEINVOICE_INV_SERIES') || '';

    const details: any[] = [];
    let totalSaleAmount = 0;
    let totalDiscountAmount = 0;
    let totalAmountWithoutVAT = 0;
    let totalVATAmount = 0;
    let totalAmount = 0;

    for (let i = 0; i < invoice.invoiceDetails.length; i++) {
      const detail = invoice.invoiceDetails[i];
      const product = detail.product;

      const quantity = detail.quantity;
      const originalPrice = Number(detail.price);
      const discountPerUnit = Number(detail.discount || 0);

      const unitPrice =
        Math.round((originalPrice / (1 + this.VAT_RATE / 100)) * 100) / 100;

      const amountOC = Math.round(unitPrice * quantity * 100) / 100;

      const discountPerUnitBeforeTax =
        Math.round((discountPerUnit / (1 + this.VAT_RATE / 100)) * 100) / 100;
      const discountAmountOC =
        Math.round(discountPerUnitBeforeTax * quantity * 100) / 100;

      const amountWithoutVAT = amountOC - discountAmountOC;
      const vatAmount = Math.round((amountWithoutVAT * this.VAT_RATE) / 100);

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
        DiscountRate: detail.discountRatio || 0,
        DiscountAmountOC: discountAmountOC,
        DiscountAmount: discountAmountOC,
        AmountOC: amountOC,
        Amount: amountOC,
        AmountWithoutVATOC: amountWithoutVAT,
        AmountWithoutVAT: amountWithoutVAT,
        VATRateName: '10%',
        VATAmountOC: vatAmount,
        VATAmount: vatAmount,
      });
    }

    if (details.length === 0) return null;

    return {
      RefID: invoice.meinvoiceRefId || uuidv4(),
      InvSeries: invSeries,
      InvoiceName: 'Hóa đơn giá trị gia tăng',
      InvDate: this.formatDate(invoice.purchaseDate),
      CurrencyCode: 'VND',
      ExchangeRate: 1.0,
      PaymentMethodName: 'TM/CK',
      BuyerLegalName:
        invoice.customer?.name || invoice.customerName || 'Khách lẻ',
      BuyerTaxCode: invoice.customer?.taxCode || '',
      BuyerAddress: invoice.customer?.address || '',
      BuyerCode: invoice.customer?.code || invoice.customerCode || '',
      BuyerPhoneNumber: invoice.customer?.contactNumber || '',
      BuyerEmail: invoice.customer?.email || '',
      BuyerFullName: invoice.customerName || invoice.customer?.name || '',
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
      IsTaxReduction: true,
      OriginalInvoiceDetail: details,
      TaxRateInfo: [
        {
          VATRateName: '10%',
          AmountWithoutVATOC: totalAmountWithoutVAT,
          VATAmountOC: totalVATAmount,
        },
      ],
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
      ReferenceType: null,
      OrgInvoiceType: null,
      OrgInvTemplateNo: null,
      OrgInvSeries: null,
      OrgInvNo: null,
      OrgInvDate: null,
    };
  }

  private formatDate(date: Date): string {
    return new Date(date).toISOString();
  }

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
