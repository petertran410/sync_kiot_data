import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { PrismaService } from '../../prisma/prisma.service';
import { firstValueFrom } from 'rxjs';
import { v4 as uuidv4 } from 'uuid';
import { MisaAuthService } from './misa-auth.service';
import { MisaDictionaryService } from './misa-dictionary.service';
import {
  MisaSaveVoucherRequestDto,
  MisaSaInvoiceDto,
  MisaSaInvoiceDetailDto,
  MisaSaveVoucherResponseDto,
} from './dto';

@Injectable()
export class MisaVoucherService {
  private readonly logger = new Logger(MisaVoucherService.name);

  // Constants
  private readonly VOUCHER_TYPE = 11; // Hóa đơn bán hàng
  private readonly REFTYPE = 3560; // Hóa đơn bán hàng hóa, dịch vụ trong nước
  private readonly VAT_RATE = 8; // 8% VAT
  private readonly DEFAULT_CREATED_BY = 'Trần Ngọc Nhân';

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prismaService: PrismaService,
    private readonly misaAuthService: MisaAuthService,
    private readonly misaDictionaryService: MisaDictionaryService,
  ) {}

  /**
   * Tạo chứng từ bán hàng Misa từ Invoice ID
   */
  async createSaleVoucherFromInvoice(invoiceId: number): Promise<{
    success: boolean;
    orgRefId: string | null;
    message: string;
  }> {
    this.logger.log(`🧾 Creating Misa voucher for invoice ID: ${invoiceId}`);

    try {
      // 1. Lấy Invoice với đầy đủ thông tin
      const invoice = await this.prismaService.invoice.findUnique({
        where: { id: invoiceId },
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
                },
              },
            },
          },
          branch: {
            select: {
              id: true,
              name: true,
            },
          },
          customer: {
            select: {
              id: true,
              name: true,
              address: true,
              taxCode: true,
            },
          },
        },
      });

      if (!invoice) {
        return {
          success: false,
          orgRefId: null,
          message: `Invoice not found: ${invoiceId}`,
        };
      }

      // 2. Kiểm tra đã sync chưa
      if (invoice.misaSyncStatus === 'SYNCED' && invoice.misaConfirmed) {
        return {
          success: false,
          orgRefId: invoice.misaOrgRefId,
          message: `Invoice already synced to Misa: ${invoice.code}`,
        };
      }

      // 3. Kiểm tra tất cả sản phẩm có misa_code không
      const productsWithoutMisaCode = invoice.invoiceDetails.filter(
        (detail) =>
          !detail.product.misa_code || detail.product.misa_code.trim() === '',
      );

      if (productsWithoutMisaCode.length > 0) {
        const productCodes = productsWithoutMisaCode
          .map((d) => d.product.code)
          .join(', ');

        this.logger.warn(
          `⚠️ Invoice ${invoice.code} has products without misa_code: ${productCodes}. Skipping...`,
        );

        // Cập nhật trạng thái SKIP
        await this.prismaService.invoice.update({
          where: { id: invoiceId },
          data: {
            misaSyncStatus: 'SKIP',
            misaErrorMessage: `Products without misa_code: ${productCodes}`,
          },
        });

        return {
          success: false,
          orgRefId: null,
          message: `Invoice ${invoice.code} skipped: products without misa_code`,
        };
      }

      // 4. Build payload
      const orgRefId = invoice.misaOrgRefId || uuidv4();
      const voucherPayload = await this.buildVoucherPayload(invoice, orgRefId);

      if (!voucherPayload) {
        return {
          success: false,
          orgRefId: null,
          message: `Failed to build voucher payload for invoice: ${invoice.code}`,
        };
      }

      // 5. Gửi API tạo voucher
      const result = await this.sendVoucherToMisa(voucherPayload);

      // 6. Cập nhật trạng thái
      if (result.success) {
        await this.prismaService.invoice.update({
          where: { id: invoiceId },
          data: {
            misaSyncStatus: 'PENDING', // Chờ callback từ Misa
            misaOrgRefId: orgRefId,
            misaSyncedAt: new Date(),
            misaSyncRetries: { increment: 1 },
            misaErrorMessage: null,
          },
        });

        this.logger.log(
          `✅ Voucher sent to Misa queue for invoice ${invoice.code}, orgRefId: ${orgRefId}`,
        );
      } else {
        await this.prismaService.invoice.update({
          where: { id: invoiceId },
          data: {
            misaSyncStatus: 'FAILED',
            misaOrgRefId: orgRefId,
            misaSyncRetries: { increment: 1 },
            misaErrorMessage: result.message,
          },
        });

        this.logger.error(
          `❌ Failed to send voucher to Misa for invoice ${invoice.code}: ${result.message}`,
        );
      }

      return {
        success: result.success,
        orgRefId: orgRefId,
        message: result.message,
      };
    } catch (error) {
      this.logger.error(
        `❌ Error creating Misa voucher for invoice ${invoiceId}: ${error.message}`,
      );

      // Cập nhật trạng thái FAILED
      await this.prismaService.invoice.update({
        where: { id: invoiceId },
        data: {
          misaSyncStatus: 'FAILED',
          misaSyncRetries: { increment: 1 },
          misaErrorMessage: error.message,
        },
      });

      return {
        success: false,
        orgRefId: null,
        message: error.message,
      };
    }
  }

  /**
   * Build payload cho voucher Misa
   */
  private async buildVoucherPayload(
    invoice: any,
    orgRefId: string,
  ): Promise<MisaSaveVoucherRequestDto | null> {
    const appId = this.configService.get<string>('MISA_APP_ID');
    const orgCompanyCode = this.configService.get<string>(
      'MISA_ORG_COMPANY_CODE',
    );
    const branchId = this.configService.get<string>('MISA_BRANCH_ID');

    // Tìm stock theo branchName
    let stock = await this.misaDictionaryService.findStockByNameFuzzy(
      invoice.branch?.name || '',
    );

    // Fallback default stock
    if (!stock) {
      stock = await this.misaDictionaryService.getDefaultStock();
    }

    if (!stock) {
      this.logger.error('❌ No stock found for invoice');
      return null;
    }

    // Tìm account object (khách hàng)
    const accountObject =
      await this.misaDictionaryService.findAccountObjectByNameFuzzy(
        invoice.customerName || invoice.customer?.name || '',
      );

    // Build details
    const details: MisaSaInvoiceDetailDto[] = [];

    for (let i = 0; i < invoice.invoiceDetails.length; i++) {
      const detail = invoice.invoiceDetails[i];
      const product = detail.product;

      // Tìm inventory item từ cache
      const inventoryItem =
        await this.misaDictionaryService.findInventoryItemByCode(
          product.misa_code,
        );

      if (!inventoryItem) {
        this.logger.warn(
          `⚠️ Inventory item not found for misa_code: ${product.misa_code}`,
        );
        continue;
      }

      const quantity = detail.quantity;
      const unitPrice = Number(detail.price);
      const discountAmount = Number(detail.discount || 0);
      const discountRate = detail.discountRatio || 0;
      const amountBeforeDiscount = quantity * unitPrice;
      const amount = amountBeforeDiscount - discountAmount;
      const vatAmount = (amount * this.VAT_RATE) / 100;
      const unitPriceAfterTax = unitPrice * (1 + this.VAT_RATE / 100);
      const mainUnitPrice = amount / quantity;

      details.push({
        inventory_item_id: inventoryItem.inventoryItemId,
        inventory_item_code: inventoryItem.inventoryItemCode,
        inventory_item_name: inventoryItem.inventoryItemName,
        inventory_item_type: 0, // Hàng hóa
        description: inventoryItem.inventoryItemName,

        unit_id: inventoryItem.unitId || undefined,
        unit_name: inventoryItem.unitName || product.misa_unit,
        main_unit_id: inventoryItem.unitId || undefined,
        main_unit_name: inventoryItem.unitName || product.misa_unit,

        quantity: quantity,
        main_quantity: quantity,
        main_convert_rate: 1,

        unit_price: unitPrice,
        unit_price_after_tax: unitPriceAfterTax,
        main_unit_price: mainUnitPrice,
        amount_oc: amount,
        amount: amount,

        discount_rate: discountRate,
        discount_amount_oc: discountAmount,
        discount_amount: discountAmount,

        vat_rate: this.VAT_RATE,
        other_vat_rate: 0,
        vat_amount_oc: vatAmount,
        vat_amount: vatAmount,

        stock_id: stock.stockId,
        stock_code: stock.stockCode,
        stock_name: stock.stockName,

        account_object_id: accountObject?.accountObjectId,
        account_object_code: accountObject?.accountObjectCode,
        account_object_name:
          accountObject?.accountObjectName ||
          invoice.customerName ||
          invoice.customer?.name,

        sort_order: i + 1,
        exchange_rate_operator: '*',
        is_promotion: false,
        not_in_vat_declaration: false,
      });
    }

    if (details.length === 0) {
      this.logger.error('❌ No valid details for voucher');
      return null;
    }

    const now = new Date();
    const invDate = this.formatDateForMisa(invoice.purchaseDate);
    const createdDate = this.formatDateForMisa(now);

    // Build voucher
    const voucher: MisaSaInvoiceDto = {
      voucher_type: this.VOUCHER_TYPE,
      org_refid: orgRefId,
      org_refno: invoice.code,
      org_reftype: null,
      org_reftype_name: 'Hóa đơn bán hàng hóa, dịch vụ trong nước',
      branch_id: branchId || '',
      reftype: this.REFTYPE,

      inv_date: invDate,
      is_posted: false,

      account_object_id: accountObject?.accountObjectId,
      account_object_code: accountObject?.accountObjectCode,
      account_object_name:
        accountObject?.accountObjectName ||
        invoice.customerName ||
        invoice.customer?.name ||
        'Khách lẻ',
      account_object_address: invoice.customer?.address || '',
      account_object_tax_code: invoice.customer?.taxCode || '',
      account_object_bank_account: '',

      employee_id: '',
      employee_code: '',
      employee_name: '',

      discount_type: 2, // Theo % hóa đơn
      discount_rate_voucher: invoice.discountRatio || 0,

      container_no: invoice.code,
      exchange_rate: 1,
      currency_id: 'VND',
      payment_method: 'TM/CK',
      buyer: invoice.customerName || invoice.customer?.name || 'Khách lẻ',

      is_created_savoucher: 1,
      invoice_type: 0,
      include_invoice: 0,

      created_date: createdDate,
      created_by: this.DEFAULT_CREATED_BY,
      modified_date: createdDate,
      modified_by: this.DEFAULT_CREATED_BY,

      detail: details,
    };

    return {
      app_id: appId || '',
      org_company_code: orgCompanyCode || '',
      voucher: [voucher],
    };
  }

  /**
   * Format date cho Misa API (YYYY-MM-DD HH:mm:ss)
   */
  private formatDateForMisa(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

  /**
   * Gửi voucher lên Misa API
   */
  private async sendVoucherToMisa(
    payload: MisaSaveVoucherRequestDto,
  ): Promise<{ success: boolean; message: string }> {
    const baseUrl = this.configService.get<string>('MISA_BASE_URL');
    const accessToken = await this.misaAuthService.getAccessToken();

    const url = `${baseUrl}/apir/sync/actopen/save`;

    try {
      const response = await firstValueFrom(
        this.httpService.post<MisaSaveVoucherResponseDto>(url, payload, {
          headers: {
            'Content-Type': 'application/json',
            'X-MISA-AccessToken': accessToken,
          },
        }),
      );

      const data = response.data;

      if (data.Success) {
        return {
          success: true,
          message: data.Data || 'Voucher queued successfully',
        };
      } else {
        return {
          success: false,
          message: `${data.ErrorCode}: ${data.ErrorMessage}`,
        };
      }
    } catch (error) {
      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Xử lý callback từ Misa
   */
  async handleMisaCallback(
    orgRefId: string,
    status: 'success' | 'failed',
    voucherId?: string,
    voucherNo?: string,
    errorCode?: string,
    errorMessage?: string,
  ): Promise<void> {
    this.logger.log(
      `📩 Received Misa callback for orgRefId: ${orgRefId}, status: ${status}`,
    );

    const invoice = await this.prismaService.invoice.findUnique({
      where: { misaOrgRefId: orgRefId },
    });

    if (!invoice) {
      this.logger.warn(`⚠️ Invoice not found for orgRefId: ${orgRefId}`);
      return;
    }

    if (status === 'success') {
      await this.prismaService.invoice.update({
        where: { id: invoice.id },
        data: {
          misaSyncStatus: 'SYNCED',
          misaCallbackReceivedAt: new Date(),
          misaConfirmed: true,
          misaErrorMessage: null,
        },
      });

      this.logger.log(
        `✅ Invoice ${invoice.code} confirmed synced to Misa (voucherId: ${voucherId}, voucherNo: ${voucherNo})`,
      );
    } else {
      await this.prismaService.invoice.update({
        where: { id: invoice.id },
        data: {
          misaSyncStatus: 'FAILED',
          misaCallbackReceivedAt: new Date(),
          misaConfirmed: false,
          misaErrorMessage: `${errorCode}: ${errorMessage}`,
        },
      });

      this.logger.error(
        `❌ Invoice ${invoice.code} failed to sync to Misa: ${errorCode} - ${errorMessage}`,
      );
    }
  }

  /**
   * Retry sync các invoice FAILED
   */
  async retryFailedInvoices(limit: number = 10): Promise<number> {
    const failedInvoices = await this.prismaService.invoice.findMany({
      where: {
        misaSyncStatus: 'FAILED',
        misaSyncRetries: { lt: 3 }, // Tối đa 3 lần retry
      },
      take: limit,
      orderBy: { misaSyncedAt: 'asc' },
    });

    let successCount = 0;

    for (const invoice of failedInvoices) {
      const result = await this.createSaleVoucherFromInvoice(invoice.id);
      if (result.success) {
        successCount++;
      }
    }

    this.logger.log(
      `🔄 Retried ${failedInvoices.length} failed invoices, ${successCount} succeeded`,
    );

    return successCount;
  }
}
