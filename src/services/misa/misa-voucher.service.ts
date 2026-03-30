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
  MisaSaVoucherDto,
  MisaSaVoucherDetailDto,
  MisaSaveVoucherResponseDto,
  MisaDeleteVoucherRequestDto,
  MisaDeleteVoucherResponseDto,
} from './dto';

@Injectable()
export class MisaVoucherService {
  private readonly logger = new Logger(MisaVoucherService.name);

  // Constants
  private readonly VOUCHER_TYPE = 13; // Chứng từ bán hàng
  private readonly REFTYPE = 3530; // Bán hàng hóa, dịch vụ trong nước - Tiền mặt
  private readonly OUTWARD_REFTYPE = 2020; // Xuất kho bán hàng
  private readonly VAT_RATE = 8; // 8% VAT
  private readonly DEFAULT_CREATED_BY = 'Trần Ngọc Nhân';

  // Default accounts
  private readonly DEBIT_ACCOUNT = '131'; // TK Phải thu khách hàng
  private readonly CREDIT_ACCOUNT = '5111'; // TK Doanh thu bán hàng
  private readonly COST_ACCOUNT = '632'; // TK Giá vốn hàng bán

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prismaService: PrismaService,
    private readonly misaAuthService: MisaAuthService,
    private readonly misaDictionaryService: MisaDictionaryService,
  ) {}

  /**
   * Tạo chứng từ bán hàng Misa từ Invoice Code
   */
  async createSaleVoucherFromInvoice(invoiceCode: string): Promise<{
    success: boolean;
    orgRefId: string | null;
    message: string;
  }> {
    this.logger.log(
      `🧾 Creating Misa voucher for invoice code: ${invoiceCode}`,
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
          message: `Invoice not found: ${invoiceCode}`,
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
          where: { id: invoice.id },
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
          where: { id: invoice.id },
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
          where: { id: invoice.id },
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
        `❌ Error creating Misa voucher for invoice ${invoiceCode}: ${error.message}`,
      );

      // Cập nhật trạng thái FAILED
      const invoice = await this.prismaService.invoice.findUnique({
        where: { code: invoiceCode },
        select: { id: true },
      });

      if (invoice) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            misaSyncStatus: 'FAILED',
            misaSyncRetries: { increment: 1 },
            misaErrorMessage: error.message,
          },
        });
      }

      return {
        success: false,
        orgRefId: null,
        message: error.message,
      };
    }
  }

  /**
   * Build payload cho chứng từ bán hàng Misa (voucher_type = 13)
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

    // Build details và tính totals
    const details: MisaSaVoucherDetailDto[] = [];
    let totalSaleAmount = 0; // Tổng tiền hàng (trước thuế)
    let totalDiscountAmount = 0; // Tổng chiết khấu
    let totalVatAmount = 0; // Tổng thuế
    let totalAmount = 0; // Tổng tiền thanh toán (sau thuế)

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
      const originalPrice = Number(detail.price);
      const discountAmount = Number(detail.discount || 0);
      const discountRate = detail.discountRatio || 0;

      // Đơn giá sau thuế = (price * quantity - discount) / quantity
      const unitPriceAfterTax = originalPrice - discountAmount;

      // Đơn giá trước thuế = đơn giá sau thuế / (1 + VAT%)
      const unitPrice =
        Math.round((unitPriceAfterTax / (1 + this.VAT_RATE / 100)) * 100) / 100;

      // Thành tiền trước thuế
      const amountBeforeTax = Math.round(unitPrice * quantity * 100) / 100;

      // Tiền thuế GTGT
      const vatAmount = Math.trunc((amountBeforeTax * this.VAT_RATE) / 100);

      // Thành tiền sau thuế = trước thuế + thuế
      const amountAfterTax = amountBeforeTax + vatAmount;

      // Accumulate totals
      totalSaleAmount += amountBeforeTax;
      totalVatAmount += vatAmount;
      totalAmount += amountAfterTax;

      details.push({
        inventory_item_id: inventoryItem.inventoryItemId,
        inventory_item_code: inventoryItem.inventoryItemCode,
        inventory_item_name: inventoryItem.inventoryItemName,
        inventory_item_type: 0,
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
        main_unit_price: unitPrice,
        amount_oc: amountBeforeTax,
        amount: amountBeforeTax,

        discount_rate: 0,
        discount_amount_oc: 0,
        discount_amount: 0,

        vat_rate: this.VAT_RATE,
        vat_amount_oc: vatAmount,
        vat_amount: vatAmount,

        debit_account: this.DEBIT_ACCOUNT,
        credit_account: this.CREDIT_ACCOUNT,
        cost_account: this.COST_ACCOUNT,

        stock_id: stock.stockId,
        stock_code: stock.stockCode,
        stock_name: stock.stockName,

        sort_order: i + 1,
        exchange_rate_operator: '*',
        is_promotion: false,
        is_description: false,
      });
    }

    if (details.length === 0) {
      this.logger.error('❌ No valid details for voucher');
      return null;
    }

    // Calculate total amount
    const now = new Date();
    const postedDate = this.formatDateForMisa(invoice.purchaseDate);
    const refDate = this.formatDateForMisa(invoice.purchaseDate);
    const inRefOrder = this.formatDateForMisa(invoice.purchaseDate);
    const createdDate = this.formatDateForMisa(now);

    // Build voucher
    const voucher: MisaSaVoucherDto = {
      voucher_type: this.VOUCHER_TYPE,
      org_refid: orgRefId,
      org_refno: invoice.code,
      org_reftype: null,
      org_reftype_name: 'Chứng từ bán hàng hóa, dịch vụ trong nước',
      branch_id: branchId || '',
      reftype: this.REFTYPE,
      posted_date: postedDate,
      refdate: refDate,
      is_sale_with_outward: true,

      // Totals
      total_sale_amount_oc: totalSaleAmount,
      total_sale_amount: totalSaleAmount,
      total_amount_oc: totalAmount,
      total_amount: totalAmount,
      total_discount_amount_oc: totalDiscountAmount,
      total_discount_amount: totalDiscountAmount,
      total_vat_amount_oc: totalVatAmount,
      total_vat_amount: totalVatAmount,

      // Customer info
      account_object_id: accountObject?.accountObjectId,
      account_object_code: accountObject?.accountObjectCode,
      account_object_name:
        accountObject?.accountObjectName ||
        invoice.customerName ||
        invoice.customer?.name ||
        'Khách lẻ',
      account_object_address: invoice.customer?.address || '',
      account_object_tax_code: invoice.customer?.taxCode || '',

      // Employee info
      employee_id: '',
      employee_code: '',
      employee_name: '',

      // Discount
      discount_type: 0,
      discount_rate_voucher: 0,

      // Other
      exchange_rate: 1,
      currency_id: 'VND',
      include_invoice: 1,
      // payer: invoice.customerName || invoice.customer?.name || 'Khách lẻ',
      journal_memo: `Bán hàng - ${invoice.code}`,

      // Phiếu xuất kho
      in_outward: {
        branch_id: branchId || '',
        reftype: this.OUTWARD_REFTYPE,
        posted_date: postedDate,
        refdate: refDate,
        in_reforder: inRefOrder,
        account_object_id: accountObject?.accountObjectId,
        account_object_code: accountObject?.accountObjectCode,
        account_object_name:
          accountObject?.accountObjectName ||
          invoice.customerName ||
          invoice.customer?.name ||
          'Khách lẻ',
        account_object_address: invoice.customer?.address || '',
        employee_id: '',
        employee_code: '',
        employee_name: '',
        journal_memo: `Xuất kho bán hàng - ${invoice.code}`,
      },

      // Audit fields
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
      const result = await this.createSaleVoucherFromInvoice(invoice.code);
      if (result.success) {
        successCount++;
      }
    }

    this.logger.log(
      `🔄 Retried ${failedInvoices.length} failed invoices, ${successCount} succeeded`,
    );

    return successCount;
  }

  /**
   * Xóa chứng từ bán hàng trên Misa theo Invoice Code
   */
  async deleteVoucherByInvoiceCode(invoiceCode: string): Promise<{
    success: boolean;
    message: string;
  }> {
    this.logger.log(
      `🗑️ Deleting Misa voucher for invoice code: ${invoiceCode}`,
    );

    try {
      // 1. Tìm invoice trong database
      const invoice = await this.prismaService.invoice.findUnique({
        where: { code: invoiceCode },
        select: {
          id: true,
          code: true,
          misaOrgRefId: true,
          misaSyncStatus: true,
        },
      });

      if (!invoice) {
        return {
          success: false,
          message: `Invoice not found: ${invoiceCode}`,
        };
      }

      if (!invoice.misaOrgRefId) {
        return {
          success: false,
          message: `Invoice ${invoiceCode} has no misaOrgRefId. Never synced to Misa.`,
        };
      }

      // 2. Gọi API xóa trên Misa
      const result = await this.sendDeleteVoucherToMisa(invoice.misaOrgRefId);

      // 3. Cập nhật trạng thái trong database
      if (result.success) {
        await this.prismaService.invoice.update({
          where: { id: invoice.id },
          data: {
            misaSyncStatus: 'SKIP',
            misaOrgRefId: null,
            misaConfirmed: false,
            misaCallbackReceivedAt: null,
            misaSyncRetries: 0,
            misaErrorMessage: null,
          },
        });

        this.logger.log(`✅ Voucher deleted for invoice ${invoiceCode}`);
      } else {
        this.logger.error(
          `❌ Failed to delete voucher for invoice ${invoiceCode}: ${result.message}`,
        );
      }

      return result;
    } catch (error) {
      this.logger.error(
        `❌ Error deleting Misa voucher for invoice ${invoiceCode}: ${error.message}`,
      );

      return {
        success: false,
        message: error.message,
      };
    }
  }

  /**
   * Gửi request xóa voucher lên Misa API
   */
  private async sendDeleteVoucherToMisa(
    orgRefId: string,
  ): Promise<{ success: boolean; message: string }> {
    const baseUrl = this.configService.get<string>('MISA_BASE_URL');
    const appId = this.configService.get<string>('MISA_APP_ID');
    const orgCompanyCode = this.configService.get<string>(
      'MISA_ORG_COMPANY_CODE',
    );
    const accessToken = await this.misaAuthService.getAccessToken();

    const url = `${baseUrl}/apir/sync/actopen/delete`;

    const payload: MisaDeleteVoucherRequestDto = {
      app_id: appId || '',
      org_company_code: orgCompanyCode || '',
      voucher: [
        {
          voucher_type: this.VOUCHER_TYPE,
          org_refid: orgRefId,
        },
      ],
    };

    try {
      const response = await firstValueFrom(
        this.httpService.delete<MisaDeleteVoucherResponseDto>(url, {
          headers: {
            'Content-Type': 'application/json',
            'X-MISA-AccessToken': accessToken,
          },
          data: payload,
        }),
      );

      const data = response.data;

      if (data.Success) {
        return {
          success: true,
          message: 'Voucher deleted successfully',
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
}
