// src/services/lark/lark-base.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as lark from '@larksuiteoapi/node-sdk';

@Injectable()
export class LarkBaseService {
  private readonly logger = new Logger(LarkBaseService.name);
  private client: lark.Client;

  // Customer
  private readonly customerBaseToken: string;
  private readonly customerTableId: string;

  // Invoice
  private readonly invoiceBaseToken: string;
  private readonly invoiceTableId: string;

  // Order
  private readonly orderBaseToken: string;
  private readonly orderTableId: string;

  constructor(private readonly configService: ConfigService) {
    const appId = this.configService.get<string>('LARK_CUSTOMER_SYNC_APP_ID');

    const appSecret = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_APP_SECRET',
    );

    const customerBaseToken = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_BASE_TOKEN',
    );

    const customerTableId = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_TABLE_ID',
    );

    const invoiceBaseToken = this.configService.get<string>(
      'LARK_INVOICE_SYNC_BASE_TOKEN',
    );

    const invoiceTableId = this.configService.get<string>(
      'LARK_INVOICE_SYNC_TABLE_ID',
    );

    const orderBaseToken = this.configService.get<string>(
      'LARK_ORDER_SYNC_BASE_TOKEN',
    );

    const orderTableId = this.configService.get<string>(
      'LARK_ORDER_SYNC_TABLE_ID',
    );

    if (!customerBaseToken || !customerTableId) {
      throw new Error('LARK customer environment vairable is not configution');
    }
    if (!invoiceBaseToken || !invoiceTableId) {
      throw new Error('LARK invoice environment variable is not configuraion');
    }
    if (!orderBaseToken || !orderTableId) {
      throw new Error('LARK order environment variable is not configution');
    }

    this.customerBaseToken = customerBaseToken;
    this.customerTableId = customerTableId;

    this.invoiceBaseToken = invoiceBaseToken;
    this.invoiceTableId = invoiceTableId;

    this.orderBaseToken = orderBaseToken;
    this.orderTableId = orderTableId;

    if (
      !appId ||
      !appSecret ||
      !this.customerBaseToken ||
      !this.customerTableId ||
      !this.invoiceBaseToken ||
      !this.invoiceTableId ||
      !this.orderBaseToken ||
      !this.orderTableId
    ) {
      throw new Error('Missing LarkSuite configuration');
    }

    this.client = new lark.Client({
      appId,
      appSecret,
      appType: lark.AppType.SelfBuild,
      domain: lark.Domain.Feishu,
    });
  }

  // ===== CUSTOMER DIRECT METHODS =====
  async directCreateCustomers(
    customers: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      const records = customers
        .map((customer) => this.mapCustomerToLarkBase(customer))
        .filter((record) => record.fields['Tên Khách Hàng'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: customers.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = customers.length - successCount;

      this.logger.log(
        `LarkBase customer direct create: ${successCount} success, ${failedCount} failed`,
      );

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(
        `LarkBase customer direct create failed: ${error.message}`,
      );
      return { success: 0, failed: customers.length };
    }
  }

  async directUpdateCustomers(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!customers.length) return { success: 0, failed: 0 };

    try {
      const records = customers
        .filter((customer) => customer.larkRecordId)
        .map((customer) => ({
          record_id: customer.larkRecordId,
          fields: this.mapCustomerToLarkBase(customer).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: customers.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.customerBaseToken,
          table_id: this.customerTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = customers.length - successCount;

      this.logger.log(
        `LarkBase customer direct update: ${successCount} success, ${failedCount} failed`,
      );

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `LarkBase customer direct update failed: ${error.message}`,
      );
      return { success: 0, failed: customers.length };
    }
  }

  // ===== ORDER DIRECT METHODS =====
  async directCreateOrders(
    orders: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!orders.length) return { success: 0, failed: 0 };

    try {
      const records = orders
        .map((order) =>
          this.mapOrderToLarkBase(
            order,
            order.branch?.name || null,
            order.customer?.name || null,
            order.soldBy?.userName || null,
          ),
        )
        .filter((record) => record.fields['Mã Đặt Hàng'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: orders.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = orders.length - successCount;

      this.logger.log(
        `LarkBase order direct create: ${successCount} success, ${failedCount} failed`,
      );

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(
        `LarkBase order direct create failed: ${error.message}`,
      );
      return { success: 0, failed: orders.length };
    }
  }

  async directUpdateOrders(
    orders: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!orders.length) return { success: 0, failed: 0 };

    try {
      const records = orders
        .filter((order) => order.larkRecordId)
        .map((order) => ({
          record_id: order.larkRecordId,
          fields: this.mapOrderToLarkBase(
            order,
            order.branch?.name || null,
            order.customer?.name || null,
            order.soldBy?.userName || null,
          ).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: orders.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.orderBaseToken,
          table_id: this.orderTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = orders.length - successCount;

      this.logger.log(
        `LarkBase order direct update: ${successCount} success, ${failedCount} failed`,
      );

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `LarkBase order direct update failed: ${error.message}`,
      );
      return { success: 0, failed: orders.length };
    }
  }

  // ===== INVOICE DIRECT METHODS =====
  async directCreateInvoices(
    invoices: any[],
  ): Promise<{ success: number; failed: number; records?: any[] }> {
    if (!invoices.length) return { success: 0, failed: 0 };

    try {
      const records = invoices
        .map((invoice) =>
          this.mapInvoiceToLarkBase(
            invoice,
            invoice.branch?.name || null,
            invoice.customer?.name || null,
            invoice.soldBy?.userName || null,
          ),
        )
        .filter((record) => record.fields['Mã Hoá Đơn'])
        .map((record) => ({ fields: record.fields }));

      if (!records.length) {
        return { success: 0, failed: invoices.length };
      }

      const response = await this.client.bitable.appTableRecord.batchCreate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = invoices.length - successCount;

      this.logger.log(
        `LarkBase invoice direct create: ${successCount} success, ${failedCount} failed`,
      );

      return {
        success: successCount,
        failed: failedCount,
        records: response.data?.records,
      };
    } catch (error) {
      this.logger.error(
        `LarkBase invoice direct create failed: ${error.message}`,
      );
      return { success: 0, failed: invoices.length };
    }
  }

  async directUpdateInvoices(
    invoices: any[],
  ): Promise<{ success: number; failed: number }> {
    if (!invoices.length) return { success: 0, failed: 0 };

    try {
      const records = invoices
        .filter((invoice) => invoice.larkRecordId)
        .map((invoice) => ({
          record_id: invoice.larkRecordId,
          fields: this.mapInvoiceToLarkBase(
            invoice,
            invoice.branch?.name || null,
            invoice.customer?.name || null,
            invoice.soldBy?.userName || null,
          ).fields,
        }));

      if (!records.length) {
        return { success: 0, failed: invoices.length };
      }

      const response = await this.client.bitable.appTableRecord.batchUpdate({
        path: {
          app_token: this.invoiceBaseToken,
          table_id: this.invoiceTableId,
        },
        data: { records },
      });

      const successCount = response.data?.records?.length || 0;
      const failedCount = invoices.length - successCount;

      this.logger.log(
        `LarkBase invoice direct update: ${successCount} success, ${failedCount} failed`,
      );

      return { success: successCount, failed: failedCount };
    } catch (error) {
      this.logger.error(
        `LarkBase invoice direct update failed: ${error.message}`,
      );
      return { success: 0, failed: invoices.length };
    }
  }

  // ===== CUSTOMER MAPPING (Based on Khách Hàng.rtf) =====
  private mapCustomerToLarkBase(customerData: any): any {
    const fields: any = {};

    // Primary field - Tên Khách Hàng (REQUIRED)
    if (customerData.name) {
      fields['Tên Khách Hàng'] = customerData.name;
    }

    // Mã Khách Hàng
    if (customerData.code) {
      fields['Mã Khách Hàng'] = customerData.code;
    }

    // Số Điện Thoại
    if (customerData.contactNumber) {
      fields['Số Điện Thoại'] = customerData.contactNumber;
    }

    // Email của Khách Hàng
    if (customerData.email) {
      fields['Email của Khách Hàng'] = customerData.email;
    }

    // Địa Chỉ Khách Hàng
    if (customerData.address) {
      fields['Địa Chỉ Khách Hàng'] = customerData.address;
    }

    // kiotvietId (IMPORTANT for deduplication)
    if (customerData.id || customerData.kiotVietId) {
      fields['kiotvietId'] = Number(customerData.id || customerData.kiotVietId);
    }

    // Nợ Hiện Tại
    if (customerData.debt !== null && customerData.debt !== undefined) {
      fields['Nợ Hiện Tại'] = Number(customerData.debt);
    }

    // Tổng Bán
    if (
      customerData.totalInvoiced !== null &&
      customerData.totalInvoiced !== undefined
    ) {
      fields['Tổng Bán'] = Number(customerData.totalInvoiced);
    }

    // Tổng Doanh Thu
    if (
      customerData.totalRevenue !== null &&
      customerData.totalRevenue !== undefined
    ) {
      fields['Tổng Doanh Thu'] = Number(customerData.totalRevenue);
    }

    // Điểm Hiện Tại
    if (
      customerData.rewardPoint !== null &&
      customerData.rewardPoint !== undefined
    ) {
      fields['Điểm Hiện Tại'] = Number(customerData.rewardPoint);
    }

    // Tổng Điểm
    if (
      customerData.totalPoint !== null &&
      customerData.totalPoint !== undefined
    ) {
      fields['Tổng Điểm'] = Number(customerData.totalPoint);
    }

    // Công Ty
    if (customerData.organization) {
      fields['Công Ty'] = customerData.organization;
    }

    // Ghi Chú
    if (customerData.comments) {
      fields['Ghi Chú'] = customerData.comments;
    }

    // Id Cửa Hàng
    if (customerData.retailerId) {
      fields['Id Cửa Hàng'] = customerData.retailerId.toString();
    }

    // Thời Gian Tạo
    if (customerData.createdDate) {
      const vietnamDate = new Date(customerData.createdDate + '+07:00');
      fields['Thời Gian Tạo'] = vietnamDate.getTime();
    }

    // Thời Gian Cập Nhật
    if (customerData.modifiedDate) {
      const vietnamDate = new Date(customerData.modifiedDate + '+07:00');
      fields['Thời Gian Cập Nhật'] = vietnamDate.getTime();
    }

    // Giới Tính (Single Select)
    if (customerData.gender !== null && customerData.gender !== undefined) {
      fields['Giới Tính'] = customerData.gender ? 'Nam' : 'Nữ';
    }

    // Khu Vực
    if (customerData.locationName) {
      fields['Khu Vực'] = customerData.locationName;
    }

    // Phường xã
    if (customerData.wardName) {
      fields['Phường xã'] = customerData.wardName;
    }

    // Mã Số Thuế
    if (customerData.taxCode) {
      fields['Mã Số Thuế'] = customerData.taxCode;
    }

    // Facebook Khách Hàng
    if (
      customerData.psidFacebook !== null &&
      customerData.psidFacebook !== undefined
    ) {
      fields['Facebook Khách Hàng'] = Number(customerData.psidFacebook);
    }

    return { fields };
  }

  // ===== ORDER MAPPING (Based on Đơn Hàng.rtf) =====
  private mapOrderToLarkBase(
    orderData: any,
    branchName?: string | null,
    customerName?: string | null,
    userName?: string | null,
  ): any {
    const fields: any = {};

    // Primary field - Mã Đặt Hàng (REQUIRED)
    if (orderData.code) {
      fields['Mã Đặt Hàng'] = orderData.code;
    }

    // Chi Nhánh - mapped from branchName
    if (branchName) {
      fields['Chi Nhánh'] = branchName;
    }

    // Tên Khách Hàng
    if (customerName) {
      fields['Tên Khách Hàng'] = customerName;
    }

    // Người bán
    if (userName) {
      fields['Người bán'] = userName;
    }

    // Khách Cần Trả
    if (orderData.total !== null && orderData.total !== undefined) {
      fields['Khách Cần Trả'] = Number(orderData.total);
    }

    // Khách Đã Trả
    if (
      orderData.totalPayment !== null &&
      orderData.totalPayment !== undefined
    ) {
      fields['Khách Đã Trả'] = Number(orderData.totalPayment);
    }

    // Tình Trạng Đặt Hàng
    if (orderData.status !== null && orderData.status !== undefined) {
      const statusMap = {
        1: 'Hoàn thành',
        2: 'Đã hủy',
        3: 'Đang xử lý',
        4: 'Chờ xử lý',
        5: 'Không giao được',
      };
      fields['Tình Trạng Đặt Hàng'] =
        statusMap[orderData.status] || 'Không xác định';
    }

    // Thu Khác (from surcharges)
    let thuKhac = 0;
    if (orderData.orderSurcharges && orderData.orderSurcharges.length > 0) {
      thuKhac = orderData.orderSurcharges.reduce((sum: any, surcharge: any) => {
        return sum + Number(surcharge.price || 0);
      }, 0);
    }
    fields['Thu Khác'] = Number(thuKhac || 0);

    // Giảm Giá
    const giamGia = Number(orderData.discount || 0);
    fields['Giảm Giá'] = giamGia;

    // Tổng Tiền Hàng = Khách cần trả + Giảm giá - Thu khác
    const khachCanTra = Number(orderData.total || 0);
    const tongTienHang = khachCanTra + giamGia - thuKhac;
    fields['Tổng Tiền Hàng'] = tongTienHang;

    // Tổng Sau Giảm Giá = Tổng tiền hàng - Giảm giá
    const tongSauGiamGia = tongTienHang - giamGia;
    fields['Tổng Sau Giảm Giá'] = tongSauGiamGia;

    // Mã Hoá Đơn (from related invoices)
    if (orderData.invoices && orderData.invoices.length > 0) {
      const invoiceCodes = orderData.invoices.map((inv) => inv.code).join(', ');
      fields['Mã Hoá Đơn'] = invoiceCodes;
    }

    // Ghi Chú
    if (orderData.description) {
      fields['Ghi Chú'] = orderData.description;
    }

    // Ngày Mua
    if (orderData.purchaseDate) {
      const vietnamDate = new Date(orderData.purchaseDate + '+07:00');
      fields['Ngày Mua'] = vietnamDate.getTime();
    }

    // Ngày Tạo Đơn
    if (orderData.createdDate) {
      const vietnamDate = new Date(orderData.createdDate + '+07:00');
      fields['Ngày Tạo Đơn'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật
    if (orderData.modifiedDate) {
      const vietnamDate = new Date(orderData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // Số Điện Thoại (from orderDelivery)
    if (orderData.orderDelivery && orderData.orderDelivery.contactNumber) {
      fields['Số Điện Thoại'] = orderData.orderDelivery.contactNumber;
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (orderData.id || orderData.kiotVietId) {
      fields['kiotVietId'] = Number(orderData.id || orderData.kiotVietId);
    }

    return { fields };
  }

  // ===== INVOICE MAPPING (Based on Hoá Đơn.rtf) =====
  private mapInvoiceToLarkBase(
    invoiceData: any,
    branchName?: string | null,
    customerName?: string | null,
    userName?: string | null,
  ): any {
    const fields: any = {};

    // Primary field - Mã Hoá Đơn (REQUIRED)
    if (invoiceData.code) {
      fields['Mã Hoá Đơn'] = invoiceData.code;
    }

    // Mã Đơn Hàng
    if (invoiceData.orderCode) {
      fields['Mã Đơn Hàng'] = invoiceData.orderCode;
    }

    // Ngày Mua
    if (invoiceData.purchaseDate) {
      const vietnamDate = new Date(invoiceData.purchaseDate + '+07:00');
      fields['Ngày Mua'] = vietnamDate.getTime();
    }

    // Chi Nhánh
    if (branchName) {
      fields['Chi Nhánh'] = branchName;
    }

    // Tên Khách Hàng
    if (customerName) {
      fields['Tên Khách Hàng'] = customerName;
    }

    // Người Bán
    if (userName) {
      fields['Người Bán'] = userName;
    }

    // Tổng Sau Giảm Giá
    if (invoiceData.total !== null && invoiceData.total !== undefined) {
      fields['Tổng Sau Giảm Giá'] = Number(invoiceData.total);
    }

    // Tổng Tiền Hàng (calculate total before discount)
    if (invoiceData.total !== null && invoiceData.total !== undefined) {
      const total = Number(invoiceData.total);
      const discount = Number(invoiceData.discount || 0);
      const totalBeforeDiscount = total + discount;
      fields['Tổng Tiền Hàng'] = totalBeforeDiscount;
    }

    // Khách Đã Trả
    if (
      invoiceData.totalPayment !== null &&
      invoiceData.totalPayment !== undefined
    ) {
      fields['Khách Đã Trả'] = Number(invoiceData.totalPayment);
    }

    // Discount
    if (invoiceData.discount !== null && invoiceData.discount !== undefined) {
      fields['Discount'] = Number(invoiceData.discount);
    }

    // Mức Độ Giảm Giá
    if (
      invoiceData.discountRatio !== null &&
      invoiceData.discountRatio !== undefined
    ) {
      fields['Mức Độ Giảm Giá'] = Number(invoiceData.discountRatio);
    }

    // Ghi Chú
    if (invoiceData.description) {
      fields['Ghi Chú'] = invoiceData.description;
    }

    // Ngày Tạo
    if (invoiceData.createdDate) {
      const vietnamDate = new Date(invoiceData.createdDate + '+07:00');
      fields['Ngày Tạo'] = vietnamDate.getTime();
    }

    // Ngày Cập Nhật
    if (invoiceData.modifiedDate) {
      const vietnamDate = new Date(invoiceData.modifiedDate + '+07:00');
      fields['Ngày Cập Nhật'] = vietnamDate.getTime();
    }

    // kiotVietId (IMPORTANT for deduplication)
    if (invoiceData.id || invoiceData.kiotVietId) {
      fields['kiotVietId'] = Number(invoiceData.id || invoiceData.kiotVietId);
    }

    return { fields };
  }

  // ===== LEGACY METHODS (Keep for backward compatibility) =====
  async syncCustomersToLarkBase(
    customers: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateCustomers(customers);
  }

  async syncOrdersToLarkBase(
    orders: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateOrders(orders);
  }

  async syncInvoicesToLarkBase(
    invoices: any[],
  ): Promise<{ success: number; failed: number }> {
    return this.directCreateInvoices(invoices);
  }
}
