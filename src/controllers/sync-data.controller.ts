import { Controller, Get, Param, Query, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Controller('sync-data')
export class SyncDataController {
  private readonly logger = new Logger(SyncDataController.name);
  private readonly DEFAULT_PAGE_SIZE = 200;

  constructor(private readonly prisma: PrismaService) {}

  // ========== HELPER ==========

  private parsePagination(query: { pageSize?: string; currentItem?: string }) {
    const pageSize = Math.min(
      parseInt(query.pageSize || `${this.DEFAULT_PAGE_SIZE}`, 10),
      500,
    );
    const currentItem = parseInt(query.currentItem || '0', 10);
    return { pageSize, skip: currentItem, take: pageSize };
  }

  private parseModifiedFrom(modifiedFrom?: string): Date | undefined {
    if (!modifiedFrom) return undefined;
    const date = new Date(modifiedFrom);
    return isNaN(date.getTime()) ? undefined : date;
  }

  // ========== LAYER 1: Dữ liệu nền tảng ==========

  @Get('branches')
  async getBranches(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.branch.findMany({
        where,
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.branch.count({ where }),
    ]);

    return { data, total, pageSize: take, currentItem: skip };
  }

  @Get('branches/:code')
  async getBranchByCode(@Param('code') code: string) {
    // sync_kiot Branch không có code, dùng name hoặc kiotVietId
    const branch = await this.prisma.branch.findFirst({
      where: {
        OR: [{ name: code }, { kiotVietId: isNaN(+code) ? undefined : +code }],
      },
    });
    return branch || null;
  }

  @Get('users')
  async getUsers(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    // sync_kiot User không có modifiedDate, chỉ có createdDate
    const where = {};

    const [data, total] = await Promise.all([
      this.prisma.user.findMany({ where, skip, take, orderBy: { id: 'asc' } }),
      this.prisma.user.count({ where }),
    ]);

    // serialize BigInt → string cho JSON response
    const serialized = data.map((u) => ({
      ...u,
      kiotVietId: u.kiotVietId.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('sale-channels')
  async getSaleChannels() {
    const data = await this.prisma.saleChannel.findMany({
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  @Get('surcharges')
  async getSurcharges() {
    const data = await this.prisma.surcharge.findMany({
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  @Get('bank-accounts')
  async getBankAccounts() {
    const data = await this.prisma.bankAccount.findMany({
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  @Get('trademarks')
  async getTrademarks() {
    const data = await this.prisma.tradeMark.findMany({
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  @Get('customer-groups')
  async getCustomerGroups() {
    const data = await this.prisma.customerGroup.findMany({
      include: { CustomerGroupRelation: true },
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  // ========== LAYER 2: Phụ thuộc Layer 1 ==========

  @Get('customers')
  async getCustomers(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.customer.findMany({
        where,
        include: { CustomerGroupRelation: true },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.customer.count({ where }),
    ]);

    const serialized = data.map((c) => ({
      ...c,
      kiotVietId: c.kiotVietId?.toString(),
      rewardPoint: c.rewardPoint?.toString(),
      psidFacebook: c.psidFacebook?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('customers/:code')
  async getCustomerByCode(@Param('code') code: string) {
    const customer = await this.prisma.customer.findFirst({
      where: { code },
      include: { CustomerGroupRelation: true },
    });
    if (!customer) return null;
    return {
      ...customer,
      kiotVietId: customer.kiotVietId?.toString(),
      rewardPoint: customer.rewardPoint?.toString(),
      psidFacebook: customer.psidFacebook?.toString(),
    };
  }

  @Get('products')
  async getProducts(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.product.findMany({
        where,
        include: { inventories: true },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.product.count({ where }),
    ]);

    const serialized = data.map((p) => ({
      ...p,
      kiotVietId: p.kiotVietId?.toString(),
      masterProductId: p.masterProductId?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('products/:code')
  async getProductByCode(@Param('code') code: string) {
    const product = await this.prisma.product.findFirst({
      where: { code },
      include: { inventories: true },
    });
    if (!product) return null;
    return {
      ...product,
      kiotVietId: product.kiotVietId?.toString(),
      masterProductId: product.masterProductId?.toString(),
    };
  }

  @Get('suppliers')
  async getSuppliers(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.supplier.findMany({
        where,
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.supplier.count({ where }),
    ]);

    const serialized = data.map((s) => ({
      ...s,
      kiotVietId: s.kiotVietId?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('price-books')
  async getPriceBooks() {
    const data = await this.prisma.priceBook.findMany({
      include: {
        details: true,
        branches: true,
        customerGroups: true,
        users: true,
      },
      orderBy: { id: 'asc' },
    });
    return { data, total: data.length };
  }

  // ========== LAYER 3: Giao dịch ==========

  @Get('orders')
  async getOrders(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.order.findMany({
        where,
        include: {
          orderDetails: true,
          orderDelivery: true,
          orderSurcharges: true,
          payments: true,
        },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.order.count({ where }),
    ]);

    const serialized = data.map((o) => ({
      ...o,
      kiotVietId: o.kiotVietId.toString(),
      soldById: o.soldById?.toString(),
      cashierId: o.cashierId?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('orders/:code')
  async getOrderByCode(@Param('code') code: string) {
    const order = await this.prisma.order.findFirst({
      where: { code },
      include: {
        orderDetails: true,
        orderDelivery: true,
        orderSurcharges: true,
        payments: true,
      },
    });
    if (!order) return null;
    return {
      ...order,
      kiotVietId: order.kiotVietId.toString(),
      soldById: order.soldById?.toString(),
      cashierId: order.cashierId?.toString(),
    };
  }

  @Get('invoices')
  async getInvoices(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.invoice.findMany({
        where,
        include: {
          invoiceDetails: true,
          invoiceDelivery: true,
          invoiceSurcharges: true,
          payments: true,
        },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.invoice.count({ where }),
    ]);

    const serialized = data.map((inv) => ({
      ...inv,
      kiotVietId: inv.kiotVietId.toString(),
      soldById: inv.soldById?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('invoices/:code')
  async getInvoiceByCode(@Param('code') code: string) {
    const invoice = await this.prisma.invoice.findFirst({
      where: { code },
      include: {
        invoiceDetails: true,
        invoiceDelivery: true,
        invoiceSurcharges: true,
        payments: true,
      },
    });
    if (!invoice) return null;
    return {
      ...invoice,
      kiotVietId: invoice.kiotVietId.toString(),
      soldById: invoice.soldById?.toString(),
    };
  }

  @Get('transfers')
  async getTransfers(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const where = modifiedFrom
      ? { lastSyncedAt: { gte: new Date(modifiedFrom) } }
      : {};

    const [data, total] = await Promise.all([
      this.prisma.transfer.findMany({
        where,
        include: { details: true },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.transfer.count({ where }),
    ]);

    const serialized = data.map((t) => ({
      ...t,
      kiotVietId: t.kiotVietId.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('cashflows')
  async getCashflows(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const where = modifiedFrom
      ? { lastSyncedAt: { gte: new Date(modifiedFrom) } }
      : {};

    const [data, total] = await Promise.all([
      this.prisma.cashflow.findMany({
        where,
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.cashflow.count({ where }),
    ]);

    const serialized = data.map((c) => ({
      ...c,
      kiotVietId: c.kiotVietId.toString(),
      createdBy: c.createdBy?.toString(),
      partnerId: c.partnerId?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('returns')
  async getReturns(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.return.findMany({
        where,
        include: { details: true, payments: true },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.return.count({ where }),
    ]);

    const serialized = data.map((r) => ({
      ...r,
      kiotVietId: r.kiotVietId.toString(),
      receivedById: r.receivedById?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('purchase-orders')
  async getPurchaseOrders(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { modifiedDate: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.purchaseOrder.findMany({
        where,
        include: { details: true, surcharges: true, payments: true },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.purchaseOrder.count({ where }),
    ]);

    const serialized = data.map((po) => ({
      ...po,
      kiotVietId: po.kiotVietId.toString(),
      purchaseById: po.purchaseById?.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }

  @Get('order-suppliers')
  async getOrderSuppliers(
    @Query('modifiedFrom') modifiedFrom?: string,
    @Query('pageSize') pageSize?: string,
    @Query('currentItem') currentItem?: string,
  ) {
    const { skip, take } = this.parsePagination({ pageSize, currentItem });
    const since = this.parseModifiedFrom(modifiedFrom);

    const where = since ? { lastSyncedAt: { gte: since } } : {};

    const [data, total] = await Promise.all([
      this.prisma.orderSupplier.findMany({
        where,
        include: {
          orderSupplierDetails: true,
          orderSupplierExpenses: true,
          purchaseOrderLinks: true,
        },
        skip,
        take,
        orderBy: { id: 'asc' },
      }),
      this.prisma.orderSupplier.count({ where }),
    ]);

    const serialized = data.map((os) => ({
      ...os,
      kiotVietId: os.kiotVietId.toString(),
    }));

    return { data: serialized, total, pageSize: take, currentItem: skip };
  }
}
