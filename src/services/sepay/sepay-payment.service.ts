import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietAuthService } from '../kiot-viet/auth.service';
import { WebhookService } from '../webhook/webhook.service';

interface ProcessResult {
  documentId: bigint;
  customerId: bigint | null;
  branchId: number;
  accountId: number;
  paymentId?: bigint;
  paymentCode?: string;
  dryRun: boolean;
}

type WrittenResult = Omit<ProcessResult, 'dryRun'>;

@Injectable()
export class SePayPaymentService {
  private readonly logger = new Logger(SePayPaymentService.name);
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly live: boolean;
  private readonly accountMap: Record<string, string>;

  constructor(
    private readonly prisma: PrismaService,
    private readonly http: HttpService,
    private readonly auth: KiotVietAuthService,
    private readonly webhookService: WebhookService,
    config: ConfigService,
  ) {
    this.baseUrl = this.required(config, 'KIOT_BASE_URL').replace(/\/$/, '');
    this.timeoutMs = Number(config.get('KIOT_HTTP_TIMEOUT_MS') ?? 20000);
    this.live = config.get<string>('SEPAY_PAYMENT_MODE') === 'live';
    this.accountMap = this.parseAccountMap(
      config.get<string>('SEPAY_ACCOUNT_MAP') ?? '{}',
    );
  }

  async processInvoice(
    code: string,
    amount: number,
    accountNumber: string,
    subAccount: string | null,
    beforeWrite?: () => Promise<void>,
    afterWrite?: (result: WrittenResult) => Promise<void>,
  ): Promise<ProcessResult> {
    const invoice = await this.get(
      `/invoices/code/${encodeURIComponent(code)}`,
    );
    this.assertPayable(invoice, amount, code);
    const accountId = await this.resolveAccountId(accountNumber, subAccount);
    const common = this.commonResult(invoice, accountId);

    if (!this.live) return { ...common, dryRun: true };

    await beforeWrite?.();
    const response = await this.post('/payments', {
      amount,
      method: 'Transfer',
      accountId,
      invoiceId: invoice.id,
    });
    const written = {
      ...common,
      paymentId:
        response.paymentId == null ? undefined : BigInt(response.paymentId),
      paymentCode: response.paymentCode ?? undefined,
    };
    await afterWrite?.(written);
    await this.webhookService.refreshInvoiceByKiotVietId(Number(invoice.id));
    await this.refreshCustomer(invoice.customerId);

    return { ...written, dryRun: false };
  }

  async processOrder(
    code: string,
    amount: number,
    accountNumber: string,
    subAccount: string | null,
    beforeWrite?: () => Promise<void>,
    afterWrite?: (result: WrittenResult) => Promise<void>,
  ): Promise<ProcessResult> {
    const order = await this.get(`/orders/code/${encodeURIComponent(code)}`);
    this.assertPayable(order, amount, code);
    const accountId = await this.resolveAccountId(accountNumber, subAccount);
    const common = this.commonResult(order, accountId);

    if (!this.live) return { ...common, dryRun: true };

    const totalPayment = Number(order.totalPayment ?? 0) + amount;
    await beforeWrite?.();
    await this.put(
      `/orders/${order.id}`,
      this.orderPaymentPayload(order, totalPayment, accountId),
    );
    await afterWrite?.(common);
    await this.webhookService.refreshOrderByKiotVietId(Number(order.id));
    await this.refreshCustomer(order.customerId);

    return { ...common, dryRun: false };
  }

  private orderPaymentPayload(
    order: any,
    totalPayment: number,
    accountId: number,
  ): Record<string, unknown> {
    const delivery = order.orderDelivery;
    const surcharges = (order.invoiceOrderSurcharges ?? [])
      .filter((item: any) => item.surchargeId != null)
      .map((item: any) => ({
        id: item.surchargeId,
        price: item.price,
      }));
    return {
      purchaseDate: order.purchaseDate,
      branchId: order.branchId,
      soldById: order.soldById ?? undefined,
      cashierId: order.cashierId ?? undefined,
      discount: order.discount ?? 0,
      description: order.description ?? '',
      method: 'Transfer',
      totalPayment,
      accountId,
      makeInvoice: false,
      saleChannelId: order.saleChannelId ?? undefined,
      orderDetails: (order.orderDetails ?? []).map((line: any) => ({
        productId: line.productId,
        productCode: line.productCode,
        productName: line.productName,
        isMaster: line.isMaster ?? true,
        quantity: line.quantity,
        price: line.price,
        discount: line.discount ?? 0,
        discountRatio: line.discountRatio ?? 0,
        note: line.note ?? '',
      })),
      orderDelivery: delivery
        ? {
            deliveryCode: delivery.deliveryCode ?? undefined,
            type: delivery.type ?? undefined,
            price: delivery.price ?? undefined,
            receiver: delivery.receiver ?? undefined,
            contactNumber: delivery.contactNumber ?? undefined,
            address: delivery.address ?? undefined,
            locationId: delivery.locationId ?? undefined,
            locationName: delivery.locationName ?? undefined,
            wardName: delivery.wardName ?? undefined,
            weight: delivery.weight ?? undefined,
            length: delivery.length ?? undefined,
            width: delivery.width ?? undefined,
            height: delivery.height ?? undefined,
            partnerDeliveryId: delivery.partnerDeliveryId ?? undefined,
            expectedDelivery: delivery.expectedDelivery ?? undefined,
          }
        : undefined,
      surchages: surcharges.length > 0 ? surcharges : undefined,
    };
  }

  private commonResult(
    document: any,
    accountId: number,
  ): Omit<ProcessResult, 'dryRun'> {
    if (!document.branchId) throw new Error('KiotViet document has no branch');
    return {
      documentId: BigInt(document.id),
      customerId:
        document.customerId == null ? null : BigInt(document.customerId),
      branchId: Number(document.branchId),
      accountId,
    };
  }

  private assertPayable(document: any, amount: number, code: string): void {
    if (!document?.id || String(document.code).toUpperCase() !== code) {
      throw new Error(`KiotViet document ${code} not found`);
    }
    const status = String(document.statusValue ?? '').toLowerCase();
    if (status.includes('hủy') || status.includes('huy')) {
      throw new Error(`KiotViet document ${code} is cancelled`);
    }
    const remaining =
      Number(document.total ?? 0) - Number(document.totalPayment ?? 0);
    if (remaining <= 0)
      throw new Error(`KiotViet document ${code} is fully paid`);
    if (amount > remaining) {
      throw new Error(
        `Payment ${amount} exceeds remaining amount ${remaining} for ${code}`,
      );
    }
  }

  private async resolveAccountId(
    accountNumber: string,
    subAccount: string | null,
  ): Promise<number> {
    const directAccountNumber = this.normalizeAccount(accountNumber);
    const direct = await this.prisma.bankAccount.findFirst({
      where: { accountNumber: directAccountNumber, deletedAt: null },
      select: { kiotVietId: true },
    });
    if (direct) return direct.kiotVietId;

    const targetAccount = subAccount
      ? this.accountMap[this.normalizeAccount(subAccount)]
      : undefined;
    if (!targetAccount) {
      throw new Error(
        `No KiotViet bank account for accountNumber ${accountNumber}` +
          (subAccount ? ` or subAccount ${subAccount}` : ''),
      );
    }
    const fallback = await this.prisma.bankAccount.findFirst({
      where: { accountNumber: targetAccount, deletedAt: null },
      select: { kiotVietId: true },
    });
    if (!fallback) {
      throw new Error(`KiotViet bank account ${targetAccount} is not synced`);
    }
    return fallback.kiotVietId;
  }

  private async refreshCustomer(customerId: unknown): Promise<void> {
    if (customerId == null) return;
    await this.webhookService.refreshCustomerByKiotVietId(Number(customerId));
  }

  private async get(path: string): Promise<any> {
    return this.request('get', path);
  }

  private async post(path: string, body: any): Promise<any> {
    return this.request('post', path, body);
  }

  private async put(path: string, body: any): Promise<any> {
    return this.request('put', path, body);
  }

  private async request(
    method: 'get' | 'post' | 'put',
    path: string,
    body?: any,
  ): Promise<any> {
    const headers = await this.auth.getRequestHeaders();
    try {
      const response = await firstValueFrom(
        this.http.request({
          method,
          url: `${this.baseUrl}${path}`,
          data: body,
          headers: { ...headers, 'Content-Type': 'application/json' },
          timeout: this.timeoutMs,
        }),
      );
      return response.data;
    } catch (error: any) {
      const status = error?.response?.status;
      const responseData = error?.response?.data;
      const details = this.errorDetails(responseData);
      const message =
        `KiotViet ${method.toUpperCase()} ${path} failed` +
        (status ? ` (${status})` : '') +
        (details ? `: ${details}` : `: ${error?.message ?? 'Unknown error'}`);
      this.logger.error(message);
      throw new Error(message);
    }
  }

  private errorDetails(data: unknown): string {
    if (data == null) return '';
    if (typeof data === 'string') return data.slice(0, 1500);
    const value = data as any;
    const responseStatus = value.responseStatus ?? value.ResponseStatus;
    const parts = [
      responseStatus?.errorCode ?? responseStatus?.ErrorCode,
      responseStatus?.message ?? responseStatus?.Message,
      value.message ?? value.Message,
      value.error ?? value.Error,
    ].filter(Boolean);
    if (parts.length > 0) return parts.join(' - ').slice(0, 1500);
    try {
      return JSON.stringify(data).slice(0, 1500);
    } catch {
      return String(data).slice(0, 1500);
    }
  }

  private parseAccountMap(value: string): Record<string, string> {
    try {
      const parsed = JSON.parse(value);
      return Object.fromEntries(
        Object.entries(parsed).map(([key, account]) => [
          this.normalizeAccount(key),
          String(account).trim(),
        ]),
      );
    } catch {
      throw new Error('SEPAY_ACCOUNT_MAP must be a JSON object');
    }
  }

  private normalizeAccount(value: string): string {
    return String(value).replace(/\s+/g, '').toUpperCase();
  }

  private required(config: ConfigService, key: string): string {
    const value = config.get<string>(key);
    if (!value) throw new Error(`${key} is required`);
    return value;
  }
}
