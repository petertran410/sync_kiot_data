import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { createHash } from 'crypto';
import { PrismaService } from '../../prisma/prisma.service';
import { KiotVietAuthService } from '../kiot-viet/auth.service';
import { RetailerContext } from '../kiot-viet/shared/retailer-context';
import { WEBHOOK_EVENT_TYPES, WebhookEventType } from './webhook-event.types';

/** One subscription as KiotViet reports it. */
export interface RemoteWebhook {
  id: number;
  type: string;
  url: string;
  isActive: boolean;
  description?: string | null;
  retailerId?: number | null;
}

export interface ReconcileReport {
  baseUrl: string;
  expected: number;
  /** Registered, active, and pointing at the right URL. */
  healthy: string[];
  /** Not registered at all. */
  missing: string[];
  /** Registered but `isActive === false` — KiotViet has stopped delivering. */
  inactive: string[];
  /** Registered but pointing somewhere else (e.g. an old deployment). */
  wrongUrl: Array<{ type: string; url: string; expectedUrl: string }>;
  /** Subscriptions we did not create, left untouched. */
  unknown: Array<{ id: number; type: string; url: string }>;
  repaired: string[];
  errors: Array<{ type: string; error: string }>;
}

/**
 * Manages KiotViet webhook subscriptions (doc 2.11.1, 2.11.2, 2.11.11, 2.11.12).
 *
 * There was previously no registration code anywhere in the project: subscriptions had
 * to be created by hand in the KiotViet admin UI, the `Webhook` model was dead code with
 * zero references, and nothing detected a subscription that KiotViet had disabled.
 *
 * That last point is the important one. Per doc 2.11.1, ANY 4xx response — including the
 * 401 the doc itself tells us to return on a signature mismatch — makes KiotViet stop
 * delivering to the endpoint permanently. A single secret mismatch therefore silently
 * kills the subscription with no error anywhere on our side. `reconcile()` is what
 * surfaces that, and `reconcile({ repair: true })` re-creates the dead subscriptions.
 *
 * On secrets: the shared secret is NOT persisted. Only a short SHA-256 fingerprint is
 * stored, which is enough to detect "registered with a different secret than the one
 * currently configured" without keeping a second copy of the credential in the database.
 */
@Injectable()
export class WebhookRegistryService {
  private readonly logger = new Logger(WebhookRegistryService.name);
  private readonly timeoutMs: number;

  constructor(
    private readonly prisma: PrismaService,
    private readonly http: HttpService,
    private readonly auth: KiotVietAuthService,
    private readonly config: ConfigService,
    private readonly retailer: RetailerContext,
  ) {
    const raw = Number(this.config.get('KIOT_HTTP_TIMEOUT_MS'));
    this.timeoutMs = Number.isFinite(raw) && raw > 0 ? raw : 20000;
  }

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  /** Public base URL KiotViet will call. Throws if unset — registering a localhost URL is useless. */
  private baseUrl(): string {
    const raw = this.config.get<string>('WEBHOOK_PUBLIC_BASE_URL');
    if (!raw) {
      throw new Error(
        'WEBHOOK_PUBLIC_BASE_URL is not configured. KiotViet must be able to reach this ' +
          'host over public HTTPS, so it cannot be inferred.',
      );
    }
    const trimmed = raw.replace(/\/+$/, '');
    if (!trimmed.startsWith('https://')) {
      // Doc 2.11.1: the payload is plaintext, so the endpoint must be HTTPS.
      this.logger.error(
        `WEBHOOK_PUBLIC_BASE_URL is not HTTPS (${trimmed}). KiotViet sends webhook ` +
          `payloads in plaintext; an http:// endpoint exposes customer data in transit.`,
      );
    }
    return trimmed;
  }

  /** Callback URL for one event type. Matches the `/webhook/kiot/:type` route. */
  callbackUrl(type: WebhookEventType): string {
    return `${this.baseUrl()}/webhook/kiot/${type}`;
  }

  private secret(): string | undefined {
    return this.config.get<string>('KIOT_WEBHOOK_SECRET') || undefined;
  }

  /** Non-reversible marker used to detect secret drift without storing the secret. */
  private secretFingerprint(): string | null {
    const s = this.secret();
    if (!s) return null;
    return `sha256:${createHash('sha256').update(s).digest('hex').slice(0, 16)}`;
  }

  private async headers() {
    return this.auth.getRequestHeaders();
  }

  private apiUrl(path = ''): string {
    const base = (this.config.get<string>('KIOT_BASE_URL') ?? '').replace(
      /\/+$/,
      '',
    );
    return `${base}/webhooks${path}`;
  }

  // ---------------------------------------------------------------------------
  // Remote operations
  // ---------------------------------------------------------------------------

  /** `GET /webhooks` (doc 2.11.11). */
  async listRemote(): Promise<RemoteWebhook[]> {
    const { data } = await firstValueFrom(
      this.http.get<any>(this.apiUrl(), {
        headers: await this.headers(),
        timeout: this.timeoutMs,
      }),
    );
    const rows: any[] = Array.isArray(data) ? data : (data?.data ?? []);
    return rows.map((w) => this.normalise(w));
  }

  /** `GET /webhooks/{id}` (doc 2.11.12). */
  async getRemote(id: number): Promise<RemoteWebhook | null> {
    try {
      const { data } = await firstValueFrom(
        this.http.get<any>(this.apiUrl(`/${id}`), {
          headers: await this.headers(),
          timeout: this.timeoutMs,
        }),
      );
      const row = data?.data ?? data;
      return row ? this.normalise(row) : null;
    } catch (error: any) {
      if (error?.response?.status === 404) return null;
      throw error;
    }
  }

  /** `POST /webhooks` (doc 2.11.1). */
  async register(
    type: WebhookEventType,
    description?: string,
  ): Promise<RemoteWebhook> {
    const url = this.callbackUrl(type);
    const secret = this.secret();

    if (!secret) {
      this.logger.warn(
        `Registering ${type} WITHOUT a secret — KiotViet will not sign deliveries and ` +
          `the endpoint cannot verify authenticity. Set KIOT_WEBHOOK_SECRET first.`,
      );
    }

    const body = {
      Webhook: {
        Type: type,
        Url: url,
        IsActive: true,
        Description: description ?? `Auto-registered by sync-kiot-data`,
        ...(secret ? { Secret: secret } : {}),
      },
    };

    const { data } = await firstValueFrom(
      this.http.post<any>(this.apiUrl(), body, {
        headers: await this.headers(),
        timeout: this.timeoutMs,
      }),
    );

    const created = this.normalise(data?.data ?? data);
    await this.persist(created);
    this.logger.log(`Registered ${type} -> ${url} (id=${created.id})`);
    return created;
  }

  /** `DELETE /webhooks/{id}` (doc 2.11.2). Keeps the local row, marked inactive. */
  async unregister(id: number): Promise<void> {
    await firstValueFrom(
      this.http.delete(this.apiUrl(`/${id}`), {
        headers: await this.headers(),
        timeout: this.timeoutMs,
      }),
    );

    // Never delete the local audit row — just mark it.
    await this.prisma.webhook
      .update({ where: { kiotVietId: id }, data: { isActive: false } })
      .catch(() => undefined);

    this.logger.log(`Unregistered webhook id=${id}`);
  }

  /** Register every documented event type that is not already healthy. */
  async registerAll(): Promise<ReconcileReport> {
    return this.reconcile({ repair: true });
  }

  // ---------------------------------------------------------------------------
  // Reconciliation
  // ---------------------------------------------------------------------------

  /**
   * Compare KiotViet's subscriptions against what this deployment expects.
   *
   * With `repair: true`, missing/inactive/wrong-URL subscriptions are re-created.
   * A dead subscription is replaced by DELETE-then-POST, because the API offers no
   * update operation for an existing webhook.
   */
  async reconcile(opts: { repair?: boolean } = {}): Promise<ReconcileReport> {
    await this.prisma.ensureConnected();
    const baseUrl = this.baseUrl();

    const report: ReconcileReport = {
      baseUrl,
      expected: WEBHOOK_EVENT_TYPES.length,
      healthy: [],
      missing: [],
      inactive: [],
      wrongUrl: [],
      unknown: [],
      repaired: [],
      errors: [],
    };

    const remote = await this.listRemote();

    // Persist whatever KiotViet reports, so the local table reflects reality even
    // for subscriptions created outside this service.
    for (const w of remote) await this.persist(w);

    const byType = new Map<string, RemoteWebhook[]>();
    for (const w of remote) {
      const list = byType.get(w.type) ?? [];
      list.push(w);
      byType.set(w.type, list);
    }

    for (const type of WEBHOOK_EVENT_TYPES) {
      const expectedUrl = this.callbackUrl(type);
      const candidates = byType.get(type) ?? [];

      if (candidates.length === 0) {
        report.missing.push(type);
        continue;
      }

      const match = candidates.find((c) => this.sameUrl(c.url, expectedUrl));

      if (!match) {
        const other = candidates[0];
        report.wrongUrl.push({ type, url: other.url, expectedUrl });
        continue;
      }

      if (!match.isActive) {
        report.inactive.push(type);
        continue;
      }

      report.healthy.push(type);
    }

    // Subscriptions for types we do not handle, or pointing elsewhere entirely.
    for (const w of remote) {
      const known = (WEBHOOK_EVENT_TYPES as readonly string[]).includes(w.type);
      if (!known) {
        report.unknown.push({ id: w.id, type: w.type, url: w.url });
      }
    }

    if (opts.repair) {
      // Missing: straightforward create.
      for (const type of report.missing) {
        try {
          await this.register(type as WebhookEventType);
          report.repaired.push(type);
        } catch (error: any) {
          report.errors.push({ type, error: error?.message ?? String(error) });
        }
      }

      // Inactive or wrong URL: no update API exists, so replace.
      const toReplace = [
        ...report.inactive,
        ...report.wrongUrl.map((w) => w.type),
      ];
      for (const type of toReplace) {
        try {
          for (const stale of byType.get(type) ?? []) {
            await this.unregister(stale.id);
          }
          await this.register(type as WebhookEventType);
          report.repaired.push(type);
        } catch (error: any) {
          report.errors.push({ type, error: error?.message ?? String(error) });
        }
      }
    }

    const drift =
      report.missing.length + report.inactive.length + report.wrongUrl.length;
    if (drift === 0) {
      this.logger.log(
        `Webhook reconcile: all ${report.healthy.length}/${report.expected} subscriptions healthy`,
      );
    } else {
      this.logger.warn(
        `Webhook reconcile: ${report.healthy.length}/${report.expected} healthy, ` +
          `${report.missing.length} missing, ${report.inactive.length} inactive ` +
          `(KiotViet stopped delivering), ${report.wrongUrl.length} wrong URL` +
          (opts.repair ? `; repaired ${report.repaired.length}` : ''),
      );
    }

    return report;
  }

  /** Local mirror of the subscriptions, for the status endpoint. */
  async listLocal() {
    await this.prisma.ensureConnected();
    return this.prisma.webhook.findMany({ orderBy: { type: 'asc' } });
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private normalise(w: any): RemoteWebhook {
    return {
      id: Number(w?.id ?? w?.Id),
      type: String(w?.type ?? w?.Type ?? ''),
      url: String(w?.url ?? w?.Url ?? ''),
      // Absent `isActive` is treated as active, matching the API default.
      isActive: (w?.isActive ?? w?.IsActive ?? true) !== false,
      description: w?.description ?? w?.Description ?? null,
      retailerId: w?.retailerId ?? w?.RetailerId ?? null,
    };
  }

  /** Compare URLs ignoring trailing slash and case of the host. */
  private sameUrl(a: string, b: string): boolean {
    const norm = (u: string) => u.trim().replace(/\/+$/, '').toLowerCase();
    return norm(a) === norm(b);
  }

  private async persist(w: RemoteWebhook): Promise<void> {
    if (!Number.isFinite(w.id)) return;
    const data = {
      type: w.type,
      url: w.url,
      isActive: w.isActive,
      description: w.description ?? null,
      // Fingerprint only — the secret itself is never written to the database.
      secret: this.secretFingerprint(),
      retailerId: this.retailer.resolve(w.retailerId),
    };
    await this.prisma.webhook
      .upsert({
        where: { kiotVietId: w.id },
        update: data,
        create: { kiotVietId: w.id, ...data },
      })
      .catch((e) =>
        this.logger.warn(`persist webhook ${w.id} failed: ${e.message}`),
      );
  }
}
