import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as lark from '@larksuiteoapi/node-sdk';

@Injectable()
export class LarkBaseClient {
  private readonly logger = new Logger(LarkBaseClient.name);
  private readonly client: lark.Client;

  constructor(config: ConfigService) {
    const appId = config.get<string>('LARK_APP_ID');
    const appSecret = config.get<string>('LARK_APP_SECRET');

    if (!appId || !appSecret) {
      throw new Error('LARK_APP_ID and LARK_APP_SECRET must be configured');
    }

    this.client = new lark.Client({
      appId,
      appSecret,
      appType: lark.AppType.SelfBuild,
      domain: lark.Domain.Lark,
    });
  }

  async batchCreate(
    baseToken: string,
    tableId: string,
    records: Array<{ fields: Record<string, unknown> }>,
  ): Promise<string[]> {
    if (!records.length) return [];
    const response = await this.client.bitable.appTableRecord.batchCreate({
      path: { app_token: baseToken, table_id: tableId },
      data: { records: records as any },
    });
    this.assertSuccess(response, 'batch create records');
    return (response.data?.records ?? [])
      .map((record) => record.record_id)
      .filter((recordId): recordId is string => Boolean(recordId));
  }

  async batchUpdate(
    baseToken: string,
    tableId: string,
    records: Array<{ record_id: string; fields: Record<string, unknown> }>,
  ): Promise<void> {
    if (!records.length) return;
    const response = await this.client.bitable.appTableRecord.batchUpdate({
      path: { app_token: baseToken, table_id: tableId },
      data: { records: records as any },
    });
    this.assertSuccess(response, 'batch update records');
  }

  async batchRemove(
    baseToken: string,
    tableId: string,
    recordIds: string[],
  ): Promise<void> {
    if (!recordIds.length) return;
    const response = await this.client.bitable.appTableRecord.batchDelete({
      path: { app_token: baseToken, table_id: tableId },
      data: { records: recordIds },
    });
    this.assertSuccess(response, 'batch delete records');
  }

  async listRecordIdsByField(
    baseToken: string,
    tableId: string,
    fieldName: string,
  ): Promise<Map<string, string>> {
    const recordIds = new Map<string, string>();
    let pageToken: string | undefined;

    do {
      const response = await this.client.bitable.appTableRecord.list({
        path: { app_token: baseToken, table_id: tableId },
        params: {
          page_size: 500,
          ...(pageToken ? { page_token: pageToken } : {}),
        },
      });
      this.assertSuccess(response, 'list records');
      for (const record of response.data?.items ?? []) {
        const key = record.fields?.[fieldName];
        if (record.record_id && key !== null && key !== undefined) {
          recordIds.set(String(key), record.record_id);
        }
      }
      pageToken = response.data?.has_more ? response.data.page_token : undefined;
    } while (pageToken);

    return recordIds;
  }

  async create(
    baseToken: string,
    tableId: string,
    fields: Record<string, unknown>,
  ): Promise<string | null> {
    const response = await this.client.bitable.appTableRecord.create({
      path: { app_token: baseToken, table_id: tableId },
      data: { fields: fields as any },
    });
    this.assertSuccess(response, 'create record');
    return response.data?.record?.record_id ?? null;
  }

  async update(
    baseToken: string,
    tableId: string,
    recordId: string,
    fields: Record<string, unknown>,
  ): Promise<void> {
    const response = await this.client.bitable.appTableRecord.update({
      path: { app_token: baseToken, table_id: tableId, record_id: recordId },
      data: { fields: fields as any },
    });
    this.assertSuccess(response, `update record ${recordId}`);
  }

  async remove(
    baseToken: string,
    tableId: string,
    recordId: string,
  ): Promise<void> {
    const response = await this.client.bitable.appTableRecord.delete({
      path: { app_token: baseToken, table_id: tableId, record_id: recordId },
    });
    this.assertSuccess(response, `delete record ${recordId}`);
  }

  async searchByKiotVietId(
    baseToken: string,
    tableId: string,
    fieldName: string,
    kiotVietId: string,
  ): Promise<string | null> {
    const response = await this.client.bitable.appTableRecord.search({
      path: { app_token: baseToken, table_id: tableId },
      data: {
        field_names: [fieldName],
        filter: {
          conjunction: 'and',
          conditions: [
            { field_name: fieldName, operator: 'is', value: [kiotVietId] },
          ],
        },
      },
    });
    this.assertSuccess(response, `search ${fieldName}`);
    return response.data?.items?.[0]?.record_id ?? null;
  }

  isNotFound(error: any): boolean {
    return error?.code === 1254034 || error?.response?.data?.code === 1254034;
  }

  private assertSuccess(response: any, operation: string): void {
    if (!response || response.code === 0 || response.code === undefined) return;
    this.logger.error(
      `Lark ${operation} failed: ${response.msg} (${response.code})`,
    );
    const error: any = new Error(response.msg || `Lark ${operation} failed`);
    error.code = response.code;
    throw error;
  }
}
