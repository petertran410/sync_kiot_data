import { Injectable, Inject, Logger } from '@nestjs/common';
import * as lark from '@larksuiteoapi/node-sdk';
import { LARK_CLIENT } from './lark-client.provider';

@Injectable()
export class LarkBaseService {
  private readonly logger = new Logger(LarkBaseService.name);

  constructor(@Inject(LARK_CLIENT) private readonly client: lark.Client) {}

  async createRecord(
    baseToken: string,
    tableId: string,
    fields: Record<string, any>,
  ): Promise<string | null> {
    try {
      const res = await this.client.bitable.appTableRecord.create({
        path: { app_token: baseToken, table_id: tableId },
        data: { fields },
      });

      if (res?.code && res.code !== 0) {
        const err: any = new Error(res.msg || `Lark API error: ${res.code}`);
        err.code = res.code;
        throw err;
      }

      return res?.data?.record?.record_id || null;
    } catch (error) {
      this.logger.error(`createRecord failed: ${error.message}`);
      throw error;
    }
  }

  async updateRecord(
    baseToken: string,
    tableId: string,
    recordId: string,
    fields: Record<string, any>,
  ): Promise<void> {
    try {
      const res = await this.client.bitable.appTableRecord.update({
        path: { app_token: baseToken, table_id: tableId, record_id: recordId },
        data: { fields },
      });

      if (res?.code && res.code !== 0) {
        const err: any = new Error(res.msg || `Lark API error: ${res.code}`);
        err.code = res.code;
        throw err;
      }
    } catch (error) {
      this.logger.error(`updateRecord ${recordId} failed: ${error.message}`);
      throw error;
    }
  }

  async batchCreateRecords(
    baseToken: string,
    tableId: string,
    records: Array<{ fields: Record<string, any> }>,
  ): Promise<string[]> {
    const recordIds: string[] = [];
    const chunks = this.chunkArray(records, 500);

    for (const chunk of chunks) {
      try {
        const res = await this.client.bitable.appTableRecord.batchCreate({
          path: { app_token: baseToken, table_id: tableId },
          data: { records: chunk },
        });

        if (res?.code && res.code !== 0) {
          const err: any = new Error(res.msg || `Lark API error: ${res.code}`);
          err.code = res.code;
          throw err;
        }

        const ids =
          res?.data?.records
            ?.map((r) => r.record_id)
            .filter((id): id is string => !!id) || [];

        this.logger.log(
          `batchCreate: ${chunk.length} input → ${ids.length} IDs`,
        );
        recordIds.push(...ids);
      } catch (error) {
        this.logger.error(`batchCreate failed: ${error.message}`);
        throw error;
      }

      if (chunks.length > 1) await this.delay(500);
    }

    return recordIds;
  }

  async batchUpdateRecords(
    baseToken: string,
    tableId: string,
    records: Array<{ record_id: string; fields: Record<string, any> }>,
  ): Promise<void> {
    const chunks = this.chunkArray(records, 500);

    for (const chunk of chunks) {
      try {
        const res = await this.client.bitable.appTableRecord.batchUpdate({
          path: { app_token: baseToken, table_id: tableId },
          data: { records: chunk },
        });

        if (res?.code && res.code !== 0) {
          const err: any = new Error(res.msg || `Lark API error: ${res.code}`);
          err.code = res.code;
          throw err;
        }
      } catch (error) {
        this.logger.error(`batchUpdate failed: ${error.message}`);
        throw error;
      }

      if (chunks.length > 1) await this.delay(500);
    }
  }

  async fetchAllRecords(
    baseToken: string,
    tableId: string,
    fieldName: string,
  ): Promise<Map<string, string>> {
    const codeToRecordId = new Map<string, string>();
    let pageToken: string | undefined;
    let hasMore = true;

    while (hasMore) {
      try {
        const res = await this.client.bitable.appTableRecord.list({
          path: { app_token: baseToken, table_id: tableId },
          params: {
            page_size: 500,
            field_names: JSON.stringify([fieldName]),
            ...(pageToken ? { page_token: pageToken } : {}),
          },
        });

        const items = res?.data?.items || [];
        for (const item of items) {
          const code = item.fields?.[fieldName];
          if (code && typeof code === 'string' && item.record_id) {
            codeToRecordId.set(code, item.record_id);
          }
        }

        hasMore = res?.data?.has_more || false;
        pageToken = res?.data?.page_token;
      } catch (error) {
        this.logger.error(`fetchAllRecords failed: ${error.message}`);
        break;
      }
    }

    return codeToRecordId;
  }

  async verifyRecordIds(
    baseToken: string,
    tableId: string,
    recordIds: string[],
  ): Promise<Set<string>> {
    const existingIds = new Set<string>();
    const chunks = this.chunkArray(recordIds, 100);

    for (const chunk of chunks) {
      try {
        const res = await this.client.bitable.appTableRecord.batchGet({
          path: { app_token: baseToken, table_id: tableId },
          data: { record_ids: chunk },
        });

        if (res?.code && res.code !== 0) {
          // Fallback an toàn: giả định tồn tại
          chunk.forEach((id) => existingIds.add(id));
          continue;
        }

        const records = res?.data?.records || [];
        for (const record of records) {
          if (record.record_id) existingIds.add(record.record_id);
        }
      } catch (error) {
        this.logger.error(`verifyRecordIds failed: ${error.message}`);
        chunk.forEach((id) => existingIds.add(id));
      }

      if (chunks.length > 1) await this.delay(300);
    }

    return existingIds;
  }

  async searchRecord(
    baseToken: string,
    tableId: string,
    fieldName: string,
    value: string,
  ): Promise<string | null> {
    try {
      const res = await this.client.bitable.appTableRecord.search({
        path: { app_token: baseToken, table_id: tableId },
        data: {
          field_names: [fieldName],
          filter: {
            conjunction: 'and',
            conditions: [
              { field_name: fieldName, operator: 'is', value: [value] },
            ],
          },
        },
      });

      const items = res?.data?.items || [];
      return items.length > 0 ? (items[0].record_id ?? null) : null;
    } catch (error) {
      this.logger.error(`searchRecord failed: ${error.message}`);
      return null;
    }
  }

  private chunkArray<T>(array: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
