import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { PrismaService } from '../../../prisma/prisma.service';
import { LarkAuthService } from '../auth/lark-auth.service';
import { firstValueFrom } from 'rxjs';

const LARK_CUSTOMER_FIELDS = {
  PRIMARY_NAME: 'Tên Khách Hàng',
  CUSTOMER_CODE: 'Mã Khách Hàng',
  PHONE_NUMBER: 'Số Điện Thoại',
  STORE_ID: 'Id Cửa Hàng',
  BRANCH: 'Branch',
  COMPANY: 'Công Ty',
  EMAIL: 'Email của Khách Hàng',
  ADDRESS: 'Địa Chỉ Khách Hàng',
  CURRENT_DEBT: 'Nợ Hiện Tại',
  TAX_CODE: 'Mã Số Thuế',
  TOTAL_POINTS: 'Tổng Điểm',
  TOTAL_REVENUE: 'Tổng Doanh Thu',
  GENDER: 'Giới Tính',
  WARD_NAME: 'Phường xã',
  CURRENT_POINTS: 'Điểm Hiện Tại',
  KIOTVIET_ID: 'kiotVietId',
  TOTAL_INVOICED: 'Tổng Bán',
  COMMENTS: 'Ghi Chú',
  MODIFIED_DATE: 'Thời Gian Cập Nhật',
  CREATED_DATE: 'Thời Gian Tạo',
  FACEBOOK_ID: 'Facebook Khách Hàng',
  LOCATION_NAME: 'Khu Vực',
  CUSTOMER_GROUPS: 'Nhóm Khách Hàng',
  DATE_OF_BIRTH: 'Ngày Sinh',
  TYPE: 'Loại Khách Hàng',
  SUB_PHONE: 'Số Điện Thoại Phụ',
  IDENTIFICATION_NUMBER: 'CCCD Của Khách Hàng',
} as const;

const GENDER_OPTIONS = {
  MALE: 'Nam',
  FEMALE: 'Nữ',
} as const;

const BRANCH_OPTIONS = {
  CUA_HANG_DIEP_TRA: 'Cửa Hàng Diệp Trà',
  KHO_HA_NOI: 'Kho Hà Nội',
  KHO_SAI_GON: 'Kho Sài Gòn',
  VAN_PHONG_HA_NOI: 'Văn Phòng Hà Nội',
};

const TYPE_CUSTOMER = {
  CONG_TY: 'Công Ty',
  CA_NHAN: 'Cá Nhân',
};

interface LarkBatchResponse {
  code: number;
  msg: string;
  data?: {
    records?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    items?: Array<{
      record_id: string;
      fields: Record<string, any>;
    }>;
    page_token?: string;
    total?: number;
  };
}

@Injectable()
export class LarkCustomerSyncService {
  private readonly logger = new Logger(LarkCustomerSyncService.name);
  private readonly baseToken: string;
  private readonly tableId: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly prismaService: PrismaService,
    private readonly larkAuthService: LarkAuthService,
  ) {
    const baseToken = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_BASE_TOKEN',
    );
    const tableId = this.configService.get<string>(
      'LARK_CUSTOMER_SYNC_TABLE_ID',
    );

    if (!baseToken || !tableId) {
      throw new Error('LarkBase customer configuration missing');
    }

    this.baseToken = baseToken;
    this.tableId = tableId;
  }

  async syncCustomersToLarkBase(customers: any[]): Promise<void> {
    const lockKey = `lark_customer_sync_lock_${Date.now()}`;

    try {
      await this.acquireSyncLock(lockKey);

      this.logger.log(
        `🚀 Starting batch sync for ${customers.length} customers...`,
      );

      const customersToSync = customers.filter(
        (c) => c.larkSyncStatus === 'PENDING' || c.larkSyncStatus === 'FAILED',
      );

      if (customersToSync.length === 0) {
        this.logger.log('✅ No customers need sync');
        await this.releaseSyncLock(lockKey);
        return;
      }

      this.logger.log(
        `📊 Syncing ${customersToSync.length} customers (PENDING + FAILED)`,
      );

      await this.testLarkBaseConnection();

      const BATCH_SIZE = 50;
      let totalSuccess = 0;
      let totalFailed = 0;

      for (let i = 0; i < customersToSync.length; i += BATCH_SIZE) {
        const batch = customersToSync.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(customersToSync.length / BATCH_SIZE);

        this.logger.log(
          `🔄 Processing batch ${batchNumber}/${totalBatches} (${batch.length} customers)`,
        );

        for (const customer of batch) {
          try {
            await this.syncSingleCustomerDirect(customer);
            totalSuccess++;
          } catch (error) {
            this.logger.error(
              `❌ Failed to sync customer ${customer.code}: ${error.message}`,
            );
            totalFailed++;
          }

          await new Promise((resolve) => setTimeout(resolve, 100));
        }

        if (i + BATCH_SIZE < customersToSync.length) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }

      this.logger.log('🎯 Batch sync completed!');
      this.logger.log(`✅ Success: ${totalSuccess}`);
      this.logger.log(`❌ Failed: ${totalFailed}`);
    } catch (error) {
      this.logger.error(`❌ Batch sync failed: ${error.message}`);
      throw error;
    } finally {
      await this.releaseSyncLock(lockKey);
    }
  }

  async syncSingleCustomerDirect(customer: any): Promise<void> {
    try {
      this.logger.log(`🔄 Syncing customer ${customer.code} to Lark...`);

      const existingRecordId = await this.searchRecordByCode(customer.code);

      const larkData = this.mapCustomerToLarkBase(customer);
      const headers = await this.larkAuthService.getCustomerHeaders();

      if (existingRecordId) {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/${existingRecordId}`;

        await firstValueFrom(
          this.httpService.put(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Updated customer ${customer.code} in Lark`);
      } else {
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;

        await firstValueFrom(
          this.httpService.post(
            url,
            { fields: larkData },
            { headers, timeout: 10000 },
          ),
        );

        this.logger.log(`✅ Created customer ${customer.code} in Lark`);
      }

      await this.prismaService.customer.update({
        where: { id: customer.id },
        data: { larkSyncStatus: 'SYNCED', larkSyncedAt: new Date() },
      });
    } catch (error) {
      this.logger.error(
        `❌ Sync customer ${customer.code} failed: ${error.message}`,
      );

      await this.prismaService.customer.update({
        where: { id: customer.id },
        data: {
          larkSyncStatus: 'FAILED',
          larkSyncRetries: { increment: 1 },
        },
      });

      throw error;
    }
  }

  private async searchRecordByCode(code: string): Promise<string | null> {
    try {
      const headers = await this.larkAuthService.getCustomerHeaders();
      const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records/search`;

      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            field_names: [LARK_CUSTOMER_FIELDS.CUSTOMER_CODE],
            filter: {
              conjunction: 'and',
              conditions: [
                {
                  field_name: LARK_CUSTOMER_FIELDS.CUSTOMER_CODE,
                  operator: 'is',
                  value: [code],
                },
              ],
            },
          },
          {
            headers,
            timeout: 10000,
          },
        ),
      );

      if (response.data.code === 0) {
        const items = response.data.data?.items || [];
        if (items.length > 0) {
          return items[0].record_id;
        }
      }

      return null;
    } catch (error) {
      this.logger.warn(`Search customer by code failed: ${error.message}`);
      return null;
    }
  }

  private mapCustomerToLarkBase(customer: any): Record<string, any> {
    const fields: Record<string, any> = {};

    // EXACT mapping từ code gốc
    fields[LARK_CUSTOMER_FIELDS.KIOTVIET_ID] = Number(customer.kiotVietId || 0);

    if (customer.name) {
      fields[LARK_CUSTOMER_FIELDS.PRIMARY_NAME] = customer.name;
    }

    if (customer.code) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_CODE] = customer.code;
    }

    if (customer.contactNumber) {
      fields[LARK_CUSTOMER_FIELDS.PHONE_NUMBER] = customer.contactNumber || '';
    }

    if (customer.retailerId) {
      fields[LARK_CUSTOMER_FIELDS.STORE_ID] = '2svn';
    }

    if (customer.organization) {
      fields[LARK_CUSTOMER_FIELDS.COMPANY] = customer.organization || '';
    }

    if (customer.email) {
      fields[LARK_CUSTOMER_FIELDS.EMAIL] = customer.email || '';
    }

    if (customer.address) {
      fields[LARK_CUSTOMER_FIELDS.ADDRESS] = customer.address || '';
    }

    if (customer.debt !== null && customer.debt !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_DEBT] = Number(customer.debt || 0);
    }

    if (customer.taxCode) {
      fields[LARK_CUSTOMER_FIELDS.TAX_CODE] = customer.taxCode || '';
    }

    if (customer.totalPoint !== null && customer.totalPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_POINTS] =
        Number(customer.totalPoint) || 0;
    }

    if (customer.totalRevenue !== null && customer.totalRevenue !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_REVENUE] =
        Number(customer.totalRevenue) || 0;
    }

    if (customer.gender !== null && customer.gender !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.GENDER] = customer.gender
        ? GENDER_OPTIONS.MALE
        : GENDER_OPTIONS.FEMALE;
    }

    if (customer.branchId !== null && customer.branchId !== undefined) {
      if (customer.branchId === 635934) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.CUA_HANG_DIEP_TRA;
      } else if (customer.branchId === 154833) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_HA_NOI;
      } else if (customer.branchId === 402819) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.KHO_SAI_GON;
      } else if (customer.branchId === 631164) {
        fields[LARK_CUSTOMER_FIELDS.BRANCH] = BRANCH_OPTIONS.VAN_PHONG_HA_NOI;
      }
    }

    if (customer.groups !== null && customer.groups !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CUSTOMER_GROUPS] = customer.groups || '';
    }

    if (customer.wardName) {
      fields[LARK_CUSTOMER_FIELDS.WARD_NAME] = customer.wardName || '';
    }

    if (customer.rewardPoint !== null && customer.rewardPoint !== undefined) {
      fields[LARK_CUSTOMER_FIELDS.CURRENT_POINTS] =
        Number(customer.rewardPoint) || 0;
    }

    if (customer.type !== null && customer.type !== undefined) {
      const typeMapping = {
        0: TYPE_CUSTOMER.CA_NHAN,
        1: TYPE_CUSTOMER.CONG_TY,
      };

      fields[LARK_CUSTOMER_FIELDS.TYPE] = typeMapping[customer.type];
    }

    if (
      customer.totalInvoiced !== null &&
      customer.totalInvoiced !== undefined
    ) {
      fields[LARK_CUSTOMER_FIELDS.TOTAL_INVOICED] =
        Number(customer.totalInvoiced) || 0;
    }

    if (customer.comments) {
      fields[LARK_CUSTOMER_FIELDS.COMMENTS] = customer.comments || '';
    }

    if (customer.birthDate) {
      fields[LARK_CUSTOMER_FIELDS.DATE_OF_BIRTH] = new Date(
        customer.birthDate,
      ).getTime();
    }

    if (customer.modifiedDate) {
      fields[LARK_CUSTOMER_FIELDS.MODIFIED_DATE] = new Date(
        customer.modifiedDate,
      ).getTime();
    }

    if (customer.createdDate) {
      fields[LARK_CUSTOMER_FIELDS.CREATED_DATE] = new Date(
        customer.createdDate,
      ).getTime();
    }

    if (customer.locationName) {
      fields[LARK_CUSTOMER_FIELDS.LOCATION_NAME] = customer.locationName || '';
    }

    if (customer.psidFacebook) {
      fields[LARK_CUSTOMER_FIELDS.FACEBOOK_ID] = String(
        customer.psidFacebook || '',
      );
    }

    if (customer.subNumber) {
      fields[LARK_CUSTOMER_FIELDS.SUB_PHONE] = customer.subNumber || '';
    }

    if (customer.identificationNumber) {
      fields[LARK_CUSTOMER_FIELDS.IDENTIFICATION_NUMBER] =
        customer.identificationNumber || '';
    }

    return fields;
  }

  async getSyncProgress(): Promise<any> {
    const total = await this.prismaService.customer.count();
    const synced = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'SYNCED' },
    });
    const pending = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'PENDING' },
    });
    const failed = await this.prismaService.customer.count({
      where: { larkSyncStatus: 'FAILED' },
    });

    const progress = total > 0 ? Math.round((synced / total) * 100) : 0;

    return {
      total,
      synced,
      pending,
      failed,
      progress,
      canRetryFailed: failed > 0,
      summary: `${synced}/${total} synced (${progress}%)`,
    };
  }

  async retryFailedCustomerSyncs(): Promise<void> {
    this.logger.log('🔄 Retrying failed customer syncs...');

    const failedCustomers = await this.prismaService.customer.findMany({
      where: {
        larkSyncStatus: 'FAILED',
        larkSyncRetries: { lt: 3 },
      },
      take: 100,
    });

    if (failedCustomers.length === 0) {
      this.logger.log('✅ No failed customers to retry');
      return;
    }

    this.logger.log(
      `Found ${failedCustomers.length} failed customers to retry`,
    );

    await this.prismaService.customer.updateMany({
      where: { id: { in: failedCustomers.map((c) => c.id) } },
      data: { larkSyncStatus: 'PENDING' },
    });

    await this.syncCustomersToLarkBase(failedCustomers);
  }

  async getCustomerSyncStats(): Promise<{
    pending: number;
    synced: number;
    failed: number;
    total: number;
  }> {
    const [pending, synced, failed, total] = await Promise.all([
      this.prismaService.customer.count({
        where: { larkSyncStatus: 'PENDING' },
      }),
      this.prismaService.customer.count({
        where: { larkSyncStatus: 'SYNCED' },
      }),
      this.prismaService.customer.count({
        where: { larkSyncStatus: 'FAILED' },
      }),
      this.prismaService.customer.count(),
    ]);

    return { pending, synced, failed, total };
  }

  private async testLarkBaseConnection(): Promise<void> {
    const maxRetries = 3;

    for (let retryCount = 0; retryCount <= maxRetries; retryCount++) {
      try {
        this.logger.log(
          `🔍 Testing LarkBase connection (attempt ${retryCount + 1}/${maxRetries + 1})...`,
        );

        const headers = await this.larkAuthService.getCustomerHeaders();
        const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${this.baseToken}/tables/${this.tableId}/records`;
        const params = new URLSearchParams({ page_size: '1' });

        const response = await firstValueFrom(
          this.httpService.get(`${url}?${params}`, {
            headers,
            timeout: 30000,
          }),
        );

        if (response.data.code === 0) {
          const totalRecords = response.data.data?.total || 0;
          this.logger.log(`✅ LarkBase connection successful`);
          this.logger.log(
            `📊 LarkBase table has ${totalRecords} existing records`,
          );
          return;
        }

        throw new Error(`Connection test failed: ${response.data.msg}`);
      } catch (error) {
        if (retryCount < maxRetries) {
          const delay = (retryCount + 1) * 2000;
          this.logger.warn(
            `⚠️  Connection attempt ${retryCount + 1} failed: ${error.message}`,
          );
          this.logger.log(`🔄 Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        } else {
          this.logger.error(
            '❌ LarkBase connection test failed after all retries',
          );
          throw new Error(`Cannot connect to LarkBase: ${error.message}`);
        }
      }
    }
  }

  private async acquireSyncLock(lockKey: string): Promise<void> {
    const syncName = 'customer_lark_sync';

    const existingLock = await this.prismaService.syncControl.findFirst({
      where: { name: syncName, isRunning: true },
    });

    if (existingLock && existingLock.startedAt) {
      const lockAge = Date.now() - existingLock.startedAt.getTime();

      if (lockAge < 10 * 60 * 1000) {
        const isProcessActive = await this.isLockProcessActive(existingLock);
        if (isProcessActive) {
          throw new Error('Another sync is already running');
        } else {
          this.logger.warn(
            `🔓 Clearing inactive lock (age: ${Math.round(lockAge / 1000)}s)`,
          );
          await this.forceReleaseLock(syncName);
        }
      } else {
        this.logger.warn(
          `🔓 Clearing stale lock (age: ${Math.round(lockAge / 60000)}min)`,
        );
        await this.forceReleaseLock(syncName);
      }
    }

    await this.waitForLockAvailability(syncName);

    await this.prismaService.syncControl.upsert({
      where: { name: syncName },
      create: {
        name: syncName,
        entities: ['customer'],
        syncMode: 'lark_sync',
        isEnabled: true,
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
      update: {
        isRunning: true,
        status: 'running',
        lastRunAt: new Date(),
        startedAt: new Date(),
        progress: {
          lockKey,
          processId: process.pid,
          hostname: require('os').hostname(),
        },
      },
    });

    this.logger.debug(
      `🔒 Acquired sync lock: ${lockKey} (PID: ${process.pid})`,
    );
  }

  private async isLockProcessActive(lockRecord: any): Promise<boolean> {
    try {
      if (!lockRecord.progress?.processId) {
        return false;
      }

      const currentHostname = require('os').hostname();
      if (lockRecord.progress.hostname !== currentHostname) {
        return false;
      }

      const lockAge = Date.now() - lockRecord.startedAt.getTime();
      if (lockAge > 5 * 60 * 1000) {
        return false;
      }

      return true;
    } catch (error) {
      this.logger.warn(`Could not verify lock process: ${error.message}`);
      return false;
    }
  }

  private async waitForLockAvailability(
    syncName: string,
    maxWaitMs: number = 30000,
  ): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const existingLock = await this.prismaService.syncControl.findFirst({
        where: { name: syncName, isRunning: true },
      });

      if (!existingLock) {
        return;
      }

      this.logger.debug(
        `⏳ Waiting for lock release... (${Math.round((Date.now() - startTime) / 1000)}s)`,
      );
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    throw new Error(`Lock wait timeout after ${maxWaitMs / 1000}s`);
  }

  private async forceReleaseLock(syncName: string): Promise<void> {
    await this.prismaService.syncControl.updateMany({
      where: { name: syncName },
      data: {
        isRunning: false,
        status: 'force_released',
        error: 'Lock force released due to inactivity',
        completedAt: new Date(),
        progress: {},
      },
    });
  }

  private async releaseSyncLock(lockKey: string): Promise<void> {
    const lockRecord = await this.prismaService.syncControl.findFirst({
      where: {
        name: 'customer_lark_sync',
        isRunning: true,
      },
    });

    if (
      lockRecord &&
      lockRecord.progress &&
      typeof lockRecord.progress === 'object' &&
      'lockKey' in lockRecord.progress &&
      lockRecord.progress.lockKey === lockKey
    ) {
      await this.prismaService.syncControl.update({
        where: {
          id: lockRecord.id,
        },
        data: {
          isRunning: false,
          status: 'completed',
          completedAt: new Date(),
          progress: {},
        },
      });

      this.logger.debug(`🔓 Released sync lock: ${lockKey}`);
    }
  }
}
