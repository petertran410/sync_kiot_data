import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { PrismaService } from '../../prisma/prisma.service';
import { firstValueFrom } from 'rxjs';
import { MisaAuthService } from './misa-auth.service';
import {
  MisaGetDictionaryRequestDto,
  MisaGetDictionaryResponseDto,
  MisaInventoryItemDto,
  MisaStockDto,
  MisaAccountObjectDto,
  MisaOrganizationUnitDto,
} from './dto';

@Injectable()
export class MisaDictionaryService {
  private readonly logger = new Logger(MisaDictionaryService.name);

  // data_type constants
  private readonly DATA_TYPE = {
    ACCOUNT_OBJECT: 1,
    INVENTORY_ITEM: 2,
    STOCK: 3,
    UNIT: 4,
    ACCOUNT: 5,
    ORGANIZATION_UNIT: 6,
  };

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prismaService: PrismaService,
    private readonly misaAuthService: MisaAuthService,
  ) {}

  // ============================================
  // SYNC ALL DICTIONARIES
  // ============================================

  /**
   * Sync tất cả danh mục từ Misa về database
   */
  async syncAllDictionaries(): Promise<void> {
    this.logger.log('🔄 Starting sync all Misa dictionaries...');

    const startTime = new Date();

    try {
      // Sync theo thứ tự: Organization Unit → Stock → Inventory → Account Object
      await this.syncOrganizationUnits();
      await this.syncStocks();
      await this.syncInventoryItems();
      await this.syncAccountObjects();

      // Log sync success
      await this.logSyncResult('ALL', 0, 'SUCCESS', null, startTime);

      this.logger.log('✅ Sync all Misa dictionaries completed');
    } catch (error) {
      await this.logSyncResult('ALL', 0, 'FAILED', error.message, startTime);
      this.logger.error(`❌ Sync all dictionaries failed: ${error.message}`);
      throw error;
    }
  }

  // ============================================
  // SYNC INVENTORY ITEMS (data_type = 2)
  // ============================================

  async syncInventoryItems(): Promise<number> {
    this.logger.log('📦 Syncing inventory items...');

    const startTime = new Date();
    let totalSynced = 0;
    let skip = 0;
    const take = 500;

    try {
      while (true) {
        const items = await this.fetchDictionary<MisaInventoryItemDto>(
          this.DATA_TYPE.INVENTORY_ITEM,
          skip,
          take,
        );

        if (!items || items.length === 0) {
          break;
        }

        // Upsert vào database
        for (const item of items) {
          await this.prismaService.misaInventoryItem.upsert({
            where: { inventoryItemId: item.inventory_item_id },
            update: {
              inventoryItemCode: item.inventory_item_code,
              inventoryItemName: item.inventory_item_name,
              inventoryItemType: item.inventory_item_type || 0,
              unitId: item.unit_id || null,
              unitName: item.unit_name || null,
              defaultStockId: item.default_stock_id || null,
              branchId: item.branch_id || null,
              unitPrice: item.unit_price ? item.unit_price : null,
              salePrice1: item.sale_price1 ? item.sale_price1 : null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
              cogsAccount: item.cogs_account || null,
              saleAccount: item.sale_account || null,
              discountAccount: item.discount_account || null,
              updatedAt: new Date(),
            },
            create: {
              inventoryItemId: item.inventory_item_id,
              inventoryItemCode: item.inventory_item_code,
              inventoryItemName: item.inventory_item_name,
              inventoryItemType: item.inventory_item_type || 0,
              unitId: item.unit_id || null,
              unitName: item.unit_name || null,
              defaultStockId: item.default_stock_id || null,
              branchId: item.branch_id || null,
              unitPrice: item.unit_price ? item.unit_price : null,
              salePrice1: item.sale_price1 ? item.sale_price1 : null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
              cogsAccount: item.cogs_account || null,
              saleAccount: item.sale_account || null,
              discountAccount: item.discount_account || null,
            },
          });
        }

        totalSynced += items.length;
        skip += take;

        this.logger.log(`   Synced ${totalSynced} inventory items...`);

        if (items.length < take) {
          break;
        }
      }

      await this.logSyncResult(
        'INVENTORY_ITEM',
        this.DATA_TYPE.INVENTORY_ITEM,
        'SUCCESS',
        null,
        startTime,
        totalSynced,
      );

      this.logger.log(`✅ Synced ${totalSynced} inventory items`);
      return totalSynced;
    } catch (error) {
      await this.logSyncResult(
        'INVENTORY_ITEM',
        this.DATA_TYPE.INVENTORY_ITEM,
        'FAILED',
        error.message,
        startTime,
        totalSynced,
      );
      throw error;
    }
  }

  // ============================================
  // SYNC STOCKS (data_type = 3)
  // ============================================

  async syncStocks(): Promise<number> {
    this.logger.log('🏭 Syncing stocks...');

    const startTime = new Date();
    let totalSynced = 0;
    let skip = 0;
    const take = 500;

    try {
      while (true) {
        const stocks = await this.fetchDictionary<MisaStockDto>(
          this.DATA_TYPE.STOCK,
          skip,
          take,
        );

        if (!stocks || stocks.length === 0) {
          break;
        }

        for (const stock of stocks) {
          await this.prismaService.misaStock.upsert({
            where: { stockId: stock.stock_id },
            update: {
              stockCode: stock.stock_code,
              stockName: stock.stock_name,
              branchId: stock.branch_id || null,
              inactive: stock.inactive || false,
              inventoryAccount: stock.inventory_account || null,
              updatedAt: new Date(),
            },
            create: {
              stockId: stock.stock_id,
              stockCode: stock.stock_code,
              stockName: stock.stock_name,
              branchId: stock.branch_id || null,
              inactive: stock.inactive || false,
              inventoryAccount: stock.inventory_account || null,
            },
          });
        }

        totalSynced += stocks.length;
        skip += take;

        if (stocks.length < take) {
          break;
        }
      }

      await this.logSyncResult(
        'STOCK',
        this.DATA_TYPE.STOCK,
        'SUCCESS',
        null,
        startTime,
        totalSynced,
      );

      this.logger.log(`✅ Synced ${totalSynced} stocks`);
      return totalSynced;
    } catch (error) {
      await this.logSyncResult(
        'STOCK',
        this.DATA_TYPE.STOCK,
        'FAILED',
        error.message,
        startTime,
        totalSynced,
      );
      throw error;
    }
  }

  // ============================================
  // SYNC ACCOUNT OBJECTS (data_type = 1)
  // ============================================

  async syncAccountObjects(): Promise<number> {
    this.logger.log('👥 Syncing account objects...');

    const startTime = new Date();
    let totalSynced = 0;
    let skip = 0;
    const take = 500;

    try {
      while (true) {
        const accountObjects = await this.fetchDictionary<MisaAccountObjectDto>(
          this.DATA_TYPE.ACCOUNT_OBJECT,
          skip,
          take,
        );

        if (!accountObjects || accountObjects.length === 0) {
          break;
        }

        for (const obj of accountObjects) {
          await this.prismaService.misaAccountObject.upsert({
            where: { accountObjectId: obj.account_object_id },
            update: {
              accountObjectCode: obj.account_object_code,
              accountObjectName: obj.account_object_name,
              accountObjectType: obj.account_object_type || 0,
              address: obj.address || null,
              companyTaxCode: obj.company_tax_code || null,
              branchId: obj.branch_id || null,
              isCustomer: obj.is_customer || false,
              isVendor: obj.is_vendor || false,
              isEmployee: obj.is_employee || false,
              inactive: obj.inactive || false,
              payAccount: obj.pay_account || null,
              receiveAccount: obj.receive_account || null,
              updatedAt: new Date(),
            },
            create: {
              accountObjectId: obj.account_object_id,
              accountObjectCode: obj.account_object_code,
              accountObjectName: obj.account_object_name,
              accountObjectType: obj.account_object_type || 0,
              address: obj.address || null,
              companyTaxCode: obj.company_tax_code || null,
              branchId: obj.branch_id || null,
              isCustomer: obj.is_customer || false,
              isVendor: obj.is_vendor || false,
              isEmployee: obj.is_employee || false,
              inactive: obj.inactive || false,
              payAccount: obj.pay_account || null,
              receiveAccount: obj.receive_account || null,
            },
          });
        }

        totalSynced += accountObjects.length;
        skip += take;

        if (accountObjects.length < take) {
          break;
        }
      }

      await this.logSyncResult(
        'ACCOUNT_OBJECT',
        this.DATA_TYPE.ACCOUNT_OBJECT,
        'SUCCESS',
        null,
        startTime,
        totalSynced,
      );

      this.logger.log(`✅ Synced ${totalSynced} account objects`);
      return totalSynced;
    } catch (error) {
      await this.logSyncResult(
        'ACCOUNT_OBJECT',
        this.DATA_TYPE.ACCOUNT_OBJECT,
        'FAILED',
        error.message,
        startTime,
        totalSynced,
      );
      throw error;
    }
  }

  // ============================================
  // SYNC ORGANIZATION UNITS (data_type = 6)
  // ============================================

  async syncOrganizationUnits(): Promise<number> {
    this.logger.log('🏢 Syncing organization units...');

    const startTime = new Date();
    let totalSynced = 0;
    let skip = 0;
    const take = 500;

    try {
      while (true) {
        const units = await this.fetchDictionary<MisaOrganizationUnitDto>(
          this.DATA_TYPE.ORGANIZATION_UNIT,
          skip,
          take,
        );

        if (!units || units.length === 0) {
          break;
        }

        for (const unit of units) {
          await this.prismaService.misaOrganizationUnit.upsert({
            where: { organizationUnitId: unit.organization_unit_id },
            update: {
              organizationUnitCode: unit.organization_unit_code,
              organizationUnitName: unit.organization_unit_name,
              organizationUnitTypeId: unit.organization_unit_type_id || 1,
              parentId: unit.parent_id || null,
              branchId: unit.branch_id || null,
              updatedAt: new Date(),
            },
            create: {
              organizationUnitId: unit.organization_unit_id,
              organizationUnitCode: unit.organization_unit_code,
              organizationUnitName: unit.organization_unit_name,
              organizationUnitTypeId: unit.organization_unit_type_id || 1,
              parentId: unit.parent_id || null,
              branchId: unit.branch_id || null,
            },
          });
        }

        totalSynced += units.length;
        skip += take;

        if (units.length < take) {
          break;
        }
      }

      await this.logSyncResult(
        'ORGANIZATION_UNIT',
        this.DATA_TYPE.ORGANIZATION_UNIT,
        'SUCCESS',
        null,
        startTime,
        totalSynced,
      );

      this.logger.log(`✅ Synced ${totalSynced} organization units`);
      return totalSynced;
    } catch (error) {
      await this.logSyncResult(
        'ORGANIZATION_UNIT',
        this.DATA_TYPE.ORGANIZATION_UNIT,
        'FAILED',
        error.message,
        startTime,
        totalSynced,
      );
      throw error;
    }
  }

  // ============================================
  // LOOKUP METHODS
  // ============================================

  /**
   * Tìm inventory item theo misa_code
   */
  async findInventoryByCode(misaCode: string): Promise<{
    inventoryItemId: string;
    inventoryItemCode: string;
    inventoryItemName: string;
    unitId: string | null;
    unitName: string | null;
    defaultStockId: string | null;
  } | null> {
    const item = await this.prismaService.misaInventoryItem.findFirst({
      where: {
        inventoryItemCode: misaCode,
        inactive: false,
      },
      select: {
        inventoryItemId: true,
        inventoryItemCode: true,
        inventoryItemName: true,
        unitId: true,
        unitName: true,
        defaultStockId: true,
      },
    });

    return item;
  }

  /**
   * Tìm stock theo branchName (fuzzy match)
   */
  async findStockByBranchName(branchName: string): Promise<{
    stockId: string;
    stockCode: string;
    stockName: string;
  } | null> {
    // Normalize branchName
    const normalizedName = this.normalizeString(branchName);

    // Lấy tất cả stocks active
    const stocks = await this.prismaService.misaStock.findMany({
      where: { inactive: false },
      select: {
        stockId: true,
        stockCode: true,
        stockName: true,
      },
    });

    // Fuzzy match
    let bestMatch: (typeof stocks)[0] | null = null;
    let bestScore = 0;

    for (const stock of stocks) {
      const normalizedStockName = this.normalizeString(stock.stockName);
      const score = this.calculateSimilarity(
        normalizedName,
        normalizedStockName,
      );

      if (score > bestScore && score >= 0.5) {
        bestScore = score;
        bestMatch = stock;
      }
    }

    if (bestMatch) {
      this.logger.debug(
        `Matched branch "${branchName}" to stock "${bestMatch.stockName}" (score: ${bestScore.toFixed(2)})`,
      );
    }

    return bestMatch;
  }

  /**
   * Lấy default stock (fallback khi không match)
   */
  async getDefaultStock(): Promise<{
    stockId: string;
    stockCode: string;
    stockName: string;
  } | null> {
    // Tìm stock có isDefault = true
    let defaultStock = await this.prismaService.misaStock.findFirst({
      where: {
        isDefault: true,
        inactive: false,
      },
      select: {
        stockId: true,
        stockCode: true,
        stockName: true,
      },
    });

    // Nếu không có, lấy từ ENV
    if (!defaultStock) {
      const defaultStockId = this.configService.get<string>(
        'MISA_DEFAULT_STOCK_ID',
      );
      const defaultStockCode = this.configService.get<string>(
        'MISA_DEFAULT_STOCK_CODE',
      );
      const defaultStockName = this.configService.get<string>(
        'MISA_DEFAULT_STOCK_NAME',
      );

      if (defaultStockId && defaultStockCode && defaultStockName) {
        defaultStock = {
          stockId: defaultStockId,
          stockCode: defaultStockCode,
          stockName: defaultStockName,
        };
      }
    }

    return defaultStock;
  }

  /**
   * Tìm account object theo customer name (fuzzy match)
   */
  async findAccountObjectByName(customerName: string): Promise<{
    accountObjectId: string;
    accountObjectCode: string;
    accountObjectName: string;
  } | null> {
    if (!customerName || customerName.trim() === '') {
      return null;
    }

    const normalizedName = this.normalizeString(customerName);

    // Lấy tất cả account objects là customer
    const accountObjects = await this.prismaService.misaAccountObject.findMany({
      where: {
        isCustomer: true,
        inactive: false,
      },
      select: {
        accountObjectId: true,
        accountObjectCode: true,
        accountObjectName: true,
      },
    });

    // Fuzzy match
    let bestMatch: (typeof accountObjects)[0] | null = null;
    let bestScore = 0;

    for (const obj of accountObjects) {
      const normalizedObjName = this.normalizeString(obj.accountObjectName);
      const score = this.calculateSimilarity(normalizedName, normalizedObjName);

      if (score > bestScore && score >= 0.6) {
        bestScore = score;
        bestMatch = obj;
      }
    }

    if (bestMatch) {
      this.logger.debug(
        `Matched customer "${customerName}" to account object "${bestMatch.accountObjectName}" (score: ${bestScore.toFixed(2)})`,
      );
    }

    return bestMatch;
  }

  // ============================================
  // HELPER METHODS
  // ============================================

  /**
   * Gọi API get_dictionary từ Misa
   */
  private async fetchDictionary<T>(
    dataType: number,
    skip: number,
    take: number,
  ): Promise<T[]> {
    const baseUrl = this.configService.get<string>('MISA_BASE_URL');
    const appId = this.configService.get<string>('MISA_APP_ID');

    const accessToken = await this.misaAuthService.getAccessToken();

    const url = `${baseUrl}/apir/sync/actopen/get_dictionary`;

    const requestBody: MisaGetDictionaryRequestDto = {
      data_type: dataType,
      skip: skip,
      take: take,
      app_id: appId || '',
    };

    try {
      const response = await firstValueFrom(
        this.httpService.post<MisaGetDictionaryResponseDto<T>>(
          url,
          requestBody,
          {
            headers: {
              'Content-Type': 'application/json',
              'X-MISA-AccessToken': accessToken,
            },
          },
        ),
      );

      const data = response.data;

      if (!data.Success) {
        throw new Error(
          `Misa get_dictionary failed: ${data.ErrorCode} - ${data.ErrorMessage}`,
        );
      }

      return data.Data || [];
    } catch (error) {
      this.logger.error(
        `❌ Failed to fetch dictionary (type=${dataType}): ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Normalize string để so sánh
   */
  private normalizeString(str: string): string {
    return str
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '') // Remove diacritics
      .replace(/đ/g, 'd')
      .replace(/Đ/g, 'd')
      .replace(/[^a-z0-9\s]/g, '') // Remove special chars
      .replace(/\s+/g, ' ')
      .trim();
  }

  /**
   * Tính độ tương đồng giữa 2 string (Jaccard similarity)
   */
  private calculateSimilarity(str1: string, str2: string): number {
    const words1 = new Set(str1.split(' '));
    const words2 = new Set(str2.split(' '));

    const intersection = new Set([...words1].filter((x) => words2.has(x)));
    const union = new Set([...words1, ...words2]);

    if (union.size === 0) return 0;

    return intersection.size / union.size;
  }

  /**
   * Log kết quả sync vào database
   */
  private async logSyncResult(
    syncType: string,
    dataType: number,
    status: string,
    errorMessage: string | null,
    startedAt: Date,
    totalRecords: number = 0,
  ): Promise<void> {
    try {
      await this.prismaService.misaSyncLog.create({
        data: {
          syncType: syncType,
          dataType: dataType,
          totalRecords: totalRecords,
          successCount: status === 'SUCCESS' ? totalRecords : 0,
          failedCount: status === 'FAILED' ? totalRecords : 0,
          status: status,
          errorMessage: errorMessage,
          startedAt: startedAt,
          completedAt: new Date(),
        },
      });
    } catch (error) {
      this.logger.error(`Failed to log sync result: ${error.message}`);
    }
  }

  /**
   * Lấy thống kê cache hiện tại
   */
  async getCacheStats(): Promise<{
    inventoryItems: number;
    stocks: number;
    accountObjects: number;
    organizationUnits: number;
    lastSync: Date | null;
  }> {
    const [
      inventoryItems,
      stocks,
      accountObjects,
      organizationUnits,
      lastSync,
    ] = await Promise.all([
      this.prismaService.misaInventoryItem.count(),
      this.prismaService.misaStock.count(),
      this.prismaService.misaAccountObject.count(),
      this.prismaService.misaOrganizationUnit.count(),
      this.prismaService.misaSyncLog.findFirst({
        where: { status: 'SUCCESS' },
        orderBy: { completedAt: 'desc' },
        select: { completedAt: true },
      }),
    ]);

    return {
      inventoryItems,
      stocks,
      accountObjects,
      organizationUnits,
      lastSync: lastSync?.completedAt || null,
    };
  }
}
