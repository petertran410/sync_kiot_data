import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { PrismaService } from '../../prisma/prisma.service';
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
  private readonly DEFAULT_CREATED_BY = 'Trần Ngọc Nhân';

  constructor(
    private readonly configService: ConfigService,
    private readonly httpService: HttpService,
    private readonly prisma: PrismaService,
    private readonly misaAuthService: MisaAuthService,
  ) {}

  /**
   * Sync tất cả danh mục từ Misa vào cache
   */
  async syncAllDictionaries(): Promise<{
    inventoryItems: number;
    stocks: number;
    accountObjects: number;
    organizationUnits: number;
  }> {
    this.logger.log('🔄 Starting full Misa dictionary sync...');

    const startedAt = new Date();
    let syncLog = await this.prisma.misaSyncLog.create({
      data: {
        syncType: 'FULL_DICTIONARY',
        dataType: 0,
        status: 'RUNNING',
        startedAt,
      },
    });

    try {
      const [inventoryItems, stocks, accountObjects, organizationUnits] =
        await Promise.all([
          this.syncInventoryItems(),
          this.syncStocks(),
          this.syncAccountObjects(),
          this.syncOrganizationUnits(),
        ]);

      const result = {
        inventoryItems,
        stocks,
        accountObjects,
        organizationUnits,
      };

      await this.prisma.misaSyncLog.update({
        where: { id: syncLog.id },
        data: {
          status: 'COMPLETED',
          totalRecords:
            inventoryItems + stocks + accountObjects + organizationUnits,
          successCount:
            inventoryItems + stocks + accountObjects + organizationUnits,
          completedAt: new Date(),
        },
      });

      this.logger.log(
        `✅ Full dictionary sync completed: ${JSON.stringify(result)}`,
      );
      return result;
    } catch (error) {
      await this.prisma.misaSyncLog.update({
        where: { id: syncLog.id },
        data: {
          status: 'FAILED',
          errorMessage: error.message,
          completedAt: new Date(),
        },
      });
      throw error;
    }
  }

  /**
   * Sync Inventory Items (data_type = 2)
   */
  async syncInventoryItems(): Promise<number> {
    this.logger.log('📦 Syncing inventory items...');
    let totalSynced = 0;
    let skip = 0;
    const take = 1000;

    while (true) {
      const items = await this.fetchDictionary<MisaInventoryItemDto>(
        2,
        skip,
        take,
      );

      if (!items || items.length === 0) {
        break;
      }

      for (const item of items) {
        // Validate required fields
        if (!item.inventory_item_id || !item.inventory_item_code) {
          this.logger.warn(
            `⚠️ Skipping inventory item with missing id or code: ${JSON.stringify(item).substring(0, 100)}`,
          );
          continue;
        }

        try {
          await this.prisma.misaInventoryItem.upsert({
            where: { inventoryItemId: item.inventory_item_id },
            update: {
              inventoryItemCode: item.inventory_item_code,
              inventoryItemName: item.inventory_item_name || '',
              inventoryItemType: item.inventory_item_type || 0,
              unitId: item.unit_id || null,
              unitName: item.unit_name || null,
              defaultStockId: item.default_stock_id || null,
              branchId: item.branch_id || null,
              unitPrice: item.unit_price ? Number(item.unit_price) : null,
              salePrice1: item.sale_price1 ? Number(item.sale_price1) : null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
              cogsAccount: item.cogs_account || null,
              saleAccount: item.sale_account || null,
              discountAccount: item.discount_account || null,
            },
            create: {
              inventoryItemId: item.inventory_item_id,
              inventoryItemCode: item.inventory_item_code,
              inventoryItemName: item.inventory_item_name || '',
              inventoryItemType: item.inventory_item_type || 0,
              unitId: item.unit_id || null,
              unitName: item.unit_name || null,
              defaultStockId: item.default_stock_id || null,
              branchId: item.branch_id || null,
              unitPrice: item.unit_price ? Number(item.unit_price) : null,
              salePrice1: item.sale_price1 ? Number(item.sale_price1) : null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
              cogsAccount: item.cogs_account || null,
              saleAccount: item.sale_account || null,
              discountAccount: item.discount_account || null,
            },
          });
          totalSynced++;
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to sync inventory item ${item.inventory_item_code}: ${error.message}`,
          );
        }
      }

      if (items.length < take) {
        break;
      }
      skip += take;
    }

    this.logger.log(`✅ Synced ${totalSynced} inventory items`);
    return totalSynced;
  }

  /**
   * Sync Stocks (data_type = 3)
   */
  async syncStocks(): Promise<number> {
    this.logger.log('🏪 Syncing stocks...');
    let totalSynced = 0;
    let skip = 0;
    const take = 1000;

    while (true) {
      const items = await this.fetchDictionary<MisaStockDto>(3, skip, take);

      if (!items || items.length === 0) {
        break;
      }

      for (const item of items) {
        // Validate required fields
        if (!item.stock_id || !item.stock_code) {
          this.logger.warn(
            `⚠️ Skipping stock with missing id or code: ${JSON.stringify(item).substring(0, 100)}`,
          );
          continue;
        }

        try {
          await this.prisma.misaStock.upsert({
            where: { stockId: item.stock_id },
            update: {
              stockCode: item.stock_code,
              stockName: item.stock_name || '',
              branchId: item.branch_id || null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
            },
            create: {
              stockId: item.stock_id,
              stockCode: item.stock_code,
              stockName: item.stock_name || '',
              branchId: item.branch_id || null,
              inactive: item.inactive || false,
              inventoryAccount: item.inventory_account || null,
            },
          });
          totalSynced++;
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to sync stock ${item.stock_code}: ${error.message}`,
          );
        }
      }

      if (items.length < take) {
        break;
      }
      skip += take;
    }

    this.logger.log(`✅ Synced ${totalSynced} stocks`);
    return totalSynced;
  }

  /**
   * Sync Account Objects (data_type = 1)
   */
  async syncAccountObjects(): Promise<number> {
    this.logger.log('👥 Syncing account objects...');
    let totalSynced = 0;
    let skip = 0;
    const take = 1000;

    while (true) {
      const items = await this.fetchDictionary<MisaAccountObjectDto>(
        1,
        skip,
        take,
      );

      if (!items || items.length === 0) {
        break;
      }

      for (const item of items) {
        // Validate required fields
        if (!item.account_object_id || !item.account_object_code) {
          this.logger.warn(
            `⚠️ Skipping account object with missing id or code: ${JSON.stringify(item).substring(0, 100)}`,
          );
          continue;
        }

        try {
          await this.prisma.misaAccountObject.upsert({
            where: { accountObjectId: item.account_object_id },
            update: {
              accountObjectCode: item.account_object_code,
              accountObjectName: item.account_object_name || '',
              accountObjectType: item.account_object_type || 0,
              address: item.address || null,
              companyTaxCode: item.company_tax_code || null,
              branchId: item.branch_id || null,
              isCustomer: item.is_customer || false,
              isVendor: item.is_vendor || false,
              isEmployee: item.is_employee || false,
              inactive: item.inactive || false,
              payAccount: item.pay_account || null,
              receiveAccount: item.receive_account || null,
            },
            create: {
              accountObjectId: item.account_object_id,
              accountObjectCode: item.account_object_code,
              accountObjectName: item.account_object_name || '',
              accountObjectType: item.account_object_type || 0,
              address: item.address || null,
              companyTaxCode: item.company_tax_code || null,
              branchId: item.branch_id || null,
              isCustomer: item.is_customer || false,
              isVendor: item.is_vendor || false,
              isEmployee: item.is_employee || false,
              inactive: item.inactive || false,
              payAccount: item.pay_account || null,
              receiveAccount: item.receive_account || null,
            },
          });
          totalSynced++;
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to sync account object ${item.account_object_code}: ${error.message}`,
          );
        }
      }

      if (items.length < take) {
        break;
      }
      skip += take;
    }

    this.logger.log(`✅ Synced ${totalSynced} account objects`);
    return totalSynced;
  }

  /**
   * Sync Organization Units (data_type = 6)
   */
  async syncOrganizationUnits(): Promise<number> {
    this.logger.log('🏢 Syncing organization units...');
    let totalSynced = 0;
    let skip = 0;
    const take = 1000;

    while (true) {
      const items = await this.fetchDictionary<MisaOrganizationUnitDto>(
        6,
        skip,
        take,
      );

      if (!items || items.length === 0) {
        break;
      }

      for (const item of items) {
        // Validate required fields
        if (!item.organization_unit_id || !item.organization_unit_code) {
          this.logger.warn(
            `⚠️ Skipping organization unit with missing id or code: ${JSON.stringify(item).substring(0, 100)}`,
          );
          continue;
        }

        try {
          await this.prisma.misaOrganizationUnit.upsert({
            where: { organizationUnitId: item.organization_unit_id },
            update: {
              organizationUnitCode: item.organization_unit_code,
              organizationUnitName: item.organization_unit_name || '',
              organizationUnitTypeId: item.organization_unit_type_id || 1,
              parentId: item.parent_id || null,
              branchId: item.branch_id || null,
            },
            create: {
              organizationUnitId: item.organization_unit_id,
              organizationUnitCode: item.organization_unit_code,
              organizationUnitName: item.organization_unit_name || '',
              organizationUnitTypeId: item.organization_unit_type_id || 1,
              parentId: item.parent_id || null,
              branchId: item.branch_id || null,
            },
          });
          totalSynced++;
        } catch (error) {
          this.logger.warn(
            `⚠️ Failed to sync organization unit ${item.organization_unit_code}: ${error.message}`,
          );
        }
      }

      if (items.length < take) {
        break;
      }
      skip += take;
    }

    this.logger.log(`✅ Synced ${totalSynced} organization units`);
    return totalSynced;
  }

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
      skip,
      take,
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
        this.logger.error(
          `❌ Misa get_dictionary failed: ${data.ErrorCode} - ${data.ErrorMessage}`,
        );
        return [];
      }

      // Xử lý Data có thể là string JSON hoặc array
      let items: T[] = [];

      if (!data.Data) {
        this.logger.warn(`⚠️ No data returned for data_type=${dataType}`);
        return [];
      }

      if (typeof data.Data === 'string') {
        try {
          items = JSON.parse(data.Data);
          this.logger.debug(
            `Parsed Data from string for data_type=${dataType}, count: ${items.length}`,
          );
        } catch (parseError) {
          this.logger.error(
            `❌ Failed to parse Data as JSON for data_type=${dataType}: ${parseError.message}`,
          );
          return [];
        }
      } else if (Array.isArray(data.Data)) {
        items = data.Data;
      } else {
        this.logger.warn(
          `⚠️ Unexpected Data format for data_type=${dataType}: ${typeof data.Data}`,
        );
        return [];
      }

      // Validate items có data hay không
      if (items.length > 0) {
        this.logger.log(
          `📥 Fetched ${items.length} items for data_type=${dataType} (skip=${skip})`,
        );
      }

      return items;
    } catch (error) {
      this.logger.error(
        `❌ Failed to fetch dictionary (type=${dataType}): ${error.message}`,
      );
      return [];
    }
  }

  /**
   * Tìm inventory item theo misa_code từ cache
   */
  async findInventoryItemByCode(misaCode: string): Promise<{
    inventoryItemId: string;
    inventoryItemCode: string;
    inventoryItemName: string;
    unitId: string | null;
    unitName: string | null;
    defaultStockId: string | null;
  } | null> {
    const item = await this.prisma.misaInventoryItem.findFirst({
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
   * Tìm stock theo tên gần giống (fuzzy match) với branchName từ KiotViet
   */
  async findStockByNameFuzzy(branchName: string): Promise<{
    stockId: string;
    stockCode: string;
    stockName: string;
  } | null> {
    // Chuẩn hóa tên để so sánh
    const normalizedBranchName = this.normalizeString(branchName);

    const stocks = await this.prisma.misaStock.findMany({
      where: { inactive: false },
      select: {
        stockId: true,
        stockCode: true,
        stockName: true,
      },
    });

    // Tìm stock có tên gần giống nhất
    let bestMatch: (typeof stocks)[0] | null = null;
    let bestScore = 0;

    for (const stock of stocks) {
      const normalizedStockName = this.normalizeString(stock.stockName);
      const score = this.calculateSimilarity(
        normalizedBranchName,
        normalizedStockName,
      );

      if (score > bestScore && score >= 0.3) {
        // Ngưỡng 30% similarity
        bestScore = score;
        bestMatch = stock;
      }
    }

    if (bestMatch) {
      this.logger.log(
        `✅ Matched branch "${branchName}" to stock "${bestMatch.stockName}" (score: ${bestScore.toFixed(2)})`,
      );
    }

    return bestMatch;
  }

  /**
   * Lấy kho mặc định (Kho Hà Nội)
   */
  async getDefaultStock(): Promise<{
    stockId: string;
    stockCode: string;
    stockName: string;
  } | null> {
    // Tìm stock có isDefault = true
    let stock = await this.prisma.misaStock.findFirst({
      where: { isDefault: true, inactive: false },
      select: {
        stockId: true,
        stockCode: true,
        stockName: true,
      },
    });

    // Fallback: tìm theo tên có chứa "Hà Nội"
    if (!stock) {
      stock = await this.prisma.misaStock.findFirst({
        where: {
          stockName: { contains: 'Hà Nội', mode: 'insensitive' },
          inactive: false,
        },
        select: {
          stockId: true,
          stockCode: true,
          stockName: true,
        },
      });
    }

    // Fallback: lấy stock đầu tiên
    if (!stock) {
      stock = await this.prisma.misaStock.findFirst({
        where: { inactive: false },
        select: {
          stockId: true,
          stockCode: true,
          stockName: true,
        },
      });
    }

    return stock;
  }

  /**
   * Tìm account object (khách hàng) theo tên gần giống
   */
  async findAccountObjectByNameFuzzy(customerName: string): Promise<{
    accountObjectId: string;
    accountObjectCode: string;
    accountObjectName: string;
  } | null> {
    const normalizedCustomerName = this.normalizeString(customerName);

    const accountObjects = await this.prisma.misaAccountObject.findMany({
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

    let bestMatch: (typeof accountObjects)[0] | null = null;
    let bestScore = 0;

    for (const ao of accountObjects) {
      const normalizedName = this.normalizeString(ao.accountObjectName);
      const score = this.calculateSimilarity(
        normalizedCustomerName,
        normalizedName,
      );

      if (score > bestScore && score >= 0.5) {
        // Ngưỡng 50% similarity cho khách hàng
        bestScore = score;
        bestMatch = ao;
      }
    }

    if (bestMatch) {
      this.logger.log(
        `✅ Matched customer "${customerName}" to account object "${bestMatch.accountObjectName}" (score: ${bestScore.toFixed(2)})`,
      );
    }

    return bestMatch;
  }

  /**
   * Chuẩn hóa string để so sánh
   */
  private normalizeString(str: string): string {
    return str
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '') // Bỏ dấu tiếng Việt
      .replace(/đ/g, 'd')
      .replace(/[^a-z0-9\s]/g, '') // Chỉ giữ chữ cái và số
      .replace(/\s+/g, ' ')
      .trim();
  }

  /**
   * Tính độ tương đồng giữa 2 string (Jaccard similarity)
   */
  private calculateSimilarity(str1: string, str2: string): number {
    const words1 = new Set(str1.split(' ').filter((w) => w.length > 0));
    const words2 = new Set(str2.split(' ').filter((w) => w.length > 0));

    const intersection = new Set([...words1].filter((w) => words2.has(w)));
    const union = new Set([...words1, ...words2]);

    if (union.size === 0) return 0;
    return intersection.size / union.size;
  }
}
