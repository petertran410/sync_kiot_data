// src/config/sync-schedule.config.ts
export interface SyncEntityConfig {
  name: string;
  service: string;
  syncMethod: string;
  syncType: 'simple' | 'full';
  dependencies?: string[];
  description?: string;
  hasLarkBaseSync: boolean;
  larkBaseBatchSize?: number;
  larkBaseRetryLimit?: number;
}

// ⭐ SIMPLIFIED: Remove schedule field, keep entity info for future scaling
export const SYNC_ENTITIES_CONFIG: SyncEntityConfig[] = [
  // === CURRENT: CUSTOMER FOCUS ===
  {
    name: 'customergroup',
    service: 'customerGroupService',
    syncMethod: 'syncCustomerGroups',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Customer group sync',
  },
  {
    name: 'customer',
    service: 'customerService',
    syncMethod: 'checkAndRunAppropriateSync',
    syncType: 'full',
    dependencies: ['customergroup'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Customer sync',
  },

  // === FUTURE: OTHER ENTITIES (Ready for scaling) ===
  {
    name: 'order',
    service: 'orderService',
    syncMethod: 'checkAndRunAppropriateSync',
    syncType: 'full',
    dependencies: ['customer'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Order sync',
  },
  {
    name: 'invoice',
    service: 'invoiceService',
    syncMethod: 'checkAndRunAppropriateSync',
    syncType: 'full',
    dependencies: ['customer', 'order'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Invoice sync',
  },

  // === BACKGROUND ENTITIES ===
  {
    name: 'user',
    service: 'userService',
    syncMethod: 'syncHistoricalUsers',
    syncType: 'full',
    hasLarkBaseSync: false,
    description: 'User sync',
  },
  {
    name: 'salechannel',
    service: 'saleChannelService',
    syncMethod: 'syncSaleChannels',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Sale channel sync',
  },
  {
    name: 'surcharge',
    service: 'surchargeService',
    syncMethod: 'syncSurcharges',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Surcharge sync',
  },
  {
    name: 'bankaccount',
    service: 'bankAccountService',
    syncMethod: 'syncBankAccounts',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Bank account sync',
  },
  {
    name: 'category',
    service: 'categoryService',
    syncMethod: 'syncCategories',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Category sync',
  },
  {
    name: 'product',
    service: 'productService',
    syncMethod: 'syncHistoricalProducts',
    syncType: 'full',
    hasLarkBaseSync: false,
    description: 'Product sync',
  },
  {
    name: 'trademark',
    service: 'tradeMarkService',
    syncMethod: 'syncTradeMarks',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Trademark sync',
  },
  {
    name: 'branch',
    service: 'branchService',
    syncMethod: 'syncBranches',
    syncType: 'simple',
    hasLarkBaseSync: false,
    description: 'Branch sync',
  },
];

// ⭐ KEEP: Useful utility functions
export function getEntitiesWithLarkBaseSync(): SyncEntityConfig[] {
  return SYNC_ENTITIES_CONFIG.filter((entity) => entity.hasLarkBaseSync);
}

export function getEntityConfig(name: string): SyncEntityConfig | undefined {
  return SYNC_ENTITIES_CONFIG.find((entity) => entity.name === name);
}
