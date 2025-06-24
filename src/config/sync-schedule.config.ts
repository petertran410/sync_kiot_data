// src/config/sync-schedule.config.ts
export type ScheduleType = 'every_15_minutes' | 'weekends' | 'disabled';

export interface SyncEntityConfig {
  name: string;
  service: string;
  syncMethod: string;
  syncType: 'simple' | 'full';
  schedule: ScheduleType;
  dependencies?: string[];
  retryCount?: number;
  description?: string;
  hasLarkBaseSync: boolean; // NEW: Does this entity sync to LarkBase?
  larkBaseBatchSize?: number; // NEW: Batch size for LarkBase operations
  larkBaseRetryLimit?: number; // NEW: Max retry attempts
}

export const SYNC_ENTITIES_CONFIG: SyncEntityConfig[] = [
  // === EVERY 15 MINUTES ===
  {
    name: 'customergroup',
    service: 'customerGroupService',
    syncMethod: 'syncCustomerGroups',
    syncType: 'simple',
    schedule: 'every_15_minutes',
    hasLarkBaseSync: false,
    description: 'Customer group sync every 15 minutes',
  },
  {
    name: 'customer',
    service: 'customerService',
    syncMethod: 'syncHistoricalCustomers',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customergroup'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Customer sync every 15 minutes',
  },
  {
    name: 'order',
    service: 'orderService',
    syncMethod: 'syncHistoricalOrders',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customer'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Order sync every 15 minutes',
  },
  {
    name: 'invoice',
    service: 'invoiceService',
    syncMethod: 'syncHistoricalInvoices',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customer', 'order'],
    hasLarkBaseSync: true,
    larkBaseBatchSize: 50,
    larkBaseRetryLimit: 3,
    description: 'Invoice sync every 15 minutes',
  },

  // === WEEKENDS ===
  {
    name: 'user',
    service: 'userService',
    syncMethod: 'syncHistoricalUsers',
    syncType: 'full',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'User sync runs on weekends only',
  },
  {
    name: 'salechannel',
    service: 'saleChannelService',
    syncMethod: 'syncSaleChannels',
    syncType: 'simple',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Sale channel sync runs on weekends only',
  },
  {
    name: 'surcharge',
    service: 'surchargeService',
    syncMethod: 'syncSurcharges',
    syncType: 'simple',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Surcharge sync runs on weekends only',
  },
  {
    name: 'bankaccount',
    service: 'bankAccountService',
    syncMethod: 'syncBankAccounts',
    syncType: 'simple',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Bank account sync runs on weekends only',
  },
  {
    name: 'category',
    service: 'categoryService',
    syncMethod: 'syncCategories',
    syncType: 'simple',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Category sync runs on weekends only',
  },
  {
    name: 'product',
    service: 'productService',
    syncMethod: 'syncHistoricalProducts',
    syncType: 'full',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Product sync runs on weekends only',
  },
  {
    name: 'trademark',
    service: 'tradeMarkService',
    syncMethod: 'syncTradeMarks',
    syncType: 'simple',
    schedule: 'weekends',
    hasLarkBaseSync: false,
    description: 'Trademark sync runs on weekends only',
  },

  // === MANUAL ONLY ===
  {
    name: 'branch',
    service: 'branchService',
    syncMethod: 'syncBranches',
    syncType: 'simple',
    schedule: 'disabled',
    hasLarkBaseSync: false,
    description: 'Branch sync disabled per requirements',
  },
];

export function getEntitiesBySchedule(
  schedule: ScheduleType,
): SyncEntityConfig[] {
  return SYNC_ENTITIES_CONFIG.filter((entity) => entity.schedule === schedule);
}

export function getEntitiesWithLarkBaseSync(): SyncEntityConfig[] {
  return SYNC_ENTITIES_CONFIG.filter((entity) => entity.hasLarkBaseSync);
}
