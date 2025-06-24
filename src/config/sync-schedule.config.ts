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
}

export const SYNC_ENTITIES_CONFIG: SyncEntityConfig[] = [
  // === DISABLED ENTITIES ===
  {
    name: 'branch',
    service: 'branchService',
    syncMethod: 'syncBranches',
    syncType: 'simple',
    schedule: 'disabled',
    description: 'Branch sync disabled per requirements',
  },

  // === WEEKEND-ONLY ENTITIES ===
  {
    name: 'user',
    service: 'userService',
    syncMethod: 'syncHistoricalUsers',
    syncType: 'full',
    schedule: 'weekends',
    description: 'User sync runs on weekends only',
  },
  {
    name: 'salechannel',
    service: 'saleChannelService',
    syncMethod: 'syncSaleChannels',
    syncType: 'simple',
    schedule: 'weekends',
    description: 'Sale channel sync runs on weekends only',
  },
  {
    name: 'surcharge',
    service: 'surchargeService',
    syncMethod: 'syncSurcharges',
    syncType: 'simple',
    schedule: 'weekends',
    description: 'Surcharge sync runs on weekends only',
  },
  {
    name: 'bankaccount',
    service: 'bankAccountService',
    syncMethod: 'syncBankAccounts',
    syncType: 'simple',
    schedule: 'weekends',
    description: 'Bank account sync runs on weekends only',
  },
  {
    name: 'category',
    service: 'categoryService',
    syncMethod: 'syncCategories',
    syncType: 'simple',
    schedule: 'weekends',
    dependencies: [],
    description: 'Category sync runs on weekends only',
  },
  {
    name: 'product',
    service: 'productService',
    syncMethod: 'syncHistoricalProducts',
    syncType: 'full',
    schedule: 'weekends',
    description: 'Product sync runs on weekends only',
  },
  {
    name: 'trademark',
    service: 'tradeMarkService',
    syncMethod: 'syncTradeMarks',
    syncType: 'simple',
    schedule: 'weekends',
    description: 'Trademark sync runs on weekends only',
  },

  // === EVERY 15 MINUTES ENTITIES ===
  {
    name: 'customergroup',
    service: 'customerGroupService',
    syncMethod: 'syncCustomerGroups',
    syncType: 'simple',
    schedule: 'every_15_minutes',
    description: 'Customer group sync every 15 minutes',
  },
  {
    name: 'customer',
    service: 'customerService',
    syncMethod: 'syncHistoricalCustomers',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customergroup'],
    description: 'Customer sync every 15 minutes',
  },
  {
    name: 'order',
    service: 'orderService',
    syncMethod: 'syncHistoricalOrders',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customer'],
    description: 'Order sync every 15 minutes',
  },
  {
    name: 'invoice',
    service: 'invoiceService',
    syncMethod: 'syncHistoricalInvoices',
    syncType: 'full',
    schedule: 'every_15_minutes',
    dependencies: ['customer', 'order'],
    description: 'Invoice sync every 15 minutes',
  },
];

// Helper function to get entities by schedule
export function getEntitiesBySchedule(
  schedule: ScheduleType,
): SyncEntityConfig[] {
  return SYNC_ENTITIES_CONFIG.filter((entity) => entity.schedule === schedule);
}

// Helper function to add new entity (for future use)
export function addNewEntity(entity: SyncEntityConfig): void {
  SYNC_ENTITIES_CONFIG.push(entity);
}
