// src/types/sync.types.ts
export interface EntityStatus {
  syncType: 'historical' | 'recent' | 'simple';
  error: string | null;
  id: number;
  name: string;
  entities: string[];
  isRunning: boolean;
  isEnabled: boolean;
  startedAt: Date | null;
  completedAt: Date | null;
  progress: any;
  completedEntities: string[];
  currentEntity: string | null;
  syncMode: string;
  status: string;
  lastRunAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface EntityStatusSummary {
  entityName: string;
  entitySyncType: 'full' | 'simple';
  statuses: EntityStatus[];
}

export interface BusStatusResponse {
  busScheduler: any;
  entities: EntityStatusSummary[];
  isRunning: boolean;
}
