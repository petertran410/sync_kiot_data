import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from '../../../prisma/prisma.module';
import { KiotVietAuthService } from '../auth.service';
import { KiotRateLimiter } from './kiot-rate-limiter';
import { KiotPageFetcher } from './kiot-page-fetcher';
import { BulkUpsertHelper } from './bulk-upsert.helper';
import { RelationMapHelper } from './relation-map.helper';
import { SyncControlHelper } from './sync-control.helper';
import { RetailerContext } from './retailer-context';
import { RemovedIdsHandler } from './removed-ids.handler';

/**
 * Shared infrastructure for KiotViet sync services.
 * Provides rate limiter, page fetcher, bulk upsert, relation map, sync control.
 */
@Module({
  imports: [HttpModule, ConfigModule, PrismaModule],
  providers: [
    KiotVietAuthService,
    KiotRateLimiter,
    KiotPageFetcher,
    BulkUpsertHelper,
    RelationMapHelper,
    SyncControlHelper,
    RetailerContext,
    RemovedIdsHandler,
  ],
  exports: [
    KiotVietAuthService,
    KiotRateLimiter,
    KiotPageFetcher,
    BulkUpsertHelper,
    RelationMapHelper,
    SyncControlHelper,
    RetailerContext,
    RemovedIdsHandler,
  ],
})
export class KiotVietSharedModule {}
