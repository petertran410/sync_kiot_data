import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import * as Joi from 'joi';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { KiotVietModule } from './services/kiot-viet/kiot-viet.module';
import { SyncModule } from './services/sync/sync.module';
import { SyncController } from './controllers/sync.controller';
import { WebhookModule } from './services/webhook/webhook.module';
import { WebhookController } from './controllers/webhook.controller';
import { WebhookAdminController } from './controllers/webhook-admin.controller';
import { HttpModule } from '@nestjs/axios';
import { LarkModule } from './services/lark/lark.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      validationSchema: Joi.object({
        // KiotViet API
        KIOT_CLIEND_ID: Joi.string().required(),
        KIOT_SECRET_KEY: Joi.string().required(),
        KIOT_TOKEN: Joi.string().required(),
        KIOT_SHOP_NAME: Joi.string().required(),
        KIOT_BASE_URL: Joi.string().uri().required(),
        // Database
        DATABASE_URL: Joi.string().required(),
        // App
        PORT: Joi.number().default(8083),
        // Webhook HMAC. Required unless WEBHOOK_ALLOW_UNSIGNED=true is set
        // explicitly. The guard fails closed: without a secret it rejects every
        // request rather than accepting unauthenticated writes.
        KIOT_WEBHOOK_SECRET: Joi.string().allow('').default(''),
        KIOT_WEBHOOK_ALLOW_UNSIGNED: Joi.boolean().default(false),
        // Webhook worker tuning
        WEBHOOK_WORKER_ENABLED: Joi.boolean().default(true),
        WEBHOOK_WORKER_BATCH_SIZE: Joi.number().default(20),
        WEBHOOK_WORKER_POLL_MS: Joi.number().default(2000),
        WEBHOOK_MAX_ATTEMPTS: Joi.number().default(8),
        KIOT_HTTP_TIMEOUT_MS: Joi.number().default(20000),
        // Public base URL used when registering webhooks with KiotViet
        WEBHOOK_PUBLIC_BASE_URL: Joi.string().uri().optional(),
        // Protects the /webhooks admin endpoints. Without it they reject everything.
        ADMIN_API_KEY: Joi.string().allow('').default(''),
        // Scheduled jobs (see SyncSchedulerService). Both default to on.
        SYNC_CRON_ENABLED: Joi.boolean().default(true),
        SYNC_FULL_SWEEP_CRON_ENABLED: Joi.boolean().default(true),
        WEBHOOK_RECONCILE_CRON_ENABLED: Joi.boolean().default(true),
        // Sync tuning (optional, with defaults)
        SYNC_FETCH_CONCURRENCY: Joi.number().default(5),
        SYNC_DB_BATCH_SIZE: Joi.number().default(500),
        SYNC_RATE_LIMIT_PER_HOUR: Joi.number().default(4500),
         SYNC_RATE_LIMIT_BURST: Joi.number().default(1),
         // Lark Base customer outbound sync
         LARK_APP_ID: Joi.string().required(),
         LARK_APP_SECRET: Joi.string().required(),
         LARK_CUSTOMER_BASE_ID: Joi.string().required(),
         LARK_CUSTOMER_TABLE_ID: Joi.string().required(),
         LARK_CUSTOMER_SYNC_CRON_ENABLED: Joi.boolean().default(false),
         LARK_CUSTOMER_SYNC_BATCH_SIZE: Joi.number().integer().min(1).max(100).default(50),
      }).unknown(true),
    }),
    // Drives the incremental sync + webhook drift cron jobs.
    ScheduleModule.forRoot(),
    PrismaModule,
    KiotVietModule,
    SyncModule,
     WebhookModule,
     LarkModule,
     HttpModule,
  ],
  controllers: [
    AppController,
    SyncController,
    WebhookController,
    WebhookAdminController,
  ],
  providers: [AppService],
})
export class AppModule {}
