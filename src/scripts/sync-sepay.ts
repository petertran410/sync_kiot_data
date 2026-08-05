import { NestFactory } from '@nestjs/core';
import { AppModule } from '../app.module';
import { SePaySyncService } from '../services/sepay/sepay-sync.service';

async function run(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  try {
    const syncService = app.get(SePaySyncService);
    const result = await syncService.syncAll();
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await app.close();
  }
}

run().catch((error: any) => {
  process.stderr.write(`SePay full sync failed: ${error?.message ?? error}\n`);
  process.exitCode = 1;
});
