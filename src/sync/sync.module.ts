import { Module } from '@nestjs/common';
import { SyncService } from './sync.service';
import { PrismaModule } from '../prisma/prisma.module';
import { KiotVietModule } from '../services/kiot-viet/kiot-viet.module';

@Module({
  imports: [PrismaModule, KiotVietModule],
  providers: [SyncService],
  controllers: [],
  exports: [SyncService],
})
export class SyncModule {}
