// src/services/lark/lark.module.ts
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { LarkBaseService } from './lark-base.service';

@Module({
  imports: [ConfigModule],
  providers: [LarkBaseService],
  exports: [LarkBaseService],
})
export class LarkModule {}
