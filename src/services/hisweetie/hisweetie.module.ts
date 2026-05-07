import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { HisweetieNotifyService } from './hisweetie-notify.service';

@Module({
  imports: [HttpModule.register({ timeout: 10000 }), ConfigModule],
  providers: [HisweetieNotifyService],
  exports: [HisweetieNotifyService],
})
export class HisweetieModule {}
