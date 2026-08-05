import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, OnModuleDestroy
{
  private connectionPromise?: Promise<void>;

  constructor() {
    super();
  }

  async onModuleInit() {
    await this.ensureConnected();
  }

  /**
   * Prisma's engine can be disconnected after a process restart or an idle DB
   * connection drop. All sync entry points call this before their first query.
   */
  async ensureConnected(): Promise<void> {
    if (!this.connectionPromise) {
      this.connectionPromise = this.$connect().catch((error) => {
        this.connectionPromise = undefined;
        throw error;
      });
    }
    await this.connectionPromise;
  }

  async onModuleDestroy() {
    await this.$disconnect();
    this.connectionPromise = undefined;
  }
}
