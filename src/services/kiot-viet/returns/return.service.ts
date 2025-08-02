// import { Inject, Injectable, Logger } from '@nestjs/common';
// import { HttpService } from '@nestjs/axios';
// import { ConfigService } from '@nestjs/config';
// import { async, firstValueFrom } from 'rxjs';
// import { PrismaService } from '../../../prisma/prisma.service';
// import { KiotVietAuthService } from '../auth.service';
// import { Prisma } from '@prisma/client';

// interface KiotVietReturn {
//   id: number;
//   code: string;
//   invoiceId: number;
//   returnDate: string;
//   branchId: number;
//   branchName: string;
//   receivedById: Number;
//   soldByName: string;
//   customerId?: number;
//   customerCode: string;
//   customerName: string;
//   returnTotal: number;
//   totalPayment: number;
//   status: number;
//   statusValue: string;
//   createdDate: string;
//   modifiedDate: string;
//   payments: Array<{
//     id: number;
//     code: string;
//     amount: number;
//     method: string;
//     status?: number;
//     statusValue: string;
//     transDate: string;
//     bankAccount: string;
//     accountId?: number;
//     description: string;
//   }>;
// }
