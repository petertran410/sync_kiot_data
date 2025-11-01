import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

@Injectable()
export class NumberFormatInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(map((data) => this.formatNumbers(data)));
  }

  private formatNumbers(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }

    if (typeof obj === 'bigint') {
      return this.formatNumber(Number(obj));
    }

    if (typeof obj === 'number') {
      return this.formatNumber(obj);
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.formatNumbers(item));
    }

    if (typeof obj === 'object') {
      const formatted: any = {};
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          formatted[key] = this.formatNumbers(obj[key]);
        }
      }
      return formatted;
    }

    return obj;
  }

  private formatNumber(num: number): string {
    return new Intl.NumberFormat('en-US').format(Math.round(num));
  }
}
