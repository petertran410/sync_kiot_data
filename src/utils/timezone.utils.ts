// src/utils/timezone.utils.ts

/**
 * ✅ FIXED: Timezone Utility for handling KiotViet → LarkBase datetime conversion
 *
 * SOLUTION: Use proper timezone handling instead of manual adjustment
 * Works correctly both in localhost and Docker container
 */

export class TimezoneUtils {
  private static readonly VIETNAM_TIMEZONE = 'Asia/Ho_Chi_Minh';

  /**
   * ✅ PROPER FIX: Convert KiotViet datetime to LarkBase timestamp
   * No manual timezone adjustment needed when Docker timezone is set correctly
   *
   * @param kiotVietDateStr - Date string from KiotViet API
   * @returns Timestamp for LarkBase (milliseconds)
   */
  static convertKiotVietDateToLarkTimestamp(
    kiotVietDateStr: string | Date,
  ): number {
    if (!kiotVietDateStr) {
      return Date.now();
    }

    try {
      let date: Date;

      if (kiotVietDateStr instanceof Date) {
        date = kiotVietDateStr;
      } else {
        // Clean the date string
        const cleanDateStr = kiotVietDateStr
          .replace('Z', '')
          .replace(/\.\d{3,7}/, '');

        // Parse the date - when Docker timezone is set correctly,
        // this will automatically handle Vietnam timezone
        date = new Date(cleanDateStr);

        // Validate the parsed date
        if (isNaN(date.getTime())) {
          console.warn(
            `[TIMEZONE] Invalid date: ${kiotVietDateStr}, using current time`,
          );
          date = new Date();
        }
      }

      const timestamp = date.getTime();

      // Debug logging (can be removed in production)
      console.log(`[TIMEZONE] Original: ${kiotVietDateStr}`);
      console.log(`[TIMEZONE] Parsed Date: ${date.toISOString()}`);
      console.log(
        `[TIMEZONE] Vietnam Time: ${date.toLocaleString('en-US', { timeZone: TimezoneUtils.VIETNAM_TIMEZONE })}`,
      );
      console.log(`[TIMEZONE] Timestamp: ${timestamp}`);

      return timestamp;
    } catch (error) {
      console.error(
        `[TIMEZONE] Failed to convert date: ${kiotVietDateStr}`,
        error,
      );
      return Date.now();
    }
  }

  /**
   * ✅ ALTERNATIVE: Parse date explicitly in Vietnam timezone
   * Use this if Docker timezone setting doesn't work
   */
  static parseAsVietnamTime(dateStr: string): Date {
    if (!dateStr) {
      return new Date();
    }

    try {
      // Remove timezone info and microseconds
      const cleanStr = dateStr.replace('Z', '').replace(/\.\d{3,7}/, '');

      // Parse date components
      const parts = cleanStr.match(
        /(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/,
      );

      if (!parts) {
        return new Date(dateStr);
      }

      const [, year, month, day, hour, minute, second] = parts;

      // Create date in Vietnam timezone using Intl.DateTimeFormat
      const vietnamTime = new Intl.DateTimeFormat('en-CA', {
        timeZone: TimezoneUtils.VIETNAM_TIMEZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });

      // Create a proper Vietnam timezone date
      const tempDate = new Date(
        `${year}-${month}-${day}T${hour}:${minute}:${second}`,
      );

      // Get UTC offset for Vietnam timezone
      const vietnamOffset = 7 * 60; // UTC+7 in minutes
      const localOffset = tempDate.getTimezoneOffset();

      // Adjust for timezone difference
      const adjustedDate = new Date(
        tempDate.getTime() + (localOffset + vietnamOffset) * 60 * 1000,
      );

      return adjustedDate;
    } catch (error) {
      console.error(`[TIMEZONE] Vietnam parsing failed: ${dateStr}`, error);
      return new Date(dateStr);
    }
  }

  /**
   * ✅ FORMAT: Display timestamp in Vietnam timezone
   */
  static formatForLarkBase(timestamp: number): string {
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      timeZone: TimezoneUtils.VIETNAM_TIMEZONE,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    });
  }

  /**
   * ✅ VALIDATION: Check if timezone conversion is working
   */
  static validateTimezoneSetup(): {
    isCorrect: boolean;
    currentTimezone: string;
    expectedTimezone: string;
    recommendation: string;
  } {
    const now = new Date();
    const currentTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const expectedTimezone = TimezoneUtils.VIETNAM_TIMEZONE;

    const isCorrect = currentTimezone === expectedTimezone;

    return {
      isCorrect,
      currentTimezone,
      expectedTimezone,
      recommendation: isCorrect
        ? 'Timezone is correctly set to Vietnam timezone'
        : `Set TZ=${expectedTimezone} in Docker environment or use parseAsVietnamTime() method`,
    };
  }

  /**
   * ✅ DEBUG: Compare different timezone handling methods
   */
  static debugTimezoneConversion(dateStr: string): {
    original: string;
    standardConversion: number;
    vietnamConversion: number;
    difference: number;
    recommendation: string;
  } {
    const standardTimestamp = this.convertKiotVietDateToLarkTimestamp(dateStr);
    const vietnamTimestamp = this.parseAsVietnamTime(dateStr).getTime();
    const difference = Math.abs(standardTimestamp - vietnamTimestamp);

    return {
      original: dateStr,
      standardConversion: standardTimestamp,
      vietnamConversion: vietnamTimestamp,
      difference,
      recommendation:
        difference > 1000
          ? 'Use parseAsVietnamTime() method for consistent results'
          : 'Standard conversion is working correctly',
    };
  }
}
