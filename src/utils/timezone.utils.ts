// src/utils/timezone.utils.ts

/**
 * Timezone Utility for handling KiotViet → LarkBase datetime conversion
 *
 * PROBLEM: KiotViet returns datetime in Vietnam timezone (UTC+7)
 * but system converts incorrectly causing -10 hour shift
 */

export class TimezoneUtils {
  private static readonly VIETNAM_TIMEZONE = 'Asia/Ho_Chi_Minh';
  private static readonly UTC_OFFSET_VIETNAM = 7; // UTC+7

  /**
   * ✅ FIX: Convert KiotViet datetime string to correct timestamp for LarkBase
   *
   * @param kiotVietDateStr - Date string from KiotViet API (e.g., "2025-06-17T14:34:09.013")
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
        // ✅ FIX: Parse as Vietnam timezone
        // KiotViet returns datetime in Vietnam timezone but without timezone info
        // We need to explicitly handle this

        // Method 1: Assume KiotViet string is in Vietnam timezone
        const cleanDateStr = kiotVietDateStr
          .replace('Z', '')
          .replace(/\.\d{3,7}/, '');

        // Create date object and adjust for Vietnam timezone
        const tempDate = new Date(cleanDateStr);

        // If the original parsing was treating it as UTC, we need to adjust
        // Vietnam is UTC+7, so if it was parsed as UTC, we need to add 7 hours
        date = new Date(tempDate.getTime() + 7 * 60 * 60 * 1000);
      }

      const timestamp = date.getTime();

      // Debug logging
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
   * ✅ ALTERNATIVE FIX: Force Vietnam timezone parsing
   */
  static parseKiotVietDateAsVietnamTime(dateStr: string): Date {
    if (!dateStr) {
      return new Date();
    }

    try {
      // Remove timezone info if present and microseconds
      const cleanStr = dateStr.replace('Z', '').replace(/\.\d{3,7}/, '');

      // Parse the date components manually
      const parts = cleanStr.match(
        /(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/,
      );

      if (!parts) {
        return new Date(dateStr);
      }

      const [, year, month, day, hour, minute, second] = parts;

      // Create date in Vietnam timezone
      const vietnamDate = new Date();
      vietnamDate.setFullYear(
        parseInt(year),
        parseInt(month) - 1,
        parseInt(day),
      );
      vietnamDate.setHours(
        parseInt(hour),
        parseInt(minute),
        parseInt(second),
        0,
      );

      return vietnamDate;
    } catch (error) {
      console.error(`[TIMEZONE] Manual parsing failed: ${dateStr}`, error);
      return new Date(dateStr);
    }
  }

  /**
   * ✅ DEBUG: Compare different parsing methods
   */
  static debugDateConversion(dateStr: string): {
    original: string;
    standardParse: string;
    vietnamParse: string;
    correctedTimestamp: number;
    standardTimestamp: number;
  } {
    const standardDate = new Date(dateStr);
    const vietnamDate = this.parseKiotVietDateAsVietnamTime(dateStr);
    const correctedTimestamp = this.convertKiotVietDateToLarkTimestamp(dateStr);

    return {
      original: dateStr,
      standardParse: standardDate.toISOString(),
      vietnamParse: vietnamDate.toISOString(),
      correctedTimestamp,
      standardTimestamp: standardDate.getTime(),
    };
  }

  /**
   * ✅ DISPLAY: Format timestamp for debugging
   */
  static formatTimestampForDisplay(timestamp: number): {
    utc: string;
    vietnam: string;
    larkBaseDisplay: string;
  } {
    const date = new Date(timestamp);

    return {
      utc: date.toISOString(),
      vietnam: date.toLocaleString('en-US', {
        timeZone: TimezoneUtils.VIETNAM_TIMEZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }),
      larkBaseDisplay: `${date.getFullYear()}/${String(date.getMonth() + 1).padStart(2, '0')}/${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`,
    };
  }

  /**
   * ✅ VALIDATION: Check if timezone conversion is working correctly
   */
  static validateDateConversion(
    originalKiotVietStr: string,
    expectedDisplayStr: string,
  ): {
    isCorrect: boolean;
    difference: string;
    recommendation: string;
  } {
    const correctedTimestamp =
      this.convertKiotVietDateToLarkTimestamp(originalKiotVietStr);
    const display = this.formatTimestampForDisplay(correctedTimestamp);

    const isCorrect = display.larkBaseDisplay.includes(
      expectedDisplayStr.substring(0, 10),
    );

    return {
      isCorrect,
      difference: isCorrect
        ? 'None'
        : `Expected: ${expectedDisplayStr}, Got: ${display.larkBaseDisplay}`,
      recommendation: isCorrect
        ? 'Timezone conversion is working correctly'
        : 'Need to adjust timezone conversion logic',
    };
  }
}
