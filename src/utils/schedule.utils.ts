// src/utils/schedule.utils.ts
export class ScheduleUtils {
  /**
   * Check if current time is weekend (Saturday or Sunday)
   */
  static isWeekend(): boolean {
    const now = new Date();
    const day = now.getDay(); // 0 = Sunday, 6 = Saturday
    return day === 0 || day === 6;
  }

  /**
   * Check if current time is Sunday
   */
  static isSunday(): boolean {
    const now = new Date();
    return now.getDay() === 0;
  }

  /**
   * Check if it's weekend time in Vietnam timezone
   */
  static isWeekendVietnamTime(): boolean {
    const now = new Date();
    const vietnamTime = new Date(
      now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }),
    );
    const day = vietnamTime.getDay();
    return day === 0 || day === 6;
  }

  /**
   * Get next weekend schedule info
   */
  static getNextWeekendInfo(): { nextSunday: Date; daysUntil: number } {
    const now = new Date();
    const currentDay = now.getDay();
    const daysUntilSunday = currentDay === 0 ? 7 : 7 - currentDay;

    const nextSunday = new Date(now);
    nextSunday.setDate(now.getDate() + daysUntilSunday);

    return {
      nextSunday,
      daysUntil: daysUntilSunday,
    };
  }

  /**
   * Log schedule status
   */
  static logScheduleStatus(logger: any): void {
    const isWeekend = this.isWeekendVietnamTime();
    const nextWeekend = this.getNextWeekendInfo();

    logger.log(`Schedule Status: ${isWeekend ? 'WEEKEND' : 'WEEKDAY'}`);
    if (!isWeekend) {
      logger.log(
        `Next weekend sync in ${nextWeekend.daysUntil} days (${nextWeekend.nextSunday.toLocaleDateString()})`,
      );
    }
  }
}
