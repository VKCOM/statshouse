import type { TimeRange } from './queryParams';
import { toNumber } from 'common/helpers';
import { TIME_RANGE_KEYS_TO, type TimeRangeKeysTo, toTimeRangeKeysTo } from 'api/enum';

export function toTimeStamp(time: number) {
  return Math.floor(time / 1000);
}

export function toDateTime(now?: number) {
  return now != null ? new Date(now * 1000) : new Date();
}

export function getNow(now?: number): number {
  if (typeof now === 'number') {
    return now;
  }
  return toTimeStamp(Date.now());
}

export function getEndDay(now?: number) {
  let time = toDateTime(now);
  time.setHours(23, 59, 59, 0);
  return toTimeStamp(+time);
}

export function getEndWeek(now?: number) {
  let time = toDateTime(now);
  time.setHours(23, 59, 59, 0);
  time.setDate(time.getDate() - (time.getDay() || 7) + 7);
  return toTimeStamp(+time);
}

export function readTimeRange(from: unknown, to: unknown): TimeRange {
  const timeNow = getNow();
  let urlTo = toNumber(to) || toTimeRangeKeysTo(to, TIME_RANGE_KEYS_TO.default); //?
  let timeTo;
  let timeAbsolute = (typeof urlTo === 'number' && urlTo > 0) || urlTo === TIME_RANGE_KEYS_TO.default;
  switch (urlTo) {
    case TIME_RANGE_KEYS_TO.EndDay:
      timeTo = getEndDay(timeNow);
      break;
    case TIME_RANGE_KEYS_TO.EndWeek:
      timeTo = getEndWeek(timeNow);
      break;
    case TIME_RANGE_KEYS_TO.Now:
      timeTo = timeNow;
      break;
    case TIME_RANGE_KEYS_TO.default:
      urlTo = timeTo = timeNow;
      break;
    default:
      timeTo = urlTo;
  }
  if (typeof urlTo === 'number' && urlTo <= 0) {
    timeTo = urlTo + timeNow;
  }

  let timeFrom = toNumber(from, 0);
  if (timeFrom > 0) {
    timeFrom = timeFrom - timeTo;
  }
  return { urlTo, to: timeTo, from: timeFrom, now: timeNow, absolute: timeAbsolute };
}

export function getTimeRangeAbsolute(timeRange: TimeRange) {
  return { from: timeRange.from + timeRange.to, to: timeRange.to };
}

export function constToTime(now: number, value: number | TimeRangeKeysTo) {
  switch (value) {
    case TIME_RANGE_KEYS_TO.EndDay:
      return getEndDay(now);
    case TIME_RANGE_KEYS_TO.EndWeek:
      return getEndWeek(now);
    default:
      return getNow(now);
  }
}
