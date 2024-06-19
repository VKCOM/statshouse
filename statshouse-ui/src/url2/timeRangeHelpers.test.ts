import 'testMock/matchMedia.mock';
import { TIME_RANGE_KEYS_TO } from 'api/enum';
import { getEndDay, getEndWeek, getNow, readTimeRange, toDateTime, toTimeStamp } from './timeRangeHelpers';

const nowDateTime = '2020-01-01 00:00:00';
jest.useFakeTimers().setSystemTime(new Date(nowDateTime));

describe('timeRangeHelpers', () => {
  test('toTimeStamp', () => {
    expect(toTimeStamp(1000)).toBe(1);
  });
  test('toDateTime', () => {
    const time = new Date(nowDateTime);
    const time2 = new Date('2020-02-02 02:02:02');
    expect(toDateTime().toISOString()).toBe(time.toISOString());
    expect(toDateTime(toTimeStamp(+time2)).toISOString()).toBe(time2.toISOString());
  });
  test('getNow', () => {
    const time = new Date(nowDateTime);
    const time2 = new Date('2020-02-02 02:02:02');
    expect(getNow()).toBe(toTimeStamp(+time));
    expect(getNow(toTimeStamp(+time2))).toBe(toTimeStamp(+time2));
  });
  test('getEndDay', () => {
    const time = new Date('2020-02-02 02:02:02');
    const time2 = new Date('2020-02-02 23:59:59');
    expect(getEndDay(toTimeStamp(+time))).toBe(toTimeStamp(+time2));
  });
  test('getEndWeek', () => {
    const time = new Date('2020-01-27 02:02:02'); //?
    const time2 = new Date('2020-02-02 23:59:59'); //?
    expect(getEndWeek(toTimeStamp(+time))).toBe(toTimeStamp(+time2));
    const time1 = new Date('2020-02-02 02:02:02'); //?
    const time12 = new Date('2020-02-02 23:59:59'); //?
    expect(getEndWeek(toTimeStamp(+time1))).toBe(toTimeStamp(+time12));
  });

  test('readTimeRange', () => {
    expect(readTimeRange(undefined, undefined)).toEqual({
      absolute: true,
      from: 0,
      now: 1577826000,
      to: 1577826000,
      urlTo: 1577826000,
    });

    expect(readTimeRange('1577825000', TIME_RANGE_KEYS_TO.default)).toEqual({
      absolute: true,
      from: -1000,
      now: 1577826000,
      to: 1577826000,
      urlTo: 1577826000,
    });

    expect(readTimeRange('-2000', '1577825000')).toEqual({
      absolute: true,
      from: -2000,
      now: 1577826000,
      to: 1577825000,
      urlTo: 1577825000,
    });

    expect(readTimeRange('-2000', TIME_RANGE_KEYS_TO.Now)).toEqual({
      absolute: false,
      from: -2000,
      now: 1577826000,
      to: 1577826000,
      urlTo: TIME_RANGE_KEYS_TO.Now,
    });

    expect(readTimeRange('-2000', TIME_RANGE_KEYS_TO.EndDay)).toEqual({
      absolute: false,
      from: -2000,
      now: 1577826000,
      to: 1577912399,
      urlTo: TIME_RANGE_KEYS_TO.EndDay,
    });

    expect(readTimeRange('-2000', TIME_RANGE_KEYS_TO.EndWeek)).toEqual({
      absolute: false,
      from: -2000,
      now: 1577826000,
      to: 1578257999,
      urlTo: TIME_RANGE_KEYS_TO.EndWeek,
    });

    expect(readTimeRange('-2000', '-2000')).toEqual({
      absolute: false,
      from: -2000,
      now: 1577826000,
      to: 1577824000,
      urlTo: -2000,
    });
  });
});
