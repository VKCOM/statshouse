import { formatByMetricType, splitByMetricType } from './formatByMetricType';
import { METRIC_TYPE } from '../api/enum';

describe('formatByMetricType', () => {
  test('none', () => {
    const formatter = formatByMetricType(METRIC_TYPE.none);
    expect(formatter(0)).toEqual('0');
    expect(formatter(0.01111)).toEqual('0.011');
    expect(formatter(0.001111)).toEqual('1.111m');
    expect(formatter(0.000001111)).toEqual('1.111μ');
    expect(formatter(0.000000001111)).toEqual('1.111n');
    expect(formatter(0.000000000001111)).toEqual('1.111p');
    expect(formatter(0.000000000000001111)).toEqual('1.111f');
    expect(formatter(0.000000000000000001111)).toEqual('1.111a');
    expect(formatter(0.000000000000000000001111)).toEqual('1.111z');
    expect(formatter(0.000000000000000000000001111)).toEqual('1.111y');
    expect(formatter(0.000000000000000000000000001111)).toEqual('0.001y');
    expect(formatter(1111)).toEqual('1.111k');
    expect(formatter(1111100)).toEqual('1.111M');
    expect(formatter(1111100000)).toEqual('1.111G');
    expect(formatter(1111100000000)).toEqual('1.111T');
    expect(formatter(1111100000000000)).toEqual('1.111P');
    expect(formatter(1111100000000000000)).toEqual('1.111E');
    expect(formatter(1111100000000000000000)).toEqual('1.111Z');
    expect(formatter(1111100000000000000000000)).toEqual('1.111Y');
    expect(formatter(1111100000000000000000000000)).toEqual('1111.1Y');
  });
  test('byte', () => {
    const formatter = formatByMetricType(METRIC_TYPE.byte);
    expect(formatter(0)).toEqual('0');
    expect(formatter(0.1111)).toEqual('0.1b');
    expect(formatter(0.01111)).toEqual('0b');
    expect(formatter(1)).toEqual('1b');
    expect(formatter(11)).toEqual('11b');
    expect(formatter(111)).toEqual('111b');
    expect(formatter(256)).toEqual('256b');
    expect(formatter(512)).toEqual('512b');
    expect(formatter(970)).toEqual('970b');
    let p = 1024;
    expect(formatter(p)).toEqual('1kb');
    expect(formatter(1.11 * p)).toEqual('1.1kb');
    expect(formatter(1.5 * p)).toEqual('1.5kb');
    expect(formatter(1.9 * p)).toEqual('1.9kb');
    expect(formatter(1.99 * p)).toEqual('2kb');
    expect(formatter(10 * p)).toEqual('10kb');
    expect(formatter(100 * p)).toEqual('100kb');
    expect(formatter(100 * p)).toEqual('100kb');
    expect(formatter(1000 * p)).toEqual('1000kb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Mb');
    expect(formatter(10 * p)).toEqual('10Mb');
    expect(formatter(100 * p)).toEqual('100Mb');
    expect(formatter(1000 * p)).toEqual('1000Mb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Gb');
    expect(formatter(10 * p)).toEqual('10Gb');
    expect(formatter(100 * p)).toEqual('100Gb');
    expect(formatter(1000 * p)).toEqual('1000Gb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Tb');
    expect(formatter(10 * p)).toEqual('10Tb');
    expect(formatter(100 * p)).toEqual('100Tb');
    expect(formatter(1000 * p)).toEqual('1000Tb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Pb');
    expect(formatter(10 * p)).toEqual('10Pb');
    expect(formatter(100 * p)).toEqual('100Pb');
    expect(formatter(1000 * p)).toEqual('1000Pb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Eb');
    expect(formatter(10 * p)).toEqual('10Eb');
    expect(formatter(100 * p)).toEqual('100Eb');
    expect(formatter(1000 * p)).toEqual('1000Eb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Zb');
    expect(formatter(10 * p)).toEqual('10Zb');
    expect(formatter(100 * p)).toEqual('100Zb');
    expect(formatter(1000 * p)).toEqual('1000Zb');
    p *= 1024;
    expect(formatter(p)).toEqual('1Yb');
    expect(formatter(10 * p)).toEqual('10Yb');
    expect(formatter(100 * p)).toEqual('100Yb');
    expect(formatter(1000 * p)).toEqual('1000Yb');
    p *= 1024;
    expect(formatter(p)).toEqual('1024Yb');
    expect(formatter(10 * p)).toEqual('10240Yb');
    expect(formatter(100 * p)).toEqual('102400Yb');
    expect(formatter(1000 * p)).toEqual('1024000Yb');
  });
  test('second', () => {
    const formatter = formatByMetricType(METRIC_TYPE.second);
    expect(formatter(0)).toEqual('0');
    expect(formatter(1)).toEqual('1s');
    expect(formatter(0.1)).toEqual('100ms');
    expect(formatter(0.1111)).toEqual('111.1ms');
    expect(formatter(0.01111)).toEqual('11.11ms');
    expect(formatter(0.0011111)).toEqual('1.111ms');
    expect(formatter(0.00011111)).toEqual('111.11μs');
    expect(formatter(0.000011111)).toEqual('11.111μs');
    expect(formatter(0.0000011111)).toEqual('1.111μs');
    expect(formatter(0.00000011111)).toEqual('111.11ns');
    expect(formatter(0.000000011111)).toEqual('11.111ns');
    expect(formatter(0.0000000011111)).toEqual('1.111ns');
    expect(formatter(0.0000000000011111)).toEqual('0.001ns');
    expect(formatter(0.00000000000011111)).toEqual('0ns');
    expect(formatter(11)).toEqual('11s');
    expect(formatter(111)).toEqual('111s');
    expect(formatter(119)).toEqual('119s');
    expect(formatter(120)).toEqual('2m');
    expect(formatter(1111)).toEqual('18.5m');
    expect(formatter(7196)).toEqual('119.9m');
    expect(formatter(7200)).toEqual('2h');
    expect(formatter(11110)).toEqual('3.1h');
    expect(formatter(111100)).toEqual('30.9h');
    expect(formatter(172619)).toEqual('47.9h');
    expect(formatter(172799)).toEqual('48h');
    expect(formatter(172800)).toEqual('2d');
    expect(formatter(1111000)).toEqual('12.9d');
  });
  test('millisecond', () => {
    const formatter = formatByMetricType(METRIC_TYPE.millisecond);
    expect(formatter(0)).toEqual('0');
    expect(formatter(1)).toEqual('1ms');
    expect(formatter(0.1)).toEqual('100μs');
    expect(formatter(0.1111)).toEqual('111.1μs');
    expect(formatter(0.01111)).toEqual('11.11μs');
    expect(formatter(0.0011111)).toEqual('1.111μs');
    expect(formatter(0.00011111)).toEqual('111.11ns');
    expect(formatter(0.000011111)).toEqual('11.111ns');
    expect(formatter(0.0000011111)).toEqual('1.111ns');
    expect(formatter(0.00000011111)).toEqual('0.111ns');
    expect(formatter(0.000000011111)).toEqual('0.011ns');
    expect(formatter(0.0000000011111)).toEqual('0.001ns');
    expect(formatter(0.0000000000011111)).toEqual('0ns');
    expect(formatter(0.00000000000011111)).toEqual('0ns');
    expect(formatter(11)).toEqual('11ms');
    expect(formatter(111)).toEqual('111ms');
    expect(formatter(119000)).toEqual('119s');
    expect(formatter(120000)).toEqual('2m');
    expect(formatter(1111000)).toEqual('18.5m');
    expect(formatter(7196000)).toEqual('119.9m');
    expect(formatter(7200000)).toEqual('2h');
    expect(formatter(11110000)).toEqual('3.1h');
    expect(formatter(111100000)).toEqual('30.9h');
    expect(formatter(172619000)).toEqual('47.9h');
    expect(formatter(172799000)).toEqual('48h');
    expect(formatter(172800000)).toEqual('2d');
    expect(formatter(1111000000)).toEqual('12.9d');
  });
  test('microsecond', () => {
    const formatter = formatByMetricType(METRIC_TYPE.microsecond);
    expect(formatter(0)).toEqual('0');
    expect(formatter(1)).toEqual('1μs');
    expect(formatter(0.1)).toEqual('100ns');
    expect(formatter(0.1111)).toEqual('111.1ns');
    expect(formatter(0.01111)).toEqual('11.11ns');
    expect(formatter(0.0011111)).toEqual('1.111ns');
    expect(formatter(0.00011111)).toEqual('0.111ns');
    expect(formatter(0.000011111)).toEqual('0.011ns');
    expect(formatter(0.0000011111)).toEqual('0.001ns');
    expect(formatter(0.00000011111)).toEqual('0ns');
    expect(formatter(0.000000011111)).toEqual('0ns');
    expect(formatter(0.0000000011111)).toEqual('0ns');
    expect(formatter(0.0000000000011111)).toEqual('0ns');
    expect(formatter(0.00000000000011111)).toEqual('0ns');
    expect(formatter(11)).toEqual('11μs');
    expect(formatter(111)).toEqual('111μs');
    expect(formatter(111000)).toEqual('111ms');
    expect(formatter(111000000)).toEqual('111s');
    expect(formatter(119000000)).toEqual('119s');
    expect(formatter(120000000)).toEqual('2m');
    expect(formatter(1111000000)).toEqual('18.5m');
    expect(formatter(7196000000)).toEqual('119.9m');
    expect(formatter(7200000000)).toEqual('2h');
    expect(formatter(11110000000)).toEqual('3.1h');
    expect(formatter(111100000000)).toEqual('30.9h');
    expect(formatter(172619000000)).toEqual('47.9h');
    expect(formatter(172799000000)).toEqual('48h');
    expect(formatter(172800000000)).toEqual('2d');
    expect(formatter(1111000000000)).toEqual('12.9d');
  });
  test('nanosecond', () => {
    const formatter = formatByMetricType(METRIC_TYPE.nanosecond);
    expect(formatter(0)).toEqual('0');
    expect(formatter(1)).toEqual('1ns');
    expect(formatter(0.1)).toEqual('0.1ns');
    expect(formatter(0.1111)).toEqual('0.111ns');
    expect(formatter(0.01111)).toEqual('0.011ns');
    expect(formatter(0.0011111)).toEqual('0.001ns');
    expect(formatter(0.00011111)).toEqual('0ns');
    expect(formatter(0.000011111)).toEqual('0ns');
    expect(formatter(11)).toEqual('11ns');
    expect(formatter(111)).toEqual('111ns');
    expect(formatter(111100)).toEqual('111.1μs');
    expect(formatter(111100000)).toEqual('111.1ms');
    expect(formatter(111100000000)).toEqual('111.1s');
    expect(formatter(111000000000)).toEqual('111s');
    expect(formatter(119000000000)).toEqual('119s');
    expect(formatter(120000000000)).toEqual('2m');
    expect(formatter(1111000000000)).toEqual('18.5m');
    expect(formatter(7196000000000)).toEqual('119.9m');
    expect(formatter(7200000000000)).toEqual('2h');
    expect(formatter(11110000000000)).toEqual('3.1h');
    expect(formatter(111100000000000)).toEqual('30.9h');
    expect(formatter(172619000000000)).toEqual('47.9h');
    expect(formatter(172799000000000)).toEqual('48h');
    expect(formatter(172800000000000)).toEqual('2d');
    expect(formatter(1111000000000000)).toEqual('12.9d');
  });
  test('splitByMetricType second', () => {
    const split = splitByMetricType(METRIC_TYPE.second);
    const p = 1;
    expect(split(null, 1, -p, p, 0.2 * p, 0)).toEqual(
      [-1, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 120 * p, 10 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, 0, p, 0.1 * p, 0)).toEqual(
      [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 30 * p, 2 * p, 0)).toEqual(
      [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30].map((n) => n * p)
    );
    expect(split(null, 1, 0, 50 * p, 5 * p, 0)).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50].map((n) => n * p));
    expect(split(null, 1, 0, 120 * p, 39 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, -0.777 * p, 0.12 * p, 0.1 * p, 0)).toEqual(
      [-0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0, 0.1].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345.6 * p, 25 * p, 0)).toEqual(
      [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 34560 * p, 2500 * p, 0)).toEqual(
      [0, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345600 * p, 25000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000, 259200, 302400, 345600].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 242291.38 * p, 20000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000].map((n) => n * p)
    );
  });
  test('splitByMetricType millisecond', () => {
    const split = splitByMetricType(METRIC_TYPE.millisecond);
    const p = 1000;
    expect(split(null, 1, -p, p, 0.2 * p, 0)).toEqual(
      [-1, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 120 * p, 10 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, 0, p, 0.1 * p, 0)).toEqual(
      [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 30 * p, 2 * p, 0)).toEqual(
      [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30].map((n) => n * p)
    );
    expect(split(null, 1, 0, 50 * p, 5 * p, 0)).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50].map((n) => n * p));
    expect(split(null, 1, 0, 120 * p, 39 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, -0.777 * p, 0.12 * p, 0.1 * p, 0)).toEqual(
      [-0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0, 0.1].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345.6 * p, 25 * p, 0)).toEqual(
      [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 34560 * p, 2500 * p, 0)).toEqual(
      [0, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345600 * p, 25000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000, 259200, 302400, 345600].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 242291.38 * p, 20000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000].map((n) => n * p)
    );
  });
  test('splitByMetricType microsecond', () => {
    const split = splitByMetricType(METRIC_TYPE.microsecond);
    const p = 1000000;
    expect(split(null, 1, -p, p, 0.2 * p, 0)).toEqual(
      [-1, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 120 * p, 10 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, 0, p, 0.1 * p, 0)).toEqual(
      [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 30 * p, 2 * p, 0)).toEqual(
      [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30].map((n) => n * p)
    );
    expect(split(null, 1, 0, 50 * p, 5 * p, 0)).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50].map((n) => n * p));
    expect(split(null, 1, 0, 120 * p, 39 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, -0.777 * p, 0.12 * p, 0.1 * p, 0)).toEqual(
      [-0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0, 0.1].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345.6 * p, 25 * p, 0)).toEqual(
      [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 34560 * p, 2500 * p, 0)).toEqual(
      [0, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345600 * p, 25000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000, 259200, 302400, 345600].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 242291.38 * p, 20000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000].map((n) => n * p)
    );
  });
  test('splitByMetricType nanosecond', () => {
    const split = splitByMetricType(METRIC_TYPE.nanosecond);
    const p = 1000000000;
    expect(split(null, 1, -p, p, 0.2 * p, 0)).toEqual(
      [-1, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 120 * p, 10 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, 0, p, 0.1 * p, 0)).toEqual(
      [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1].map((n) => n * p)
    );
    expect(split(null, 1, 0, 30 * p, 2 * p, 0)).toEqual(
      [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30].map((n) => n * p)
    );
    expect(split(null, 1, 0, 50 * p, 5 * p, 0)).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50].map((n) => n * p));
    expect(split(null, 1, 0, 120 * p, 39 * p, 0)).toEqual([0, 30, 60, 90, 120].map((n) => n * p));
    expect(split(null, 1, -0.777 * p, 0.12 * p, 0.1 * p, 0)).toEqual(
      [-0.7, -0.6, -0.5, -0.4, -0.3, -0.2, -0.1, 0, 0.1].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345.6 * p, 25 * p, 0)).toEqual(
      [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 34560 * p, 2500 * p, 0)).toEqual(
      [0, 3600, 7200, 10800, 14400, 18000, 21600, 25200, 28800, 32400].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 345600 * p, 25000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000, 259200, 302400, 345600].map((n) => n * p)
    );
    expect(split(null, 1, -0.777 * p, 242291.38 * p, 20000 * p, 0)).toEqual(
      [0, 43200, 86400, 129600, 172800, 216000].map((n) => n * p)
    );
  });
  test('splitByMetricType byte', () => {
    const split = splitByMetricType(METRIC_TYPE.byte);
    expect(split(null, 1, 0, 10, 1, 0)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(split(null, 1, 0, 120, 10, 0)).toEqual([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]);
    expect(split(null, 1, 0, 1024, 100, 0)).toEqual([0, 256, 512, 768, 1024]);
    expect(split(null, 1, 0, 100 * 1024, 10000, 0)).toEqual([
      0, 10240, 20480, 30720, 40960, 51200, 61440, 71680, 81920, 92160, 102400,
    ]);
    expect(split(null, 1, 0, 1024 * 1024, 200000, 0)).toEqual([0, 262144, 524288, 786432, 1048576]);
    expect(split(null, 1, 0, 100 * 1024 * 1024, 10000000, 0)).toEqual([
      0, 9961472, 19922944, 29884416, 39845888, 49807360, 59768832, 69730304, 79691776, 89653248, 99614720,
    ]);
    expect(split(null, 1, 0, 1024 * 1024 * 1024, 200000000, 0)).toEqual([
      0, 134217728, 268435456, 402653184, 536870912, 671088640, 805306368, 939524096, 1073741824,
    ]);
  });
});
