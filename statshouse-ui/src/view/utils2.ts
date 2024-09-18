import { MetricMetaValue } from '../api/metric';
import { isTagKey, TAG_KEY, TagKey } from '../api/enum';
import uPlot from 'uplot';
import { TimeRange } from '../common/TimeRange';
import { formatFixed } from '../common/formatFixed';
import { formatFixedFloor } from '../common/formatFixedFloor';
import { RawValueKind } from './api';

export function isValidVariableName(name: string): boolean {
  const regex = /^[a-z][a-z0-9_]*$/gi;
  return regex.test(name);
}

export function isTagEnabled(meta: MetricMetaValue | undefined, tagKey: TagKey | null): boolean {
  if (meta && tagKey != null) {
    if (tagKey === TAG_KEY._s) {
      return !!meta.string_top_description || !!meta.string_top_name;
    } else {
      return meta.tags?.[+tagKey].description !== '-';
    }
  }
  return false;
}

export function getTagDescription(meta: MetricMetaValue | undefined, tagKey: number | string | TagKey | null): string {
  if (meta && tagKey != null) {
    if (tagKey === -1 || tagKey === 'skey' || tagKey === TAG_KEY._s) {
      return meta.string_top_description || meta.string_top_name || 'tag _s';
    }

    if (isTagKey(tagKey)) {
      return meta.tags?.[+tagKey].description || meta.tags?.[+tagKey].name || `tag ${tagKey}`;
    }

    if (typeof tagKey === 'number' && tagKey > -1) {
      return meta.tags?.[tagKey].description || meta.tags?.[tagKey].name || `tag ${tagKey}`;
    }
  }
  return `tag ${tagKey}`;
}

export function secondsRangeToString(seconds: number, short?: boolean): string {
  const suffix: Array<[number, string, string, string]> = [
    [60, 'second', 'seconds', 's'],
    [60, 'minute', 'minutes', 'm'],
    [24, 'hour', 'hours', 'h'],
    [365, 'day', 'days', 'd'],
    [0, 'year', 'years', 'y'],
  ];
  let range = seconds;
  let result = [];
  for (let key in suffix) {
    if (suffix[key][0] > 0) {
      const num = range % (suffix[key][0] as number);
      if (num > 0) {
        result.unshift(`${num}${short ? '' : ' '}${suffix[key][short ? 3 : num === 1 ? 1 : 2]}`);
      }
      range = Math.floor(range / (suffix[key][0] as number));
    } else if (range > 0) {
      result.unshift(`${range}${short ? '' : ' '}${suffix[key][short ? 3 : range === 1 ? 1 : 2]}`);
      range = 0;
    }
    if (range === 0) {
      break;
    }
  }
  if (result.length > 2) {
    result.pop();
  }
  return String(result.join(short ? '' : ' '));
}

export type timeShiftAbbrev =
  | '-24h'
  | '-48h'
  | '-1w'
  | '-2w'
  | '-3w'
  | '-4w'
  | '-365d'
  | '-1M'
  | '-1Y'
  | '-2M'
  | '-3M'
  | '-6M';
const defaultShiftAbbrevs: timeShiftAbbrev[] = ['-24h', '-48h', '-1w', '-2w', '-3w', '-4w', '-365d'];
const weekShiftAbbrevs: timeShiftAbbrev[] = ['-1w', '-2w', '-3w', '-4w'];
const monthShiftAbbrevs: timeShiftAbbrev[] = ['-1M', '-2M', '-3M', '-6M', '-1Y'];

export function getTimeShifts(customAgg: number): timeShiftAbbrev[] {
  switch (customAgg) {
    case 31 * 24 * 60 * 60:
      return monthShiftAbbrevs;
    case 7 * 24 * 60 * 60:
      return weekShiftAbbrevs;
    default:
      return defaultShiftAbbrevs;
  }
}

export function timeShiftAbbrevExpand(ts: timeShiftAbbrev): number {
  switch (ts) {
    case '-24h':
      return -24 * 3600;
    case '-48h':
      return -48 * 3600;
    case '-1w':
      return -7 * 24 * 3600;
    case '-2w':
      return -2 * 7 * 24 * 3600;
    case '-3w':
      return -3 * 7 * 24 * 3600;
    case '-4w':
      return -4 * 7 * 24 * 3600;
    case '-365d':
      return -365 * 24 * 3600;
    case '-1M':
      return -31 * 24 * 3600;
    case '-2M':
      return -2 * 31 * 24 * 3600;
    case '-3M':
      return -3 * 31 * 24 * 3600;
    case '-6M':
      return -6 * 31 * 24 * 3600;
    case '-1Y':
      return -12 * 31 * 24 * 3600;
  }
}

export const minusSignChar = '−'; //&#8722;
// XXX: keep in sync with Go
export function timeShiftDesc(ts: number): string {
  switch (ts) {
    case 0:
      return '';
    case -24 * 3600:
      return minusSignChar + '24h';
    case -48 * 3600:
      return minusSignChar + '48h';
    case -7 * 24 * 3600:
      return minusSignChar + '1w';
    case -7 * 2 * 24 * 3600:
      return minusSignChar + '2w';
    case -7 * 3 * 24 * 3600:
      return minusSignChar + '3w';
    case -7 * 4 * 24 * 3600:
      return minusSignChar + '4w';
    case -365 * 24 * 3600:
      return minusSignChar + '365d';
    case -31 * 24 * 3600:
      return minusSignChar + '1M';
    case -2 * 31 * 24 * 3600:
      return minusSignChar + '2M';
    case -3 * 31 * 24 * 3600:
      return minusSignChar + '3M';
    case -6 * 31 * 24 * 3600:
      return minusSignChar + '6M';
    case -12 * 31 * 24 * 3600:
      return minusSignChar + '1Y';
    default:
      return minusSignChar + secondsRangeToString(Math.abs(ts), true);
  }
}

export function now(): number {
  return Math.floor(Date.now() / 1000);
}

export function formatInputDate(n: number): string {
  const d = new Date(n * 1000);
  return fmtInputDate(d);
}

export function formatInputTime(n: number): string {
  const d = new Date(n * 1000);
  return fmtInputTime(d);
}

export function parseInputDate(v: string): [number, number, number] {
  const y = parseInt(v.substring(0, 4));
  const m = parseInt(v.substring(5, 7));
  const d = parseInt(v.substring(8, 10));
  return [y, m - 1, d];
}

export function parseInputTime(v: string): [number, number, number] {
  const h = parseInt(v.substring(0, 2));
  const m = parseInt(v.substring(3, 5));
  const s = parseInt(v.substring(6, 8));
  return [h, m, s];
}

export interface timeRange {
  readonly from: number;
  readonly to: number;
}

export const maxTimeRange = 10 * 365 * 24 * 3600;

export function timeRangePanLeft(r: timeRange): timeRange {
  const t = now();
  const delta = Math.floor(Math.min((r.to - r.from) / 4, r.from - (t - maxTimeRange)));
  return {
    from: r.from - delta,
    to: r.to - delta,
  };
}

export function timeRangePanRight(r: timeRange): timeRange {
  const t = now();
  const delta = Math.floor(Math.min((r.to - r.from) / 4, t - r.to));
  return {
    from: r.from + delta,
    to: r.to + delta,
  };
}

export function timeRangePanNow(r: timeRange): timeRange {
  const t = now();
  const delta = t - r.to;
  return {
    from: r.from + delta,
    to: r.to + delta,
  };
}

export function timeRangeZoomIn(r: timeRange): timeRange {
  const delta = (r.to - r.from) / 4;
  return {
    from: Math.floor(r.from + delta),
    to: Math.ceil(r.to - delta),
  };
}

export function timeRangeZoomOut(r: timeRange): timeRange {
  const delta = (r.to - r.from) / 2;
  const t = now();
  return {
    from: Math.max(Math.floor(r.from - delta), t - maxTimeRange),
    to: Math.min(Math.ceil(r.to + delta), t),
  };
}

const fmtInputDate = uPlot.fmtDate('{YYYY}-{MM}-{DD}');
const fmtInputTime = uPlot.fmtDate('{HH}:{mm}:{ss}');
export const fmtInputDateTime = uPlot.fmtDate('{YYYY}-{MM}-{DD} {HH}:{mm}:{ss}');

export function timeRangeString(r: timeRange): string {
  const to = r.to <= 0 ? now() + r.to : r.to;
  return secondsRangeToString(r.from <= 0 ? -r.from : to - r.from);
}

export type timeRangeAbbrev =
  | 'last-5m'
  | 'last-15m'
  | 'last-1h'
  | 'last-2h'
  | 'last-6h'
  | 'last-12h'
  | 'last-1d'
  | 'last-2d'
  | 'last-3d'
  | 'last-7d'
  | 'last-14d'
  | 'last-30d'
  | 'last-90d'
  | 'last-180d'
  | 'last-1y'
  | 'last-2y';

export function timeRangeAbbrevExpand(abbr: timeRangeAbbrev, to: number): timeRange {
  const t = to === -1 ? now() : to;
  switch (abbr) {
    case 'last-5m':
      return { from: -60 * 5, to: t };
    case 'last-15m':
      return { from: -60 * 15, to: t };
    case 'last-1h':
      return { from: -3600, to: t };
    case 'last-2h':
      return { from: -3600 * 2, to: t };
    case 'last-6h':
      return { from: -3600 * 6, to: t };
    case 'last-12h':
      return { from: -3600 * 12, to: t };
    case 'last-1d':
      return { from: -3600 * 24, to: t };
    case 'last-2d':
      return { from: -3600 * 24 * 2, to: t };
    case 'last-3d':
      return { from: -3600 * 24 * 3, to: t };
    case 'last-7d':
      return { from: -3600 * 24 * 7, to: t };
    case 'last-14d':
      return { from: -3600 * 24 * 14, to: t };
    case 'last-30d':
      return { from: -3600 * 24 * 30, to: t };
    case 'last-90d':
      return { from: -3600 * 24 * 90, to: t };
    case 'last-180d':
      return { from: -3600 * 24 * 180, to: t };
    case 'last-1y':
      return { from: -3600 * 24 * 365, to: t };
    case 'last-2y':
      return { from: -3600 * 24 * 365 * 2, to: t };
  }
}

export function timeRangeToAbbrev(r: TimeRange): timeRangeAbbrev | '' {
  const tolerance = 60;
  const candidates: timeRangeAbbrev[] = [
    'last-5m',
    'last-15m',
    'last-1h',
    'last-2h',
    'last-6h',
    'last-12h',
    'last-1d',
    'last-2d',
    'last-3d',
    'last-7d',
    'last-14d',
    'last-30d',
    'last-90d',
    'last-180d',
    'last-1y',
    'last-2y',
  ];

  for (const abbr of candidates) {
    const rr = timeRangeAbbrevExpand(abbr, now());
    if (
      Math.abs(rr.from - r.relativeFrom) <= tolerance &&
      (Math.abs(rr.to - r.to) <= tolerance || Math.abs(r.relativeTo) <= tolerance)
    ) {
      return abbr;
    }
  }
  return '';
}

export function timeRangeToAbbrev2(r: timeRange): timeRangeAbbrev | '' {
  const tolerance = 60;
  const candidates: timeRangeAbbrev[] = [
    'last-5m',
    'last-15m',
    'last-1h',
    'last-2h',
    'last-6h',
    'last-12h',
    'last-1d',
    'last-2d',
    'last-3d',
    'last-7d',
    'last-14d',
    'last-30d',
    'last-90d',
    'last-180d',
    'last-1y',
    'last-2y',
  ];

  for (const abbr of candidates) {
    const rr = timeRangeAbbrevExpand(abbr, now());
    if (Math.abs(rr.from - r.from) <= tolerance && Math.abs(rr.to - r.to) <= tolerance) {
      return abbr;
    }
  }
  return '';
}

export function formatNumberDigit(n: string | number): string {
  const z = n.toString().split('.');
  z[0] = z[0].replace(/(\d)(?=(\d\d\d)+([^\d]|$))/g, '$1 ');
  return z.join('.');
}

export function formatPercent(n: number): string {
  if (isNaN(n)) {
    n = 0; // work around careless division by zero
  }
  n *= 100;
  const frac = n < 0.1 ? 3 : n < 1 ? 2 : n < 10 ? 1 : 0;
  return formatFixed(n, frac) + '%';
}

const dashes = [
  [4, 4],
  [6, 6],
  [8, 4],
];

export function timeShiftToDash(ts: number, usedDashes: Record<string, number[]>): number[] {
  if (usedDashes[ts.toString()]) {
    return usedDashes[ts.toString()];
  }
  const nextKey = Math.min(dashes.length - 1, Object.keys(usedDashes).length);
  return (usedDashes[ts.toString()] = dashes[nextKey]);
}

export function formatLegendValue(value: number | null): string {
  if (value === null) {
    return '';
  }
  const abs = Math.abs(value);
  const maxFrac = abs > 1000 ? 0 : abs > 100 ? 1 : abs > 10 ? 2 : abs > 0.001 ? 3 : 9;
  return formatNumberDigit(formatFixedFloor(value, maxFrac));
}

export function ieee32ToFloat(intval: number): number {
  let fval = 0.0;
  let x; //exponent
  let m; //mantissa
  let s; //sign
  s = intval & 0x80000000 ? -1 : 1;
  x = (intval >> 23) & 0xff;
  m = intval & 0x7fffff;
  switch (x) {
    case 0:
      //zero, do nothing, ignore negative zero and subnormals
      break;
    case 0xff:
      if (m) fval = NaN;
      else if (s > 0) fval = Number.POSITIVE_INFINITY;
      else fval = Number.NEGATIVE_INFINITY;
      break;
    default:
      x -= 127;
      m += 0x800000;
      fval = s * (m / 8388608.0) * Math.pow(2, x);
      break;
  }
  return fval;
}

export function lexDecode(intval: number): number {
  return ieee32ToFloat(intval < 0 ? (intval >>> 0) ^ 0x7fffffff : intval >>> 0);
}

export function convert(kind: RawValueKind | undefined, input: number): string {
  switch (kind) {
    case 'hex':
      return '0x' + `00000000${(input >>> 0).toString(16)}`.slice(-8);
    case 'hex_bswap':
      return (
        '0x' +
        (`00${(input & 255).toString(16)}`.slice(-2) +
          `00${((input >> 8) & 255).toString(16)}`.slice(-2) +
          `00${((input >> 16) & 255).toString(16)}`.slice(-2) +
          `00${((input >> 24) & 255).toString(16)}`.slice(-2))
      );
    case 'timestamp':
      return fmtInputDateTime(uPlot.tzDate(new Date(input * 1000), 'UTC'));
    case 'timestamp_local':
      return fmtInputDateTime(new Date(input * 1000));
    case 'ip':
      return ((input >> 24) & 255) + '.' + ((input >> 16) & 255) + '.' + ((input >> 8) & 255) + '.' + (input & 255);
    case 'ip_bswap':
      return (input & 255) + '.' + ((input >> 8) & 255) + '.' + ((input >> 16) & 255) + '.' + ((input >> 24) & 255);
    case 'uint':
      return (input >>> 0).toString(10);
    case 'lexenc_float':
      return parseFloat(lexDecode(input).toPrecision(8)).toString(10);
    case 'float':
      const buffer = new ArrayBuffer(4);
      const dataView = new DataView(buffer);
      dataView.setInt32(0, input, false);
      return parseFloat(dataView.getFloat32(0, false).toPrecision(8)).toString(10);
    default:
      return input.toString(10);
  }
}
