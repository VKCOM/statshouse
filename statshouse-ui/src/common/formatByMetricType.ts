import { formatFixed } from './formatFixed';
import { METRIC_TYPE, MetricType, QUERY_WHAT, QueryWhat, toMetricType } from '../api/enum';
import { round } from './helpers';
import { MetricMetaValue } from '../api/metric';

const siPrefixes = ['y', 'z', 'a', 'f', 'p', 'n', 'μ', 'm', '', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];

export function formatSI(n: number): string {
  if (n === 0) {
    return n.toString();
  }
  const base = Math.floor(Math.log10(Math.abs(n)));
  const siBase = base < 0 ? Math.ceil(base / 3) : Math.floor(base / 3);
  if (siBase === 0) {
    return formatFixed(n, 3);
  }
  const baseNum = formatFixed(n / Math.pow(10, siBase * 3), 3);
  const prefix = siPrefixes[siBase + 8];
  return `${baseNum}${prefix}`;
}

type ConfigConvertMetric = {
  baseOffset: number;
  getBase: (n: number) => number;
  format: (n: number) => string;
  suffix: Record<string, string>;
};
const getMetricTypeBase = (n: number) => {
  const base = Math.floor(Math.log10(Math.abs(n))) / 3;
  const resBase = base < 0 ? Math.ceil(base) : Math.floor(base);
  return resBase === -0 ? 0 : resBase;
};
const baseMetricTypeSi: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    return Math.max(-8, Math.min(8, getMetricTypeBase(n)));
  },
  format(n) {
    const base = this.getBase(n);
    return round(n / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
  },
  suffix: {
    '-8': 'y',
    '-7': 'z',
    '-6': 'a',
    '-5': 'f',
    '-4': 'p',
    '-3': 'n',
    '-2': 'μ',
    '-1': 'm',
    '0': '',
    '1': 'k',
    '2': 'M',
    '3': 'G',
    '4': 'T',
    '5': 'P',
    '6': 'E',
    '7': 'Z',
    '8': 'Y',
  },
};
const baseMetricTypeSecond: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    const p =
      this.baseOffset < 0
        ? Math.pow(10, this.baseOffset * 3)
        : this.baseOffset === 3
        ? 86400
        : Math.pow(60, this.baseOffset);
    const an = Math.abs(n * p);
    if (an < 1) {
      const base = Math.floor(Math.log10(an)) - 2;
      return Math.max(-3, Math.ceil(base / 3));
    } else if (an >= 172800) {
      // 2 * 60 * 60 * 24, >2 days
      return 3;
    } else if (an >= 7200) {
      // 2 * 60 * 60, >2 hours
      return 2;
    } else if (an >= 120) {
      // 2 * 60, >2 minutes
      return 1;
    } else {
      return 0;
    }
  },
  format(n) {
    const p =
      this.baseOffset < 0
        ? Math.pow(10, this.baseOffset * 3)
        : this.baseOffset === 3
        ? 86400
        : Math.pow(60, this.baseOffset);

    const normalizeN = n * p;
    const base = this.getBase(n);
    if (base < 0) {
      return round(normalizeN / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
    } else if (base === 3) {
      return round(normalizeN / 86400, 1) + (this.suffix[base] ?? '');
    } else {
      return round(normalizeN / Math.pow(60, base), 1) + (this.suffix[base] ?? '');
    }
  },
  suffix: {
    '-3': 'ns',
    '-2': 'μs',
    '-1': 'ms',
    '0': 's',
    '1': 'm',
    '2': 'h',
    '3': 'd',
  },
};
const baseMetricTypeByte: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    const base = Math.floor(Math.floor(Math.log2(Math.abs(n))) / 10);
    return Math.max(0, Math.min(8, base === -0 ? 0 : base));
  },
  format(n) {
    const base = this.getBase(n);
    return round(n / Math.pow(1024, base), 1) + (this.suffix[base] ?? '');
  },
  suffix: {
    '0': 'b',
    '1': 'kb',
    '2': 'Mb',
    '3': 'Gb',
    '4': 'Tb',
    '5': 'Pb',
    '6': 'Eb',
    '7': 'Zb',
    '8': 'Yb',
  },
};
export const suffixesByMetricType: Record<MetricType, ConfigConvertMetric> = {
  [METRIC_TYPE.none]: {
    ...baseMetricTypeSi,
  },
  [METRIC_TYPE.byte]: {
    ...baseMetricTypeByte,
  },
  [METRIC_TYPE.second]: {
    ...baseMetricTypeSecond,
  },
  [METRIC_TYPE.millisecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -1,
  },
  [METRIC_TYPE.microsecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -2,
  },
  [METRIC_TYPE.nanosecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -3,
  },
};

export function formatByMetricType(metricType: MetricType): (n: number) => string {
  const conf = suffixesByMetricType[metricType];
  return (n: number): string => {
    if (n === 0) {
      return n.toString();
    }
    return conf.format(n);
  };
}

export function splitByMetricType(metricType: MetricType) {
  return (
    self: unknown, //uPlot unknown for test
    axisIdx: number,
    scaleMin: number,
    scaleMax: number,
    foundIncr: number,
    foundSpace: number
  ): number[] => {
    let splits: number[] = [];
    const conf = suffixesByMetricType[metricType];
    const base = conf.getBase(Math.max(Math.abs(scaleMin), Math.abs(scaleMax)));
    function fixFloat(v: number) {
      return round(v, 14);
    }
    function incrRoundUp(num: number, incr: number) {
      return fixFloat(Math.ceil(fixFloat(num / incr)) * incr);
    }
    let p = 1;
    switch (metricType) {
      case METRIC_TYPE.nanosecond:
        p = 0.000000001;
        break;
      case METRIC_TYPE.microsecond:
        p = 0.000001;
        break;
      case METRIC_TYPE.millisecond:
        p = 0.001;
        break;
    }
    let incr = fixFloat(foundIncr);
    let start = incrRoundUp(scaleMin, incr);
    let end = scaleMax + incr / 100;
    switch (metricType) {
      case METRIC_TYPE.nanosecond:
      case METRIC_TYPE.microsecond:
      case METRIC_TYPE.millisecond:
      case METRIC_TYPE.second:
        if (base === 3) {
          incr = round(foundIncr * p, -1, 43200) / p || round(2 * foundIncr * p, -1, 43200) / p;
          start = round(incrRoundUp(round(scaleMin * p, -1, 86400), incr)) / p;
          end = scaleMax + incr / 100;
        } else if (base > 0) {
          incr = round(foundIncr * p, -base, 30 * base) / p || round(2 * foundIncr * p, -base, 30 * base) / p;
          start = round(incrRoundUp(round(scaleMin * p, -base, 60), incr)) / p;
          end = scaleMax + incr / 100;
        }
        break;
      case METRIC_TYPE.byte:
      default:
        const r1 = Math.pow(2, 10 * base - base - 1);
        const r2 = Math.pow(2, 10 * base - base);
        const radix = Math.abs(foundIncr - r1) < Math.abs(foundIncr - r2) ? r1 : r2;
        incr = round(foundIncr * p, -1, radix) / p || round(2 * foundIncr * p, -1, radix) / p;
        start = round(incrRoundUp(round(scaleMin * p, -1, radix), incr)) / p;
        end = scaleMax + incr / 100;
        break;
    }
    if (incr > 0) {
      for (let val = start; val <= end; val = val + incr) {
        const pos = round(val, 10);
        splits.push(pos === -0 ? 0 : pos);
      }
    }
    return splits;
  };
}

const excludeWhat: QueryWhat[] = [
  QUERY_WHAT.count,
  QUERY_WHAT.countNorm,
  QUERY_WHAT.cuCount,
  QUERY_WHAT.dvCount,
  QUERY_WHAT.dvCountNorm,
  QUERY_WHAT.cardinality,
  QUERY_WHAT.cardinalityNorm,
  QUERY_WHAT.cuCardinality,
  QUERY_WHAT.maxCountHost,
  QUERY_WHAT.unique,
  QUERY_WHAT.uniqueNorm,
  QUERY_WHAT.dvUnique,
  QUERY_WHAT.dvUniqueNorm,
];
export function getMetricType(whats: QueryWhat[], meta?: MetricMetaValue) {
  if (whats.some((w) => excludeWhat.indexOf(w) > -1)) {
    return METRIC_TYPE.none;
  }
  return toMetricType(meta?.metric_type, METRIC_TYPE.none);
}
