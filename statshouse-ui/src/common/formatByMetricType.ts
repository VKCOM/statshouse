// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { formatFixed } from './formatFixed';
import { METRIC_TYPE, MetricType, QUERY_WHAT, QueryWhat, toMetricType } from '@/api/enum';
import { floor, round } from './helpers';

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
const minusZero = -0;

const getMetricTypeBase = (n: number) => {
  const base = Math.floor(Math.log10(Math.abs(n))) / 3;
  const resBase = base < 0 ? Math.ceil(base) : Math.floor(base);
  return resBase === minusZero ? 0 : resBase;
};
const baseMetricTypeSi: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    return Math.max(-8, Math.min(8, getMetricTypeBase(n)));
  },
  format(n) {
    const base = this.getBase(n);
    return floor(n / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
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
const baseMetricTypeByteAsBits: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    return Math.max(-8, Math.min(8, getMetricTypeBase(n)));
  },
  format(n) {
    const base = this.getBase(8 * n);
    return floor((8 * n) / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
  },
  suffix: {
    '-8': 'yb',
    '-7': 'zb',
    '-6': 'ab',
    '-5': 'fb',
    '-4': 'pb',
    '-3': 'nb',
    '-2': 'μb',
    '-1': 'mb',
    '0': 'b',
    '1': 'Kb',
    '2': 'Mb',
    '3': 'Gb',
    '4': 'Tb',
    '5': 'Pb',
    '6': 'Eb',
    '7': 'Zb',
    '8': 'Yb',
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
    return Math.max(0, Math.min(8, base === minusZero ? 0 : base));
  },
  format(n) {
    const base = this.getBase(n);
    return round(n / Math.pow(1024, base), 1) + (this.suffix[base] ?? '');
  },
  suffix: {
    '0': 'B',
    '1': 'KiB',
    '2': 'MiB',
    '3': 'GiB',
    '4': 'TiB',
    '5': 'PiB',
    '6': 'EiB',
    '7': 'ZiB',
    '8': 'YiB',
  },
};

export const suffixesByMetricType: Record<MetricType, ConfigConvertMetric> = {
  [METRIC_TYPE.none]: {
    ...baseMetricTypeSi,
  },
  [METRIC_TYPE.byte_as_bits]: {
    ...baseMetricTypeByteAsBits,
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

const excludeWhat: QueryWhat[] = [
  QUERY_WHAT.count,
  QUERY_WHAT.countNorm,
  QUERY_WHAT.countSec,
  QUERY_WHAT.cuCount,
  QUERY_WHAT.dvCount,
  QUERY_WHAT.dvCountNorm,
  QUERY_WHAT.cardinality,
  QUERY_WHAT.cardinalityNorm,
  QUERY_WHAT.cardinalitySec,
  QUERY_WHAT.cuCardinality,
  QUERY_WHAT.maxCountHost,
  QUERY_WHAT.unique,
  QUERY_WHAT.uniqueNorm,
  QUERY_WHAT.uniqueSec,
  QUERY_WHAT.dvUnique,
  QUERY_WHAT.dvUniqueNorm,
];
export function getMetricType(whats?: QueryWhat[], metricType?: string) {
  if (whats && whats.some((w) => excludeWhat.indexOf(w) > -1)) {
    return METRIC_TYPE.none;
  }
  return toMetricType(metricType, METRIC_TYPE.none);
}
