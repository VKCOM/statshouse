// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { invertObj, isEnum, toEnum, toNumber } from '@/common/helpers';

export type Enum<T> = T[keyof T];

export const GET_PARAMS = {
  numResults: 'n',
  version: 'v',
  metricName: 's',
  metricId: 'id',
  metricApiVersion: 'ver',
  metricUrlVersion: 'mv',
  metricCustomName: 'cn',
  /**
   * metric unit
   */
  metricMetricUnit: 'mt',
  metricCustomDescription: 'cd',
  fromTime: 'f',
  toTime: 't',
  width: 'w',
  metricWhat: 'qw',
  metricTimeShifts: 'ts',
  metricLocalTimeShifts: 'lts',
  metricGroupBy: 'qb',
  metricFilter: 'qf',
  metricFilterSync: 'fs',
  metricVerbose: 'qv',
  metricTagID: 'k',
  metricDownloadFile: 'df',
  metricTabNum: 'tn',
  metricLockMin: 'yl',
  metricLockMax: 'yh',
  metricMaxHost: 'mh',
  metricAgg: 'g',
  metricPromQL: 'q',
  metricGroupKey: 'dg',
  metricLayout: 'dl',
  /**
   * widget type
   */
  metricType: 'qt',
  metricEvent: 'qe',
  metricFromRow: 'fr',
  metricToRow: 'tr',
  metricFromEnd: 'fe',
  metricEventFrom: 'ef',
  metricEventBy: 'eb',
  metricEventHide: 'eh',
  viewTotalLine: 'vtl',
  viewFilledGraph: 'vfg',
  viewLogScale: 'vl',
  dataFormat: 'df',
  plotPrefix: 't',
  dashboardID: 'id',
  dashboardApiVersion: 'ver',
  dashboardName: 'dn',
  dashboardDescription: 'dd',
  dashboardVersion: 'dv',
  dashboardSchemeVersion: 'ver',
  metricsGroupID: 'id',
  metricsNamespacesID: 'id',
  metricsListAndDisabled: 'sd',
  dashboardGroupInfoPrefix: 'g',
  dashboardGroupInfoName: 't',
  dashboardGroupInfoShow: 'v',
  dashboardGroupInfoCount: 'n',
  dashboardGroupInfoSize: 's',
  dashboardGroupInfoDescription: 'd',
  metricLive: 'live',
  theme: 'theme',
  avoidCache: 'ac',
  excessPoints: 'ep',
  variablePrefix: 'v',
  variableName: 'n',
  variableNamePrefix: 'var',
  variableDescription: 'd',
  variableValue: 'v',
  variableValuePrefix: 'v',
  variableLinkPlot: 'l',
  variableSourcePrefix: 's',
  variableSourceMetricName: 's',
  variableSourceTag: 't',
  variableSourceFilter: 'qf',
  variableGroupBy: 'g',
  variableNegative: 'nv',
  priority: 'priority',
  orderPlot: 'op',
  orderGroup: 'og',
  orderVariable: 'ov',
  prometheusCompat: 'compat',
} as const;
export type GetParams = Enum<typeof GET_PARAMS>;

export const METRIC_VALUE_BACKEND_VERSION = {
  v1: '1',
  v2: '2',
  v3: '3',
} as const;
export type MetricValueBackendVersion = Enum<typeof METRIC_VALUE_BACKEND_VERSION>;

export const isMetricValueBackendVersion = isEnum<MetricValueBackendVersion>(METRIC_VALUE_BACKEND_VERSION);
export const toMetricValueBackendVersion = toEnum(isMetricValueBackendVersion);

export const GET_BOOLEAN = {
  true: '1',
  false: '0',
} as const;
export type GetBoolean = Enum<typeof GET_BOOLEAN>;

export const QUERY_WHAT = {
  count: 'count',
  countNorm: 'count_norm',
  countSec: 'countsec',
  cuCount: 'cu_count',
  cardinality: 'cardinality',
  cardinalityNorm: 'cardinality_norm',
  cardinalitySec: 'cardinalitysec',
  cuCardinality: 'cu_cardinality',
  min: 'min',
  max: 'max',
  avg: 'avg',
  cuAvg: 'cu_avg',
  sum: 'sum',
  sumNorm: 'sum_norm',
  sumSec: 'sumsec',
  cuSum: 'cu_sum',
  stddev: 'stddev',
  maxHost: 'max_host',
  maxCountHost: 'max_count_host',
  p0_1: 'p0_1',
  p1: 'p1',
  p5: 'p5',
  p10: 'p10',
  p25: 'p25',
  p50: 'p50',
  p75: 'p75',
  p90: 'p90',
  p95: 'p95',
  p99: 'p99',
  p999: 'p999',
  unique: 'unique',
  uniqueNorm: 'unique_norm',
  uniqueSec: 'uniquesec',
  dvCount: 'dv_count',
  dvCountNorm: 'dv_count_norm',
  dvSum: 'dv_sum',
  dvSumNorm: 'dv_sum_norm',
  dvAvg: 'dv_avg',
  dvMin: 'dv_min',
  dvMax: 'dv_max',
  dvUnique: 'dv_unique',
  dvUniqueNorm: 'dv_unique_norm',
} as const;
export type QueryWhat = Enum<typeof QUERY_WHAT>;

export type QueryWhatSelector = QueryWhat | '-';

export const TAG_KEY = {
  _s: '_s',
  _0: '0',
  _1: '1',
  _2: '2',
  _3: '3',
  _4: '4',
  _5: '5',
  _6: '6',
  _7: '7',
  _8: '8',
  _9: '9',
  _10: '10',
  _11: '11',
  _12: '12',
  _13: '13',
  _14: '14',
  _15: '15',
  _16: '16',
  _17: '17',
  _18: '18',
  _19: '19',
  _20: '20',
  _21: '21',
  _23: '22',
  _24: '24',
  _25: '25',
  _26: '26',
  _27: '27',
  _28: '28',
  _29: '29',
  _30: '30',
  _31: '31',
  _32: '32',
  _33: '33',
  _34: '34',
  _35: '35',
  _36: '36',
  _37: '37',
  _38: '38',
  _39: '39',
  _40: '40',
  _41: '41',
  _42: '42',
  _43: '43',
  _44: '44',
  _45: '45',
  _46: '46',
} as const;
export type TagKey = Enum<typeof TAG_KEY>;

export const METRIC_META_KIND = {
  counter: 'counter',
  value: 'value',
  valueP: 'value_p',
  unique: 'unique',
  mixed: 'mixed',
  mixedP: 'mixed_p',
} as const;
export type MetricMetaKind = Enum<typeof METRIC_META_KIND>;

export const METRIC_META_TAG_RAW_KIND = {
  int: 'int',
  uint: 'uint',
  hex: 'hex',
  hexBswap: 'hex_bswap',
  timestamp: 'timestamp',
  timestampLocal: 'timestamp_local',
  ip: 'ip',
  ipBswap: 'ip_bswap',
  lexencFloat: 'lexenc_float',
  int64: 'int64',
  uint64: 'uint64',
  hex64: 'hex64',
  hex64_bswap: 'hex64_bswap',
  /** @deprecated 'float' raw value kind */
  float: 'float',
} as const;
export type MetricMetaTagRawKind = Enum<typeof METRIC_META_TAG_RAW_KIND>;
export const isMetricMetaTagRawKind = isEnum<MetricMetaTagRawKind>(METRIC_META_TAG_RAW_KIND);
export const toMetricMetaTagRawKind = toEnum(isMetricMetaTagRawKind);

export const API_FETCH_OPT_METHODS = {
  get: 'GET',
  post: 'POST',
  put: 'PUT',
} as const;
export type ApiFetchOptMethods = Enum<typeof API_FETCH_OPT_METHODS>;

export const isQueryWhat = isEnum<QueryWhat>(QUERY_WHAT);
export const isTagKey = isEnum<TagKey>(TAG_KEY);
export const toTagKey = toEnum(isTagKey);

/**
 * metric unit type
 */
export const METRIC_TYPE = {
  none: '',
  // time
  second: 'second',
  millisecond: 'millisecond',
  microsecond: 'microsecond',
  nanosecond: 'nanosecond',
  // data size
  byte: 'byte',
  byte_as_bits: 'byte_as_bits',
} as const;
export type MetricType = Enum<typeof METRIC_TYPE>;
export const isMetricType = isEnum<MetricType>(METRIC_TYPE);
export const toMetricType = toEnum(isMetricType);
export const valueMapMetricType = invertObj(METRIC_TYPE);
export const METRIC_TYPE_DESCRIPTION: Record<MetricType, string> = {
  [METRIC_TYPE.none]: 'no unit',
  [METRIC_TYPE.second]: 'second',
  [METRIC_TYPE.millisecond]: 'millisecond',
  [METRIC_TYPE.microsecond]: 'microsecond',
  [METRIC_TYPE.nanosecond]: 'nanosecond',
  [METRIC_TYPE.byte]: 'byte',
  [METRIC_TYPE.byte_as_bits]: 'byte (shown as bits)',
};

export const METRIC_TYPE_URL = {
  [METRIC_TYPE.none]: 'si',
  // time
  [METRIC_TYPE.second]: 's',
  [METRIC_TYPE.millisecond]: 'ms',
  [METRIC_TYPE.microsecond]: 'mcs',
  [METRIC_TYPE.nanosecond]: 'ns',
  // data size
  [METRIC_TYPE.byte]: 'b',
  [METRIC_TYPE.byte_as_bits]: 'bbt',
} as const;
export type MetricTypeUrl = Enum<typeof METRIC_TYPE_URL>;
export const isMetricTypeUrl = isEnum<MetricTypeUrl>(METRIC_TYPE_URL);
export const toMetricTypeUrl = toEnum(isMetricTypeUrl);
export const valueMapMetricTypeUrl = invertObj(METRIC_TYPE_URL);
export const metricTypeUrlToMetricType = (s: unknown) => {
  const value = toMetricTypeUrl(s);
  if (value != null) {
    return toMetricType(valueMapMetricTypeUrl[value]);
  }
  return null;
};
export const metricTypeToMetricTypeUrl = (s: unknown) => {
  const value = toMetricType(s, METRIC_TYPE.none);
  return toMetricTypeUrl(METRIC_TYPE_URL[value], METRIC_TYPE_URL[METRIC_TYPE.none]);
};

/**
 * metric aggregation
 */
export const METRIC_AGGREGATION = {
  auto: 0,
  autoLow: -1,
  second1: 1,
  second5: 5,
  second15: 15,
  minute1: 60,
  minute5: 5 * 60,
  minute15: 15 * 60,
  hour1: 60 * 60,
  hour4: 4 * 60 * 60,
  hour24: 24 * 60 * 60,
  day7: 7 * 24 * 60 * 60,
  month1: 31 * 24 * 60 * 60,
} as const;
export type MetricAggregation = Enum<typeof METRIC_AGGREGATION>;
export const isMetricAggregation = isEnum<MetricAggregation, number>(METRIC_AGGREGATION);
export const toMetricAggregation = toEnum(isMetricAggregation, toNumber);

export const METRIC_AGGREGATION_DESCRIPTION: Record<MetricAggregation, string> = {
  [METRIC_AGGREGATION.auto]: 'Auto',
  [METRIC_AGGREGATION.autoLow]: 'Auto (low)',
  [METRIC_AGGREGATION.second1]: '1 second',
  [METRIC_AGGREGATION.second5]: '5 seconds',
  [METRIC_AGGREGATION.second15]: '15 seconds',
  [METRIC_AGGREGATION.minute1]: '1 minute',
  [METRIC_AGGREGATION.minute5]: '5 minutes',
  [METRIC_AGGREGATION.minute15]: '15 minutes',
  [METRIC_AGGREGATION.hour1]: '1 hour',
  [METRIC_AGGREGATION.hour4]: '4 hours',
  [METRIC_AGGREGATION.hour24]: '24 hours',
  [METRIC_AGGREGATION.day7]: '7 days',
  [METRIC_AGGREGATION.month1]: '1 month',
};

/**
 * metric num series
 */
export const METRIC_NUM_SERIES = {
  top1: 1,
  top2: 2,
  top3: 3,
  top4: 4,
  top5: 5,
  top10: 10,
  top20: 20,
  top30: 30,
  top40: 40,
  top50: 50,
  top100: 100,
  all: 0,
  bottom1: -1,
  bottom2: -2,
  bottom3: -3,
  bottom4: -4,
  bottom5: -5,
  bottom10: -10,
  bottom20: -20,
  bottom30: -30,
  bottom40: -40,
  bottom50: -50,
  bottom100: -100,
} as const;
export type MetricNumSeries = Enum<typeof METRIC_NUM_SERIES>;
export const isMetricNumSeries = isEnum<MetricNumSeries, number>(METRIC_NUM_SERIES);
export const toMetricNumSeries = toEnum(isMetricNumSeries, toNumber);

export const METRIC_NUM_SERIES_DESCRIPTION: Record<MetricNumSeries, string> = {
  [METRIC_NUM_SERIES.top1]: 'Top 1',
  [METRIC_NUM_SERIES.top2]: 'Top 2',
  [METRIC_NUM_SERIES.top3]: 'Top 3',
  [METRIC_NUM_SERIES.top4]: 'Top 4',
  [METRIC_NUM_SERIES.top5]: 'Top 5',
  [METRIC_NUM_SERIES.top10]: 'Top 10',
  [METRIC_NUM_SERIES.top20]: 'Top 20',
  [METRIC_NUM_SERIES.top30]: 'Top 30',
  [METRIC_NUM_SERIES.top40]: 'Top 40',
  [METRIC_NUM_SERIES.top50]: 'Top 50',
  [METRIC_NUM_SERIES.top100]: 'Top 100',
  [METRIC_NUM_SERIES.all]: 'All',
  [METRIC_NUM_SERIES.bottom1]: 'Bottom 1',
  [METRIC_NUM_SERIES.bottom2]: 'Bottom 2',
  [METRIC_NUM_SERIES.bottom3]: 'Bottom 3',
  [METRIC_NUM_SERIES.bottom4]: 'Bottom 4',
  [METRIC_NUM_SERIES.bottom5]: 'Bottom 5',
  [METRIC_NUM_SERIES.bottom10]: 'Bottom 10',
  [METRIC_NUM_SERIES.bottom20]: 'Bottom 20',
  [METRIC_NUM_SERIES.bottom30]: 'Bottom 30',
  [METRIC_NUM_SERIES.bottom40]: 'Bottom 40',
  [METRIC_NUM_SERIES.bottom50]: 'Bottom 50',
  [METRIC_NUM_SERIES.bottom100]: 'Bottom 100',
};

export const TIME_RANGE_KEYS_TO = {
  /**
   * relative now timestamp
   */
  Now: '0',
  /**
   * relative end day timestamp
   */
  EndDay: 'ed',
  /**
   * relative end week timestamp
   */
  EndWeek: 'ew',
  /**
   * absolute now timestamp
   */
  default: 'd',
} as const;

export type TimeRangeKeysTo = Enum<typeof TIME_RANGE_KEYS_TO>;
export const isTimeRangeKeysTo = isEnum<TimeRangeKeysTo>(TIME_RANGE_KEYS_TO);
export const toTimeRangeKeysTo = toEnum(isTimeRangeKeysTo);

export const PLOT_TYPE = {
  Metric: '0',
  Event: '1',
} as const;

export type PlotType = Enum<typeof PLOT_TYPE>;
export const isPlotType = isEnum<PlotType>(PLOT_TYPE);
export const toPlotType = toEnum(isPlotType);

export const TIME_RANGE_ABBREV = {
  last5m: 'last-5m',
  last15m: 'last-15m',
  last1h: 'last-1h',
  last2h: 'last-2h',
  last6h: 'last-6h',
  last12h: 'last-12h',
  last1d: 'last-1d',
  last2d: 'last-2d',
  last3d: 'last-3d',
  last7d: 'last-7d',
  last14d: 'last-14d',
  last30d: 'last-30d',
  last90d: 'last-90d',
  last180d: 'last-180d',
  last1y: 'last-1y',
  last2y: 'last-2y',
} as const;
export type TimeRangeAbbrev = Enum<typeof TIME_RANGE_ABBREV>;
export const isTimeRangeAbbrev = isEnum<TimeRangeAbbrev>(TIME_RANGE_ABBREV);
export const toTimeRangeAbbrev = toEnum(isTimeRangeAbbrev);
export const TIME_RANGE_ABBREV_DESCRIPTION: Record<TimeRangeAbbrev, string> = {
  [TIME_RANGE_ABBREV.last5m]: 'Last 5 minutes',
  [TIME_RANGE_ABBREV.last15m]: 'Last 15 minutes',
  [TIME_RANGE_ABBREV.last1h]: 'Last hour',
  [TIME_RANGE_ABBREV.last2h]: 'Last 2 hours',
  [TIME_RANGE_ABBREV.last6h]: 'Last 6 hours',
  [TIME_RANGE_ABBREV.last12h]: 'Last 12 hours',
  [TIME_RANGE_ABBREV.last1d]: 'Last 24 hours',
  [TIME_RANGE_ABBREV.last2d]: 'Last 48 hours',
  [TIME_RANGE_ABBREV.last3d]: 'Last 72 hours',
  [TIME_RANGE_ABBREV.last7d]: 'Last 7 days',
  [TIME_RANGE_ABBREV.last14d]: 'Last 14 days',
  [TIME_RANGE_ABBREV.last30d]: 'Last 30 days',
  [TIME_RANGE_ABBREV.last90d]: 'Last 90 days',
  [TIME_RANGE_ABBREV.last180d]: 'Last 180 days',
  [TIME_RANGE_ABBREV.last1y]: 'Last year',
  [TIME_RANGE_ABBREV.last2y]: 'Last 2 years',
};
