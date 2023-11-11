// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { isEnum, toEnum, toNumber } from '../common/helpers';

export type Enum<T> = T[keyof T];

export const GET_PARAMS = {
  numResults: 'n',
  version: 'v',
  metricName: 's',
  metricCustomName: 'cn',
  metricCustomDescription: 'cd',
  fromTime: 'f',
  toTime: 't',
  width: 'w',
  metricWhat: 'qw',
  metricTimeShifts: 'ts',
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
  metricType: 'qt',
  metricEvent: 'qe',
  metricFromRow: 'fr',
  metricToRow: 'tr',
  metricFromEnd: 'fe',
  metricEventFrom: 'ef',
  metricEventBy: 'eb',
  metricEventHide: 'eh',
  dataFormat: 'df',
  plotPrefix: 't',
  dashboardID: 'id',
  metricsGroupID: 'id',
  metricsNamespacesID: 'id',
  metricsListAndDisabled: 'sd',
  dashboardGroupInfoPrefix: 'g',
  dashboardGroupInfoName: 't',
  dashboardGroupInfoShow: 'v',
  dashboardGroupInfoCount: 'n',
  dashboardGroupInfoSize: 's',
  metricLive: 'live',
  theme: 'theme',
  avoidCache: 'ac',
  excessPoints: 'ep',
  variablePrefix: 'v',
  variableName: 'n',
  variableNamePrefix: 'var',
  variableDescription: 'd',
  variableValue: 'v',
  variableValuePrefix: 'v.',
  variableLinkPlot: 'l',
  variableSource: 's',
  variableGroupBy: 'g',
  variableNegative: 'nv',
} as const;
export type GetParams = Enum<typeof GET_PARAMS>;

export const METRIC_VALUE_BACKEND_VERSION = {
  v1: '1',
  v2: '2',
} as const;
export type MetricValueBackendVersion = Enum<typeof METRIC_VALUE_BACKEND_VERSION>;

export const GET_BOOLEAN = {
  true: '1',
  false: '0',
} as const;
export type GetBoolean = Enum<typeof GET_BOOLEAN>;

export const QUERY_WHAT = {
  count: 'count',
  countNorm: 'count_norm',
  cuCount: 'cu_count',
  cardinality: 'cardinality',
  cardinalityNorm: 'cardinality_norm',
  cuCardinality: 'cu_cardinality',
  min: 'min',
  max: 'max',
  avg: 'avg',
  cuAvg: 'cu_avg',
  sum: 'sum',
  sumNorm: 'sum_norm',
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
  uint: 'uint',
  hex: 'hex',
  hexBswap: 'hex_bswap',
  timestamp: 'timestamp',
  timestampLocal: 'timestamp_local',
  ip: 'ip',
  ipBswap: 'ip_bswap',
  lexencFloat: 'lexenc_float',
  float: 'float',
} as const;
export type MetricMetaTagRawKind = Enum<typeof METRIC_META_TAG_RAW_KIND>;

export const API_FETCH_OPT_METHODS = {
  get: 'GET',
  post: 'POST',
  put: 'PUT',
} as const;
export type ApiFetchOptMethods = Enum<typeof API_FETCH_OPT_METHODS>;

export const isQueryWhat = isEnum<QueryWhat>(QUERY_WHAT);
export const isTagKey = isEnum<TagKey>(TAG_KEY);

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
} as const;
export type MetricType = Enum<typeof METRIC_TYPE>;
export const isMetricType = isEnum<MetricType>(METRIC_TYPE);
export const toMetricType = toEnum(isMetricType);
export const METRIC_TYPE_DESCRIPTION: Record<MetricType, string> = {
  [METRIC_TYPE.none]: 'no unit',
  [METRIC_TYPE.second]: 'second',
  [METRIC_TYPE.millisecond]: 'millisecond',
  [METRIC_TYPE.microsecond]: 'microsecond',
  [METRIC_TYPE.nanosecond]: 'nanosecond',
  [METRIC_TYPE.byte]: 'byte',
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
