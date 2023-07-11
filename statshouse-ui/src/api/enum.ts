// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export type Enum<T> = T[keyof T];

export const GET_PARAMS = {
  numResults: 'n',
  version: 'v',
  metricName: 's',
  metricCustomName: 'cn',
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
  dashboardGroupInfoPrefix: 'g',
  dashboardGroupInfoName: 't',
  dashboardGroupInfoShow: 'v',
  dashboardGroupInfoCount: 'n',
  dashboardGroupInfoSize: 's',
  metricLive: 'live',
  avoidCache: 'ac',
  excessPoints: 'ep',
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
  p0_001: 'p0_001',
  p0_01: 'p0_01',
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
} as const;
export type TagKey = Enum<typeof TAG_KEY>;

export const TAG_KEY_OLD = {
  skey: 'skey',
  key0: 'key0',
  key1: 'key1',
  key2: 'key2',
  key3: 'key3',
  key4: 'key4',
  key5: 'key5',
  key6: 'key6',
  key7: 'key7',
  key8: 'key8',
  key9: 'key9',
  key10: 'key10',
  key11: 'key11',
  key12: 'key12',
  key13: 'key13',
  key14: 'key14',
  key15: 'key15',
} as const;
export type TagKeyOld = Enum<typeof TAG_KEY_OLD>;

export type TagKeyAll = TagKey | TagKeyOld;

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
