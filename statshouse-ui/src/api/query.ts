// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GetBoolean, GetParams, metricValueBackendVersion } from './GetParams';
import { MetricMetaTagRawKind, MetricMetaValue } from './metric';
import { apiFetch } from './api';

const ApiQueryEndpoint = '/api/query';

/**
 * Response endpoint api/query
 */
export type ApiQuery = {
  data: SeriesResponse;
};

/**
 * Get params endpoint api/query
 */
export type ApiQueryGet = {
  [GetParams.metricName]: string;
  [GetParams.numResults]: string;
  [GetParams.metricWhat]: QueryWhat[];
  [GetParams.toTime]: string;
  [GetParams.fromTime]: string;
  [GetParams.width]: string;
  [GetParams.version]?: metricValueBackendVersion;
  [GetParams.metricFilter]?: string[];
  [GetParams.metricGroupBy]?: string[];
  [GetParams.metricAgg]?: string;
  [GetParams.metricPromQL]?: string;
  [GetParams.metricTimeShifts]?: string[];
  [GetParams.metricMaxHost]?: GetBoolean.true;
  [GetParams.metricVerbose]?: GetBoolean.true;
  [GetParams.dataFormat]?: string;
  [GetParams.avoidCache]?: string;
  // [GetParams.metricFromEnd]?:string;
  // [GetParams.metricFromRow]?:string;
  // [GetParams.metricToRow]?:string;
};

/**
 * Post params endpoint api/query
 */
export type ApiQueryPost = Partial<ApiQueryGet>;

export type SeriesResponse = {
  series: QuerySeries;
  receive_errors_legacy: number;
  sampling_factor_src: number;
  sampling_factor_agg: number;
  mapping_flood_events_legacy: number;
  receive_errors: number;
  receive_warnings: number;
  mapping_errors: number;
  promql: string;
  __debug_queries: string[];
  promqltestfailed: boolean;
  metric: MetricMetaValue;
};

export type QuerySeries = {
  time: number[];
  series_meta: QuerySeriesMeta[];
  series_data: number[][];
};

export type QuerySeriesMeta = {
  time_shift: number;
  tags: Record<string, SeriesMetaTag>;
  max_hosts: string[];
  name: string;
  what: QueryWhat;
  total: number;
};

export type SeriesMetaTag = {
  value: string;
  comment?: string;
  raw?: boolean;
  raw_kind?: MetricMetaTagRawKind;
};

export type TagKey =
  | '_s'
  | '0'
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | '10'
  | '11'
  | '12'
  | '13'
  | '14'
  | '15';

export enum QueryWhat {
  count = 'count',
  countNorm = 'count_norm',
  cuCount = 'cu_count',
  cardinality = 'cardinality',
  cardinalityNorm = 'cardinality_norm',
  cuCardinality = 'cu_cardinality',
  min = 'min',
  max = 'max',
  avg = 'avg',
  cuAvg = 'cu_avg',
  sum = 'sum',
  sumNorm = 'sum_norm',
  cuSum = 'cu_sum',
  stddev = 'stddev',
  maxHost = 'max_host',
  maxCountHost = 'max_count_host',
  p25 = 'p25',
  p50 = 'p50',
  p75 = 'p75',
  p90 = 'p90',
  p95 = 'p95',
  p99 = 'p99',
  p999 = 'p999',
  unique = 'unique',
  uniqueNorm = 'unique_norm',
  dvCount = 'dv_count',
  dvCountNorm = 'dv_count_norm',
  dvSum = 'dv_sum',
  dvSumNorm = 'dv_sum_norm',
  dvAvg = 'dv_avg',
  dvMin = 'dv_min',
  dvMax = 'dv_max',
  dvUnique = 'dv_unique',
  dvUniqueNorm = 'dv_unique_norm',
}

export async function apiQueryFetch(params: ApiQueryGet, keyRequest?: unknown) {
  return await apiFetch<ApiQuery>({ url: ApiQueryEndpoint, get: params, keyRequest });
}
