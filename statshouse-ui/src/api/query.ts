// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_BOOLEAN, GET_PARAMS, MetricMetaTagRawKind, MetricValueBackendVersion, QueryWhat, TagKey } from './enum';
import { MetricMetaValue } from './metric';
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
export type ApiQueryGet =
  | {
      [GET_PARAMS.metricName]?: string;
      [GET_PARAMS.numResults]: string;
      [GET_PARAMS.metricWhat]: QueryWhat[];
      [GET_PARAMS.toTime]: string;
      [GET_PARAMS.fromTime]: string;
      [GET_PARAMS.width]: string;
      [GET_PARAMS.version]?: MetricValueBackendVersion;
      [GET_PARAMS.metricFilter]?: string[];
      [GET_PARAMS.metricGroupBy]?: string[];
      [GET_PARAMS.metricAgg]?: string;
      [GET_PARAMS.metricPromQL]?: string;
      [GET_PARAMS.metricTimeShifts]?: string[];
      [GET_PARAMS.metricMaxHost]?: typeof GET_BOOLEAN.true;
      [GET_PARAMS.metricVerbose]?: typeof GET_BOOLEAN.true | typeof GET_BOOLEAN.false;
      [GET_PARAMS.dataFormat]?: string;
      [GET_PARAMS.avoidCache]?: string;
      [GET_PARAMS.excessPoints]?: typeof GET_BOOLEAN.true;
      [GET_PARAMS.priority]?: string;
      // [GetParams.metricFromEnd]?:string;
      // [GetParams.metricFromRow]?:string;
      // [GetParams.metricToRow]?:string;
    }
  | Partial<Record<string, string | string[]>>;

export type ApiQueryVariableGet = Partial<Record<string, string>>;

/**
 * Post params endpoint api/query
 */
export type ApiQueryPost = Partial<ApiQueryGet>;

export type SeriesResponse = {
  series: QuerySeries;
  sampling_factor_src: number;
  sampling_factor_agg: number;
  receive_errors: number;
  receive_warnings: number;
  mapping_errors: number;
  promql: string;
  __debug_queries: string[];
  promqltestfailed?: boolean;
  metric: MetricMetaValue | null;
};

export type QuerySeries = {
  time: number[];
  series_meta: QuerySeriesMeta[];
  series_data: number[][];
};

export type QuerySeriesMeta = {
  time_shift: number;
  tags: Partial<Record<TagKey, SeriesMetaTag>>;
  max_hosts: null | string[];
  name?: string;
  what: QueryWhat;
  total: number;
  color: string;
  metric_type?: string;
};

export type SeriesMetaTag = {
  value: string;
  comment?: string;
  raw?: boolean;
  raw_kind?: MetricMetaTagRawKind;
};

export async function apiQueryFetch(params: ApiQueryGet, keyRequest?: unknown) {
  return await apiFetch<ApiQuery>({ url: ApiQueryEndpoint, get: params, keyRequest });
}
