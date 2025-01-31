// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, MetricValueBackendVersion, QueryWhat } from './enum';
import { apiFetch } from './api';

const ApiMetricTagValuesEndpoint = '/api/metric-tag-values';

/**
 * Response endpoint api/metric-tag-values
 */
export type ApiMetricTagValues = {
  data: GetMetricTagValuesResp;
};

/**
 * Get params endpoint api/metric-tag-values
 */
export type ApiMetricTagValuesGet = {
  [GET_PARAMS.metricName]: string;
  [GET_PARAMS.metricTagID]: string;
  [GET_PARAMS.version]?: MetricValueBackendVersion;
  [GET_PARAMS.numResults]?: string;
  [GET_PARAMS.toTime]: string;
  [GET_PARAMS.fromTime]: string;
  [GET_PARAMS.metricFilter]?: string[];
  [GET_PARAMS.metricWhat]: QueryWhat[];
};

export type GetMetricTagValuesResp = {
  tag_values: MetricTagValueInfo[];
  tag_values_more: boolean;
};

export type MetricTagValueInfo = {
  value: string;
  count: number;
};

export async function apiMetricTagValuesFetch(params: ApiMetricTagValuesGet, keyRequest?: unknown) {
  return await apiFetch<ApiMetricTagValues>({ url: ApiMetricTagValuesEndpoint, get: params, keyRequest });
}
