// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GetParams, metricValueBackendVersion } from './GetParams';
import { QueryWhat } from './query';
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
  [GetParams.metricName]: string;
  [GetParams.metricTagID]: string;
  [GetParams.version]?: metricValueBackendVersion;
  [GetParams.numResults]?: string;
  [GetParams.toTime]: string;
  [GetParams.fromTime]: string;
  [GetParams.metricFilter]?: string[];
  [GetParams.metricWhat]: QueryWhat[];
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
