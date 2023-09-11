// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, MetricMetaKind, MetricMetaTagRawKind } from './enum';
import { apiFetch } from './api';

const ApiMetricEndpoint = '/api/metric';
/**
 * Response endpoint api/metric
 */
export type ApiMetric = {
  data: MetricInfo;
};

/**
 * Get params endpoint api/metric
 */
export type ApiMetricGet = {
  [GET_PARAMS.metricName]: string;
};

/**
 * Post params endpoint api/metric
 */
export type ApiMetricPost = {
  metric: MetricMetaValue;
};

export type MetricInfo = {
  metric: MetricMetaValue;
};

export type MetricMetaValue = {
  metric_id: number;
  name: string;
  version?: number;
  update_time: number;
  description?: string;
  tags?: MetricMetaTag[];
  visible?: boolean;
  kind: MetricMetaKind;
  weight?: number;
  resolution?: number;
  string_top_name?: string;
  string_top_description?: string;
  pre_key_tag_id?: string;
  pre_key_from?: number;
  skip_max_host?: boolean;
  skip_min_host?: boolean;
  skip_sum_square?: boolean;
  pre_key_only?: boolean;
  metric_type?: string;
};

export type MetricMetaTag = {
  name?: string;
  description?: string;
  raw?: boolean;
  raw_kind?: MetricMetaTagRawKind;
  id2value?: Record<number, string>;
  value_comments?: Record<string, string>;
};

export async function apiMetricFetch(params: ApiMetricGet, keyRequest?: unknown) {
  return await apiFetch<ApiMetric>({ url: ApiMetricEndpoint, get: params, keyRequest });
}
