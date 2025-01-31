// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch } from './api';

const ApiMetricListEndpoint = '/api/metrics-list';

/**
 * Response endpoint api/metrics-list
 */
export type ApiMetricList = {
  data: GetMetricsListResp;
};

export type GetMetricsListResp = {
  metrics: MetricShortInfo[];
};

export type MetricShortInfo = {
  name: string;
};

export async function apiMetricListFetch(keyRequest?: unknown) {
  return await apiFetch<ApiMetricList>({ url: ApiMetricListEndpoint, keyRequest });
}
