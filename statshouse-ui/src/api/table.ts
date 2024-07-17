// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, MetricValueBackendVersion, QueryWhat } from './enum';
import { SeriesMetaTag } from './query';
import { apiFetch } from './api';

const ApiTableEndpoint = '/api/table';

/**
 * Response endpoint api/table
 */
export type ApiTable = {
  data: GetTableResp;
};

/**
 * Get params endpoint api/table
 */
export type ApiTableGet = {
  [GET_PARAMS.metricName]: string;
  [GET_PARAMS.numResults]: string;
  [GET_PARAMS.metricWhat]: QueryWhat[];
  [GET_PARAMS.toTime]: string;
  [GET_PARAMS.fromTime]: string;
  [GET_PARAMS.width]: string;
  [GET_PARAMS.version]?: MetricValueBackendVersion;
  [GET_PARAMS.metricFilter]?: string[];
  [GET_PARAMS.metricGroupBy]?: string[];
  [GET_PARAMS.metricAgg]?: string;
  [GET_PARAMS.dataFormat]?: string;
  [GET_PARAMS.avoidCache]?: string;
  [GET_PARAMS.metricFromEnd]?: string;
  [GET_PARAMS.metricFromRow]?: string;
  [GET_PARAMS.metricToRow]?: string;
};

export type GetTableResp = {
  rows: QueryTableRow[] | null;
  what: QueryWhat[];
  from_row: string;
  to_row: string;
  more: boolean;
  __debug_queries: string[];
};

export type QueryTableRow = {
  time: number;
  data: number[];
  tags: Record<string, SeriesMetaTag>;
};

export async function apiTableFetch(params: ApiTableGet, keyRequest?: unknown) {
  return await apiFetch<ApiTable>({ url: ApiTableEndpoint, get: params, keyRequest });
}
