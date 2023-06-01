// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GetParams, metricValueBackendVersion } from './GetParams';
import { QueryWhat, SeriesMetaTag } from './query';
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
  [GetParams.dataFormat]?: string;
  [GetParams.avoidCache]?: string;
  [GetParams.metricFromEnd]?: string;
  [GetParams.metricFromRow]?: string;
  [GetParams.metricToRow]?: string;
};

export type GetTableResp = {
  rows: QueryTableRow[];
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
