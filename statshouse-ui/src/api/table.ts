// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, type MetricValueBackendVersion, type QueryWhat } from './enum';
import { ApiQueryEndpoint, type SeriesMetaTag } from './query';
import { ApiAbortController, apiFetch, ApiFetchResponse, ExtendedError } from './api';
import type { PlotParams, QueryParams } from '@/url2';
import {
  CancelledError,
  type QueryClient,
  type UndefinedInitialDataOptions,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { getLoadTableUrlParams } from '@/store2/plotEventsDataStore';
import { useLiveModeStore } from '@/store2/liveModeStore';
import { queryClient } from '@/common/queryClient';

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
  [GET_PARAMS.metricLive]?: string;
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

export function getTableOptions<T = ApiTable>(
  queryClient: QueryClient,
  plot: PlotParams,
  params: QueryParams,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
): UndefinedInitialDataOptions<ApiTable, ExtendedError, T, [string, ApiTableGet | null]> {
  const keyParams = getLoadTableUrlParams(plot.id, params, undefined, key, fromEnd, limit);
  const fetchParams = getLoadTableUrlParams(plot.id, params, interval, key, fromEnd, limit);

  const gcTime = interval ? interval * 2000 : queryClient.getDefaultOptions().queries?.gcTime;

  const controller = new ApiAbortController(`${ApiTableEndpoint}_${fromEnd ? '1' : '0'}_${plot.id}`);
  controller.signal.addEventListener('abort', () => {
    queryClient.cancelQueries({ queryKey: [ApiQueryEndpoint, keyParams], exact: true });
  });

  return {
    queryKey: [ApiTableEndpoint, keyParams],
    queryFn: async ({ signal }) => {
      if (!keyParams || !fetchParams) {
        throw new ExtendedError('no request params');
      }
      const { response, error } = await apiTableFetch(fetchParams, signal);
      if (error) {
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }
      return response;
    },
    placeholderData: (previousData, previousQuery) => {
      if (
        previousQuery?.queryKey[1]?.[GET_PARAMS.metricName] !== fetchParams?.[GET_PARAMS.metricName] ||
        !!previousQuery?.state.error
      ) {
        return undefined;
      }
      return previousData;
    },
    gcTime: gcTime,
    staleTime: gcTime,
  };
}

export async function apiTable(
  plot: PlotParams,
  params: QueryParams,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
) {
  const result: ApiFetchResponse<ApiTable> = { ok: false, status: 0 };
  try {
    result.response = await queryClient.fetchQuery(
      getTableOptions<ApiTable>(queryClient, plot, params, interval, key, fromEnd, limit)
    );
    result.ok = true;
  } catch (error) {
    result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    if (error instanceof ExtendedError) {
      result.error = error;
      result.status = error.status;
    } else if (error instanceof CancelledError) {
      result.error = new ExtendedError(error, ExtendedError.ERROR_STATUS_ABORT);
      result.status = ExtendedError.ERROR_STATUS_ABORT;
    } else {
      result.error = new ExtendedError(error);
    }
  }
  return result;
}

export function useApiTable<T = ApiTable>(
  plot: PlotParams,
  params: QueryParams,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000,
  select?: (response?: ApiTable) => T,
  enabled: boolean = true
) {
  const queryClient = useQueryClient();

  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));

  const options = getTableOptions<ApiTable>(queryClient, plot, params, interval, key, fromEnd, limit);

  return useQuery({
    ...options,
    select,
    enabled,
  });
}
