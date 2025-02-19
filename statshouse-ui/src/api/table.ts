// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, type MetricValueBackendVersion, type QueryWhat } from './enum';
import type { SeriesMetaTag } from './query';
import { apiFetch, ApiFetchResponse, ExtendedError } from './api';
import { PlotParams, TimeRange, VariableKey, VariableParams } from '@/url2';
import {
  CancelledError,
  type QueryClient,
  UndefinedInitialDataInfiniteOptions,
  type UndefinedInitialDataOptions,
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { useLiveModeStore } from '@/store2/liveModeStore';
import { queryClient } from '@/common/queryClient';
import { useMemo } from 'react';
import { emptyFunction } from '@/common/helpers';
import type { InfiniteData } from '@tanstack/query-core';
import { getLoadTableUrlParams } from '@/common/getLoadTableUrlParams';

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
  timeRange: TimeRange,
  variables: Partial<Record<VariableKey, VariableParams>>,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
): UndefinedInitialDataOptions<ApiTable, ExtendedError, T, [string, ApiTableGet | null]> {
  const keyParams = getLoadTableUrlParams(plot, timeRange, variables, undefined, key, fromEnd, limit);
  const fetchParams = getLoadTableUrlParams(plot, timeRange, variables, interval, key, fromEnd, limit);

  const gcTime = interval ? interval * 2000 : queryClient.getDefaultOptions().queries?.gcTime;

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
  timeRange: TimeRange,
  variables: Partial<Record<VariableKey, VariableParams>>,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000
) {
  const result: ApiFetchResponse<ApiTable> = { ok: false, status: 0 };
  try {
    result.response = await queryClient.fetchQuery(
      getTableOptions<ApiTable>(queryClient, plot, timeRange, variables, interval, key, fromEnd, limit)
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
  timeRange: TimeRange,
  variables: Partial<Record<VariableKey, VariableParams>>,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000,
  select?: (response?: ApiTable) => T,
  enabled: boolean = true
) {
  const queryClient = useQueryClient();

  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));

  const options = useMemo(
    () => getTableOptions<ApiTable>(queryClient, plot, timeRange, variables, interval, key, fromEnd, limit),
    [queryClient, plot, timeRange, variables, interval, key, fromEnd, limit]
  );

  return useQuery({
    ...options,
    select,
    enabled,
  });
}

type QueryFnTableInfinitePageParam = {
  fromEnd: boolean;
  key: string;
};

const getNextPageParamTableInfinite = (lastPage: ApiTable): QueryFnTableInfinitePageParam | undefined =>
  lastPage.data.more ? { fromEnd: true, key: lastPage.data.to_row } : undefined;

export function useApiTableInfinite(
  plot: PlotParams,
  timeRange: TimeRange,
  variables: Partial<Record<VariableKey, VariableParams>>,
  enabled: boolean = true
) {
  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));
  const options = useMemo<
    UndefinedInitialDataInfiniteOptions<
      ApiTable,
      ExtendedError,
      InfiniteData<ApiTable>,
      [string, ApiTableGet | null],
      QueryFnTableInfinitePageParam | undefined
    >
  >(() => {
    const keyParams = getLoadTableUrlParams(plot, timeRange, variables, interval, undefined, true, 1000, true);

    return {
      queryKey: [ApiTableEndpoint, keyParams],
      queryFn: async ({ pageParam, signal }) => {
        const fetchParams = getLoadTableUrlParams(
          plot,
          timeRange,
          variables,
          interval,
          pageParam?.key,
          pageParam?.fromEnd ?? true,
          1000,
          true
        );
        if (!fetchParams) {
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
      initialPageParam: undefined,
      getNextPageParam: interval ? getNextPageParamTableInfinite : emptyFunction,
      getPreviousPageParam: emptyFunction,
      placeholderData: (previousData, previousQuery) => {
        if (previousQuery?.queryKey[1]?.[GET_PARAMS.metricName] !== plot.metricName || !!previousQuery?.state.error) {
          return undefined;
        }
        return previousData;
      },
    };
  }, [interval, plot, timeRange, variables]);

  return useInfiniteQuery({
    ...options,
    refetchInterval: interval ? interval * 1000 : undefined,
    maxPages: interval ? 1 : 0,
    enabled,
  });
}
