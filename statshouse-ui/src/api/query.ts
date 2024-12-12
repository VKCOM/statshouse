// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_BOOLEAN, GET_PARAMS, MetricMetaTagRawKind, MetricValueBackendVersion, QueryWhat, TagKey } from './enum';
import { ApiMetric, ApiMetricEndpoint, MetricMetaValue } from './metric';
import { ApiAbortController, apiFetch, ApiFetchResponse, ExtendedError } from './api';
import { queryClient } from '../common/queryClient';
import { PlotParams, QueryParams } from '../url2';
import {
  CancelledError,
  QueryClient,
  UndefinedInitialDataOptions,
  useQuery,
  useQueryClient,
  UseQueryResult,
} from '@tanstack/react-query';
import { useLiveModeStore } from '../store2/liveModeStore';
import { useMemo } from 'react';
import { getLoadPlotUrlParams } from '../store2/plotDataStore/loadPlotData';

export const ApiQueryEndpoint = '/api/query';

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
      [GET_PARAMS.metricLive]?: string;
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
  series_data: (number | null)[][];
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

export function getQueryOptions<T = ApiQuery>(
  queryClient: QueryClient,
  plot: PlotParams,
  params: QueryParams,
  interval?: number,
  priority?: number
): UndefinedInitialDataOptions<ApiQuery, ExtendedError, T, [string, ApiQueryGet | null]> {
  const keyParams = getLoadPlotUrlParams(plot?.id, params);
  const fetchParams = getLoadPlotUrlParams(plot?.id, params, interval, false, priority);

  const gcTime = interval ? interval * 2000 : queryClient.getDefaultOptions().queries?.gcTime;

  const controller = new ApiAbortController(`${ApiQueryEndpoint}_${plot.id}`);
  controller.signal.addEventListener('abort', () => {
    queryClient.cancelQueries({ queryKey: [ApiQueryEndpoint, keyParams], exact: true });
  });

  return {
    queryKey: [ApiQueryEndpoint, keyParams],
    queryFn: async ({ signal }) => {
      if (!keyParams || !fetchParams) {
        throw new ExtendedError('no request params');
      }
      const { response, error } = await apiQueryFetch(fetchParams, signal);
      if (error) {
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }
      if (response.data.metric) {
        //save metric meta cache
        queryClient.setQueryData<ApiMetric>([ApiMetricEndpoint, response?.data.metric.name], {
          data: { metric: response.data.metric },
        });
      }
      return response;
    },
    placeholderData: (previousData, previousQuery) => {
      if (
        previousQuery?.queryKey[1]?.[GET_PARAMS.metricName] !== fetchParams?.[GET_PARAMS.metricName] ||
        previousQuery?.queryKey[1]?.[GET_PARAMS.metricPromQL] !== fetchParams?.[GET_PARAMS.metricPromQL] ||
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

//for store request
export async function apiQuery(
  plot: PlotParams,
  params: QueryParams,
  interval?: number,
  priority?: number
): Promise<ApiFetchResponse<ApiQuery>> {
  const result: ApiFetchResponse<ApiQuery> = { ok: false, status: 0 };
  try {
    result.response = await queryClient.fetchQuery(
      getQueryOptions<ApiQuery>(queryClient, plot, params, interval, priority)
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

export function useApiQuery<T = ApiQuery>(
  plot: PlotParams,
  params: QueryParams,
  select?: (response?: ApiQuery) => T,
  enabled: boolean = true
): UseQueryResult<T, ExtendedError> {
  const queryClient = useQueryClient();

  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));

  const priority = useMemo(() => {
    if (plot?.id === params.tabNum) {
      return 1;
    }
    return enabled ? 2 : 3;
  }, [enabled, params.tabNum, plot?.id]);
  const options = getQueryOptions<ApiQuery>(queryClient, plot, params, interval, priority);

  return useQuery({
    ...options,
    select,
    enabled,
  });
}
