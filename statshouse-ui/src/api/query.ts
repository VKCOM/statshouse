// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_BOOLEAN, GET_PARAMS, MetricMetaTagRawKind, MetricValueBackendVersion, QueryWhat, TagKey } from './enum';
import { ApiMetric, ApiMetricEndpoint, MetricMetaValue } from './metric';
import { apiFetch, ApiFetchResponse, ExtendedError } from './api';
import { queryClient } from '../common/queryClient';
import { PlotKey, PlotParams, QueryParams } from '../url2';
import {
  CancelledError,
  QueryClient,
  UndefinedInitialDataOptions,
  useQueries,
  useQuery,
  useQueryClient,
  UseQueryResult,
} from '@tanstack/react-query';
import { useLiveModeStore } from '../store2/liveModeStore';
import { useCallback, useMemo } from 'react';
import { getLoadPlotUrlParams } from '../store2/plotDataStore/loadPlotData';
import { PlotVisibilityStore, usePlotVisibilityStore } from '@/store2/plotVisibilityStore';
import { isNotNil } from '@/common/helpers';
import { useStatsHouse } from '@/store2';
import { ApiBadgesEndpoint } from '@/api/badges';

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
  const keyParams = getLoadPlotUrlParams(plot.id, params, interval);
  const fetchParams = getLoadPlotUrlParams(plot.id, params, interval, false, priority);

  const gcTime = interval ? interval * 2000 : queryClient.getDefaultOptions().queries?.gcTime;

  // const controller = new ApiAbortController(`${ApiQueryEndpoint}_${plot.id}`);
  // controller.signal.addEventListener('abort', () => {
  //   console.log('abort', keyParams);
  //   queryClient.cancelQueries({ queryKey: [ApiQueryEndpoint, keyParams], exact: true });
  // });
  const { setPlotHeal } = useStatsHouse.getState();

  return {
    queryKey: [ApiQueryEndpoint, keyParams],
    queryFn: async ({ signal }) => {
      if (!keyParams || !fetchParams) {
        throw new ExtendedError('no request params');
      }
      console.log('queryFn', [ApiQueryEndpoint, keyParams]);
      const { response, error } = await apiQueryFetch(fetchParams, signal);

      if (error) {
        if (error.status !== ExtendedError.ERROR_STATUS_ABORT) {
          setPlotHeal(plot.id, false);
        }
        throw error;
      }
      if (!response) {
        throw new ExtendedError('empty response');
      }
      setPlotHeal(plot.id, true);
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
        !previousQuery ||
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
  const iconVisible = usePlotVisibilityStore(useCallback(({ plotPreviewList }) => plotPreviewList[plot.id], [plot.id]));

  const plotHeals = useStatsHouse((s) => {
    const status = s.plotHeals[plot.id];
    return !(!!status && !status.status && status.lastTimestamp + status.timeout * 1000 > Date.now());
  });

  const priority = useMemo(() => {
    if (plot?.id === params.tabNum) {
      return 1;
    }
    return enabled ? 2 : 3;
  }, [enabled, params.tabNum, plot?.id]);
  const options = useMemo(
    () => getQueryOptions<ApiQuery>(queryClient, plot, params, interval, priority),
    [interval, params, plot, priority, queryClient]
  );
  return useQuery({
    ...options,
    select,
    enabled: plotHeals && (enabled || iconVisible),

    refetchInterval: interval ? interval * 1000 : undefined,
  });
}

const plotVisibilitySelector = ({ plotPreviewList }: PlotVisibilityStore) => plotPreviewList;

export function useApiQueries<T = ApiQuery>(
  plotsKey: PlotKey[],
  params: QueryParams,
  select?: (response?: ApiQuery) => T,
  enabled: boolean = true
) {
  const queryClient = useQueryClient();

  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));
  const iconsVisible = usePlotVisibilityStore(plotVisibilitySelector);

  const queries = useMemo(
    () =>
      plotsKey
        .map((plotKey) => {
          const priority = plotKey === params.tabNum ? 1 : 2;
          const plot = params.plots[plotKey];
          if (plot) {
            return {
              ...getQueryOptions<ApiQuery>(queryClient, plot, params, interval, priority),
              select,
              enabled: enabled || iconsVisible[plotKey],
            };
          }
          return null;
        })
        .filter(isNotNil),
    [enabled, iconsVisible, interval, params, plotsKey, queryClient, select]
  );

  return useQueries({
    queries,
    combine: (result) => ({
      data: Object.fromEntries(result.map((r, index) => [plotsKey[index], r.data])),
      isLoading: result.some((r) => r.isLoading),
    }),
  });
}

export function refetchQuery(plot: PlotParams, params: QueryParams) {
  const interval = useLiveModeStore.getState().status ? useLiveModeStore.getState().interval : undefined;
  const keyParams = getLoadPlotUrlParams(plot.id, params, interval);
  //invalidate data
  queryClient.invalidateQueries({ queryKey: [ApiQueryEndpoint, keyParams], type: 'all', exact: true });
  //invalidate badges
  queryClient.invalidateQueries({ queryKey: [ApiBadgesEndpoint, keyParams], type: 'all', exact: true });
}
