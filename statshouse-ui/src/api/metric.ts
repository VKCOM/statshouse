// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, type MetricMetaKind, type MetricMetaTagRawKind } from './enum';
import { apiFetch, type ApiFetchResponse, ExtendedError } from './api';
import {
  type UndefinedInitialDataOptions,
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryResult,
} from '@tanstack/react-query';
import { queryClient } from '@/common/queryClient';
import { promQLMetric } from '@/url2';
import { debug } from '@/common/debug';
import { MetricMeta, tagsArrToObject } from '@/store2/metricsMetaStore';
import { useMemo } from 'react';
import { produce } from 'immer';
import { API_HISTORY } from '@/api/history';

export const ApiMetricEndpoint = '/api/metric';
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
 * Get params endpoint api/metric for old version
 */
export type ApiMetricVersionGet = {
  [GET_PARAMS.metricId]: string;
  [GET_PARAMS.metricApiVersion]: string;
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
  currentVersion?: number;
  update_time?: number;
  description?: string;
  tags?: MetricMetaTag[];
  /**
   * @deprecated
   */
  visible?: boolean;
  disable?: boolean;
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
  group_id?: number;
  fair_key_tag_ids?: string[];
  [key: string]: unknown;
};

export type MetricMetaTag = {
  name?: string;
  description?: string;
  /**
   * @deprecated
   */
  raw?: boolean;
  raw_kind?: MetricMetaTagRawKind;
  id2value?: Record<number, string>;
  value_comments?: Record<string, string>;
};

export async function apiMetricFetch(params: ApiMetricGet, keyRequest?: unknown) {
  return await apiFetch<ApiMetric>({ url: ApiMetricEndpoint, get: params, keyRequest });
}

export async function apiMetricVersionFetch(params: ApiMetricVersionGet, keyRequest?: unknown) {
  return await apiFetch<ApiMetric>({ url: ApiMetricEndpoint, get: params, keyRequest });
}

export function getMetricOptions<T = ApiMetric>(
  metricName: string,
  version: number | null = null,
  enabled: boolean = true
): UndefinedInitialDataOptions<ApiMetric | undefined, ExtendedError, T, [string, string, number | null]> {
  return {
    enabled,
    queryKey: [ApiMetricEndpoint, metricName, version],
    queryFn: async ({ signal, client }) => {
      if (!metricName) {
        throw new ExtendedError('no metric name');
      }
      const { response, error } = await apiMetricFetch({ [GET_PARAMS.metricName]: metricName }, signal);
      if (error) {
        throw error;
      }
      if (response) {
        response.data.metric.currentVersion = response.data.metric.version;
        client.setQueryData([ApiMetricEndpoint, metricName, response.data.metric.version], response);
      }
      if (response && version) {
        client.setQueryData([ApiMetricEndpoint, metricName, null], response);
        const historyVersion = await apiMetricVersionFetch(
          {
            [GET_PARAMS.metricId]: response.data.metric.metric_id.toString(),
            [GET_PARAMS.metricApiVersion]: version.toString(),
          },
          signal
        );

        if (historyVersion.response) {
          return produce(historyVersion.response, (res) => {
            res.data.metric.currentVersion = response.data.metric.version;
          });
        }
      }
      return response;
    },
  };
}

export async function apiMetric<T = ApiMetric>(
  metricName: string,
  version: number | null = null
): Promise<ApiFetchResponse<T>> {
  const result: ApiFetchResponse<T> = { ok: false, status: 0 };

  try {
    const { queryKey, queryFn } = getMetricOptions(metricName, version);
    result.response = await queryClient.fetchQuery({ queryKey, queryFn });
    result.ok = true;
  } catch (error) {
    result.status = ExtendedError.ERROR_STATUS_UNKNOWN;
    if (error instanceof ExtendedError) {
      result.error = error;
      result.status = error.status;
    } else {
      result.error = new ExtendedError(error);
    }
  }
  return result;
}
export async function loadMetricMeta(metricName: string) {
  if (!metricName || metricName === promQLMetric) {
    return null;
  }

  const { response, error } = await apiMetric(metricName);

  if (response && !error) {
    debug.log('loading meta for', response.data.metric.name);
    const metricMeta: MetricMeta = {
      ...response.data.metric,
      ...tagsArrToObject(response.data.metric.tags),
    };
    return metricMeta;
  }
  return null;
}

export function useApiMetric<T = ApiMetric>(
  metricName: string,
  version: number | null = null,
  select?: (response?: ApiMetric) => T,
  enabled: boolean = true
): UseQueryResult<T, ExtendedError> {
  const options = useMemo(() => getMetricOptions(metricName, version, enabled), [enabled, metricName, version]);
  return useQuery({
    ...options,
    select,
  });
}

export function getMetricMeta(metricName: string): MetricMeta | null {
  const meta = queryClient.getQueryData<ApiMetric>([ApiMetricEndpoint, metricName])?.data.metric;
  if (meta) {
    return {
      ...meta,
      ...tagsArrToObject(meta.tags),
    };
  }
  return null;
}

async function postMetricMeta(metric: MetricMetaValue) {
  const { response, error } = await apiFetch<ApiMetric, undefined, ApiMetricPost>({
    url: ApiMetricEndpoint,
    post: { metric },
    method: 'POST',
  });
  if (error) {
    throw error;
  }
  return response;
}

export function useMutationMetricMeta() {
  const queryClient = useQueryClient();
  return useMutation<Awaited<ReturnType<typeof postMetricMeta>>, ExtendedError, MetricMetaValue>({
    mutationFn: async (metric: MetricMetaValue) => {
      const originalMetric = queryClient.getQueryData<ApiMetric>([ApiMetricEndpoint, metric.name, metric.version])?.data
        .metric;
      return postMetricMeta(
        produce({ ...originalMetric, ...metric }, (p) => {
          if (p.currentVersion) {
            p.version = p.currentVersion;
          }
          delete p.currentVersion;
          delete p.update_time;
        })
      );
    },
    onSuccess: (data, variables) => {
      if (data) {
        const metric = data.data.metric;
        const metricName = metric.name;
        queryClient.setQueryData([ApiMetricEndpoint, metricName, null], data);
        queryClient.setQueryData([ApiMetricEndpoint, metricName, metric.version ?? null], data);
        queryClient.invalidateQueries({ queryKey: [API_HISTORY, metric.metric_id], type: 'all' });
      } else {
        queryClient.invalidateQueries({ queryKey: [ApiMetricEndpoint, variables.name], type: 'all' });
      }
    },
    onError: (_error, variables) => {
      queryClient.invalidateQueries({ queryKey: [ApiMetricEndpoint, variables.name], type: 'all' });
    },
  });
}
