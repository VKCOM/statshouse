// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GET_PARAMS, type MetricMetaKind, type MetricMetaTagRawKind } from './enum';
import { apiFetch, type ApiFetchResponse, ExtendedError } from './api';
import { type UndefinedInitialDataOptions, useQuery, type UseQueryResult } from '@tanstack/react-query';
import { queryClient } from '@/common/queryClient';
import { promQLMetric } from '@/url2';
import { debug } from '@/common/debug';
import { MetricMeta, tagsArrToObject } from '@/store2/metricsMetaStore';
import { useMemo } from 'react';

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

export function getMetricOptions<T = ApiMetric>(
  metricName: string,
  enabled: boolean = true
): UndefinedInitialDataOptions<ApiMetric | undefined, ExtendedError, T, [string, string]> {
  return {
    enabled,
    queryKey: [ApiMetricEndpoint, metricName],
    queryFn: async ({ signal }) => {
      if (!metricName) {
        throw new ExtendedError('no metric name');
      }
      const { response, error } = await apiMetricFetch({ [GET_PARAMS.metricName]: metricName }, signal);
      if (error) {
        throw error;
      }
      return response;
    },
  };
}

export async function apiMetric<T = ApiMetric>(metricName: string): Promise<ApiFetchResponse<T>> {
  const result: ApiFetchResponse<T> = { ok: false, status: 0 };

  try {
    const { queryKey, queryFn } = getMetricOptions(metricName);
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
  select?: (response?: ApiMetric) => T,
  enabled: boolean = true
): UseQueryResult<T, ExtendedError> {
  const options = useMemo(() => getMetricOptions(metricName, enabled), [enabled, metricName]);
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
