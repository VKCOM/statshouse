// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch, ExtendedError } from './api';
import type { ApiQueryGet } from './query';
import type { PlotParams, QueryParams } from '@/url2';
import { useLiveModeStore } from '@/store2/liveModeStore';
import { useMemo } from 'react';
import { getLoadPlotUrlParams } from '@/store2/plotDataStore/loadPlotData';
import { useQuery, type UseQueryResult } from '@tanstack/react-query';

export const ApiBadgesEndpoint = '/api/badges';

/**
 * Response endpoint api/query
 */
export type ApiBadges = {
  data: BadgesResponse;
};

export type BadgesResponse = {
  sampling_factor_src: number;
  sampling_factor_agg: number;
  receive_errors: number;
  receive_warnings: number;
  mapping_errors: number;
};

export async function apiBadgesFetch(params: ApiQueryGet, keyRequest?: unknown) {
  return await apiFetch<ApiBadges>({ url: ApiBadgesEndpoint, get: params, keyRequest });
}

export function useApiBadges<T = ApiBadges>(
  plot: PlotParams,
  params: QueryParams,
  select?: (response?: ApiBadges) => T,
  enabled: boolean = true
): UseQueryResult<T, ExtendedError> {
  const interval = useLiveModeStore(({ interval, status }) => (status ? interval : undefined));

  const priority = useMemo(() => {
    if (plot?.id === params.tabNum) {
      return 1;
    }
    return enabled ? 2 : 3;
  }, [enabled, params.tabNum, plot?.id]);

  const keyParams = useMemo(() => {
    if (!plot?.id) {
      return null;
    }
    return getLoadPlotUrlParams(plot?.id, params);
  }, [params, plot?.id]);

  const fetchParams = useMemo(() => {
    if (!plot?.id) {
      return null;
    }
    return getLoadPlotUrlParams(plot?.id, params, interval, false, priority);
  }, [interval, params, plot?.id, priority]);

  return useQuery({
    enabled,
    select,
    queryKey: [ApiBadgesEndpoint, keyParams],
    queryFn: async ({ signal }) => {
      if (!fetchParams) {
        throw new ExtendedError('no request params');
      }
      const { response, error } = await apiBadgesFetch(fetchParams, signal);
      if (error) {
        throw error;
      }

      return response;
    },
    placeholderData: (previousData) => previousData,
  });
}
