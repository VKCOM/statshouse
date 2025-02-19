// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { apiFetch, ExtendedError } from './api';
import type { ApiQueryGet } from './query';
import type { PlotParams, TimeRange, VariableKey, VariableParams } from '@/url2';
import { useLiveModeStore } from '@/store2/liveModeStore';
import { useMemo } from 'react';
import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { useStatsHouse } from '@/store2';
import { getLoadPlotUrlParamsLight } from '@/store2/plotDataStore/loadPlotData2';

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
  timeRange: TimeRange,
  timeShifts: number[],
  variables: Partial<Record<VariableKey, VariableParams>>,
  select?: (response?: ApiBadges) => T,
  enabled: boolean = false,
  priority: number = 2
): UseQueryResult<T, ExtendedError> {
  const interval = useLiveModeStore(({ interval, status }) => (status ? interval * 2 : undefined));

  const keyParams = useMemo(() => {
    if (!plot?.id) {
      return null;
    }
    return getLoadPlotUrlParamsLight(plot, timeRange, timeShifts, variables, interval);
  }, [interval, plot, timeRange, timeShifts, variables]);

  const plotHeals = useStatsHouse((s) => {
    const status = s.plotHeals[plot.id];
    return !(!!status && !status.status && status.lastTimestamp + status.timeout * 1000 > Date.now());
  });

  const fetchParams = useMemo(() => {
    if (!plot?.id) {
      return null;
    }
    return getLoadPlotUrlParamsLight(plot, timeRange, timeShifts, variables, interval, false, priority);
  }, [interval, plot, priority, timeRange, timeShifts, variables]);

  return useQuery({
    enabled: plotHeals && enabled,
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
    refetchInterval: interval ? interval * 1000 : undefined, //live mode badge x2 interval
  });
}
