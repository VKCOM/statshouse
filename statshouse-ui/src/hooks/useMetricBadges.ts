// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ApiBadges, useApiBadges } from '../api/badges';
import { getNewMetric, PlotParams, type TimeRange, type VariableKey, type VariableParams } from '../url2';
import { useMemo } from 'react';

const badgesSelector = (r?: ApiBadges) => ({
  receiveErrors: r?.data?.receive_errors,
  receiveWarnings: r?.data?.receive_warnings,
  samplingFactorSrc: r?.data?.sampling_factor_src,
  samplingFactorAgg: r?.data?.sampling_factor_agg,
  mappingFloodEvents: r?.data?.mapping_errors,
});
const defaultPlot = getNewMetric();

export function useMetricBadges(
  plot: PlotParams = defaultPlot,
  timeRange: TimeRange,
  timeShifts: number[],
  variables: Partial<Record<VariableKey, VariableParams>>
) {
  const queryData = useApiBadges(plot, timeRange, timeShifts, variables, badgesSelector);
  return useMemo(
    () => ({
      isLoading: queryData.isLoading,
      receiveErrors: queryData.data?.receiveErrors ?? 0,
      receiveWarnings: queryData.data?.receiveWarnings ?? 0,
      samplingFactorSrc: queryData.data?.samplingFactorSrc ?? 1,
      samplingFactorAgg: queryData.data?.samplingFactorAgg ?? 1,
      mappingFloodEvents: queryData.data?.mappingFloodEvents ?? 0,
    }),
    [
      queryData.data?.mappingFloodEvents,
      queryData.data?.receiveErrors,
      queryData.data?.receiveWarnings,
      queryData.data?.samplingFactorAgg,
      queryData.data?.samplingFactorSrc,
      queryData.isLoading,
    ]
  );
}
