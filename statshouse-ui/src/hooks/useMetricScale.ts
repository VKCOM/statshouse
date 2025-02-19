// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetParamsContext, useWidgetPlotContext } from '@/contexts';
import { useMemo } from 'react';
import { UPlotWrapperPropsScales } from '@/components/UPlotWrapper';

export function useMetricScale() {
  const { plot } = useWidgetPlotContext();
  const { params } = useWidgetParamsContext();
  return useMemo<UPlotWrapperPropsScales>(() => {
    const res: UPlotWrapperPropsScales = {};
    res.x = { min: params.timeRange.from + params.timeRange.to, max: params.timeRange.to };
    if (plot?.yLock && (plot?.yLock.min !== 0 || plot?.yLock.max !== 0)) {
      res.y = { ...plot?.yLock };
    }
    return res;
  }, [params.timeRange.from, params.timeRange.to, plot?.yLock]);
}
