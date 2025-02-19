// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useMemo } from 'react';
import { isPromQL } from '@/store2/helpers';
import { usePlotsDataStore } from '@/store2/plotDataStore';

export function useMetricName(real: boolean = false) {
  const { plot } = useWidgetPlotContext();
  const { id, metricName } = plot;
  const isProm = isPromQL(plot);
  const seriesMetricName = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plot.id]?.metricName, [plot.id]));

  return useMemo(() => {
    const nameById = real ? '' : `plot#${id}`;
    if (!isProm) {
      return metricName || nameById;
    }
    return seriesMetricName || nameById;
  }, [id, isProm, metricName, real, seriesMetricName]);
}
