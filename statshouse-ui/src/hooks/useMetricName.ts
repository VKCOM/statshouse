// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useContext } from 'react';
import { usePlotsDataStore } from '@/store2/plotDataStore';
import { createSelector, weakMapMemoize } from 'reselect';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { PlotKey, promQLMetric } from '@/url2';

const selectorMetricName = createSelector(
  [
    (state: StatsHouseStore, plotKey: PlotKey) => state.params.plots[plotKey]?.metricName,
    (state: StatsHouseStore, plotKey: PlotKey) => !!state.params.plots[plotKey]?.promQL,
    (_state: StatsHouseStore, plotKey: PlotKey) => plotKey,
    (_state: StatsHouseStore, _plotKey: PlotKey, real: boolean) => real,
  ],
  (metricName, promQL, plotKey, real) => {
    const nameById = real ? '' : `plot#${plotKey}`;
    if (metricName === promQLMetric || promQL) {
      return nameById;
    }
    return metricName || nameById;
  },
  {
    memoize: weakMapMemoize,
    argsMemoize: weakMapMemoize,
  }
);

export function useMetricName(real: boolean = false) {
  const plotKey = useContext(WidgetPlotContext);
  const metricName = useStatsHouse(useCallback((state) => selectorMetricName(state, plotKey, real), [plotKey, real]));
  const seriesMetricName = usePlotsDataStore(useCallback(({ plotsData }) => plotsData[plotKey]?.metricName, [plotKey]));
  return seriesMetricName || metricName;
}
