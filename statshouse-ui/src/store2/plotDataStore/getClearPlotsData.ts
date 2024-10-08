// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, PlotParams } from 'url2';
import { type StatsHouseStore } from '../statsHouseStore';

const clearProps: (keyof PlotParams)[] = [
  'metricName',
  'promQL',
  // 'what',
  // 'filterIn',
  // 'filterNotIn',
  // 'groupBy',
  'backendVersion',
];

export function getClearPlotsData(state: StatsHouseStore, prevState: StatsHouseStore): PlotKey[] {
  const clearPlotKey: PlotKey[] = [];
  state.params.orderPlot.forEach((plotKey) => {
    if (
      clearProps.some(
        (clearProp) => state.params.plots[plotKey]?.[clearProp] !== prevState.params.plots[plotKey]?.[clearProp]
      )
    ) {
      clearPlotKey.push(plotKey);
    }
  });
  return clearPlotKey;
}
