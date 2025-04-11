// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey } from '@/url2';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '@/store2';
import { getMinMaxY } from '@/store2/urlStore/getMinMaxY';
import { usePlotsDataStore } from '@/store2/plotDataStore';

export function updatePlotYLock(
  plotKey: PlotKey,
  status: boolean,
  yLock?: { min: number; max: number }
): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const plot = state.params.plots[plotKey];
    if (plot) {
      const prevYLock = plot.yLock;
      const prevStatus = prevYLock.max !== 0 || prevYLock.min !== 0;
      if (prevStatus !== status || prevYLock.max !== (yLock?.max ?? 0) || prevYLock.min !== (yLock?.min ?? 0)) {
        if (status) {
          if (!yLock) {
            yLock = getMinMaxY(usePlotsDataStore.getState().plotsData[plotKey]);
          }
          plot.yLock = yLock;
        } else {
          plot.yLock = { min: 0, max: 0 };
        }
      }
    }
  };
}
