// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey } from '@/url2';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '../statsHouseStore';

export function updateClearPlotError(plotKey: PlotKey): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const plotData = state.plotsData[plotKey];
    if (plotData) {
      plotData.error = '';
    }
    delete state.plotHeals[plotKey];
  };
}
