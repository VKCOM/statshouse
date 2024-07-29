// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { calcYRange2 } from 'common/calcYRange';
import { type PlotKey } from 'url2';
import { type StatsHouseStore } from '../statsHouseStore';

export function getMinMaxY(
  plotKey: PlotKey,
  state: Pick<StatsHouseStore, 'plotsData'>
): {
  min: number;
  max: number;
} {
  const plotData = state.plotsData[plotKey];
  if (plotData) {
    const [min, max] = calcYRange2(plotData.series, plotData.data, true);
    return { min, max };
  }
  return { min: 0, max: 0 };
}
