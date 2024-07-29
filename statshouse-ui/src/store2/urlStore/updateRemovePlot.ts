// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { PlotKey } from 'url2';
import { ProduceUpdate } from '../helpers';
import { StatsHouseStore } from '../statsHouseStore';

export function updateRemovePlot(plotKey: PlotKey): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const plot = state.params.plots[plotKey];
    // todo: remove plot
    // if (plot) {
    //   plot.yLock = { min: 0, max: 0 };
    // }
    // const from = timeRangeAbbrevExpand(state.baseRange);
    // state.params.timeRange = readTimeRange(
    //   from,
    //   state.params.timeRange.absolute ? TIME_RANGE_KEYS_TO.default : TIME_RANGE_KEYS_TO.Now
    // );
  };
}
