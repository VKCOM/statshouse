// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, PlotParams } from 'url2';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '../statsHouseStore';
import { produce } from 'immer';

export function updatePlot(plotKey: PlotKey, next: ProduceUpdate<PlotParams>): ProduceUpdate<StatsHouseStore> {
  return (s) => {
    const plot = s.params.plots[plotKey];
    if (plot) {
      s.params.plots[plotKey] = produce(plot, next);
    }
  };
}
