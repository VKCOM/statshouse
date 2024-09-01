// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { StoreSlice } from '../createStore';
import { StatsHouseStore } from '../statsHouseStore';
import { PlotKey } from 'url2';
import { getPlotLoader } from '../plotQueryStore';

export type PlotVisibilityStore = {
  plotVisibilityList: Partial<Record<PlotKey, boolean>>;
  plotPreviewList: Partial<Record<PlotKey, boolean>>;
  setPlotVisibility(plotKey: PlotKey, toggle: boolean): void;
  setPlotPreviewVisibility(plotKey: PlotKey, toggle: boolean): void;
  clearPlotVisibility(plotKey: PlotKey): void;
};

export const plotVisibilityStore: StoreSlice<StatsHouseStore, PlotVisibilityStore> = (setState, getState) => ({
  plotVisibilityList: {},
  plotPreviewList: {},
  setPlotVisibility(plotKey, toggle) {
    setState((state) => {
      state.plotVisibilityList[plotKey] = state.params.tabNum === plotKey || toggle;
    });
    if (toggle) {
      if (!getPlotLoader(plotKey)) {
        getState().loadPlotData(plotKey);
      }
    }
  },
  setPlotPreviewVisibility(plotKey, toggle) {
    setState((state) => {
      state.plotPreviewList[plotKey] = state.params.tabNum === plotKey || toggle;
    });
    if (toggle) {
      if (!getPlotLoader(plotKey)) {
        getState().loadPlotData(plotKey);
      }
    }
  },
  clearPlotVisibility(plotKey) {
    setState((state) => {
      delete state.plotVisibilityList[plotKey];
      delete state.plotPreviewList[plotKey];
    });
  },
});
