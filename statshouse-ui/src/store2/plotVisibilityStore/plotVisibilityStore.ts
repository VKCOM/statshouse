// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { StoreSlice } from '../createStore';
import { StatsHouseStore } from '../statsHouseStore';
import { PlotKey } from 'url2';

export type PlotVisibilityStore = {
  plotVisibilityList: Partial<Record<PlotKey, boolean>>;
  plotPreviewList: Partial<Record<PlotKey, boolean>>;
  setPlotVisibility(plotKey: PlotKey, toggle: boolean): void;
  setPlotPreviewVisibility(plotKey: PlotKey, toggle: boolean): void;
  clearPlotVisibility(plotKey: PlotKey): void;
};

export const plotVisibilityStore: StoreSlice<StatsHouseStore, PlotVisibilityStore> = (setState, getState, store) => ({
  plotVisibilityList: {},
  plotPreviewList: {},
  setPlotVisibility(plotKey, toggle) {
    setState((state) => {
      state.plotVisibilityList[plotKey] = state.params.tabNum === plotKey || toggle;
    });

    // todo:
    // if (toggle) {
    //   if (!useUrlStore.getState().numQueriesPlot[plotKey]) {
    //     useUrlStore.getState().loadPlot(plotKey);
    //   }
    //   useUrlStore.getState().params.plots[plotKey]?.events.forEach((iPlot) => {
    //     if (!useUrlStore.getState().numQueriesPlot[iPlot]) {
    //       useUrlStore.getState().loadPlot(iPlot);
    //     }
    //   });
    // }
  },
  setPlotPreviewVisibility(plotKey, toggle) {
    setState((state) => {
      state.plotPreviewList[plotKey] = state.params.tabNum === plotKey || toggle;
    });

    // todo:
    // if (toggle && !useUrlStore.getState().numQueriesPlot[plotKey]) {
    //   useUrlStore.getState().loadPlot(plotKey);
    // }
  },
  clearPlotVisibility(plotKey) {
    setState((state) => {
      delete state.plotVisibilityList[plotKey];
      delete state.plotPreviewList[plotKey];
    });
  },
});
