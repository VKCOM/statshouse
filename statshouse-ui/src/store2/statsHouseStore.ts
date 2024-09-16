// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, type Store } from './createStore';
import { updateTitle, urlStore, type UrlStore } from './urlStore';
import { useShallow } from 'zustand/react/shallow';
import { userStore, type UserStore } from './userStore';
import { plotsInfoStore, type PlotsInfoStore } from './plotsInfoStore';
import { updateLiveMode, useLiveModeStore } from './liveModeStore';
import { metricMetaStore, MetricMetaStore } from './metricsMetaStore';
import { usePlotVisibilityStore } from './plotVisibilityStore';
import { usePlotPreviewStore } from './plotPreviewStore';
import { plotHealsStore, type PlotHealsStore } from './plotHealsStore';
import { plotsDataStore, PlotsDataStore } from './plotDataStore';
import { plotEventsDataStore, PlotEventsDataStore } from './plotEventsDataStore';
import { updateFavicon } from './helpers/updateFavicon';

export type StatsHouseStore = UrlStore &
  UserStore &
  PlotsInfoStore &
  MetricMetaStore &
  // PlotVisibilityStore &
  // PlotPreviewStore &
  PlotHealsStore &
  PlotsDataStore &
  PlotEventsDataStore;
// TVModeStore;

const statsHouseStore: Store<StatsHouseStore> = (...props) => ({
  ...urlStore(...props),
  ...userStore(...props),
  ...plotsInfoStore(...props),
  ...metricMetaStore(...props),
  // ...plotVisibilityStore(...props),
  // ...plotPreviewStore(...props),
  ...plotHealsStore(...props),
  ...plotsDataStore(...props),
  ...plotEventsDataStore(...props),
  // ...tvModeStore(...props),
});
export const useStatsHouse = createStore<StatsHouseStore>(statsHouseStore);

export function useStatsHouseShallow<T = unknown>(selector: (state: StatsHouseStore) => T): T {
  return useStatsHouse(useShallow(selector));
}

useLiveModeStore.setState(updateLiveMode(useStatsHouse.getState()));

useStatsHouse.subscribe((state, prevState) => {
  const {
    params: { tabNum, plots, dashboardName },
    plotsData,
  } = state;
  const {
    params: { tabNum: prevTabNum, plots: prevPlots, dashboardName: prevDashboardName },
    plotsData: prevPlotsData,
  } = prevState;

  if (
    state.params.plots !== prevState.params.plots ||
    state.params.timeRange.urlTo !== prevState.params.timeRange.urlTo ||
    state.params.timeRange.from !== prevState.params.timeRange.from ||
    state.params.live !== prevState.params.live
  ) {
    useLiveModeStore.setState(updateLiveMode(state));
  }

  if (
    tabNum !== prevTabNum ||
    plots[tabNum] !== prevPlots[prevTabNum] ||
    plotsData[tabNum] !== prevPlotsData[prevTabNum] ||
    dashboardName !== prevDashboardName
  ) {
    updateTitle(state);
    updateFavicon(usePlotPreviewStore.getState().plotPreviewUrlList[tabNum]);
  }
});

usePlotVisibilityStore.subscribe((state, prevState) => {});

usePlotPreviewStore.subscribe((state, prevState) => {
  const tabNum = useStatsHouse.getState().params.tabNum;
  if (state.plotPreviewUrlList[tabNum] !== prevState.plotPreviewUrlList[tabNum]) {
    updateFavicon(state.plotPreviewUrlList[tabNum]);
  }
});
