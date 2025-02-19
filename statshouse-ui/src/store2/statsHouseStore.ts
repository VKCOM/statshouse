// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createStore, type Store } from './createStore';
import { updateTitle, urlStore, type UrlStore } from './urlStore';
import { useShallow } from 'zustand/react/shallow';
import { type UserStore, userStore } from './userStore';
import { type PlotsInfoStore, plotsInfoStore } from './plotsInfoStore';
import { updateLiveMode, useLiveModeStore } from './liveModeStore';
import { usePlotPreviewStore } from './plotPreviewStore';
import { type PlotHealsStore, plotHealsStore } from './plotHealsStore';
import { updateFavicon } from './helpers/updateFavicon';
import { useVariableChangeStatusStore } from './variableChangeStatusStore';
import { dequal } from 'dequal/lite';
import { useLinkPlots } from '@/hooks/useLinkPlot';
import { updateTheme } from './themeStore';

export type StatsHouseStore = UrlStore & UserStore & PlotsInfoStore & PlotHealsStore;

const statsHouseStore: Store<StatsHouseStore> = (...props) => ({
  ...urlStore(...props),
  ...userStore(...props),
  ...plotsInfoStore(...props),
  ...plotHealsStore(...props),
});
export const useStatsHouse = createStore<StatsHouseStore>(statsHouseStore);

export function useStatsHouseShallow<T = unknown>(selector: (state: StatsHouseStore) => T): T {
  return useStatsHouse(useShallow(selector));
}

useLiveModeStore.setState(updateLiveMode(useStatsHouse.getState()));

useStatsHouse.subscribe((state, prevState) => {
  const {
    params: { tabNum, plots, dashboardName, variables, orderVariables },
    dashboardLayoutEdit,
  } = state;
  const {
    params: { tabNum: prevTabNum, plots: prevPlots, dashboardName: prevDashboardName, variables: prevVariables },
  } = prevState;

  if (
    state.params.plots !== prevState.params.plots ||
    state.params.timeRange.urlTo !== prevState.params.timeRange.urlTo ||
    state.params.timeRange.from !== prevState.params.timeRange.from ||
    state.params.live !== prevState.params.live
  ) {
    useLiveModeStore.setState(updateLiveMode(state));
  }

  useVariableChangeStatusStore.setState((s) => {
    orderVariables.forEach((kV) => {
      s.changeVariable[kV] = variables[kV] !== prevVariables[kV];
    });
  });

  if (tabNum !== prevTabNum || plots[tabNum] !== prevPlots[prevTabNum] || dashboardName !== prevDashboardName) {
    updateTitle(state.params.tabNum, state.params.dashboardName, state.params.plots[state.params.tabNum]);
    updateFavicon(usePlotPreviewStore.getState().plotPreviewUrlList[tabNum]);
  }

  if (+tabNum >= 0 && dashboardLayoutEdit) {
    state.setDashboardLayoutEdit(false);
  }
  if (state.params !== prevState.params) {
    if (!dequal({ ...state.params, tabNum: '0' }, { ...prevState.params, tabNum: '0' })) {
      useLinkPlots.setState({
        plotLinks: {},
        singlePlotLinks: {},
      });
    }
  }
  if (state.params.theme !== prevState.params.theme) {
    updateTheme();
  }
});

usePlotPreviewStore.subscribe((state, prevState) => {
  const tabNum = useStatsHouse.getState().params.tabNum;
  if (state.plotPreviewUrlList[tabNum] !== prevState.plotPreviewUrlList[tabNum]) {
    updateFavicon(state.plotPreviewUrlList[tabNum]);
  }
});

updateTheme();
