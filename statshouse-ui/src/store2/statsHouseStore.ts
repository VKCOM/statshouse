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
import { usePlotPreviewStore } from './plotPreviewStore';
import { plotHealsStore, type PlotHealsStore } from './plotHealsStore';
import { plotsDataStore, PlotsDataStore } from './plotDataStore';
import { plotEventsDataStore, PlotEventsDataStore } from './plotEventsDataStore';
import { updateFavicon } from './helpers/updateFavicon';
import { useVariableChangeStatusStore } from './variableChangeStatusStore';
import { filterVariableByPromQl } from './helpers/filterVariableByPromQl';

export type StatsHouseStore = UrlStore &
  UserStore &
  PlotsInfoStore &
  MetricMetaStore &
  PlotHealsStore &
  PlotsDataStore &
  PlotEventsDataStore;

const statsHouseStore: Store<StatsHouseStore> = (...props) => ({
  ...urlStore(...props),
  ...userStore(...props),
  ...plotsInfoStore(...props),
  ...metricMetaStore(...props),
  ...plotHealsStore(...props),
  ...plotsDataStore(...props),
  ...plotEventsDataStore(...props),
});
export const useStatsHouse = createStore<StatsHouseStore>(statsHouseStore);

export function useStatsHouseShallow<T = unknown>(selector: (state: StatsHouseStore) => T): T {
  return useStatsHouse(useShallow(selector));
}

useLiveModeStore.setState(updateLiveMode(useStatsHouse.getState()));

useStatsHouse.subscribe((state, prevState) => {
  const {
    params: { tabNum, plots, orderPlot, dashboardName, variables, orderVariables },
    plotsData,
    dashboardLayoutEdit,
  } = state;
  const {
    params: {
      tabNum: prevTabNum,
      plots: prevPlots,
      dashboardName: prevDashboardName,
      variables: prevVariables,
      orderVariables: prevOrderVariables,
    },
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

  // if (variables !== prevVariables) {
  useVariableChangeStatusStore.setState((s) => {
    // s.changeVariable = {};
    // s.updatePlot = {};
    orderVariables.forEach((kV) => {
      s.changeVariable[kV] = variables[kV] !== prevVariables[kV];
      // variables[kV]?.link.forEach(([pK]) => {
      //   s.updatePlot[pK] = true;
      // });
      // prevVariables[kV]?.link.forEach(([pK]) => {
      //   s.updatePlot[pK] = true;
      // });
      // orderPlot
      //   .filter((pK) => filterVariableByPromQl(plots[pK]?.promQL)(variables[kV]))
      //   .forEach((pK) => {
      //     s.updatePlot[pK] = true;
      //   });
    });
  });
  // }
  if (
    tabNum !== prevTabNum ||
    plots[tabNum] !== prevPlots[prevTabNum] ||
    plotsData[tabNum] !== prevPlotsData[prevTabNum] ||
    dashboardName !== prevDashboardName
  ) {
    updateTitle(state);
    updateFavicon(usePlotPreviewStore.getState().plotPreviewUrlList[tabNum]);
  }

  if (+tabNum >= 0 && dashboardLayoutEdit) {
    state.setDashboardLayoutEdit(false);
  }
});

usePlotPreviewStore.subscribe((state, prevState) => {
  const tabNum = useStatsHouse.getState().params.tabNum;
  if (state.plotPreviewUrlList[tabNum] !== prevState.plotPreviewUrlList[tabNum]) {
    updateFavicon(state.plotPreviewUrlList[tabNum]);
  }
});
