// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { produce } from 'immer';
import {
  getDefaultParams,
  getNewGroup,
  GroupInfo,
  type GroupKey,
  type PlotKey,
  type PlotParams,
  type QueryParams,
} from 'url2';
import { type StoreSlice } from '../createStore';
import { appHistory } from 'common/appHistory';
import { getAbbrev, getUrl, isEmbedPath, isValidPath, type ProduceUpdate } from '../helpers';
import { getUrlState } from './getUrlState';
import { type StatsHouseStore } from '../statsHouseStore';
import { type PlotType, type TimeRangeKeysTo } from 'api/enum';
import { updatePlot } from './updatePlot';
import { updateTimeRange } from './updateTimeRange';
import { updateParams } from './updateParams';
import { updatePlotType } from './updatePlotType';
import {
  timeRangePanLeft,
  timeRangePanRight,
  timeRangeZoomIn,
  timeRangeZoomOut,
  updateResetZoom,
} from './timeRangeNavigate';
import { updatePlotYLock } from './updatePlotYLock';
import { toggleGroupShow } from './toggleGroupShow';
import { updateParamsPlotStruct } from './updateParamsPlotStruct';
import { getAutoSearchVariable } from './getAutoSearchVariable';
import { updateTitle } from './updateTitle';
import { defaultBaseRange } from '../constants';
import { useErrorStore } from '../../store';
import { debug } from '../../common/debug';
import { saveDashboard } from './saveDashboard';
import { readDataDashboard } from './readDataDashboard';
import { mergeParams } from './mergeParams';

export type UrlStore = {
  params: QueryParams;
  saveParams: QueryParams;
  isEmbed: boolean;
  dashboardLayoutEdit: boolean;
  updateUrlState(): Promise<void>;
  setUrlStore(next: ProduceUpdate<UrlStore>, replace?: boolean): void;
  setParams(next: ProduceUpdate<QueryParams>, replace?: boolean): void;
  setTimeRange(tr: { from: number; to: number | TimeRangeKeysTo }, replace?: boolean): void;
  setPlot(plotKey: PlotKey, next: ProduceUpdate<PlotParams>, replace?: boolean): void;
  setPlotType(plotKey: PlotKey, nextType: PlotType, replace?: boolean): void;
  setPlotYLock(plotKey: PlotKey, status: boolean, yLock?: { min: number; max: number }): void;
  resetZoom(plotKey: PlotKey): void;
  removePlot(plotKey: PlotKey): void;
  timeRangePanLeft(): void;
  timeRangePanRight(): void;
  timeRangeZoomIn(): void;
  timeRangeZoomOut(): void;
  toggleGroupShow(groupKey: GroupKey): void;
  setDashboardLayoutEdit(status: boolean): void;
  moveDashboardGroup(groupKey: GroupKey, direction: -1 | 1): void;
  addDashboardGroup(groupKey: GroupKey): void;
  removeDashboardGroup(groupKey: GroupKey): void;
  setDashboardGroup(groupKey: GroupKey, next: ProduceUpdate<GroupInfo>): void;
  moveDashboardPlot(index: PlotKey | null, indexTarget: PlotKey | null, indexGroup: GroupKey | null): void;
  autoSearchVariable(): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>>;
  saveDashboard(): Promise<void>;
  removeDashboard(): Promise<void>;
  updateTitle(): void;
};

/*export function checkUpdatePlot(plotKey: PlotKey, store: StatsHouseStore, prevStore: StatsHouseStore): boolean {
  if (store.liveMode && store.plotsData[plotKey]?.numQueries) {
    return false;
  }
  const plot = store.params.plots[plotKey];
  const prevPlot = prevStore.params.plots[plotKey];
  const dataPlot = store.plotsData[plotKey]?.lastPlotParams;
  if (plot) {
    if (!prevPlot || !dataPlot) {
      return true;
    }
    if (
      plot.filterIn !== prevPlot.filterIn ||
      plot.filterNotIn !== prevPlot.filterNotIn ||
      plot.groupBy !== prevPlot.groupBy ||
      plot.numSeries !== prevPlot.numSeries ||
      plot.what !== prevPlot.what ||
      plot.promQL !== prevPlot.promQL ||
      plot.customAgg !== prevPlot.customAgg ||
      plot.maxHost !== prevPlot.maxHost ||
      plot.useV2 !== prevPlot.useV2 ||
      plot.type !== prevPlot.type ||
      plot.filterIn !== dataPlot.filterIn ||
      plot.filterNotIn !== dataPlot.filterNotIn ||
      plot.groupBy !== dataPlot.groupBy ||
      plot.numSeries !== dataPlot.numSeries ||
      plot.what !== dataPlot.what ||
      plot.promQL !== dataPlot.promQL ||
      plot.customAgg !== dataPlot.customAgg ||
      plot.maxHost !== dataPlot.maxHost ||
      plot.useV2 !== dataPlot.useV2 ||
      plot.type !== dataPlot.type
    ) {
      return true;
    }
  }

  // change plot param
  // if (!dequal(store.params.plots[plotKey], prevStore.params.plots[plotKey])) {
  //   return true;
  // }
  // change plot param
  // if (
  //   !dequal(store.params.plots[plotKey], store.plotsData[plotKey]?.lastPlotParams) &&
  //   !store.plotsData[plotKey]?.numQueries
  // ) {
  //   return true;
  // }

  return false;
}

export function updatePlotList(store: StatsHouseStore, prevStore: StatsHouseStore): PlotKey[] {
  const first: PlotKey[] = [];
  const second: PlotKey[] = [];
  const third: PlotKey[] = [];

  store.params.orderPlot.forEach((pK) => {
    if (pK === store.params.tabNum && checkUpdatePlot(pK, store, prevStore)) {
      first.push(pK);
    } else if (store.plotVisibilityList[pK] && checkUpdatePlot(pK, store, prevStore)) {
      second.push(pK);
    } else if (store.plotPreviewList[pK] && checkUpdatePlot(pK, store, prevStore)) {
      third.push(pK);
    }
  });

  const plotsKeyUpdate = [...first, ...second, ...third];
  if (plotsKeyUpdate.length) {
    console.log('plotsKeyUpdate', { first, second, third, plotsKeyUpdate });
  } else {
    console.log('plotsKeyUpdate skip');
  }
  // console.log('list', list);
  return plotsKeyUpdate;
}*/

export const urlStore: StoreSlice<StatsHouseStore, UrlStore> = (setState, getState, store) => {
  let prevLocation = appHistory.location;
  let prevSearch = prevLocation.search;

  async function updateUrlState() {
    return getUrlState(getState().saveParams, prevLocation).then((res) => {
      setState((s) => {
        s.isEmbed = isEmbedPath(prevLocation);
        // s.params = mergeLeft(s.params, {
        //   ...res.params,
        //   timeRange:
        //     res.params.timeRange.urlTo !== s.params.timeRange.urlTo ||
        //     res.params.timeRange.from !== s.params.timeRange.from
        //       ? res.params.timeRange
        //       : s.params.timeRange,
        // });
        s.params = mergeParams(s.params, {
          ...res.params,
          timeRange:
            res.params.timeRange.urlTo !== s.params.timeRange.urlTo ||
            res.params.timeRange.from !== s.params.timeRange.from
              ? res.params.timeRange
              : s.params.timeRange,
        });
        // s.params = {
        //   ...res.params,
        //   timeRange:
        //     res.params.timeRange.urlTo !== s.params.timeRange.urlTo ||
        //     res.params.timeRange.from !== s.params.timeRange.from
        //       ? res.params.timeRange
        //       : s.params.timeRange,
        // };
        // s.saveParams = mergeLeft(s.saveParams, res.saveParams);
        s.saveParams = res.saveParams;
        if (res.error != null) {
          useErrorStore.getState().addError(res.error);
        }
        if (s.params.tabNum === '-2') {
          s.dashboardLayoutEdit = true;
        }
      });
      debug.log('updateUrlState', getState().params);
      if (res.reset) {
        updateHistory(
          produce(getState(), (p) => {
            p.params = res.params;
          }),
          true
        );
      }
      getState().updatePlotsData();
    });
  }

  function updateHistory(state: StatsHouseStore, replace: boolean = false) {
    const search = '?' + getUrl(state);
    if (prevSearch !== search) {
      prevSearch = search;
      if (replace) {
        appHistory.replace({ search });
      } else {
        appHistory.push({ search });
      }
    }
  }

  function setUrlStore(next: ProduceUpdate<StatsHouseStore>, replace: boolean = false) {
    const nextState = produce(getState(), next);
    updateHistory(nextState, replace);
    setState(nextState, true);
  }

  appHistory.listen(({ location }) => {
    if (prevLocation.search !== location.search || prevLocation.pathname !== location.pathname) {
      prevLocation = location;
      if (isValidPath(prevLocation) && prevSearch !== prevLocation.search) {
        prevSearch = prevLocation.search;
        updateUrlState();
      }
    }
  });

  store.subscribe((state, prevState) => {
    const {
      params: { tabNum, plots, dashboardName },
      plotsData,
    } = state;
    const {
      params: { tabNum: prevTabNum, plots: prevPlots, dashboardName: prevDashboardName },
      plotsData: prevPlotsData,
    } = prevState;
    if (
      tabNum !== prevTabNum ||
      plots[tabNum] !== prevPlots[prevTabNum] ||
      plotsData[tabNum] !== prevPlotsData[prevTabNum] ||
      dashboardName !== prevDashboardName
    ) {
      getState().updateTitle();
    }
  });

  const saveParams = getDefaultParams();
  if (isValidPath(prevLocation)) {
    setTimeout(() => {
      updateUrlState().then(() => {
        getState().setBaseRange(getAbbrev(getState().params.timeRange) || defaultBaseRange);
      });
    }, 0);
  }
  return {
    params: saveParams,
    saveParams: saveParams,
    isEmbed: isEmbedPath(prevLocation),
    updateUrlState,
    setUrlStore,
    dashboardLayoutEdit: false,
    setParams(next: ProduceUpdate<QueryParams>, replace) {
      setUrlStore(updateParams(next), replace);
      getState().updatePlotsData();
    },
    setTimeRange({ from, to }, replace) {
      setUrlStore(updateTimeRange(from, to), replace);
      getState().updatePlotsData();
    },
    setPlot(plotKey, next, replace) {
      setUrlStore(updatePlot(plotKey, next), replace);
      getState().loadPlotData(plotKey);
    },
    setPlotType(plotKey, nextType, replace) {
      setUrlStore(updatePlotType(plotKey, nextType), replace);
      getState().loadPlotData(plotKey);
    },
    setPlotYLock(plotKey, status, yLock?: { min: number; max: number }) {
      setUrlStore(updatePlotYLock(plotKey, status, yLock));
    },
    resetZoom(plotKey: PlotKey) {
      setUrlStore(updateResetZoom(plotKey));
      getState().updatePlotsData();
    },
    removePlot(plotKey: PlotKey) {
      setUrlStore(
        updateParamsPlotStruct((plotStruct) => {
          const sourceGroupIndex = plotStruct.mapGroupIndex[plotStruct.mapPlotToGroup[plotKey] ?? ''];
          const sourceIndex = plotStruct.mapPlotIndex[plotKey];
          if (sourceGroupIndex != null && sourceIndex != null) {
            plotStruct.groups[sourceGroupIndex].plots.splice(sourceIndex, 1);
          }
        })
      );
    },
    timeRangePanLeft() {
      setUrlStore(timeRangePanLeft());
      getState().updatePlotsData();
    },
    timeRangePanRight() {
      setUrlStore(timeRangePanRight());
      getState().updatePlotsData();
    },
    timeRangeZoomIn() {
      setUrlStore(timeRangeZoomIn());
      getState().updatePlotsData();
    },
    timeRangeZoomOut() {
      setUrlStore(timeRangeZoomOut());
      getState().updatePlotsData();
    },
    toggleGroupShow(groupKey) {
      setUrlStore(toggleGroupShow(groupKey));
    },
    setDashboardLayoutEdit(status) {
      setState((s) => {
        s.dashboardLayoutEdit = status;
      });
      if (!status) {
        setUrlStore((s) => {
          if (s.params.tabNum === '-2') {
            s.params.tabNum = '-1';
          }
        });
      }
    },
    moveDashboardGroup(groupKey, direction) {
      setUrlStore(
        updateParamsPlotStruct((plotStruct) => {
          const sourceIndex = plotStruct.mapGroupIndex[groupKey];
          if (sourceIndex != null) {
            const targetIndex = Math.max(0, Math.min(plotStruct.groups.length - 1, sourceIndex + direction));
            const targetGroup = plotStruct.groups[targetIndex];
            plotStruct.groups[targetIndex] = plotStruct.groups[sourceIndex];
            plotStruct.groups[sourceIndex] = targetGroup;
          }
        })
      );
    },
    addDashboardGroup(groupKey) {
      setUrlStore(
        updateParamsPlotStruct((plotStruct) => {
          const sourceIndex = plotStruct.mapGroupIndex[groupKey];
          const nextGroup = { groupInfo: getNewGroup(), plots: [] };
          if (sourceIndex != null) {
            plotStruct.groups.splice(sourceIndex, 0, nextGroup);
          } else {
            plotStruct.groups.push(nextGroup);
          }
        })
      );
    },
    removeDashboardGroup(groupKey) {
      setUrlStore(
        updateParamsPlotStruct((plotStruct) => {
          const sourceIndex = plotStruct.mapGroupIndex[groupKey];
          if (sourceIndex != null) {
            plotStruct.groups.splice(sourceIndex, 1);
          }
        })
      );
    },
    setDashboardGroup(groupKey, next) {
      setUrlStore(updateGroup(groupKey, next));
    },
    moveDashboardPlot(plotKey, plotKeyTarget, groupKey) {
      if (plotKey != null && groupKey != null) {
        setUrlStore(
          updateParamsPlotStruct((plotStruct) => {
            const sourceGroupKey = plotStruct.mapPlotToGroup[plotKey] ?? '';
            const sourceGroupIndex = plotStruct.mapGroupIndex[sourceGroupKey];
            const sourcePlotIndex = plotStruct.mapPlotIndex[plotKey];
            const targetGroupIndex = plotStruct.mapGroupIndex[groupKey ?? sourceGroupKey];
            let targetPlotIndex = plotStruct.mapPlotIndex[plotKeyTarget ?? plotKey];
            if (sourceGroupIndex != null && sourcePlotIndex != null) {
              const sourcePlots = plotStruct.groups[sourceGroupIndex].plots.splice(sourcePlotIndex, 1);
              if (targetGroupIndex == null) {
                plotStruct.groups.push({
                  plots: [...sourcePlots],
                  groupInfo: {
                    ...getNewGroup(),
                    id: groupKey,
                  },
                });
              } else {
                if (targetPlotIndex) {
                  if (sourceGroupIndex === targetGroupIndex) {
                    targetPlotIndex = plotStruct.groups[targetGroupIndex].plots.findIndex(
                      ({ plotInfo }) => plotInfo.id === plotKeyTarget
                    );
                  }
                  plotStruct.groups[targetGroupIndex].plots.splice(targetPlotIndex, 0, ...sourcePlots);
                } else {
                  plotStruct.groups[targetGroupIndex].plots.push(...sourcePlots);
                }
              }
            }
          })
        );
      }
    },
    async autoSearchVariable() {
      return getAutoSearchVariable(getState);
    },
    async saveDashboard() {
      const { response, error } = await saveDashboard(getState().params);
      if (error) {
        useErrorStore.getState().addError(error);
      }
      if (response) {
        const saveParams = readDataDashboard(response.data);
        setUrlStore((store) => {
          store.saveParams = saveParams;
          store.params.dashboardVersion = saveParams.dashboardVersion;
        });
      }
    },
    async removeDashboard() {
      //todo: remove dash
    },
    updateTitle() {
      updateTitle(getState());
    },
  };
};

export function updateGroup(groupKey: GroupKey, next: ProduceUpdate<GroupInfo>): ProduceUpdate<StatsHouseStore> {
  return (s) => {
    const group = s.params.groups[groupKey];
    if (group) {
      s.params.groups[groupKey] = produce(group, next);
    }
  };
}
