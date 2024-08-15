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
import { getUrl, isEmbedPath, isValidPath, type ProduceUpdate } from '../helpers';
import { mergeLeft } from 'common/helpers';
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

export type UrlStore = {
  params: QueryParams;
  saveParams: QueryParams;
  isEmbed: boolean;
  dashboardLayoutEdit: boolean;
  updateUrlState(): void;
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
};

export const urlStore: StoreSlice<StatsHouseStore, UrlStore> = (setState, getState) => {
  let prevLocation = appHistory.location;
  let prevSearch = prevLocation.search;

  function updateUrlState() {
    getUrlState(getState().saveParams, prevLocation, getState().setUrlStore)
      .then((res) => {
        setState((s) => {
          s.isEmbed = isEmbedPath(prevLocation);
          s.params = mergeLeft(s.params, res.params);
          s.saveParams = mergeLeft(s.saveParams, res.saveParams);
        });
      })
      .finally(() => {
        getState().updatePlotsInfo();
      });
  }

  function setUrlStore(next: ProduceUpdate<StatsHouseStore>, replace: boolean = false) {
    const nextState = produce(getState(), next);
    const search = getUrl(nextState);
    if (prevSearch !== search) {
      // prevSearch = search;
      if (replace) {
        appHistory.replace({ search });
      } else {
        appHistory.push({ search });
      }
    }
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

  const saveParams = getDefaultParams();
  if (isValidPath(prevLocation)) {
    setTimeout(updateUrlState, 0);
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
    },
    setTimeRange({ from, to }, replace) {
      setUrlStore(updateTimeRange(from, to), replace);
    },
    setPlot(plotKey, next, replace) {
      setUrlStore(updatePlot(plotKey, next), replace);
    },
    setPlotType(plotKey, nextType, replace) {
      setUrlStore(updatePlotType(plotKey, nextType), replace);
    },
    setPlotYLock(plotKey, status, yLock?: { min: number; max: number }) {
      setUrlStore(updatePlotYLock(plotKey, status, yLock));
    },
    resetZoom(plotKey: PlotKey) {
      setUrlStore(updateResetZoom(plotKey));
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
    },
    timeRangePanRight() {
      setUrlStore(timeRangePanRight());
    },
    timeRangeZoomIn() {
      setUrlStore(timeRangeZoomIn());
    },
    timeRangeZoomOut() {
      setUrlStore(timeRangeZoomOut());
    },
    toggleGroupShow(groupKey) {
      setUrlStore(toggleGroupShow(groupKey));
    },
    setDashboardLayoutEdit(status) {
      setState((s) => {
        s.dashboardLayoutEdit = status;
      });
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
      //todo: save dash
    },
    async removeDashboard() {
      //todo: remove dash
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
