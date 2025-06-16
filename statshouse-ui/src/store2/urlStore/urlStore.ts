// Copyright 2025 V Kontakte LLC
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
} from '@/url2';
import type { StoreSlice } from '../createStore';
import { appHistory } from '@/common/appHistory';
import { getAbbrev, getUrl, isEmbedPath, isValidPath, type ProduceUpdate } from '../helpers';
import { getUrlState } from './getUrlState';
import type { StatsHouseStore } from '@/store2';
import { defaultBaseRange } from '@/store2';
import type { PlotType, TimeRangeKeysTo } from '@/api/enum';
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
import { getNextGroupKey } from './updateParamsPlotStruct';
import { getAutoSearchVariable } from './getAutoSearchVariable';
import { useErrorStore } from '@/store2/errors';
import { debug } from '@/common/debug';
import { readDataDashboard } from './readDataDashboard';
import { mergeParams } from './mergeParams';
import { setLiveMode } from '../liveModeStore';
import { filterVariableByPlot } from '../helpers/filterVariableByPlot';
import { fixMessageTrouble } from '@/url/fixMessageTrouble';
import { getUrlObject } from '@/common/getUrlObject';
import { ApiDashboard, apiDashboardSave } from '@/api/dashboard';
import { ExtendedError } from '@/api/api';
import { findGroupPositionLayout } from '@/store2/urlStore/findGroupPositionLayout';
import { selectorMapGroupPlotKeys } from '@/store2/selectors';
import { updatePlotRemove } from '@/store2/urlStore/updatePlotRemove';

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
  setPlotGroup(plotKey: PlotKey, groupKey: GroupKey): void;
  autoSearchVariable(): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>>;
  saveDashboard(copy?: boolean): Promise<void | ApiDashboard>;
  removeDashboard(): Promise<void>;
  removeVariableLinkByPlotKey(plotKey: PlotKey): void;
};

export const urlStore: StoreSlice<StatsHouseStore, UrlStore> = (setState, getState) => {
  let prevLocation = appHistory.location;
  let prevSearch = prevLocation.search;
  async function updateUrlState() {
    return getUrlState(prevLocation).then((res) => {
      setState((s) => {
        s.isEmbed = isEmbedPath(prevLocation);
        s.params = mergeParams(s.params, {
          ...res.params,
          timeRange:
            res.params.timeRange.urlTo !== s.params.timeRange.urlTo ||
            res.params.timeRange.from !== s.params.timeRange.from
              ? res.params.timeRange
              : s.params.timeRange,
        });
        s.saveParams = res.saveParams;
        if (res.error != null && res.error.status !== ExtendedError.ERROR_STATUS_ABORT) {
          useErrorStore.getState().addError(res.error);
        }
        if (s.params.tabNum === '-2' || s.params.tabNum === '-3') {
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
    });
  }

  function updateHistory(state: StatsHouseStore, replace: boolean = false) {
    const search = fixMessageTrouble('?' + getUrl(state));
    if (prevSearch !== search) {
      prevSearch = search;
      const to = getUrlObject(search);
      if (replace) {
        appHistory.replace(to);
      } else {
        appHistory.push(to);
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
      if (isValidPath(prevLocation) && (prevSearch !== prevLocation.search || prevSearch === '')) {
        prevSearch = prevLocation.search;
        updateUrlState();
      }
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
    setPlotYLock(plotKey, status, yLock) {
      setUrlStore(updatePlotYLock(plotKey, status, yLock));
    },
    resetZoom(plotKey: PlotKey) {
      setUrlStore(updateResetZoom(plotKey));
    },
    removePlot(plotKey: PlotKey) {
      getState().setParams(updatePlotRemove(plotKey));
    },
    timeRangePanLeft() {
      setLiveMode(false);
      setUrlStore(timeRangePanLeft());
    },
    timeRangePanRight() {
      if (getState().params.timeRange.absolute) {
        setLiveMode(false);
      }
      setUrlStore(timeRangePanRight());
    },
    timeRangeZoomIn() {
      setLiveMode(false);
      setUrlStore(timeRangeZoomIn());
    },
    timeRangeZoomOut() {
      setLiveMode(false);
      setUrlStore(timeRangeZoomOut());
    },
    toggleGroupShow(groupKey) {
      setUrlStore(toggleGroupShow(groupKey));
    },
    setDashboardLayoutEdit(status) {
      setState((s) => {
        s.dashboardLayoutEdit = status;
      });
      if (!status) {
        setState((s) => {
          if (s.params.tabNum === '-2' || s.params.tabNum === '-3') {
            s.params.tabNum = '-1';
          }
        });
      }
    },
    moveDashboardGroup(groupKey, direction) {
      getState().setParams((params) => {
        const sourceIndex = params.orderGroup.indexOf(groupKey);
        if (sourceIndex > -1) {
          const targetIndex = Math.max(0, Math.min(params.orderGroup.length - 1, sourceIndex + direction));
          const targetKey = params.orderGroup[targetIndex];
          if (targetKey !== groupKey) {
            params.orderGroup[targetIndex] = groupKey;
            params.orderGroup[sourceIndex] = targetKey;
          }
        }
      });
    },
    addDashboardGroup(groupKey) {
      getState().setParams((params) => {
        const sourceIndex = params.orderGroup.indexOf(groupKey);
        const nextGroupKey = getNextGroupKey(params);
        params.groups[nextGroupKey] = { ...getNewGroup(), id: nextGroupKey };
        if (sourceIndex > -1) {
          params.orderGroup.splice(sourceIndex, 0, nextGroupKey);
        } else {
          params.orderGroup.push(nextGroupKey);
        }
      });
    },
    removeDashboardGroup(groupKey) {
      getState().setParams((params) => {
        const plotKeys = selectorMapGroupPlotKeys({ params })[groupKey]?.plotKeys;
        const sourceIndex = params.orderGroup.indexOf(groupKey);

        if (sourceIndex > -1) {
          plotKeys?.forEach((plotKey) => {
            updatePlotRemove(plotKey)(params);
          });
          params.orderGroup.splice(sourceIndex, 1);
          delete params.groups[groupKey];
        }
      });
    },
    setDashboardGroup(groupKey, next) {
      setUrlStore(updateGroup(groupKey, next));
    },
    setPlotGroup(plotKey, groupKey) {
      getState().setParams((params) => {
        const plot = params.plots[plotKey];
        if (plot) {
          plot.layout = findGroupPositionLayout(params, plot.layout ?? { x: 0, y: 0, w: 1, h: 1 }, groupKey);
          plot.group = groupKey;
        }
      });
    },
    async autoSearchVariable() {
      return getAutoSearchVariable(getState);
    },
    async saveDashboard(copy?: boolean) {
      const { response, error } = await apiDashboardSave(getState().params, false, copy);
      if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
        useErrorStore.getState().addError(error);
      }
      if (response) {
        const saveParams = readDataDashboard(response.data);

        setUrlStore((store) => {
          store.saveParams = saveParams;
          store.params.dashboardVersion = saveParams.dashboardVersion;
          store.params.dashboardId = saveParams.dashboardId;
        });

        return response;
      }
    },
    async removeDashboard() {
      const { response, error } = await apiDashboardSave(getState().params, true);
      if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
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
    removeVariableLinkByPlotKey(plotKey: PlotKey) {
      const plotFilter = filterVariableByPlot(getState().params.plots[plotKey]);
      const variables = getState().params.variables;
      const variableKeys = getState().params.orderVariables.filter((vK) => plotFilter(variables[vK]));
      if (variableKeys.length) {
        getState().setParams((params) => {
          variableKeys.forEach((vK) => {
            const variable = params.variables[vK];
            if (variable) {
              variable.link = variable.link.filter(([pKey]) => pKey !== plotKey);
            }
          });
        }, true);
      }
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
