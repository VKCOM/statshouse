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
import { updateParamsPlotStruct, VariableLinks } from './updateParamsPlotStruct';
import { getAutoSearchVariable } from './getAutoSearchVariable';
import { defaultBaseRange } from '../constants';
import { useErrorStore } from 'store/errors';
import { debug } from '../../common/debug';
import { saveDashboard } from './saveDashboard';
import { readDataDashboard } from './readDataDashboard';
import { mergeParams } from './mergeParams';
import { setLiveMode } from '../liveModeStore';
import { filterVariableByPlot } from '../helpers/filterVariableByPlot';
import { fixMessageTrouble } from 'url/fixMessageTrouble';
import { isNotNil } from '../../common/helpers';
import { getUrlObject } from '../../common/getUrlObject';

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
  // moveDashboardPlot(index: PlotKey | null, indexTarget: PlotKey | null, indexGroup: GroupKey | null): void;
  setNextDashboardSchemePlot(nextScheme: { groupKey: GroupKey; plots: PlotKey[] }[]): void;
  autoSearchVariable(): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>>;
  saveDashboard(): Promise<void>;
  removeDashboard(): Promise<void>;
  removeVariableLinkByPlotKey(plotKey: PlotKey): void;
};

export const urlStore: StoreSlice<StatsHouseStore, UrlStore> = (setState, getState) => {
  let prevLocation = appHistory.location;
  let prevSearch = prevLocation.search;
  async function updateUrlState() {
    return getUrlState(getState().saveParams, prevLocation).then((res) => {
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
          //remove event link
          plotStruct.groups.forEach((g) => {
            g.plots.forEach((p) => {
              p.plotInfo.events = p.plotInfo.events.filter((pK) => plotKey !== pK);
            });
          });
        })
      );
    },
    timeRangePanLeft() {
      setLiveMode(false);
      setUrlStore(timeRangePanLeft());
      getState().updatePlotsData();
    },
    timeRangePanRight() {
      if (getState().params.timeRange.absolute) {
        setLiveMode(false);
      }
      setUrlStore(timeRangePanRight());
      getState().updatePlotsData();
    },
    timeRangeZoomIn() {
      setLiveMode(false);
      setUrlStore(timeRangeZoomIn());
      getState().updatePlotsData();
    },
    timeRangeZoomOut() {
      setLiveMode(false);
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
    // moveDashboardPlot(plotKey, plotKeyTarget, groupKey) {
    //   if (plotKey != null && groupKey != null) {
    //     setUrlStore(
    //       updateParamsPlotStruct((plotStruct) => {
    //         const sourceGroupKey = plotStruct.mapPlotToGroup[plotKey] ?? '';
    //         const sourceGroupIndex = plotStruct.mapGroupIndex[sourceGroupKey];
    //         const sourcePlotIndex = plotStruct.mapPlotIndex[plotKey];
    //         const targetGroupIndex = plotStruct.mapGroupIndex[groupKey ?? sourceGroupKey];
    //         let targetPlotIndex = plotStruct.mapPlotIndex[plotKeyTarget ?? plotKey];
    //         if (sourceGroupIndex != null && sourcePlotIndex != null) {
    //           const sourcePlots = plotStruct.groups[sourceGroupIndex].plots.splice(sourcePlotIndex, 1);
    //           if (targetGroupIndex == null) {
    //             plotStruct.groups.push({
    //               plots: [...sourcePlots],
    //               groupInfo: {
    //                 ...getNewGroup(),
    //                 id: groupKey,
    //               },
    //             });
    //           } else {
    //             if (targetPlotIndex) {
    //               if (sourceGroupIndex === targetGroupIndex) {
    //                 targetPlotIndex = plotStruct.groups[targetGroupIndex].plots.findIndex(
    //                   ({ plotInfo }) => plotInfo.id === plotKeyTarget
    //                 );
    //               }
    //               plotStruct.groups[targetGroupIndex].plots.splice(targetPlotIndex, 0, ...sourcePlots);
    //             } else {
    //               plotStruct.groups[targetGroupIndex].plots.push(...sourcePlots);
    //             }
    //           }
    //         }
    //       })
    //     );
    //   }
    // },
    setNextDashboardSchemePlot(nextScheme) {
      setUrlStore(
        updateParamsPlotStruct((plotStruct) => {
          plotStruct.groups = nextScheme.map((g) => {
            const sourceGroupIndex = plotStruct.mapGroupIndex[g.groupKey];
            const plots: { plotInfo: PlotParams; variableLinks: VariableLinks[] }[] = g.plots
              .map((pK) => {
                const sourceGroupKey = plotStruct.mapPlotToGroup[pK] ?? '';
                const sourceGroupIndex = plotStruct.mapGroupIndex[sourceGroupKey];
                const sourcePlotIndex = plotStruct.mapPlotIndex[pK];
                if (sourceGroupIndex != null && sourcePlotIndex != null) {
                  return plotStruct.groups[sourceGroupIndex].plots[sourcePlotIndex];
                }
                return null;
              })
              .filter(isNotNil);
            if (sourceGroupIndex != null) {
              return {
                groupInfo: plotStruct.groups[sourceGroupIndex].groupInfo,
                plots,
              };
            }
            return {
              groupInfo: {
                ...getNewGroup(),
                id: g.groupKey,
              },
              plots,
            };
          });
        })
      );
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
      const { response, error } = await saveDashboard(getState().params, true);
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
