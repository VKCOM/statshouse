// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import uPlot from 'uplot';
import { produce, setAutoFreeze } from 'immer';
import { dequal } from 'dequal/lite';

import {
  metaToBaseLabel,
  metaToLabel,
  MetricsGroup,
  MetricsGroupInfo,
  MetricsGroupInfoList,
  metricsGroupListURL,
  MetricsGroupShort,
  metricsGroupURL,
  PromConfigInfo,
  promConfigURL,
  queryResult,
  querySeriesMeta,
  querySeriesMetaTag,
  queryTable,
  queryTableRow,
  queryTableURL,
  queryURL,
} from '../view/api';
import { defaultTimeRange, SetTimeRangeValue, TIME_RANGE_KEYS_TO, TimeRange } from '../common/TimeRange';
import {
  apiGet,
  apiPost,
  apiPut,
  Error403,
  loadAllMeta,
  paramToVariable,
  plotLoadPrioritySort,
  readJSONLD,
  replaceVariable,
  sortByKey,
  tagSyncToVariableConvert,
} from '../view/utils';
import { globalSettings, pxPerChar } from '../common/settings';
import { debug } from '../common/debug';
import { calcYRange2 } from '../common/calcYRange';
import { rgba, selectColor } from '../view/palette';
import { filterPoints } from '../common/filterPoints';
import { getNextState } from '../common/getNextState';
import { stackData } from '../common/stackData';
import { ErrorCustom, useErrorStore } from './errors';
import { apiMetricFetch, MetricMetaValue } from '../api/metric';
import { GET_PARAMS, isQueryWhat, METRIC_TYPE, QUERY_WHAT, type QueryWhat, TAG_KEY, type TagKey } from 'api/enum';
import { deepClone, defaultBaseRange, mergeLeft, sortEntity, toNumber } from '../common/helpers';
import { promiseRun } from '../common/promiseRun';
import { appHistory } from '../common/appHistory';
import {
  decodeDashboardIdParam,
  decodeDashboardVersionParam,
  decodeParams,
  encodeParams,
  freeKeyPrefix,
  getDefaultParams,
  getNewPlot,
  isNotNilVariableLink,
  PLOT_TYPE,
  PlotParams,
  PlotType,
  QueryParams,
  toIndexTag,
  toPlotKey,
  VariableParams,
} from '../url/queryParams';
import { clearPlotVisibility, resortPlotVisibility, usePlotVisibilityStore } from './plot/plotVisibilityStore';
import { clearAllPlotPreview, clearPlotPreview, resortPlotPreview } from './plot/plotPreviewStore';
import { createStoreWithEqualityFn } from './createStore';
import { setLiveMode, setLiveModeInterval, useLiveModeStore } from './liveMode';
import { addStatus, removePlotHeals, resortPlotHeals, skipRequestPlot, usePlotHealsStore } from './plot/plotHealsStore';
import { formatByMetricType, getMetricType } from '../common/formatByMetricType';
import { dashboardURL } from '../view/dashboardURL';
import { promQLMetric } from '../view/promQLMetric';
import { whatToWhatDesc } from '../view/whatToWhatDesc';
import {
  fmtInputDateTime,
  formatLegendValue,
  formatPercent,
  getTimeShifts,
  now,
  timeRangeAbbrev,
  timeRangeAbbrevExpand,
  timeShiftAbbrevExpand,
  timeShiftToDash,
} from '../view/utils2';
import { DashboardInfo, normalizeDashboard } from '../view/normalizeDashboard';
import { SelectOptionProps } from '../components/Select';
import { UPlotWrapperPropsScales } from '../components/UPlotWrapper';
import { fixMessageTrouble } from '../url/fixMessageTrouble';

export type PlotStore = {
  nameMetric: string;
  whats: QueryWhat[];
  metricType: string;
  error: string;
  error403?: string;
  errorSkipCount: number;
  data: uPlot.AlignedData;
  stacked?: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: uPlot.Series[];
  seriesTimeShift: number[];
  seriesShow: boolean[];
  scales: Record<string, { min: number; max: number }>;
  lastPlotParams?: PlotParams;
  lastTimeRange?: TimeRange;
  lastTimeShifts?: number[];
  lastQuerySeriesMeta?: querySeriesMeta[];
  receiveErrors: number;
  receiveWarnings: number;
  samplingFactorSrc: number;
  samplingFactorAgg: number;
  mappingFloodEvents: number;
  legendValueWidth: number;
  legendMaxDotSpaceWidth: number;
  legendNameWidth: number;
  legendPercentWidth: number;
  legendMaxHostWidth: number;
  legendMaxHostPercentWidth: number;
  topInfo?: TopInfo;
  maxHostLists: SelectOptionProps[][];
  promqltestfailed?: boolean;
  promQL: string;
};

export type TopInfo = {
  top: string;
  total: string;
  info: string;
};

export type PlotValues = {
  rawValue: number | null;
  value: string;
  metricName: string;
  label: string;
  baseLabel: string;
  timeShift: number;
  max_host: string;
  total: number;
  percent: string;
  max_host_percent: string;
  top_max_host: string;
  top_max_host_percent: string;
};

function getEmptyPlotData(): PlotStore {
  return {
    nameMetric: '',
    whats: [],
    metricType: '',
    error: '',
    errorSkipCount: 0,
    data: [[]],
    stacked: undefined,
    series: [],
    seriesTimeShift: [],
    seriesShow: [],
    scales: {},
    receiveErrors: 0,
    receiveWarnings: 0,
    samplingFactorSrc: 0,
    samplingFactorAgg: 0,
    mappingFloodEvents: 0,
    legendValueWidth: 0,
    legendMaxDotSpaceWidth: 0,
    legendNameWidth: 0,
    legendPercentWidth: 0,
    legendMaxHostWidth: 0,
    legendMaxHostPercentWidth: 0,
    lastPlotParams: undefined,
    lastTimeRange: undefined,
    lastTimeShifts: undefined,
    lastQuerySeriesMeta: undefined,
    topInfo: undefined,
    maxHostLists: [],
    promQL: '',
  };
}

type EventDataChunk = queryTable & { to: number; from: number; fromEnd: boolean };

export type EventDataRow = {
  key: string;
  idChunk: number;
  timeString: string;
  time: number;
  data: number[];
} & Partial<Record<string, querySeriesMetaTag>>;

export type EventData = {
  chunks: EventDataChunk[];
  rows: EventDataRow[];
  what: QueryWhat[];
  nextKey?: string;
  prevKey?: string;
  range: TimeRange;
  nextAbortController?: AbortController;
  prevAbortController?: AbortController;
  error?: string;
  error403?: string;
};

export type StatsHouseStore = {
  defaultParams: QueryParams;
  setDefaultParams(nextState: React.SetStateAction<QueryParams>): void;
  timeRange: TimeRange;
  params: QueryParams;
  updateParamsByUrl(abortSignal?: AbortSignal): void;
  updateUrl(replace?: boolean): void;
  updateTitle(): void;
  setTimeRange(value: SetTimeRangeValue, force?: boolean): void;
  setParams(nextState: React.SetStateAction<QueryParams>, replace?: boolean, force?: boolean): void;
  setPlotParams(index: number, nextState: React.SetStateAction<PlotParams>, replace?: boolean): void;
  removePlot(index: number): void;
  setTabNum(id: number): void;
  error: string;
  globalNumQueriesPlot: number;
  setGlobalNumQueriesPlot(nextState: React.SetStateAction<number>): void;
  numQueriesPlot: number[];
  setNumQueriesPlot(index: number, nextState: React.SetStateAction<number>): void;
  baseRange: timeRangeAbbrev;
  setBaseRange(nextState: React.SetStateAction<timeRangeAbbrev>): void;
  plotsData: PlotStore[];
  plotsDataAbortController: AbortController[];
  loadPlot(index: number, force?: boolean): void;
  setPlotShow(indexPlot: number, idx: number, show?: boolean, single?: boolean): void;
  setPlotLastError(index: number, error: string): void;
  uPlotsWidth: number[];
  setUPlotWidth(index: number, weight: number): void;
  setYLockChange(index: number, status: boolean): void;
  metricsMeta: Record<string, MetricMetaValue>;
  loadMetricsMeta(metricName: string): Promise<void>;
  clearMetricsMeta(metricName: string): void;
  compact: boolean;
  setCompact(compact: boolean): void;
  setPlotParamsTag(
    indexPlot: number,
    tagKey: TagKey,
    nextState: React.SetStateAction<string[]>,
    positive: React.SetStateAction<boolean>
  ): void;
  setPlotParamsTagGroupBy(indexPlot: number, tagKey: TagKey, nextState: React.SetStateAction<boolean>): void;
  setPlotType(indexPlot: number, nextState: React.SetStateAction<PlotType>): void;
  serverParamsAbortController?: AbortController;
  loadServerParams(id: number, v?: number | null): Promise<QueryParams>;
  saveServerParams(): Promise<QueryParams>;
  removeServerParams(): Promise<QueryParams>;
  saveDashboardParams?: QueryParams;
  setSaveDashboardParams(nextState: React.SetStateAction<QueryParams | undefined>): void;
  moveAndResortPlot(indexSelectPlot?: number | null, indexTargetPlot?: number | null, indexGroup?: number | null): void;
  dashboardLayoutEdit: boolean;
  setDashboardLayoutEdit(nextStatus: boolean): void;
  setGroupName(indexGroup: number, name: string): void;
  setGroupDescription(indexGroup: number, description: string): void;
  setGroupShow(indexGroup: number, show: React.SetStateAction<boolean>): void;
  setGroupSize(indexGroup: number, size: React.SetStateAction<string>): void;
  listMetricsGroup: MetricsGroupShort[];
  loadListMetricsGroup(): Promise<MetricsGroupShort[]>;
  saveMetricsGroup(metricsGroup: MetricsGroup): Promise<MetricsGroupInfo | undefined>;
  removeMetricsGroup(metricsGroup: MetricsGroup): Promise<MetricsGroupInfo | undefined>;
  selectMetricsGroup?: MetricsGroupInfo;
  loadMetricsGroup(id: number): Promise<MetricsGroupInfo | undefined>;
  setSelectMetricsGroup(metricsGroup?: MetricsGroupInfo): void;
  promConfig?: PromConfigInfo;
  loadPromConfig(): Promise<PromConfigInfo | undefined>;
  savePromConfig(nextPromConfig: PromConfigInfo): Promise<PromConfigInfo | undefined>;
  events: EventData[];
  loadEvents(indexPlot: number, key?: string, fromEnd?: boolean, from?: number): Promise<EventData | null>;
  clearEvents(indexPlot: number): void;
};

export type Store = StatsHouseStore;
export const useStore = createStoreWithEqualityFn<Store>((setState, getState, store) => {
  let prevLocation = appHistory.location;
  let controller: AbortController;
  appHistory.listen(({ location }) => {
    if (prevLocation.search !== location.search || prevLocation.pathname !== location.pathname) {
      prevLocation = location;
      if (location.pathname === '/view' || location.pathname === '/embed') {
        controller?.abort();
        controller = new AbortController();
        getState().updateParamsByUrl(controller.signal);
      }
    }
  });
  store.subscribe((store, prevStore) => {
    if (store.timeRange.relativeFrom !== prevStore.timeRange.relativeFrom) {
      setLiveModeInterval(store.timeRange.relativeFrom);
    }
  });
  return {
    defaultParams: getDefaultParams(),
    setDefaultParams(nextState) {
      const nextDefaultParams = getNextState(getState().defaultParams, nextState);
      setState((state) => {
        state.defaultParams = nextDefaultParams;
      });
    },
    timeRange: new TimeRange({ to: TIME_RANGE_KEYS_TO.default, from: 0 }),
    params: getDefaultParams(),
    setTimeRange(value, force?) {
      const tr = new TimeRange(getState().params.timeRange);
      tr.setRange(value);
      const nextTimeRange = tr.getRangeUrl();
      if (
        force ||
        nextTimeRange.to !== getState().params.timeRange.to ||
        nextTimeRange.from !== getState().params.timeRange.from
      ) {
        getState().setParams(
          produce((params) => {
            params.timeRange = nextTimeRange;
            if (nextTimeRange.from < params.eventFrom && nextTimeRange.to > params.eventFrom) {
              params.eventFrom = 0;
            }
          }),
          false,
          force
        );
      }
    },
    async updateParamsByUrl(abortSignal: AbortSignal) {
      const urlSearchParams = new URLSearchParams(document.location.search);
      const searchParams = [...urlSearchParams.entries()];
      const id = decodeDashboardIdParam(urlSearchParams);
      const dashVersion = decodeDashboardVersionParam(urlSearchParams);
      if (id && getState().params.dashboard?.dashboard_id && id !== getState().params.dashboard?.dashboard_id) {
        setState((state) => {
          state.params.plots = [];
          state.params.dashboard = getDefaultParams().dashboard;
          state.params.tagSync = [];
          state.params.eventFrom = 0;
        });
      }
      const saveParams = id ? await getState().loadServerParams(id, dashVersion) : undefined;
      const localDefaultParams: QueryParams = {
        ...(saveParams ?? getDefaultParams()),
        timeRange: {
          to:
            saveParams && !(typeof saveParams?.timeRange.to === 'number' && saveParams.timeRange.to > 0)
              ? saveParams.timeRange.to
              : id
              ? 0
              : getDefaultParams().timeRange.to,
          from: saveParams?.timeRange.from ?? getDefaultParams().timeRange.from,
        },
      };

      let decodeP = decodeParams(searchParams, localDefaultParams);
      if (!decodeP) {
        return;
      }
      let params = decodeP;
      if (decodeP.tagSync.length) {
        if (!decodeP.dashboard?.dashboard_id) {
          useErrorStore
            .getState()
            .addError(
              new ErrorCustom(
                'Возможно вы перешли по сохранённой ссылке, которая имеет старый формат,мы обновили ссылку в адресной строке браузера, пожалуйста пересохраните ссылку в закладках',
                'Link format deprecated'
              )
            );
        }
        await loadAllMeta(decodeP, getState().loadMetricsMeta);
        const metricsMeta = getState().metricsMeta;
        setAutoFreeze(false);
        params = tagSyncToVariableConvert(decodeP, metricsMeta);
        setAutoFreeze(true);
      }

      getState().setDefaultParams(localDefaultParams);
      if (abortSignal?.aborted) {
        return;
      }

      let reset = false;
      const nowTime = now();

      if (!dequal(params.variables, decodeP.variables)) {
        reset = true;
      }

      if (params.tabNum >= 0 && !params.plots[params.tabNum]) {
        params.tabNum = getState().defaultParams.tabNum;
        reset = true;
      }

      if (params.timeRange.from === defaultTimeRange.from && params.timeRange.to === defaultTimeRange.to) {
        params.timeRange = timeRangeAbbrevExpand(defaultBaseRange, nowTime);
        reset = true;
      } else if (params.timeRange.to === defaultTimeRange.to) {
        params.timeRange.to = nowTime;
        reset = true;
      } else if (params.timeRange.from > nowTime) {
        params.timeRange = {
          to: nowTime,
          from: new TimeRange(params.timeRange).relativeFrom,
        };
        reset = true;
      }

      if (params.plots.length === 0) {
        const np: PlotParams = getNewPlot();
        np.id = '0';
        params.plots = [np];
        reset = true;
      }

      if (globalSettings.disabled_v1) {
        params.plots = params.plots.map((item) => (item.useV2 ? item : { ...item, useV2: true }));
        reset = true;
      }
      const paramsFix = normalizeParamsGroupCount(params);
      if (params.dashboard !== paramsFix.dashboard) {
        params = paramsFix;
        reset = true;
      }

      const resetPlot = params.dashboard?.dashboard_id !== getState().params.dashboard?.dashboard_id;
      const prevParams = getState().params;
      const changed = !dequal(params, prevParams);
      const changedTimeRange = !dequal(params.timeRange, prevParams.timeRange);

      const changedTimeShifts = !dequal(params.timeShifts, prevParams.timeShifts);
      if (changed) {
        debug.log('updateParamsByUrl', deepClone(params), deepClone(getState().params));
        setState((store) => {
          if (
            store.params.timeRange.to !== params.timeRange.to ||
            store.params.timeRange.from !== params.timeRange.from
          ) {
            store.timeRange = new TimeRange(params.timeRange);
          }
          store.params = mergeLeft(store.params, params);
          if (resetPlot) {
            store.plotsData = [];
            store.dashboardLayoutEdit = false;
          }
          if (store.params.tabNum < -1) {
            store.dashboardLayoutEdit = true;
          }
          if (store.params.tabNum >= 0) {
            store.dashboardLayoutEdit = false;
          }
        });
        if (resetPlot) {
          clearAllPlotPreview();
        }
        plotLoadPrioritySort(getState().params).forEach(({ plot, indexPlot }) => {
          if (changedTimeRange || changedTimeShifts || prevParams.plots[indexPlot] !== plot) {
            getState().loadPlot(indexPlot);
          }
        });
      }
      getState().updateTitle();
      if (reset) {
        getState().updateUrl(true);
      }
      if (params.live) {
        setLiveMode(true);
      }
    },
    setParams(nextState, replace?, force?) {
      const prevParams = getState().params;
      const nextParams = getNextState(prevParams, nextState);
      const changed = force || !dequal(nextParams, prevParams);
      const changedTimeRange = force || !dequal(nextParams.timeRange, prevParams.timeRange);
      const changedTimeShifts = !dequal(nextParams.timeShifts, prevParams.timeShifts);
      if (changed) {
        setState((state) => {
          if (changedTimeRange) {
            state.timeRange = new TimeRange(nextParams.timeRange);
          }
          state.params = { ...nextParams, variables: nextParams.variables.map((v) => ({ ...v })) };
          if (state.params.tabNum >= 0) {
            state.dashboardLayoutEdit = false;
          }
          state.params.plots.forEach((plot, indexPlot) => {
            if (
              state.params.plots.length === prevParams.plots.length &&
              (plot.metricName === promQLMetric || plot.metricName !== prevParams.plots[indexPlot].metricName) &&
              state.params.variables === prevParams.variables
            ) {
              const plotKey = toPlotKey(indexPlot);
              if (plotKey != null) {
                state.params.variables.forEach((variable) => {
                  const nextLinks = variable.link.filter(([iPlot]) => iPlot !== plotKey);
                  if (variable.link.length !== nextLinks.length) {
                    variable.link = nextLinks;
                  }
                });
              }
            }
            if (
              plot.metricName === promQLMetric &&
              state.params.tagSync.some((g) => g[indexPlot] !== null && g[indexPlot] !== undefined)
            ) {
              state.params.tagSync = state.params.tagSync.map((g) =>
                g.map((tags, plot) => (plot !== indexPlot ? tags : null))
              );
            }
          });
        });
        const changedVariablesPlot: Set<number | null> = new Set();
        const plotsPromQL = getState()
          .params.plots.map((plot, indexPlot) => ({ plot, indexPlot }))
          .filter(({ plot }) => plot.promQL.indexOf('$') > -1 || plot.promQL.indexOf('__bind__') > -1);
        getState().params.variables.forEach((variable, indexVariable) => {
          if (prevParams.variables[indexVariable] !== variable) {
            variable.link.forEach(([iPlot]) => {
              changedVariablesPlot.add(toNumber(iPlot));
            });
            plotsPromQL.forEach(({ plot, indexPlot }) => {
              if (
                plot.promQL.indexOf('$' + variable.name) > -1 ||
                (plot.promQL.indexOf('__bind__') > -1 && plot.promQL.indexOf(variable.name) > -1)
              ) {
                changedVariablesPlot.add(indexPlot);
              }
            });
          }
        });
        plotLoadPrioritySort(getState().params).forEach(({ plot, indexPlot }) => {
          if (
            changedTimeRange ||
            changedTimeShifts ||
            prevParams.plots[indexPlot] !== plot ||
            changedVariablesPlot.has(indexPlot)
          ) {
            getState().loadPlot(indexPlot, force);
          }
        });
        if (getState().params.plots.some(({ useV2 }) => !useV2) && useLiveModeStore.getState().live) {
          setLiveMode(false);
        }
        getState().updateUrl(replace);
      }
    },
    setPlotParams(index, nextState, replace?) {
      const prev = getState().params.plots[index];
      const next = getNextState(prev, nextState);
      const changed = !dequal(next, prev);
      const noUpdate = changed && dequal({ ...next, customName: '' }, { ...prev, customName: '' });
      if (changed) {
        setState((state) => {
          if (
            next.metricName !== prev.metricName &&
            state.params.tagSync.some((g) => g[index] !== null && g[index] !== undefined)
          ) {
            state.params.tagSync = state.params.tagSync.map((g) =>
              g.map((tags, plot) => (plot !== index ? tags : null))
            );
          }
          state.params.plots[index] = next;
          const plotKey = toPlotKey(index);
          if ((next.metricName === promQLMetric || next.metricName !== prev.metricName) && plotKey != null) {
            state.params.variables.forEach((variable) => {
              const nextLinks = variable.link.filter(([iPlot]) => iPlot !== plotKey);
              if (variable.link.length !== nextLinks.length) {
                variable.link = nextLinks;
              }
            });
          }
        });
        if (!noUpdate) {
          getState().loadPlot(index);
        }
        if (!next.useV2 && useLiveModeStore.getState().live) {
          setLiveMode(false);
        }
        if (next.metricName !== prev.metricName) {
          const metrics = getState().params.plots.map(({ metricName }) => metricName);
          Object.keys(getState().metricsMeta)
            .filter((name) => name && !metrics.includes(name))
            .forEach(getState().clearMetricsMeta);
        }
        getState().updateUrl(replace);
      }
    },
    removePlot(index) {
      const plotKey = toPlotKey(index);
      if (plotKey == null) {
        return;
      }
      setState((state) => {
        state.plotsData.splice(index, 1);
        state.plotsData = state.plotsData.slice(0, state.params.plots.length);
      });
      clearPlotPreview(index, true);
      clearPlotVisibility(index, true);
      getState().setParams(
        produce((params) => {
          const groups = params.dashboard?.groupInfo?.flatMap((g, indexG) => new Array(g.count).fill(indexG)) ?? [];
          if (groups.length !== params.plots.length) {
            while (groups.length < params.plots.length) {
              groups.push(Math.max(0, (params.dashboard?.groupInfo?.length ?? 0) - 1));
            }
          }
          if (params.plots.length > 1) {
            params.plots.splice(index, 1);
            params.plots = params.plots.map((p, indexPlot) => ({
              ...p,
              id: indexPlot.toString(), // fix fallback
              events: p.events.filter((v) => v !== index).map((v) => (v > index ? v - 1 : v)),
            }));
            params.tagSync = params.tagSync.map((g) => g.filter((tags, plot) => plot !== index));
            groups.splice(index, 1);
            if (params.dashboard?.groupInfo?.length) {
              params.dashboard.groupInfo = params.dashboard.groupInfo.map((g, index) => ({
                ...g,
                count:
                  groups.reduce((res: number, item) => {
                    if (item === index) {
                      res = res + 1;
                    }
                    return res;
                  }, 0 as number) ?? 0,
              }));
            }
            params.variables = params.variables.map((variable) => ({
              ...variable,
              link: variable.link
                .filter(([keyP]) => keyP !== plotKey)
                .map(([keyP, keyT]) => {
                  const indexP = toNumber(keyP, 0);
                  return [toPlotKey(indexP > index ? indexP - 1 : indexP, keyP), keyT];
                })
                .filter(isNotNilVariableLink),
            }));
          }
          if (params.tabNum > index) {
            params.tabNum--;
          }
          if (params.tabNum === index && params.plots.length - 1 < params.tabNum) {
            params.tabNum--;
          }
          if (params.tabNum === -1 && params.plots.length === 1) {
            params.tabNum = 0;
          }
        })
      );
      const metrics = getState().params.plots.map(({ metricName }) => metricName);
      Object.keys(getState().metricsMeta)
        .filter((name) => name && !metrics.includes(name))
        .forEach(getState().clearMetricsMeta);
    },
    updateUrl(replace?: boolean) {
      const prevState = getState();
      const autoReplace =
        prevState.params.timeRange.from === defaultTimeRange.from ||
        prevState.params.timeRange.to === defaultTimeRange.to ||
        useLiveModeStore.getState().live ||
        prevState.timeRange.from > now();
      const p = encodeParams(prevState.params, prevState.defaultParams);
      const search = '?' + fixMessageTrouble(new URLSearchParams(p).toString());
      let pathname = document.location.pathname;

      if (pathname !== '/view' && pathname !== '/embed') {
        pathname = '/view';
      }
      if (document.location.search !== search) {
        if (replace || autoReplace) {
          appHistory.replace({ search, pathname });
        } else {
          appHistory.push({ search, pathname });
        }
      }
      getState().updateTitle();
    },
    updateTitle() {
      const s = getState();
      switch (s.params.tabNum) {
        case -1:
          if (s.params.dashboard?.dashboard_id !== undefined) {
            document.title = `${s.params.dashboard?.name} — StatsHouse`;
          } else {
            document.title = 'Dashboard — StatsHouse';
          }
          break;
        case -2:
          document.title = 'Dashboard setting — StatsHouse';
          break;
        default:
          if (s.params.plots[s.params.tabNum] && s.params.plots[s.params.tabNum].metricName === promQLMetric) {
            if (s.plotsData[s.params.tabNum].nameMetric) {
              document.title =
                s.plotsData[s.params.tabNum].nameMetric +
                (s.plotsData[s.params.tabNum].whats.length
                  ? ': ' + s.plotsData[s.params.tabNum].whats.map((qw) => whatToWhatDesc(qw)).join(',')
                  : '') +
                ' — StatsHouse';
            } else {
              document.title = 'StatsHouse';
            }
          } else {
            document.title =
              (s.params.plots[s.params.tabNum] &&
                s.params.plots[s.params.tabNum].metricName !== '' &&
                `${s.params.plots[s.params.tabNum].metricName}: ${s.params.plots[s.params.tabNum].what
                  .map((qw) => whatToWhatDesc(qw))
                  .join(',')} — StatsHouse`) ||
              '';
          }
          break;
      }
    },
    setTabNum(id) {
      setState((store) => {
        store.params.tabNum = id;
      });
      getState().updateUrl();
    },
    error: '',
    globalNumQueriesPlot: 0,
    setGlobalNumQueriesPlot(nextState) {
      setState((state) => {
        state.globalNumQueriesPlot = getNextState(state.globalNumQueriesPlot, nextState);
      });
    },
    numQueriesPlot: [],
    setNumQueriesPlot(index, nextState) {
      setState((state) => {
        state.numQueriesPlot[index] = getNextState(state.numQueriesPlot[index] ?? 0, nextState);
      });
    },
    baseRange: defaultBaseRange,
    setBaseRange(nextState) {
      setState((state) => {
        state.baseRange = getNextState(state.baseRange, nextState);
      });
    },
    plotsData: [],
    plotsDataAbortController: [],
    loadPlot(index, force: boolean = false) {
      const plotKey = toPlotKey(index);
      if (!getState().plotsData[index]) {
        setState((state) => {
          state.plotsData[index] = getEmptyPlotData();
        });
      }
      const prevStateLiveMode = useLiveModeStore.getState().live;
      const {
        numQueriesPlot: prevStateNumQueriesPlot,
        params: { plots: prevStatePlots, variables: prevStateVariables, timeShifts: prevStateTimeShifts },
        uPlotsWidth: prevStateuPlotsWidth,
        compact: prevStateCompact,
        timeRange: { to: prevStateTo, from: prevStateFrom },
      } = getState();

      // if liveMode and there is a queries then wait request
      if (prevStateNumQueriesPlot[index] > 0 && prevStateLiveMode) {
        return;
      }
      const isSubVisible = prevStatePlots.some(
        (plot, iPlot) => plot.events.indexOf(index) > -1 && usePlotVisibilityStore.getState().visibilityList[iPlot]
      );

      if (
        !isSubVisible &&
        !usePlotVisibilityStore.getState().visibilityList[index] &&
        !usePlotVisibilityStore.getState().previewList[index]
      ) {
        return;
      }
      if (plotKey == null) {
        return;
      }
      const width = prevStateuPlotsWidth[index] ?? prevStateuPlotsWidth.find((w) => w && w > 0);
      const compact = prevStateCompact;
      const lastPlotParams: PlotParams | undefined = replaceVariable(
        plotKey,
        prevStatePlots[index],
        prevStateVariables
      );
      const prev: PlotStore = getState().plotsData[index];

      const deltaTime = Math.floor((prevStateTo - prevStateFrom) / 5);
      if (
        !usePlotVisibilityStore.getState().visibilityList[index] &&
        usePlotVisibilityStore.getState().previewList[index] &&
        prev.lastTimeRange &&
        Math.abs(prev.lastTimeRange.to - prevStateTo) < deltaTime &&
        Math.abs(prev.lastTimeRange.from - prevStateFrom) < deltaTime
      ) {
        setState((state) => {
          if (state.plotsData[index].scales.x) {
            state.plotsData[index].scales.x = { min: prevStateFrom, max: prevStateTo };
          }
        });
        return;
      }
      if (lastPlotParams && lastPlotParams.metricName === '' && lastPlotParams.promQL === '') {
        return;
      }
      const changeMetric = !dequal(
        { metricName: lastPlotParams.metricName },
        { metricName: prev.lastPlotParams?.metricName }
      );
      if (!changeMetric && prev.error403) {
        return;
      }

      const resetCache = !dequal(lastPlotParams, prev.lastPlotParams);
      if (
        width &&
        lastPlotParams &&
        (resetCache ||
          getState().timeRange !== prev.lastTimeRange ||
          prevStateTimeShifts !== prev.lastTimeShifts ||
          (lastPlotParams.promQL && prevStateVariables.some(({ name }) => lastPlotParams.promQL.indexOf(name) > -1)) ||
          force)
      ) {
        const agg =
          lastPlotParams.customAgg === -1
            ? `${Math.floor(width / 4)}`
            : lastPlotParams.customAgg === 0
            ? `${Math.floor(width * devicePixelRatio)}`
            : `${lastPlotParams.customAgg}s`;
        if (!getState().metricsMeta[lastPlotParams.metricName]) {
          getState().loadMetricsMeta(lastPlotParams.metricName);
        }
        getState().setNumQueriesPlot(index, (n) => n + 1);
        const controller = new AbortController();
        const isPromQl = lastPlotParams.metricName === promQLMetric;

        const promQLForm = new FormData();
        promQLForm.append('q', lastPlotParams.promQL);
        const priority =
          index === getState().params.tabNum ? 1 : usePlotVisibilityStore.getState().visibilityList[index] ? 2 : 3;
        const url = queryURL(
          lastPlotParams,
          getState().timeRange,
          getState().params.timeShifts,
          agg,
          !compact,
          getState().params,
          priority
        );
        setState((state) => {
          state.plotsDataAbortController[index]?.abort();
          state.plotsDataAbortController[index] = controller;
          const scales: UPlotWrapperPropsScales = {};
          scales.x = { min: getState().timeRange.from, max: getState().timeRange.to };
          if (lastPlotParams.yLock.min !== 0 || lastPlotParams.yLock.max !== 0) {
            scales.y = { ...lastPlotParams.yLock };
          }
          state.plotsData[index].scales = scales;
          if (changeMetric || (isPromQl && !lastPlotParams.promQL)) {
            state.plotsData[index] = getEmptyPlotData();
            clearPlotPreview(index);
            removePlotHeals(index.toString());
          }
        });
        if ((isPromQl && !lastPlotParams.promQL) || skipRequestPlot(index.toString())) {
          getState().setNumQueriesPlot(index, (n) => n - 1);
          return;
        }
        debug.log(
          '%crequesting data for %s %s %d %o %O %o %o %d',
          'color:green',
          lastPlotParams.useV2,
          lastPlotParams.metricName,
          agg,
          lastPlotParams.what,
          lastPlotParams.groupBy,
          lastPlotParams.filterIn,
          lastPlotParams.filterNotIn,
          Math.round(-getState().timeRange.relativeFrom),
          lastPlotParams.maxHost
        );
        (isPromQl
          ? apiPost<queryResult>(url, promQLForm, controller.signal, true)
          : apiGet<queryResult>(url, controller.signal, true)
        )
          .then((resp) => {
            const promqltestfailed = !!resp?.promqltestfailed;
            const uniqueWhat: Set<QueryWhat> = new Set();
            const uniqueName = new Set();
            const uniqueMetricType: Set<string> = new Set();
            let series_meta = [...resp?.series.series_meta];
            let series_data = [...resp.series.series_data] as (number | null)[][];
            const totalLineId = lastPlotParams.totalLine ? series_meta.length : null;
            const totalLineLabel = 'Total';
            if (lastPlotParams.totalLine) {
              const totalLineData = resp.series.time.map((time, idx) =>
                series_data.reduce((res, d) => res + (d[idx] ?? 0), 0)
              );
              series_meta.push({
                name: totalLineLabel,
                time_shift: 0,
                tags: { '0': { value: totalLineLabel } },
                max_hosts: null,
                what: QUERY_WHAT.sum,
                total: 0,
                color: '#333333',
              });
              series_data.push(totalLineData);
            }
            if (lastPlotParams.type === PLOT_TYPE.Event) {
              series_meta = [];
              series_data = [];
              const colorIndex = new Map<string, number>();
              resp?.series.series_meta.forEach((series, indexSeries) => {
                const indexColor = colorIndex.get(series.color);
                if (series.color && indexColor != null) {
                  resp.series.series_data[indexSeries].forEach((value, indexValue) => {
                    if (value != null) {
                      series_data[indexColor][indexValue] = (series_data[indexColor][indexValue] ?? 0) + value;
                    }
                  });
                } else {
                  const index = series_meta.push(series) - 1;
                  series_data.push([...(resp.series.series_data[indexSeries] as (number | null)[])]);
                  colorIndex.set(series.color, index);
                }
              });
            }

            for (const meta of series_meta) {
              if (isQueryWhat(meta.what)) {
                uniqueWhat.add(meta.what);
              }
              meta.name && uniqueName.add(meta.name);
              if (meta.metric_type) {
                uniqueMetricType.add(meta.metric_type);
              }
            }
            const currentPrevState = getState();

            const { lastPlotParams: currentPrevLastPlotParams, seriesShow: currentPrevSeriesShow } =
              getState().plotsData[index];
            const currentPrevSeries = getState().plotsData[index].series.map((s) => ({ ...s, values: undefined }));
            if (uniqueName.size === 0 && lastPlotParams.metricName !== promQLMetric) {
              uniqueName.add(lastPlotParams.metricName);
            }
            const metricType = uniqueMetricType.size === 1 ? [...uniqueMetricType.keys()][0] : '';

            const maxLabelLength = Math.max(
              'Time'.length,
              ...series_meta.map((meta) => {
                const label = metaToLabel(meta, uniqueWhat.size);
                return label.length;
              })
            );
            const legendNameWidth = (series_meta.length ?? 0) > 5 ? maxLabelLength * pxPerChar : 1_000_000;
            let legendMaxHostWidth = 0;
            const legendMaxHostPercentWidth = 0;
            const data: uPlot.AlignedData = [resp.series.time as number[], ...series_data];

            const stacked = lastPlotParams.type === PLOT_TYPE.Event ? stackData(data) : undefined;
            const usedDashes = {};
            const usedBaseColors = {};
            const baseColors: Record<string, string> = {};
            let changeColor = false;
            let changeType = currentPrevLastPlotParams?.type !== lastPlotParams.type;
            const changeView =
              currentPrevLastPlotParams?.totalLine !== lastPlotParams.totalLine ||
              currentPrevLastPlotParams?.filledGraph !== lastPlotParams.filledGraph;
            const widthLine =
              (width ?? 0) > resp.series.time.length
                ? devicePixelRatio > 1
                  ? 2 / devicePixelRatio
                  : 1
                : 1 / devicePixelRatio;

            const topInfoCounts: Record<string, number> = {};
            const topInfoTotals: Record<string, number> = {};
            let topInfo: TopInfo | undefined = undefined;
            const maxHostLists: SelectOptionProps[][] = new Array(series_meta.length).fill([]);
            const oneGraph = series_meta.filter((s) => s.time_shift === 0).length <= 1;
            const seriesShow: boolean[] = new Array(series_meta.length).fill(true);
            const seriesTimeShift: number[] = [];
            const series: uPlot.Series[] = series_meta.map((meta, indexMeta): uPlot.Series => {
              const timeShift = meta.time_shift !== 0;
              seriesTimeShift[indexMeta] = meta.time_shift;
              const label = totalLineId !== indexMeta ? metaToLabel(meta, uniqueWhat.size) : totalLineLabel;
              const baseLabel = totalLineId !== indexMeta ? metaToBaseLabel(meta, uniqueWhat.size) : totalLineLabel;
              const isValue = baseLabel.indexOf('Value') === 0;
              const prefColor = '9'; // it`s magic prefix
              const metricName = isValue
                ? `${meta.name || (lastPlotParams.metricName !== promQLMetric ? lastPlotParams.metricName : '')}: `
                : '';
              const colorKey = `${prefColor}${metricName}${oneGraph ? label : baseLabel}`;
              const baseColor = meta.color ?? baseColors[colorKey] ?? selectColor(colorKey, usedBaseColors);
              baseColors[colorKey] = baseColor;
              if (baseColor !== getState().plotsData[index]?.series[indexMeta]?.stroke) {
                changeColor = true;
              }
              if (meta.max_hosts) {
                const max_hosts_l = meta.max_hosts
                  .map((host) => host.length * pxPerChar * 1.25 + 65)
                  .filter(Boolean)
                  .sort();
                const full = max_hosts_l[0] ?? 0;
                const p75 = max_hosts_l[Math.floor(max_hosts_l.length * 0.25)] ?? 0;
                legendMaxHostWidth = Math.max(legendMaxHostWidth, full - p75 > 20 ? p75 : full);
              }
              const max_host_map =
                meta.max_hosts?.reduce(
                  (res, host) => {
                    if (host) {
                      res[host] = (res[host] ?? 0) + 1;
                    }
                    return res;
                  },
                  {} as Record<string, number>
                ) ?? {};
              const max_host_total = meta.max_hosts?.filter(Boolean).length ?? 1;
              seriesShow[indexMeta] =
                currentPrevSeries[indexMeta]?.label === label ? currentPrevSeriesShow[indexMeta] : true;
              maxHostLists[indexMeta] = Object.entries(max_host_map)
                .sort(([k, a], [n, b]) => (a > b ? -1 : a < b ? 1 : k > n ? 1 : k < n ? -1 : 0))
                .map(([host, count]) => {
                  const percent = formatPercent(count / max_host_total);
                  return {
                    value: host,
                    title: `${host}: ${percent}`,
                    name: `${host}: ${percent}`,
                    html: `<div class="d-flex"><div class="flex-grow-1 me-2 overflow-hidden text-nowrap">${host}</div><div class="text-end">${percent}</div></div>`,
                  };
                });
              if (totalLineId !== indexMeta) {
                const key = `${meta.what}|${meta.time_shift}`;
                topInfoCounts[key] = (topInfoCounts[key] ?? 0) + 1;
                topInfoTotals[key] = meta.total;
              }
              const paths =
                lastPlotParams.type === PLOT_TYPE.Event
                  ? uPlot.paths.bars!({ size: [0.7], gap: 0, align: 1 })
                  : uPlot.paths.stepped!({
                      align: 1,
                    });
              return {
                show: seriesShow[indexMeta] ?? true,
                auto: false, // we control the scaling manually
                label,
                stroke: baseColor,
                width: widthLine,
                dash: timeShift ? timeShiftToDash(meta.time_shift, usedDashes) : undefined,
                fill:
                  totalLineId !== indexMeta && lastPlotParams.filledGraph
                    ? rgba(baseColor, timeShift ? 0.1 : 0.15)
                    : undefined,
                points:
                  lastPlotParams.type === PLOT_TYPE.Event
                    ? { show: false, size: 0 }
                    : {
                        filter: filterPoints,
                        size: 5,
                      },
                paths,
                values(u, seriesIdx, idx): PlotValues {
                  if (idx === null) {
                    return {
                      metricName: '',
                      rawValue: null,
                      value: '',
                      label: '',
                      baseLabel: '',
                      timeShift: 0,
                      max_host: '',
                      total: 0,
                      percent: '',
                      max_host_percent: '',
                      top_max_host: '',
                      top_max_host_percent: '',
                    };
                  }
                  const localData = (stacked ? getState().plotsData[index]?.data : u.data) ?? [];
                  const rawValue = localData[seriesIdx]?.[idx] ?? null;
                  let total = 0;
                  for (let i = 1; i < u.series.length; i++) {
                    const v = localData[i]?.[idx];
                    if (v !== null && v !== undefined && i - 1 !== totalLineId) {
                      total += v;
                    }
                  }
                  const value = formatLegendValue(rawValue);
                  const max_host = meta.max_hosts !== null && idx < meta.max_hosts.length ? meta.max_hosts[idx] : '';

                  const max_host_percent =
                    meta.max_hosts !== null && max_host_map && meta.max_hosts[idx]
                      ? formatPercent((max_host_map[meta.max_hosts[idx]] ?? 0) / max_host_total)
                      : '';
                  const percent = rawValue !== null ? formatPercent(rawValue / total) : '';
                  return {
                    metricName,
                    rawValue,
                    value,
                    label,
                    baseLabel,
                    timeShift: meta.time_shift,
                    max_host,
                    total,
                    percent: totalLineId !== indexMeta ? percent : '100%',
                    max_host_percent,
                    top_max_host: maxHostLists[indexMeta]?.[0]?.value ?? '',
                    top_max_host_percent: maxHostLists[indexMeta]?.[0]?.title ?? '',
                  };
                },
              };
            });

            const topInfoTop = {
              min: Math.min(...Object.values(topInfoCounts)),
              max: Math.max(...Object.values(topInfoCounts)),
            };
            const topInfoTotal = {
              min: Math.min(...Object.values(topInfoTotals)),
              max: Math.max(...Object.values(topInfoTotals)),
            };
            const topInfoFunc = lastPlotParams.what.length;
            const topInfoShifts = currentPrevState.params.timeShifts.length;
            const info: string[] = [];

            if (topInfoTop.min !== topInfoTotal.min && topInfoTop.max !== topInfoTotal.max) {
              if (topInfoFunc > 1) {
                info.push(`${topInfoFunc} functions`);
              }
              if (topInfoShifts > 0) {
                info.push(`${topInfoShifts} time-shift${topInfoShifts > 1 ? 's' : ''}`);
              }
              topInfo = {
                top:
                  topInfoTop.max === topInfoTop.min ? topInfoTop.max.toString() : `${topInfoTop.min}-${topInfoTop.max}`,
                total:
                  topInfoTotal.max === topInfoTotal.min
                    ? topInfoTotal.max.toString()
                    : `${topInfoTotal.min}-${topInfoTotal.max}`,
                info: info.length ? ` (${info.join(',')})` : '',
              };
            }

            const scales: UPlotWrapperPropsScales = {};
            scales.x = { min: getState().timeRange.from, max: getState().timeRange.to };
            if (lastPlotParams.yLock.min !== 0 || lastPlotParams.yLock.max !== 0) {
              scales.y = { ...lastPlotParams.yLock };
            } else {
              scales.y = { min: 0, max: 0 };
            }

            const maxLengthValue = series.reduce(
              (res, s, indexSeries) => {
                if (s.show) {
                  const v =
                    (data[indexSeries + 1] as (number | null)[] | undefined)?.reduce(
                      (res2, d) => {
                        if (d && (res2?.toString().length ?? 0) < d.toString().length) {
                          return d;
                        }
                        return res2;
                      },
                      null as null | number
                    ) ?? null;
                  if (v && (v.toString().length ?? 0) > (res?.toString().length ?? 0)) {
                    return v;
                  }
                }
                return res;
              },
              null as null | number
            );

            const [yMinAll, yMaxAll] = calcYRange2(series, data, false);
            const legendExampleValue = Math.max(
              Math.abs(Math.floor(yMinAll) - 0.001),
              Math.abs(Math.ceil(yMaxAll) + 0.001)
            );
            const legendValueWidth = (formatLegendValue(legendExampleValue).length + 2) * pxPerChar; // +2 - focus marker

            const legendMaxDotSpaceWidth =
              Math.max(4, (formatLegendValue(maxLengthValue).split('.', 2)[1]?.length ?? 0) + 2) * pxPerChar;
            const legendPercentWidth = (4 + 2) * pxPerChar; // +2 - focus marker
            setState((state) => {
              delete state.plotsDataAbortController[index];
              const noUpdateData = dequal(
                stacked?.data || data,
                state.plotsData[index]?.stacked || state.plotsData[index]?.data
              );
              if (resp.metric != null && !dequal(state.metricsMeta[resp.metric.name], resp.metric)) {
                state.metricsMeta[resp.metric.name] = resp.metric;
              }
              const whats = uniqueName.size === 1 ? [...uniqueWhat.keys()] : [];
              state.plotsData[index] = {
                nameMetric: uniqueName.size === 1 ? ([...uniqueName.keys()][0] as string) : '',
                whats: dequal(whats, state.plotsData[index]?.whats) ? state.plotsData[index]?.whats : whats,
                metricType,
                error: usePlotHealsStore.getState().status[index]?.status ? '' : state.plotsData[index]?.error,
                errorSkipCount: 0,
                data: noUpdateData ? state.plotsData[index]?.data : data,
                stacked: noUpdateData ? state.plotsData[index]?.stacked : stacked?.data,
                bands: dequal(state.plotsData[index]?.bands, stacked?.bands)
                  ? state.plotsData[index]?.bands
                  : stacked?.bands,
                series:
                  dequal(resp.series.series_meta, state.plotsData[index]?.lastQuerySeriesMeta) &&
                  !changeColor &&
                  !changeType &&
                  !changeView
                    ? state.plotsData[index]?.series
                    : series,
                seriesTimeShift: dequal(seriesTimeShift, state.plotsData[index].seriesTimeShift)
                  ? state.plotsData[index].seriesTimeShift
                  : seriesTimeShift,
                seriesShow: dequal(seriesShow, state.plotsData[index]?.seriesShow)
                  ? state.plotsData[index]?.seriesShow
                  : seriesShow,
                scales: dequal(scales, state.plotsData[index]?.scales) ? state.plotsData[index]?.scales : scales,
                receiveErrors: resp.receive_errors,
                receiveWarnings: resp.receive_warnings,
                samplingFactorSrc: resp.sampling_factor_src,
                samplingFactorAgg: resp.sampling_factor_agg,
                mappingFloodEvents: resp.mapping_errors,
                legendValueWidth,
                legendMaxDotSpaceWidth,
                legendNameWidth,
                legendPercentWidth,
                legendMaxHostWidth,
                legendMaxHostPercentWidth,
                lastPlotParams,
                lastQuerySeriesMeta: [...resp.series.series_meta],
                lastTimeRange: getState().timeRange,
                lastTimeShifts: getState().params.timeShifts,
                topInfo,
                maxHostLists,
                promqltestfailed,
                promQL: resp.promql ?? '',
              };
            });
            addStatus(index.toString(), true);
          })
          .catch((error) => {
            if (!getState().metricsMeta[lastPlotParams.metricName]) {
              getState().loadMetricsMeta(lastPlotParams.metricName);
            }
            if (error instanceof Error403) {
              setState((state) => {
                state.plotsData[index] = {
                  ...getEmptyPlotData(),
                  error403: error.toString(),
                };
                addStatus(index.toString(), false);
              });
            } else if (error.name !== 'AbortError') {
              debug.error(error);
              setState((state) => {
                if (resetCache) {
                  state.plotsData[index] = {
                    ...getEmptyPlotData(),
                    error: error.toString(),
                  };
                } else {
                  state.plotsData[index].error = error.toString();
                }

                addStatus(index.toString(), false);
              });
            }
            clearPlotPreview(index);
          })
          .finally(() => {
            getState().setNumQueriesPlot(index, (n) => n - 1);
            getState().updateTitle();
          });

        if (lastPlotParams.type === PLOT_TYPE.Event) {
          const { timeRange, params } = getState();
          const from =
            timeRange.from < params.eventFrom && timeRange.to > params.eventFrom ? params.eventFrom : undefined;
          getState()
            .loadEvents(index, undefined, undefined, from)
            .catch(() => undefined);
        }
      }
    },
    setPlotShow(indexPlot, idx, show, single) {
      setState((state) => {
        if (single) {
          const otherShow = state.plotsData[indexPlot].seriesShow.some((_show, indexSeries) =>
            indexSeries === idx ? false : _show
          );
          state.plotsData[indexPlot].seriesShow = state.plotsData[indexPlot].seriesShow.map((s, indexSeries) =>
            indexSeries === idx ? true : !otherShow
          );
        } else {
          state.plotsData[indexPlot].seriesShow[idx] = show ?? !state.plotsData[indexPlot].seriesShow[idx];
        }
      });
    },
    setPlotLastError(index, error) {
      setState((state) => {
        if (state.plotsData[index]) {
          state.plotsData[index].error = error;
        }
      });
    },
    uPlotsWidth: [],
    setUPlotWidth(index, weight) {
      if (getState().uPlotsWidth[index] !== weight) {
        setState((state) => {
          state.uPlotsWidth[index] = weight;
        });
        getState().loadPlot(index);
      }
    },
    setYLockChange(index, status) {
      const prevYLock = getState().params.plots[index].yLock;
      const prevPlotData = getState().plotsData[index];
      const prevStatus = prevYLock.max !== 0 || prevYLock.min !== 0;
      if (prevStatus !== status) {
        let next = { min: 0, max: 0 };
        if (status) {
          const [min, max] = calcYRange2(prevPlotData.series, prevPlotData.data, true);
          next = { min, max };
        }
        getState().setPlotParams(
          index,
          produce((state) => {
            state.yLock = next;
          })
        );
      }
    },
    metricsMeta: {},
    async loadMetricsMeta(metricName) {
      if (!metricName || metricName === promQLMetric) {
        return;
      }
      const prevState = getState();
      if (prevState.metricsMeta[metricName] && prevState.metricsMeta[metricName].name) {
        return;
      }
      const requestKey = `loadMetricsMeta_${metricName}`;
      const [request, first] = promiseRun(
        requestKey,
        apiMetricFetch,
        { [GET_PARAMS.metricName]: metricName },
        requestKey
      );
      prevState.setGlobalNumQueriesPlot((n) => n + 1);
      const { response, error, status } = await request;
      prevState.setGlobalNumQueriesPlot((n) => n - 1);
      if (!first) {
        // if request already then await and skip
        return;
      }
      if (response) {
        debug.log('loading meta for', response.data.metric.name);
        setState((state) => {
          state.metricsMeta[response.data.metric.name] = response.data.metric;
        });
      }
      if (error) {
        if (status !== 403) {
          useErrorStore.getState().addError(error);
        }
      }
      return;
    },
    clearMetricsMeta(metricName) {
      if (getState().metricsMeta[metricName]) {
        setState((state) => {
          delete state.metricsMeta[metricName];
        });
      }
    },
    compact: false,
    setCompact(compact) {
      setState((state) => {
        state.compact = compact;
      });
    },
    setPlotParamsTag(indexPlot, tagKey, nextState, nextPositive) {
      const prevState = getState();
      const prev = prevState.params.plots[indexPlot];
      const next = sortEntity(
        getNextState([...(prev.filterNotIn[tagKey] ?? []), ...(prev.filterIn[tagKey] ?? [])], nextState)
      );
      const positive = getNextState(!prev.filterNotIn[tagKey]?.length, nextPositive);
      prevState.setParams(
        produce((params) => {
          const nonEmpty = positive ? 'filterIn' : 'filterNotIn';
          const empty = positive ? 'filterNotIn' : 'filterIn';

          if (next.length) {
            params.plots[indexPlot][nonEmpty][tagKey] = next;
          } else {
            delete params.plots[indexPlot][nonEmpty][tagKey];
          }
          delete params.plots[indexPlot][empty][tagKey];
        })
      );
    },
    setPlotParamsTagGroupBy(indexPlot, tagKey, nextState) {
      const prevState = getState();
      const prev = prevState.params.plots[indexPlot];
      const next = getNextState(prev.groupBy.includes(tagKey), nextState);
      getState().setParams(
        produce((params) => {
          const nextGroupBy = next
            ? sortEntity([...params.plots[indexPlot].groupBy, tagKey])
            : params.plots[indexPlot].groupBy.filter((t) => t !== tagKey);
          if (!dequal(params.plots[indexPlot].groupBy, nextGroupBy)) {
            params.plots[indexPlot].groupBy = nextGroupBy;
          }
        })
      );
    },
    setPlotType(indexPlot, nextState) {
      const prev = getState();
      const prevPlot = getState().params.plots[indexPlot];
      const nextType = getNextState(prevPlot.type, nextState);
      const meta = prev.metricsMeta[prevPlot.metricName];
      if (prevPlot.type !== nextType && prevPlot.metricName !== promQLMetric) {
        prev.setParams(
          produce((params) => {
            params.plots[indexPlot].type = nextType;
            params.plots[indexPlot].eventsHide = [];
            switch (params.plots[indexPlot].type) {
              case PLOT_TYPE.Metric:
                params.plots[indexPlot].customAgg = 0;
                params.plots[indexPlot].eventsBy = [];
                params.plots[indexPlot].what = [QUERY_WHAT.countNorm];
                params.plots[indexPlot].numSeries = 5;
                break;
              case PLOT_TYPE.Event:
                params.plots[indexPlot].what = [QUERY_WHAT.count];
                params.plots[indexPlot].numSeries = 0;
                params.plots[indexPlot].customAgg = -1;

                const eventsBy = [
                  ...((meta &&
                    meta.tags?.reduce((res, tag, index) => {
                      if (tag.description !== '-') {
                        res.push(index.toString());
                      }
                      return res;
                    }, [] as string[])) ??
                    []),
                  ...(meta?.string_top_name || meta?.string_top_description ? [TAG_KEY._s] : []),
                ];
                params.plots[indexPlot].eventsBy = eventsBy;

                break;
            }
            if (params.plots[indexPlot].type === PLOT_TYPE.Metric) {
              params.plots = params.plots.map((plot, indexP, plotList) => {
                const eventsFilter = plot.events.filter((eventPlot) => plotList[eventPlot]?.type === PLOT_TYPE.Event);
                return {
                  ...plot,
                  events: eventsFilter,
                };
              });
            }
            const timeShiftsSet = getTimeShifts(params.plots[indexPlot].customAgg);
            const shifts = params.timeShifts.filter(
              (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
            );
            if (!dequal(params.timeShifts, shifts)) {
              params.timeShifts = shifts;
            }
          })
        );
      }
    },
    metricsListAbortController: undefined,
    loadServerParams(id, v) {
      return new Promise((resolve) => {
        const paramsLD = readJSONLD<QueryParams>('QueryParams');
        if (paramsLD?.dashboard?.dashboard_id && paramsLD.dashboard.dashboard_id === id) {
          resolve(paramsLD);
          return;
        }
        const cache = getState().saveDashboardParams;
        if (cache?.dashboard?.dashboard_id === id) {
          resolve(deepClone(cache));
          return;
        }
        getState().setSaveDashboardParams(undefined);
        const url = dashboardURL(id, v);
        getState().serverParamsAbortController?.abort();
        const controller = new AbortController();
        setState((state) => {
          state.serverParamsAbortController = controller;
        });
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiGet<DashboardInfo>(url, controller.signal, true)
          .then((data) => {
            if (data) {
              const p = normalizeDashboard(data);
              getState().setSaveDashboardParams(p);
              resolve(deepClone(p));
            }
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
          });
      });
    },
    saveServerParams() {
      return new Promise((resolve, reject) => {
        const to = getState().params.timeRange.to;
        const paramsData: Record<string, unknown> = {
          ...getState().params,
          live: getDefaultParams().live,
          theme: getDefaultParams().theme,
          tabNum: -1,
          dashboard: {
            name: '',
            description: '',
            ...getState().params.dashboard,
          },
          timeRange: {
            ...getState().params.timeRange,
            to: typeof to === 'number' && to > 0 ? 0 : to,
          },
          variables: getState().params.variables.map((v) => ({
            ...v,
            link: v.link
              .map(([plotKey, tagKey]) => [toNumber(plotKey), toIndexTag(tagKey)])
              .filter(([p, t]) => p != null && t != null),
          })),
        };
        const params: DashboardInfo = {
          dashboard: {
            dashboard_id: getState().params.dashboard?.dashboard_id,
            name: getState().params.dashboard?.name ?? '',
            description: getState().params.dashboard?.description ?? '',
            version: getState().params.dashboard?.version ?? 0,
            data: { ...paramsData, searchParams: encodeParams(getState().params) },
          },
        };
        const controller = new AbortController();
        const url = dashboardURL();
        getState().setSaveDashboardParams(undefined);
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        (params.dashboard.dashboard_id !== undefined
          ? apiPost<DashboardInfo>(url, params, controller.signal, true)
          : apiPut<DashboardInfo>(url, params, controller.signal, true)
        )
          .then((data) => {
            if (data) {
              const nextParams = normalizeDashboard(data);
              getState().setSaveDashboardParams(nextParams);
              getState().setDefaultParams(deepClone(nextParams));
              getState().setParams(deepClone(nextParams));
              resolve(deepClone(nextParams));
            }
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error.toString());
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
          });
      });
    },
    removeServerParams() {
      return new Promise((resolve, reject) => {
        const paramsData: QueryParams = {
          ...getState().params,
          tabNum: -1,
          dashboard: {
            name: '',
            description: '',
            ...getState().params.dashboard,
          },
        };
        const params: DashboardInfo = {
          dashboard: {
            dashboard_id: paramsData.dashboard?.dashboard_id,
            name: paramsData.dashboard?.name ?? '',
            description: paramsData.dashboard?.description ?? '',
            version: paramsData.dashboard?.version ?? 0,
            data: paramsData,
          },
          delete_mark: true,
        };

        if (params.dashboard.dashboard_id === undefined) {
          reject('no dashboard');
          return;
        }

        const controller = new AbortController();
        const url = dashboardURL();

        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiPost<DashboardInfo>(url, params, controller.signal, true)
          .then((data) => {
            if (data) {
              const nextParams = normalizeDashboard(data);
              getState().setParams(nextParams);
              resolve(nextParams);
            }
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error.toString());
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
          });
      });
    },
    saveDashboardParams: undefined,
    setSaveDashboardParams(nextState) {
      setState((state) => {
        state.saveDashboardParams = getNextState(state.saveDashboardParams, nextState);
      });
    },
    moveAndResortPlot(indexSelectPlot, indexTargetPlot, indexGroup) {
      const prevState = getState();
      const {
        store: { plotsData, events, params },
        remapIndexPlot,
      } = moveAndResortPlot(getState(), indexSelectPlot, indexTargetPlot, indexGroup);

      if (remapIndexPlot) {
        resortPlotPreview(remapIndexPlot);
        resortPlotVisibility(remapIndexPlot);
        resortPlotHeals(remapIndexPlot);
      }
      setState((state) => {
        state.plotsData = plotsData;
        state.events = events;
      });
      prevState.setParams(params);
    },
    dashboardLayoutEdit: false,
    setDashboardLayoutEdit(nextStatus: boolean) {
      setState((state) => {
        state.dashboardLayoutEdit = nextStatus;
      });
      if (nextStatus) {
        setLiveMode(false);
      }
      if (!nextStatus && getState().params.tabNum < -1) {
        getState().setTabNum(-1);
      }
    },
    setGroupName(indexGroup, name) {
      getState().setParams(
        produce<QueryParams>((state) => {
          if (state.dashboard) {
            state.dashboard.groupInfo = state.dashboard.groupInfo ?? [];
            if (state.dashboard.groupInfo[indexGroup]) {
              state.dashboard.groupInfo[indexGroup].name = name;
            } else {
              state.dashboard.groupInfo[indexGroup] = { show: true, name, count: 0, size: '2', description: '' };
            }
          }
        })
      );
    },
    setGroupDescription(indexGroup, description) {
      getState().setParams(
        produce<QueryParams>((state) => {
          if (state.dashboard) {
            state.dashboard.groupInfo = state.dashboard.groupInfo ?? [];
            if (state.dashboard.groupInfo[indexGroup]) {
              state.dashboard.groupInfo[indexGroup].description = description;
            } else {
              state.dashboard.groupInfo[indexGroup] = { show: true, name: '', count: 0, size: '2', description };
            }
          }
        })
      );
    },
    setGroupShow(indexGroup, show) {
      const nextShow = getNextState(getState().params.dashboard?.groupInfo?.[indexGroup]?.show ?? true, show);
      getState().setParams(
        produce<QueryParams>((state) => {
          if (state.dashboard) {
            state.dashboard.groupInfo = state.dashboard.groupInfo ?? [];
            if (state.dashboard.groupInfo[indexGroup]) {
              state.dashboard.groupInfo[indexGroup].show = nextShow;
            } else {
              state.dashboard.groupInfo[indexGroup] = {
                show: nextShow,
                name: '',
                count: state.dashboard.groupInfo.length ? 0 : state.plots.length,
                size: '2',
                description: '',
              };
            }
          }
        })
      );
    },
    setGroupSize(indexGroup, size) {
      const nextSize = getNextState(getState().params.dashboard?.groupInfo?.[indexGroup]?.size ?? '2', size);
      getState().setParams(
        produce<QueryParams>((state) => {
          if (state.dashboard) {
            state.dashboard.groupInfo = state.dashboard.groupInfo ?? [];
            if (state.dashboard.groupInfo[indexGroup]) {
              state.dashboard.groupInfo[indexGroup].size = nextSize;
            } else {
              state.dashboard.groupInfo[indexGroup] = {
                show: true,
                name: '',
                count: state.dashboard.groupInfo.length ? 0 : state.plots.length,
                size: nextSize,
                description: '',
              };
            }
          }
        })
      );
    },
    listMetricsGroup: [],
    loadListMetricsGroup() {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = metricsGroupListURL();
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiGet<MetricsGroupInfoList>(url, controller.signal, true)
          .then((data) => {
            setState((state) => {
              state.listMetricsGroup = [...(data?.groups ?? [])];
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().listMetricsGroup);
          });
      });
    },
    saveMetricsGroup(metricsGroup) {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = metricsGroupURL();
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        (typeof metricsGroup.group_id !== 'undefined'
          ? apiPost<MetricsGroupInfo>(url, { group: metricsGroup }, controller.signal, true)
          : apiPut<MetricsGroupInfo>(url, { group: metricsGroup }, controller.signal, true)
        )
          .then((data) => {
            setState((state) => {
              state.selectMetricsGroup = data;
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().selectMetricsGroup);
          });
      });
    },
    removeMetricsGroup(metricsGroup) {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = metricsGroupURL();

        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiPost<MetricsGroupInfo>(url, { group: metricsGroup, delete_mark: true }, controller.signal, true)
          .then((data) => {
            setState((state) => {
              state.selectMetricsGroup = data;
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().selectMetricsGroup);
          });
      });
    },
    selectMetricsGroup: undefined,
    loadMetricsGroup(id) {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = metricsGroupURL(id);
        setState((state) => {
          state.selectMetricsGroup = undefined;
        });
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiGet<MetricsGroupInfo>(url, controller.signal, true)
          .then((data) => {
            setState((state) => {
              state.selectMetricsGroup = data;
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().selectMetricsGroup);
          });
      });
    },
    setSelectMetricsGroup(metricsGroup) {
      setState((state) => {
        state.selectMetricsGroup = metricsGroup;
      });
    },
    promConfig: undefined,
    loadPromConfig() {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = promConfigURL();
        setState((state) => {
          state.selectMetricsGroup = undefined;
        });
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiGet<PromConfigInfo>(url, controller.signal, true)
          .then((data) => {
            setState((state) => {
              state.promConfig = data;
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().promConfig);
          });
      });
    },
    savePromConfig(nextPromConfig) {
      return new Promise((resolve, reject) => {
        const controller = new AbortController();
        const url = promConfigURL();
        setState((state) => {
          state.selectMetricsGroup = undefined;
        });
        getState().setGlobalNumQueriesPlot((s) => s + 1);
        apiPost<PromConfigInfo>(url, nextPromConfig, controller.signal, true)
          .then((data) => {
            setState((state) => {
              state.promConfig = data;
            });
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              useErrorStore.getState().addError(error);
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().promConfig);
          });
      });
    },
    events: [],
    loadEvents(indexPlot, key, fromEnd = false, from) {
      return new Promise((resolve, reject) => {
        const plotKey = toPlotKey(indexPlot);
        if (!getState().events[indexPlot]) {
          setState((state) => {
            state.events[indexPlot] = {
              chunks: [],
              rows: [],
              what: [],
              range: new TimeRange(state.params.timeRange),
            };
          });
        }
        const prevState = getState();
        const prevEvent = prevState.events[indexPlot];
        const prevPlot = prevState.params.plots[indexPlot];
        const prevStateVariables = prevState.params.variables;
        const compact = prevState.compact;
        if (compact || prevPlot.type !== PLOT_TYPE.Event || prevPlot.metricName === promQLMetric) {
          resolve(null);
          return;
        }
        if (plotKey == null) {
          return;
        }
        if (fromEnd) {
          prevEvent.prevAbortController?.abort();
        } else {
          prevEvent.nextAbortController?.abort();
        }
        const controller = new AbortController();
        const range = new TimeRange(prevState.timeRange.getRangeUrl());
        if (from) {
          range.setRange(({ to }) => ({ to, from }));
        }
        const width = prevState.uPlotsWidth[indexPlot] ?? prevState.uPlotsWidth.find((w) => w && w > 0);
        const agg =
          prevPlot.customAgg === -1
            ? `${Math.floor(width / 4)}`
            : prevPlot.customAgg === 0
            ? `${Math.floor(width * devicePixelRatio)}`
            : `${prevPlot.customAgg}s`;

        const lastPlotParams: PlotParams | undefined = replaceVariable(plotKey, prevPlot, prevStateVariables);

        const url = queryTableURL(lastPlotParams, range, agg, key, fromEnd);
        setState((state) => {
          if (fromEnd) {
            state.events[indexPlot].prevAbortController = controller;
          } else {
            state.events[indexPlot].nextAbortController = controller;
          }
        });
        apiGet<queryTable>(url, controller.signal, true)
          .then(async (resp) => {
            await getState().loadMetricsMeta(getState().params.plots[indexPlot].metricName);
            setState((state) => {
              state.events[indexPlot] ??= {
                chunks: [],
                rows: [],
                what: [],
                range: new TimeRange(range.getRangeUrl),
              };
              const prevPlot = state.params.plots[indexPlot];
              const meta = state.metricsMeta[prevPlot.metricName];
              const metricType = getMetricType(prevPlot.what, prevPlot.metricType ?? meta?.metric_type);
              const formatMetric = metricType !== METRIC_TYPE.none && formatByMetricType(metricType);
              const chunk: EventDataChunk = {
                ...resp,
                ...range.getRange(),
                fromEnd,
                rows:
                  resp.rows?.map(
                    (value) =>
                      ({
                        ...value,
                        tags:
                          value.tags &&
                          Object.fromEntries(
                            Object.entries(value.tags).map(([tagKey, tagValue]) => [freeKeyPrefix(tagKey), tagValue])
                          ),
                      }) as queryTableRow
                  ) ?? null,
              };
              if (chunk.more) {
                if (chunk.fromEnd) {
                  chunk.from = chunk.rows?.[0]?.time ?? range.from;
                } else {
                  chunk.to = chunk.rows?.[chunk.rows?.length - 1]?.time ?? range.to;
                }
              }
              if (key) {
                if (fromEnd) {
                  state.events[indexPlot].chunks.unshift(chunk);
                } else {
                  state.events[indexPlot].chunks.push(chunk);
                }
              } else {
                state.events[indexPlot].chunks = [chunk];
              }
              state.events[indexPlot].what = chunk.what;
              state.events[indexPlot].rows = state.events[indexPlot].chunks.flatMap(
                (chunk, idChunk) =>
                  chunk.rows?.map(
                    (row, index): EventDataRow =>
                      ({
                        key: `${chunk.from_row}_${index}`,
                        idChunk,
                        timeString: fmtInputDateTime(new Date(row.time * 1000)),
                        data: row.data,
                        time: row.time,
                        ...Object.fromEntries(
                          state.events[indexPlot].what.map((whatKey, indexWhat) => [
                            whatKey,
                            {
                              value: row.data[indexWhat],
                              formatValue: formatMetric
                                ? formatMetric(row.data[indexWhat])
                                : formatLegendValue(row.data[indexWhat]),
                            },
                          ])
                        ),
                        ...row.tags,
                      }) as EventDataRow
                  ) ?? []
              );

              const first = state.events[indexPlot].chunks[0];
              if ((first?.more && first?.fromEnd) || from) {
                state.events[indexPlot].prevKey = first?.from_row;
              } else {
                state.events[indexPlot].prevKey = undefined;
              }
              const last = state.events[indexPlot].chunks[state.events[indexPlot].chunks.length - 1];
              if (last?.more && !last?.fromEnd) {
                state.events[indexPlot].nextKey = last?.to_row;
              } else {
                state.events[indexPlot].nextKey = undefined;
              }

              state.events[indexPlot].range = new TimeRange({
                from: state.events[indexPlot].chunks[0]?.from ?? range.from,
                to: state.events[indexPlot].chunks[state.events[indexPlot].chunks.length - 1]?.to ?? range.to,
              });
              state.events[indexPlot].error = undefined;
              state.events[indexPlot].error403 = undefined;
            });
            resolve(getState().events[indexPlot]);
          })
          .catch((error) => {
            setState((state) => {
              state.events[indexPlot] = {
                chunks: [],
                rows: [],
                what: [],
                range: new TimeRange(state.params.timeRange),
              };
              if (error instanceof Error403) {
                state.events[indexPlot].error403 = error.toString();
              } else if (error.name !== 'AbortError') {
                debug.error(error);
                state.events[indexPlot].error = error.toString();
              }
            });
            reject();
          })
          .finally(() => {
            setState((state) => {
              if (fromEnd) {
                state.events[indexPlot].prevAbortController = undefined;
              } else {
                state.events[indexPlot].nextAbortController = undefined;
              }
            });
          });
      });
    },
    clearEvents(indexPlot) {
      setState((state) => {
        state.events[indexPlot] = { chunks: [], rows: [], what: [], range: new TimeRange(state.params.timeRange) };
      });
    },
  };
}, 'StatsHouseStore');

export function setValuesVariable(nameVariable: string | undefined, values: string[]) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].values = values;
      }
    })
  );
}

export function setGroupByVariable(nameVariable: string | undefined, value: boolean) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].args.groupBy = value;
      }
    })
  );
}

export function setNegativeVariable(nameVariable: string | undefined, value: boolean) {
  useStore.getState().setParams(
    produce((p) => {
      const i = p.variables.findIndex((v) => v.name === nameVariable);
      if (i > -1) {
        p.variables[i].args.negative = value;
      }
    })
  );
}

export function setVariable(variables: VariableParams[]) {
  useStore.getState().setParams(
    produce((p) => {
      const newVariable = variables.reduce(
        (res, { name }) => {
          res[name] = true;
          return res;
        },
        {} as Record<string, boolean>
      );
      p.variables.forEach((variable) => {
        if (!newVariable[variable.name]) {
          variable.link.forEach(([plotKey, tagKey]) => {
            const iPlot = toNumber(plotKey);
            const iTag = toIndexTag(tagKey);
            if (iPlot != null && iTag != null) {
              if (variable.args.groupBy) {
                p.plots[iPlot].groupBy = [...p.plots[iPlot].groupBy, tagKey];
              } else {
                p.plots[iPlot].groupBy = p.plots[iPlot].groupBy.filter((tag) => tag !== tagKey);
              }
              if (variable.args.negative) {
                p.plots[iPlot].filterNotIn[tagKey] = variable.values;
              } else {
                p.plots[iPlot].filterIn[tagKey] = variable.values;
              }
            }
          });
        }
      });
      const updateParam = paramToVariable({ ...p, variables: variables });
      p.variables = updateParam.variables;
      p.plots = updateParam.plots;
    })
  );
}

export function addDashboardGroup(indexGroup: number) {
  useStore.getState().setParams(
    produce((p) => {
      if (p.dashboard) {
        p.dashboard.groupInfo ??= [];
        if (p.dashboard.groupInfo.length === 0) {
          p.dashboard.groupInfo.push({ name: '', count: p.plots.length, show: true, size: '2', description: '' });
        }
        if (indexGroup >= p.dashboard.groupInfo.length) {
          p.dashboard.groupInfo.push({ name: '', count: 0, show: true, size: '2', description: '' });
        } else {
          p.dashboard.groupInfo.splice(indexGroup, 0, { name: '', count: 0, show: true, size: '2', description: '' });
        }
      }
    })
  );
}

export function removeDashboardGroup(indexGroup: number) {
  useStore.getState().setParams(
    produce((p) => {
      if (p.dashboard) {
        p.dashboard.groupInfo ??= [];
        if (p.dashboard.groupInfo.length === 0) {
          p.dashboard.groupInfo.push({ name: '', count: p.plots.length, show: true, size: '2', description: '' });
        }
        p.dashboard.groupInfo.splice(indexGroup, 1);
      }
    })
  );
}

export function moveAndResortPlot(
  prevState: Store,
  indexSelectPlot?: number | null,
  indexTargetPlot?: number | null,
  indexGroup?: number | null,
  remapIndexPlot?: Record<string, number>
) {
  //normalize group length
  prevState = normalizeGroupCount(prevState);
  if (indexGroup != null && indexGroup < 0) {
    return { store: prevState, remapIndexPlot: remapIndexPlot };
  }
  const groups: number[] =
    prevState.params.dashboard?.groupInfo?.flatMap((g, indexG) => new Array(g.count).fill(indexG)) ?? [];
  if (groups.length !== prevState.params.plots.length) {
    while (groups.length < prevState.params.plots.length) {
      groups.push(Math.max(0, (prevState.params.dashboard?.groupInfo?.length ?? 0) - 1));
    }
  }
  if (indexSelectPlot != null && indexGroup != null) {
    groups[indexSelectPlot] = indexGroup;
  }
  const normalize = prevState.params.plots.map((plot, indexPlot) => ({
    plot,
    plotEventLink: plot.events.map((eId) => prevState.params.plots[eId]),
    group: groups[indexPlot] ?? 0,
    tagSync: prevState.params.tagSync.map((group, indexGroup) => ({ indexGroup, indexTag: group[indexPlot] })),
    plotsData: prevState.plotsData[indexPlot],
    plotsEvent: prevState.events[indexPlot],
    oldIndex: indexPlot,
    remapIndex: remapIndexPlot?.[indexPlot] ?? indexPlot,
  }));
  if (indexSelectPlot != null && indexTargetPlot != null && indexSelectPlot !== indexTargetPlot) {
    const [drop] = normalize.splice(indexSelectPlot, 1);
    if (drop) {
      normalize.splice(indexSelectPlot < indexTargetPlot ? Math.max(0, indexTargetPlot - 1) : indexTargetPlot, 0, drop);
    }
  } else if (indexSelectPlot != null && indexTargetPlot == null) {
    const [drop] = normalize.splice(indexSelectPlot, 1);
    if (drop) {
      normalize.push(drop);
    }
  }
  const resort = normalize.sort(sortByKey.bind(undefined, 'group'));
  const plots = resort.map(({ plot }) => plot);
  const plotsData = resort.map(({ plotsData }) => plotsData);
  const plotsEvent = resort.map(({ plotsEvent }) => plotsEvent);
  const plotEventLink = resort.map(({ plotEventLink }) => plotEventLink.map((eP) => plots.indexOf(eP)));
  const localRemapIndexPlot = resort.reduce(
    (res, { oldIndex }, newIndex) => {
      res[oldIndex] = newIndex;
      return res;
    },
    {} as Record<string, number>
  );
  const globalRemapIndexPlot = resort.reduce(
    (res, { remapIndex }, newIndex) => {
      res[remapIndex] = newIndex;
      return res;
    },
    {} as Record<string, number>
  );
  const variables: VariableParams[] = prevState.params.variables.map((variable) => ({
    ...variable,
    link: variable.link.map(([plotKey, tagKey]) => {
      let indexP = toNumber(plotKey);
      return [toPlotKey(indexP == null ? indexP : localRemapIndexPlot[indexP]) ?? plotKey, tagKey];
    }),
  }));
  const tagSync = resort.reduce(
    (res, item, indexPlot) => {
      item.tagSync.forEach(({ indexGroup, indexTag }) => {
        res[indexGroup] = res[indexGroup] ?? [];
        res[indexGroup][indexPlot] = indexTag;
      });
      return res;
    },
    [] as (number | null)[][]
  );
  // resortPlotPreview(remapIndexPlot);
  // resortPlotVisibility(remapIndexPlot);
  const nextStore = produce(prevState, (state) => {
    state.plotsData = plotsData;
    state.events = plotsEvent;

    state.params.plots = plots.map((p, indexP) => ({
      ...p,
      id: indexP.toString(), // fix fallback
      events: plotEventLink[indexP].filter((i) => i > -1) ?? [],
    }));
    state.params.tagSync = tagSync;
    state.params.variables = variables;
    if (state.params.dashboard && indexGroup != null) {
      state.params.dashboard.groupInfo = state.params.dashboard.groupInfo ?? [];
      state.params.dashboard.groupInfo[indexGroup] = state.params.dashboard.groupInfo[indexGroup] ?? {
        name: '',
        count: 0,
        show: true,
        size: 2,
      };
      for (let i = 0, max = state.params.dashboard.groupInfo.length; i < max; i++) {
        if (!state.params.dashboard.groupInfo[i]) {
          state.params.dashboard.groupInfo[i] = {
            name: '',
            count: 0,
            show: true,
            size: '2',
            description: '',
          };
        }
      }

      state.params.dashboard.groupInfo = state.params.dashboard.groupInfo.map((g, index) => ({
        ...g,
        count:
          groups.reduce((res: number, item) => {
            if (item === index) {
              res = res + 1;
            }
            return res;
          }, 0 as number) ?? 0,
      }));
    }
  });
  return { store: nextStore, remapIndexPlot: globalRemapIndexPlot };
}

export function moveGroup(indexGroup: number, direction: -1 | 1) {
  let store = normalizeGroupCount(useStore.getState());
  const groups = store.params.dashboard?.groupInfo ?? [];
  const group = groups[indexGroup];
  const targetGroup = groups[indexGroup + direction];
  const count = group.count ?? 0;
  const targetCount = targetGroup.count ?? 0;
  if (targetCount === 0) {
    store = produce(store, (p) => {
      if (p.params.dashboard?.groupInfo) {
        const g = p.params.dashboard.groupInfo[indexGroup];
        p.params.dashboard.groupInfo[indexGroup] = {
          ...p.params.dashboard.groupInfo[indexGroup + direction],
          count: 0,
        };
        p.params.dashboard.groupInfo[indexGroup + direction] = g;
      }
    });
    useStore.getState().setParams(store.params);
  } else if (count === 0) {
    store = produce(store, (p) => {
      if (p.params.dashboard?.groupInfo) {
        const g = p.params.dashboard.groupInfo[indexGroup + direction];
        p.params.dashboard.groupInfo[indexGroup + direction] = {
          ...p.params.dashboard.groupInfo[indexGroup],
          count: 0,
        };
        p.params.dashboard.groupInfo[indexGroup] = g;
      }
    });
    useStore.getState().setParams(store.params);
  } else if (group && targetGroup && count && targetCount) {
    let remapIndexPlot: Record<string, number> | undefined = undefined;
    let startIndex = groups.slice(0, indexGroup).reduce((res, { count }) => res + count, 0);
    const targetGroupIndex = Math.max(0, indexGroup + (direction > 0 ? direction * 2 : direction));
    // add new group
    store = produce(store, (p) => {
      if (p.params.dashboard) {
        p.params.dashboard.groupInfo ??= [];
        if (p.params.dashboard.groupInfo.length === 0) {
          p.params.dashboard.groupInfo.push({
            name: '',
            count: p.params.plots.length,
            show: true,
            size: '2',
            description: '',
          });
        }
        if (targetGroupIndex > p.params.dashboard.groupInfo.length) {
          p.params.dashboard.groupInfo.push({ ...group, count: 0 });
        } else {
          p.params.dashboard.groupInfo.splice(targetGroupIndex, 0, { ...group, count: 0 });
        }
      }
    });
    // move plot in new group
    if (direction > 0) {
      for (let i = 0; i < count; i++) {
        let next = moveAndResortPlot(store, startIndex, null, targetGroupIndex, remapIndexPlot);
        store = next.store;
        remapIndexPlot = next.remapIndexPlot;
      }
    } else {
      for (let i = 0; i < count; i++) {
        let next = moveAndResortPlot(store, startIndex + i, null, targetGroupIndex, remapIndexPlot);
        store = next.store;
        remapIndexPlot = next.remapIndexPlot;
      }
    }
    // remove old group
    store = produce(store, (p) => {
      if (p.params.dashboard) {
        p.params.dashboard.groupInfo ??= [];
        if (p.params.dashboard.groupInfo.length === 0) {
          p.params.dashboard.groupInfo.push({
            name: '',
            count: p.params.plots.length,
            show: true,
            size: '2',
            description: '',
          });
        }
        p.params.dashboard.groupInfo.splice(direction > 0 ? indexGroup : indexGroup + 1, 1);
      }
    });
    if (remapIndexPlot) {
      resortPlotPreview(remapIndexPlot);
      resortPlotVisibility(remapIndexPlot);
      resortPlotHeals(remapIndexPlot);
    }
    useStore.setState((state) => {
      state.plotsData = store.plotsData;
      state.events = store.events;
    });
    useStore.getState().setParams(store.params);
    // moveAndResortPlot(store);
  }
}
function normalizeParamsGroupCount(prevState: QueryParams) {
  return produce(prevState, (p) => {
    const count = p.plots.length;
    if (p.dashboard?.groupInfo?.length) {
      let deltaCount = p.dashboard.groupInfo.reduce((res, { count }) => res + count, 0) - count;
      let i = p.dashboard.groupInfo.length - 1;
      if (deltaCount) {
        if (deltaCount < 0) {
          const g = p.dashboard.groupInfo[i];
          g.count -= deltaCount;
        } else {
          while (deltaCount > 0 && i >= 0) {
            const g = p.dashboard.groupInfo[i];
            const c = Math.max(0, g.count - deltaCount);
            deltaCount -= g.count;
            g.count = c;
            i--;
          }
        }
      }
    }
  });
}
function normalizeGroupCount(prevState: Store) {
  return produce(prevState, (p) => {
    p.params = normalizeParamsGroupCount(p.params);
  });
}

if (document.location.pathname === '/view' || document.location.pathname === '/embed') {
  useStore.getState().updateParamsByUrl();
}
