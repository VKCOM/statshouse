// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import create from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { defaultTimeRange, SetTimeRangeValue, TIME_RANGE_KEYS_TO, TimeRange } from '../common/TimeRange';
import {
  defaultParams,
  getLiveParams,
  PlotParams,
  QueryParams,
  readDashboardID,
  readParams,
  setLiveParams,
  sortEntity,
  writeDashboard,
  writeParams,
} from '../common/plotQueryParams';
import uPlot from '../view/lib/uPlot/uPlot.esm';
import { dequal } from 'dequal/lite';
import React from 'react';
import { URLSearchParamsInit } from 'react-router-dom';
import produce from 'immer';
import {
  apiGet,
  apiPost,
  apiPut,
  deepClone,
  defaultBaseRange,
  Error403,
  formatLegendValue,
  formatPercent,
  normalizeDashboard,
  notNull,
  now,
  readJSONLD,
  sortByKey,
  timeRangeAbbrev,
  timeRangeAbbrevExpand,
  uniqueArray,
} from '../view/utils';
import { globalSettings, pxPerChar } from '../common/settings';
import { debug } from '../common/debug';
import * as api from '../view/api';
import {
  DashboardInfo,
  dashboardListURL,
  dashboardShortInfo,
  dashboardURL,
  GetDashboardListResp,
  metaToLabel,
  metricMeta,
  metricResult,
  MetricsGroup,
  MetricsGroupInfo,
  MetricsGroupInfoList,
  metricsGroupListURL,
  MetricsGroupShort,
  metricsGroupURL,
  metricsListURL,
  metricTagValueInfo,
  metricTagValuesURL,
  metricURL,
  PromConfigInfo,
  promConfigURL,
  queryResult,
  querySeriesMeta,
  queryURL,
} from '../view/api';
import { calcYRange2 } from '../common/calcYRange';
import { rgba, selectColor } from '../view/palette';
import { filterPoints } from '../common/filterPoints';
import { UPlotWrapperPropsScales } from '../components';

export type PlotStore = {
  error: string;
  error403?: string;
  data: uPlot.AlignedData;
  series: uPlot.Series[];
  seriesShow: boolean[];
  scales: Record<string, { min: number; max: number }>;
  lastPlotParams?: PlotParams;
  lastTimeRange?: TimeRange;
  lastQuerySeriesMeta?: querySeriesMeta[];
  receiveErrors: number;
  samplingFactorSrc: number;
  samplingFactorAgg: number;
  mappingFloodEvents: number;
  legendValueWidth: number;
  legendMaxDotSpaceWidth: number;
  legendNameWidth: number;
  legendPercentWidth: number;
  legendMaxHostWidth: number;
  legendMaxHostPercentWidth: number;
};

export type PlotValues = {
  rawValue: number | null;
  value: string;
  max_host: string;
  total: number;
  percent: string;
  max_host_percent: string;
};

type SetSearchParams = (
  nextInit: URLSearchParamsInit,
  navigateOptions?:
    | {
        replace?: boolean | undefined;
        state?: any;
      }
    | undefined
) => void;

export type Store = {
  defaultParams: QueryParams;
  setDefaultParams(nextState: React.SetStateAction<QueryParams>): void;
  timeRange: TimeRange;
  params: QueryParams;
  liveMode: boolean;
  setLiveMode(nextStatus: React.SetStateAction<boolean>): void;
  updateParamsByUrl(): void;
  updateUrl(replace?: boolean): void;
  setTimeRange(value: SetTimeRangeValue, force?: boolean): void;
  setParams(nextState: React.SetStateAction<QueryParams>, replace?: boolean, force?: boolean): void;
  setPlotParams(index: number, nextState: React.SetStateAction<PlotParams>, replace?: boolean): void;
  removePlot(index: number): void;
  setSearchParams?: SetSearchParams;
  initSetSearchParams(setSearchParams: SetSearchParams): void;
  setTabNum(id: number): void;
  error: string;
  previews: string[];
  setPreviews(index: number, link: React.SetStateAction<string>): void;
  globalNumQueriesPlot: number;
  setGlobalNumQueriesPlot(nextState: React.SetStateAction<number>): void;
  numQueriesPlot: number[];
  setNumQueriesPlot(index: number, nextState: React.SetStateAction<number>): void;
  baseRange: timeRangeAbbrev;
  setBaseRange(nextState: React.SetStateAction<timeRangeAbbrev>): void;
  lastError: string;
  setLastError(nextState: React.SetStateAction<string>): void;
  plotsData: PlotStore[];
  plotsDataAbortController: AbortController[];
  loadPlot(index: number, force?: boolean): void;
  setPlotShow(indexPlot: number, idx: number, show?: boolean, single?: boolean): void;
  setPlotLastError(index: number, error: string): void;
  uPlotsWidth: number[];
  setUPlotWidth(index: number, weight: number): void;
  setYLockChange(index: number, status: boolean): void;
  metricsList: { name: string; value: string }[];
  metricsListAbortController?: AbortController;
  loadMetricsList(): void;
  metricsMeta: Record<string, metricMeta>;
  metricsMetaAbortController: Record<string, AbortController>;
  loadMetricsMeta(metricName: string): void;
  clearMetricsMeta(metricName: string): void;
  compact: boolean;
  setCompact(compact: boolean): void;
  setTagSync(indexGroup: number, indexPlot: number, indexTag: number, status: boolean): void;
  setPlotParamsTag(
    indexPlot: number,
    keyTag: string,
    nextState: React.SetStateAction<string[]>,
    positive: React.SetStateAction<boolean>
  ): void;
  setPlotParamsTagGroupBy(indexPlot: number, keyTag: string, nextState: React.SetStateAction<boolean>): void;
  tagsList: metricTagValueInfo[][][]; // [indexPlot][indexTag]
  tagsListSKey: metricTagValueInfo[][]; // [indexPlot]
  tagsListMore: boolean[][]; // [indexPlot][indexTag]
  tagsListSKeyMore: boolean[]; // [indexPlot]
  tagsListAbortController: (AbortController | null)[][]; // [indexPlot][indexTag]
  tagsListSKeyAbortController: (AbortController | null)[]; //[indexPlot]
  setTagsList(
    indexPlot: number,
    indexTag: number,
    nextState: React.SetStateAction<metricTagValueInfo[]>,
    more?: boolean
  ): void;
  loadTagsList(indexPlot: number, indexTag: number, limit?: number): void;
  preSync(): void;
  serverParamsAbortController?: AbortController;
  loadServerParams(id: number): Promise<QueryParams>;
  saveServerParams(): Promise<QueryParams>;
  removeServerParams(): Promise<QueryParams>;
  saveDashboardParams?: QueryParams;
  setSaveDashboardParams(nextState: React.SetStateAction<QueryParams | undefined>): void;
  listServerDashboard: dashboardShortInfo[];
  listServerDashboardAbortController?: AbortController;
  loadListServerDashboard(): void;
  setDashboardGroup(indexPlot: number, indexGroup: number): void;
  moveAndResortPlot(indexSelectPlot?: number, indexTargetPlot?: number): void;
  dashboardLayoutEdit: boolean;
  setDashboardLayoutEdit(nextStatus: boolean): void;
  setGroupName(indexGroup: number, name: string): void;
  setGroupShow(indexGroup: number, show: React.SetStateAction<boolean>): void;
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
};

export function getNextState<T>(prevState: T, nextState: React.SetStateAction<T>): T {
  return nextState instanceof Function ? nextState(prevState) : nextState;
}

export const useStore = create<Store>()(
  immer((setState, getState) => ({
    defaultParams: { ...defaultParams },
    setDefaultParams(nextState) {
      const nextDefaultParams = getNextState(getState().defaultParams, nextState);
      setState((state) => {
        state.defaultParams = nextDefaultParams;
      });
    },
    timeRange: new TimeRange({ to: TIME_RANGE_KEYS_TO.default, from: 0 }),
    params: {
      timeRange: { to: TIME_RANGE_KEYS_TO.default, from: 0 },
      tagSync: [],
      plots: [],
      tabNum: 0,
    },
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
          }),
          false,
          force
        );
      }
    },
    async updateParamsByUrl() {
      const id = readDashboardID(new URLSearchParams(document.location.search));
      const saveParams = id ? await getState().loadServerParams(id) : undefined;
      getState().setDefaultParams({
        ...defaultParams,
        tabNum: saveParams ? -1 : defaultParams.tabNum,
        timeRange: {
          to:
            saveParams && !(typeof saveParams?.timeRange.to === 'number' && saveParams.timeRange.to > 0)
              ? saveParams.timeRange.to
              : defaultParams.timeRange.to,
          from: saveParams?.timeRange.from ?? defaultParams.timeRange.from,
        },
      });
      const urlParams = readParams(
        getState().params,
        new URLSearchParams(document.location.search),
        getState().defaultParams
      );
      const params = saveParams ?? urlParams;
      if (!params) {
        return;
      }
      let reset = false;
      const nowTime = now();
      if (saveParams) {
        params.tabNum = urlParams.tabNum ?? params.tabNum;
        params.timeRange.to =
          (typeof params.timeRange.to === 'number' && params.timeRange.to > 0) ||
          urlParams.timeRange.to !== getState().defaultParams.timeRange.to
            ? urlParams.timeRange.to
            : params.timeRange.to;
        params.timeRange.from =
          urlParams.timeRange.from !== getState().defaultParams.timeRange.from
            ? urlParams.timeRange.from
            : params.timeRange.from;
        if (params.dashboard?.dashboard_id === getState().params.dashboard?.dashboard_id) {
          params.plots = getState().params.plots;
          params.tagSync = getState().params.tagSync;
          params.dashboard = getState().params.dashboard;
        }
        if (params.tabNum >= 0 && !params.plots[params.tabNum]) {
          params.tabNum = getState().defaultParams.tabNum;
          reset = true;
        }
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
        const np: PlotParams = {
          metricName: globalSettings.default_metric,
          groupBy: [...globalSettings.default_metric_group_by],
          filterIn: { ...globalSettings.default_metric_filter_in },
          what: [...globalSettings.default_metric_what],
          customAgg: 0,
          filterNotIn: { ...globalSettings.default_metric_filter_not_in },
          numSeries: 5,
          timeShifts: [],
          useV2: true,
          yLock: {
            min: 0,
            max: 0,
          },
        };
        params.plots = [np];
        reset = true;
      }

      if (globalSettings.disabled_v1) {
        params.plots = params.plots.map((item) => (item.useV2 ? item : { ...item, useV2: true }));
        reset = true;
      }

      const resetPlot = params.dashboard?.dashboard_id !== getState().params.dashboard?.dashboard_id;
      if (!dequal(params, getState().params)) {
        debug.log(
          'updateParamsByUrl',
          JSON.parse(JSON.stringify(params)),
          JSON.parse(JSON.stringify(getState().params))
        );
        setState((store) => {
          if (
            store.params.timeRange.to !== params.timeRange.to ||
            store.params.timeRange.from !== params.timeRange.from
          ) {
            store.timeRange = new TimeRange(params.timeRange);
          }
          store.params = params;
          if (resetPlot) {
            store.plotsData = [];
            store.previews = [];
          }
        });
        getState().params.plots.forEach((plot, index) => {
          getState().loadPlot(index);
        });
      }
      if (reset) {
        getState().updateUrl(true);
      }
      if (getLiveParams(new URLSearchParams(document.location.search))) {
        getState().setLiveMode(true);
      }
    },
    setParams(nextState, replace?, force?) {
      const prevParams = getState().params;
      const nextParams = getNextState(prevParams, nextState);
      const changed = force || !dequal(nextParams, prevParams);
      const changedTimeRange = force || !dequal(nextParams.timeRange, prevParams.timeRange);
      if (changed) {
        setState((state) => {
          if (changedTimeRange) {
            state.timeRange = new TimeRange(nextParams.timeRange);
          }
          state.params = nextParams;
        });
        getState().params.plots.forEach((plot, index) => {
          getState().loadPlot(index, force);
        });
        if (getState().params.plots.some(({ useV2 }) => !useV2) && getState().liveMode) {
          getState().setLiveMode(false);
        }
        getState().updateUrl(replace);
      }
    },
    setPlotParams(index, nextState, replace?) {
      const prev = getState().params.plots[index];
      const next = getNextState(prev, nextState);
      const changed = !dequal(next, prev);
      if (changed) {
        setState((state) => {
          if (next.metricName !== prev.metricName) {
            state.params.tagSync = state.params.tagSync.map((g) => g.filter((tags, plot) => plot !== index));
          }
          state.params.plots[index] = next;
        });
        getState().loadPlot(index);
        if (!next.useV2 && getState().liveMode) {
          getState().setLiveMode(false);
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
      getState().setParams(
        produce((params) => {
          if (params.plots.length > 1) {
            params.plots.splice(index, 1);
            params.tagSync = params.tagSync.map((g) => g.filter((tags, plot) => plot !== index));
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
      setState((state) => {
        state.tagsList = [];
      });
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
        prevState.liveMode ||
        prevState.timeRange.from > now();

      if (prevState.params.dashboard?.dashboard_id) {
        const live = getLiveParams(new URLSearchParams(document.location.search)); // save live param in url
        prevState.setSearchParams?.(
          writeDashboard(prevState.params, setLiveParams(live, new URLSearchParams()), prevState.defaultParams),
          {
            replace: replace || autoReplace,
          }
        );
        return;
      }
      const p = writeParams(prevState.params, new URLSearchParams(document.location.search), prevState.defaultParams);
      prevState.setSearchParams?.(p, { replace: replace || autoReplace });
    },
    setSearchParams: undefined,
    initSetSearchParams(setSearchParams) {
      setState((s) => {
        s.setSearchParams = setSearchParams;
      });
    },
    setTabNum(id) {
      setState((store) => {
        store.params.tabNum = id;
      });
      getState().updateUrl();
    },
    error: '',
    liveMode: false,
    setLiveMode(nextStatus) {
      setState((state) => {
        const nextState = getNextState(state.liveMode, nextStatus);
        if (state.liveMode !== nextState) {
          state.liveMode = nextState;
          if (!state.liveMode) {
            getState().setSearchParams?.(setLiveParams(state.liveMode, new URLSearchParams(document.location.search)));
          }
        }
      });
    },
    previews: [],
    setPreviews: (index, link) => {
      setState((state) => {
        state.previews[index] = getNextState(state.previews[index], link);
      });
    },
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
    lastError: '',
    setLastError(nextState) {
      setState((state) => {
        state.lastError = getNextState(state.lastError, nextState);
      });
    },
    plotsData: [],
    plotsDataAbortController: [],
    loadPlot(index, force: boolean = false) {
      const oldData = getState().plotsData[index];
      if (!getState().plotsData[index]) {
        setState((state) => {
          state.plotsData[index] = {
            error: '',
            data: [[]],
            series: [],
            seriesShow: [],
            scales: {},
            receiveErrors: 0,
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
            lastQuerySeriesMeta: undefined,
          };
        });
      }
      const prevState = getState();

      // if liveMode and there is a queries then wait request
      if (prevState.numQueriesPlot[index] > 0 && prevState.liveMode) {
        return;
      }

      const width = prevState.uPlotsWidth[index] ?? prevState.uPlotsWidth.find((w) => w && w > 0);
      const compact = prevState.compact;
      const lastPlotParams: PlotParams | undefined = prevState.params.plots[index];
      const prev: PlotStore = prevState.plotsData[index];
      if (
        width &&
        lastPlotParams &&
        (lastPlotParams !== prev.lastPlotParams || prevState.timeRange !== prev.lastTimeRange || force)
      ) {
        const agg =
          lastPlotParams.customAgg === -1
            ? `${Math.floor(width / 2)}`
            : lastPlotParams.customAgg === 0
            ? `${Math.floor(width * devicePixelRatio)}`
            : `${lastPlotParams.customAgg}s`;
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
          Math.round(-prevState.timeRange.relativeFrom)
        );
        prevState.setNumQueriesPlot(index, (n) => n + 1);
        const controller = new AbortController();

        const url = queryURL(lastPlotParams, prevState.timeRange, agg, !compact);
        prevState.plotsDataAbortController[index]?.abort();
        setState((state) => {
          state.plotsDataAbortController[index] = controller;
        });
        apiGet<queryResult>(url, controller.signal, true)
          .then((resp) => {
            const uniqueWhat = new Set();

            for (const meta of resp?.series.series_meta ?? []) {
              uniqueWhat.add(meta.what);
            }
            const maxLabelLength = Math.max(
              'Time'.length,
              ...(resp?.series.series_meta ?? []).map((meta) => {
                const label = metaToLabel(meta, uniqueWhat.size);
                return label.length;
              })
            );
            const legendNameWidth = (resp?.series.series_meta.length ?? 0) > 5 ? maxLabelLength * pxPerChar : 1_000_000;
            let legendMaxHostWidth = 0;
            const legendMaxHostPercentWidth = 0;
            const data: uPlot.AlignedData = [
              resp.series.time as number[],
              ...(resp.series.series_data as (number | null)[][]),
            ];

            const usedColors = {};
            let changeColor = false;
            const widthLine =
              (width ?? 0) > resp.series.time.length
                ? devicePixelRatio > 1
                  ? 2 / devicePixelRatio
                  : 1
                : 1 / devicePixelRatio;

            const seriesShow = new Array(resp.series.series_meta.length).fill(true);
            const series: uPlot.Series[] = resp.series.series_meta.map((meta, indexMeta): uPlot.Series => {
              const label = metaToLabel(meta, uniqueWhat.size);
              const color = selectColor(`${lastPlotParams.metricName}: ${label}`, usedColors);
              if (color !== getState().plotsData[index]?.series[indexMeta]?.stroke) {
                changeColor = true;
              }
              if (meta.max_hosts) {
                const max_hosts_l = meta.max_hosts
                  .map((host) => host.length * pxPerChar)
                  .filter(Boolean)
                  .sort();
                const full = max_hosts_l[0] ?? 0;
                const p75 = max_hosts_l[max_hosts_l.length * 0.25] ?? 0;
                legendMaxHostWidth = Math.max(legendMaxHostWidth, full - p75 > 20 ? p75 : full);
              }
              const max_host_map =
                meta.max_hosts?.reduce((res, host) => {
                  if (host) {
                    res[host] = (res[host] ?? 0) + 1;
                  }
                  return res;
                }, {} as Record<string, number>) ?? {};
              const max_host_total = meta.max_hosts?.filter(Boolean).length ?? 1;
              seriesShow[indexMeta] =
                oldData?.series?.[indexMeta]?.label === label ? oldData?.series?.[indexMeta]?.show : true;
              return {
                show: seriesShow[indexMeta] ?? true,
                auto: false, // we control the scaling manually
                label,
                stroke: color,
                width: widthLine,
                fill: rgba(color, 0.15),
                points: {
                  filter: filterPoints,
                  size: 5,
                },
                paths: uPlot.paths.stepped!({
                  align: 1,
                }),
                values(u, seriesIdx, idx): PlotValues {
                  const rawValue = u.data[seriesIdx]?.[idx] ?? null;
                  let total = 0;
                  for (let i = 1; i < u.series.length; i++) {
                    const v = u.data[i]?.[idx];
                    if (v !== null && v !== undefined) {
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
                  return { rawValue, value, max_host, total, percent, max_host_percent };
                },
              };
            });

            const scales: UPlotWrapperPropsScales = {};
            scales.x = { min: getState().timeRange.from, max: getState().timeRange.to };
            if (lastPlotParams.yLock.min !== 0 || lastPlotParams.yLock.max !== 0) {
              scales.y = { ...lastPlotParams.yLock };
            }

            const maxLengthValue = series.reduce((res, s, indexSeries) => {
              if (s.show) {
                const v =
                  (data[indexSeries + 1] as (number | null)[] | undefined)?.reduce((res2, d) => {
                    if (d && (res2?.toString().length ?? 0) < d.toString().length) {
                      return d;
                    }
                    return res2;
                  }, null as null | number) ?? null;
                if (v && (v.toString().length ?? 0) > (res?.toString().length ?? 0)) {
                  return v;
                }
              }
              return res;
            }, null as null | number);

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
              state.plotsData[index] = {
                error: '',
                data: dequal(data, state.plotsData[index]?.data) ? state.plotsData[index]?.data : data,
                series:
                  dequal(resp.series.series_meta, state.plotsData[index]?.lastQuerySeriesMeta) && !changeColor
                    ? state.plotsData[index]?.series
                    : series,
                seriesShow: dequal(seriesShow, state.plotsData[index]?.seriesShow)
                  ? state.plotsData[index]?.seriesShow
                  : seriesShow,
                scales: dequal(scales, state.plotsData[index]?.scales) ? state.plotsData[index]?.scales : scales,
                receiveErrors: resp.receive_errors_legacy,
                samplingFactorSrc: resp.sampling_factor_src,
                samplingFactorAgg: resp.sampling_factor_agg,
                mappingFloodEvents: resp.mapping_flood_events_legacy,
                legendValueWidth,
                legendMaxDotSpaceWidth,
                legendNameWidth,
                legendPercentWidth,
                legendMaxHostWidth,
                legendMaxHostPercentWidth,
                lastPlotParams,
                lastQuerySeriesMeta: [...resp.series.series_meta],
                lastTimeRange: getState().timeRange,
              };
            });
          })
          .catch((error) => {
            if (error instanceof Error403) {
              setState((state) => {
                state.plotsData[index] = {
                  error: '',
                  error403: error.toString(),
                  data: [[]],
                  series: [],
                  seriesShow: [],
                  scales: {},
                  receiveErrors: 0,
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
                  lastQuerySeriesMeta: undefined,
                };
                delete state.previews[index];
                state.liveMode = false;
              });
            } else if (error.name !== 'AbortError') {
              debug.error(error);
              setState((state) => {
                state.plotsData[index] = {
                  error: error.toString(),
                  data: [[]],
                  series: [],
                  seriesShow: [],
                  scales: {},
                  receiveErrors: 0,
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
                  lastQuerySeriesMeta: undefined,
                };
                delete state.previews[index];
                state.liveMode = false;
              });
            }
          })
          .finally(() => {
            getState().setNumQueriesPlot(index, (n) => n - 1);
          });
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
    metricsList: [],
    loadMetricsList() {
      const prevState = getState();
      prevState.metricsListAbortController?.abort();
      const controller = new AbortController();
      setState((state) => {
        state.metricsListAbortController = controller;
      });
      prevState.setGlobalNumQueriesPlot((n) => n + 1);
      apiGet<api.metricsListResult>(metricsListURL(), controller.signal, true)
        .then(
          (resp) => {
            setState((state) => {
              state.metricsList = resp.metrics.map((m) => ({ name: m.name, value: m.name }));
            });
          },
          (err) => {
            if (err.name !== 'AbortError') {
              debug.error(err);
              setState((state) => {
                state.lastError = err.toString();
              });
            }
          }
        )
        .finally(() => {
          prevState.setGlobalNumQueriesPlot((n) => n - 1);
        });
    },
    metricsMeta: { '': { name: '', metric_id: 0, kind: 'counter', description: '', tags: [] } },
    metricsMetaAbortController: {},
    loadMetricsMeta(metricName) {
      if (!metricName) {
        return;
      }
      const prevState = getState();
      if (
        prevState.metricsMeta[metricName] &&
        (prevState.metricsMeta[metricName].name || prevState.metricsMetaAbortController[metricName])
      ) {
        return;
      }
      prevState.metricsMetaAbortController[metricName]?.abort();
      const controller = new AbortController();
      setState((state) => {
        state.metricsMetaAbortController[metricName] = controller;
        state.metricsMeta[metricName] = { name: '', metric_id: 0, kind: 'counter', description: '', tags: [] };
      });
      prevState.setGlobalNumQueriesPlot((n) => n + 1);
      apiGet<metricResult>(metricURL(metricName), controller.signal, true)
        .then((response) => {
          debug.log('loading meta for', response.metric.name);
          setState((state) => {
            state.lastError = '';
            state.metricsMeta[response.metric.name] = {
              ...response.metric,
              tags: response.metric.tags && [...response.metric.tags],
            };
          });
        })
        .catch((error) => {
          if (error instanceof Error403) {
          } else if (error.name !== 'AbortError') {
            setState((state) => {
              state.lastError = error.toString();
            });
          }
          getState().clearMetricsMeta(metricName);
        })
        .finally(() => {
          prevState.setGlobalNumQueriesPlot((n) => n - 1);
          setState((state) => {
            delete state.metricsMetaAbortController[metricName];
          });
        });
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
    setTagSync(indexGroup, indexPlot, indexTag, status) {
      if (indexGroup >= 0 && indexPlot >= 0 && indexTag >= 0) {
        getState().setParams(
          produce((params) => {
            params.tagSync[indexGroup][indexPlot] = status ? indexTag : null;
          })
        );
      } else if (indexGroup === -1 && indexPlot === -1 && indexTag === -1 && status) {
        getState().setParams(
          produce((params) => {
            params.tagSync.push([]);
          })
        );
      } else if (indexGroup >= 0 && indexPlot === -1 && indexTag === -1 && !status) {
        getState().setParams(
          produce((params) => {
            params.tagSync.splice(indexGroup, 1);
          })
        );
      }
    },
    setPlotParamsTag(indexPlot, keyTag, nextState, nextPositive) {
      const prevState = getState();
      const prev = prevState.params.plots[indexPlot];
      const next = sortEntity(
        getNextState([...(prev.filterNotIn[keyTag] ?? []), ...(prev.filterIn[keyTag] ?? [])], nextState)
      );
      const positive = getNextState(!prev.filterNotIn[keyTag]?.length, nextPositive);
      const indexTag = parseInt(keyTag.match(/\d+/)?.[0] ?? '-1');
      const syncGroups = prevState.params.tagSync.filter((g) => g[indexPlot] === indexTag);
      prevState.setParams(
        produce((params) => {
          const nonEmpty = positive ? 'filterIn' : 'filterNotIn';
          const empty = positive ? 'filterNotIn' : 'filterIn';
          if (syncGroups.length) {
            syncGroups.forEach((g) => {
              g.forEach((tagKeyIndex, syncPlotIndex) => {
                if (tagKeyIndex !== null) {
                  const tagKey = `key${tagKeyIndex}`;
                  if (next.length) {
                    params.plots[syncPlotIndex][nonEmpty][tagKey] = next;
                  } else {
                    delete params.plots[syncPlotIndex][nonEmpty][tagKey];
                  }
                  delete params.plots[syncPlotIndex][empty][tagKey];
                }
              });
            });
          } else {
            if (next.length) {
              params.plots[indexPlot][nonEmpty][keyTag] = next;
            } else {
              delete params.plots[indexPlot][nonEmpty][keyTag];
            }
            delete params.plots[indexPlot][empty][keyTag];
          }
        })
      );
    },
    setPlotParamsTagGroupBy(indexPlot, keyTag, nextState) {
      const prevState = getState();
      const prev = prevState.params.plots[indexPlot];
      const next = getNextState(prev.groupBy.includes(keyTag), nextState);

      const tagIndex = parseInt(keyTag.match(/\d+/)?.[0] ?? '-1');
      const syncGroups = prevState.params.tagSync.filter((g) => g[indexPlot] === tagIndex);
      getState().setParams(
        produce((params) => {
          if (syncGroups.length) {
            syncGroups.forEach((g) => {
              g.forEach((tagKeyIndex, syncPlotIndex) => {
                if (tagKeyIndex !== null) {
                  const tagKey = `key${tagKeyIndex}`;
                  params.plots[syncPlotIndex].groupBy = next
                    ? sortEntity([...params.plots[syncPlotIndex].groupBy, tagKey])
                    : params.plots[syncPlotIndex].groupBy.filter((t) => t !== tagKey);
                }
              });
            });
          } else {
            params.plots[indexPlot].groupBy = next
              ? sortEntity([...params.plots[indexPlot].groupBy, keyTag])
              : params.plots[indexPlot].groupBy.filter((t) => t !== keyTag);
          }
        })
      );
    },
    metricsListAbortController: undefined,
    tagsList: [],
    tagsListSKey: [],
    tagsListMore: [],
    tagsListSKeyMore: [],
    tagsListAbortController: [],
    tagsListSKeyAbortController: [],
    setTagsList(indexPlot, indexTag, nextState, more = false) {
      const prevState = getState();
      const next = getNextState(
        indexPlot === -1 ? prevState.tagsListSKey[indexPlot] : prevState.tagsList[indexPlot]?.[indexTag] ?? [],
        nextState
      );
      setState((state) => {
        if (indexTag === -1) {
          state.tagsListSKey[indexPlot] = next;
          state.tagsListSKeyMore[indexPlot] = more;
        } else {
          if (!state.tagsList[indexPlot]) {
            state.tagsList[indexPlot] = new Array(state.params.plots.length ?? 0).fill([]);
          }
          if (!state.tagsListMore[indexPlot]) {
            state.tagsListMore[indexPlot] = new Array(state.params.plots.length ?? 0).fill(false);
          }
          state.tagsList[indexPlot][indexTag] = next;
          state.tagsListMore[indexPlot][indexTag] = more;
        }
      });
    },
    loadTagsList(indexPlot, indexTag, limit = 1000) {
      const prevState = getState();
      const plot = prevState.params.plots[indexPlot];
      const tag = prevState.metricsMeta[plot.metricName]?.tags?.[indexTag];
      const tagID = indexTag === -1 ? 'skey' : `key${indexTag}`;
      const otherFilterIn = { ...plot.filterIn };
      delete otherFilterIn[tagID];
      const otherFilterNotIn = { ...plot.filterNotIn };
      delete otherFilterNotIn[tagID];
      if (plot.metricName && (tag || indexTag === -1)) {
        if (indexTag === -1) {
          prevState.tagsListSKeyAbortController[indexPlot]?.abort();
        } else {
          prevState.tagsListAbortController[indexPlot]?.[indexTag]?.abort();
        }
        const controller = new AbortController();
        setState((state) => {
          if (indexTag === -1) {
            state.tagsListSKeyAbortController[indexPlot] = controller;
          } else {
            if (!state.tagsListAbortController[indexPlot]) {
              state.tagsListAbortController[indexPlot] = new Array(
                prevState.metricsMeta[plot.metricName]?.tags?.length ?? 0
              ).fill(null);
            }
            state.tagsListAbortController[indexPlot][indexTag] = controller;
          }
        });
        const url = metricTagValuesURL(
          limit,
          globalSettings.disabled_v1 ? true : plot.useV2,
          plot.metricName,
          tagID,
          prevState.timeRange.from,
          prevState.timeRange.to,
          plot.what,
          otherFilterIn,
          otherFilterNotIn
        );
        apiGet<api.metricTagValuesResult>(url, controller.signal, true)
          .then((resp) => {
            getState().setTagsList(indexPlot, indexTag, resp.tag_values.slice(), resp.tag_values_more);
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              debug.error(error);
              getState().setLastError(error.toString());
            }
          })
          .finally(() => {
            setState((state) => {
              if (indexTag === -1) {
                state.tagsListSKeyAbortController[indexPlot] = null;
              } else {
                state.tagsListAbortController[indexPlot][indexTag] = null;
              }
            });
          });
      }
    },
    preSync() {
      const prevState = getState();
      prevState.params.tagSync.forEach((group) => {
        const filterIn = uniqueArray(
          group.flatMap((tag, indexPlot) => prevState.params.plots[indexPlot].filterIn[`key${tag}`] ?? [])
        );
        const filterNotIn = uniqueArray(
          group.flatMap((tag, indexPlot) => prevState.params.plots[indexPlot].filterNotIn[`key${tag}`] ?? [])
        );
        const indexPlot = group.findIndex(notNull);
        const keyTag = `key${group[indexPlot]}`;
        const byGroup = prevState.params.plots.some((plot) => plot.groupBy.indexOf(keyTag) >= 0);

        if (filterIn.length) {
          prevState.setPlotParamsTag(indexPlot, keyTag, filterIn, true);
        } else if (filterNotIn.length) {
          prevState.setPlotParamsTag(indexPlot, keyTag, filterNotIn, false);
        }
        prevState.setPlotParamsTagGroupBy(indexPlot, keyTag, byGroup);
      });
    },
    loadServerParams(id) {
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
        const url = dashboardURL(id);
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
          });
      });
    },
    saveServerParams() {
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
              getState().setParams(deepClone(nextParams));
              resolve(deepClone(nextParams));
            }
          })
          .catch((error) => {
            if (error.name !== 'AbortError') {
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
    listServerDashboard: [],
    listServerDashboardAbortController: undefined,
    loadListServerDashboard() {
      const controller = new AbortController();
      const url = dashboardListURL();
      getState().setGlobalNumQueriesPlot((s) => s + 1);
      apiGet<GetDashboardListResp>(url, controller.signal, true)
        .then((data) => {
          setState((state) => {
            state.listServerDashboard = [...(data?.dashboards ?? [])];
          });
        })
        .catch((error) => {
          if (error.name !== 'AbortError') {
            debug.error(error);
            setState((state) => {
              state.lastError = error.toString();
            });
          }
        })
        .finally(() => {
          getState().setGlobalNumQueriesPlot((s) => s - 1);
        });
    },
    setDashboardGroup(indexPlot, indexGroup) {
      getState().setParams(
        produce((params) => {
          if (params.dashboard?.dashboard_id) {
            params.dashboard.groups = params.dashboard?.groups ?? [];
            params.dashboard.groups[indexPlot] = indexGroup;
          }
        })
      );
    },
    moveAndResortPlot(indexSelectPlot, indexTargetPlot) {
      const prevState = getState();
      const normalize = prevState.params.plots.map((plot, indexPlot) => ({
        plot,
        group: prevState.params.dashboard?.groups?.[indexPlot] ?? 0,
        tagSync: prevState.params.tagSync.map((group, indexGroup) => ({ indexGroup, indexTag: group[indexPlot] })),
        preview: prevState.previews[indexPlot],
        plotsData: prevState.plotsData[indexPlot],
      }));

      if (
        typeof indexSelectPlot !== 'undefined' &&
        typeof indexTargetPlot !== 'undefined' &&
        indexSelectPlot !== indexTargetPlot
      ) {
        const [drop] = normalize.splice(indexSelectPlot, 1);
        normalize.splice(
          indexSelectPlot < indexTargetPlot ? Math.max(0, indexTargetPlot - 1) : indexTargetPlot,
          0,
          drop
        );
      }
      const resort = normalize.sort(sortByKey.bind(undefined, 'group'));
      const plots = resort.map(({ plot }) => plot);
      const groups = resort.map(({ group }) => group);
      const previews = resort.map(({ preview }) => preview);
      const plotsData = resort.map(({ plotsData }) => plotsData);
      const tagSync = resort.reduce((res, item, indexPlot) => {
        item.tagSync.forEach(({ indexGroup, indexTag }) => {
          res[indexGroup] = res[indexGroup] ?? [];
          res[indexGroup][indexPlot] = indexTag;
        });
        return res;
      }, [] as (number | null)[][]);
      prevState.setParams(
        produce((params) => {
          params.plots = plots;
          params.tagSync = tagSync;
          if (params.dashboard?.dashboard_id) {
            params.dashboard.groups = groups;
          }
        })
      );
      setState((state) => {
        state.previews = previews;
        state.plotsData = plotsData;
      });
    },
    dashboardLayoutEdit: false,
    setDashboardLayoutEdit(nextStatus: boolean) {
      setState((state) => {
        state.dashboardLayoutEdit = nextStatus;
      });
    },
    setGroupName(indexGroup, name) {
      setState((state) => {
        if (state.params.dashboard) {
          state.params.dashboard.groupInfo = state.params.dashboard.groupInfo ?? [];
          if (state.params.dashboard.groupInfo[indexGroup]) {
            state.params.dashboard.groupInfo[indexGroup].name = name;
          } else {
            state.params.dashboard.groupInfo[indexGroup] = { show: true, name };
          }
        }
      });
    },
    setGroupShow(indexGroup, show) {
      const nextShow = getNextState(getState().params.dashboard?.groupInfo?.[indexGroup]?.show ?? true, show);
      setState((state) => {
        if (state.params.dashboard) {
          state.params.dashboard.groupInfo = state.params.dashboard.groupInfo ?? [];
          if (state.params.dashboard.groupInfo[indexGroup]) {
            state.params.dashboard.groupInfo[indexGroup].show = nextShow;
          } else {
            state.params.dashboard.groupInfo[indexGroup] = { show: nextShow, name: '' };
          }
        }
      });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
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
              debug.error(error);
              setState((state) => {
                state.lastError = error.toString();
              });
              reject(error);
            }
          })
          .finally(() => {
            getState().setGlobalNumQueriesPlot((s) => s - 1);
            resolve(getState().promConfig);
          });
      });
    },
  }))
);
