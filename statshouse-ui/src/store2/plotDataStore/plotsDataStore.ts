// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';
import { type MetricType, PLOT_TYPE, type QueryWhat } from 'api/enum';
import { type SelectOptionProps, type UPlotWrapperPropsScales } from '../../components';
import { type PlotKey, type PlotParams, type TimeRange } from 'url2';
import { type QuerySeriesMeta } from 'api/query';
import { type StoreSlice } from '../createStore';
import { type StatsHouseStore } from '../statsHouseStore';
import { loadPlotData } from './loadPlotData';
import { getClearPlotsData } from './getClearPlotsData';
import { updateClearPlotError } from './updateClearPlotError';
import { getEmptyPlotData } from './getEmptyPlotData';

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

export type TopInfo = {
  top: string;
  total: string;
  info: string;
};

export type PlotData = {
  data: uPlot.AlignedData;
  dataView: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: uPlot.Series[];
  seriesShow: boolean[];
  scales: UPlotWrapperPropsScales;
  promQL: string;
  metricName: string;
  metricWhat: string;
  whats: QueryWhat[];
  plotAgg: string;
  showMetricName: string;
  metricUnit: MetricType;
  error: string;
  error403?: string;
  errorSkipCount: number;
  seriesTimeShift: number[];
  lastPlotParams?: PlotParams;
  lastTimeRange?: TimeRange;
  lastTimeShifts?: number[];
  lastQuerySeriesMeta?: QuerySeriesMeta[];
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
  promqlExpand: boolean;
  numQueries: number;
};

export type PlotsDataStore = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
  globalNumQueries: number;
  togglePromqlExpand(plotKey: PlotKey, status?: boolean): void;
  updatePlotsData(): void;
  loadPlotData(plotKey: PlotKey): void;
  globalQueryStart(): () => void;
  queryStart(plotKey: PlotKey): () => void;
  clearPlotError(plotKey: PlotKey): void;
  setPlotShow(plotKey: PlotKey, idx: number, show?: boolean, single?: boolean): void;
};
export const plotsDataStore: StoreSlice<StatsHouseStore, PlotsDataStore> = (setState, getState, store) => {
  store.subscribe((state, prevState) => {
    if (state.params !== prevState.params) {
      setState((s) => {
        getClearPlotsData(state, prevState).forEach((plotKey) => {
          delete s.plotsData[plotKey];
        });
      });
    }
  });
  return {
    plotsData: {},
    globalNumQueries: 0,
    togglePromqlExpand(plotKey, status) {
      setState((state) => {
        const pd = state.plotsData[plotKey];
        if (pd) {
          pd.promqlExpand = status ?? !pd.promqlExpand;
        }
      });
    },
    updatePlotsData() {
      getState().viewOrderPlot.forEach((plotKey) => {
        getState().loadPlotData(plotKey);
      });
    },
    loadPlotData(plotKey) {
      const queryEnd = getState().queryStart(plotKey);
      loadPlotData(plotKey, getState().params).then((updatePlotData) => {
        if (updatePlotData) {
          queryEnd();
          setState(updatePlotData);
        }
      });
      const plot = getState().params.plots[plotKey];
      if (plot?.type === PLOT_TYPE.Event) {
        const { params } = getState();
        const from =
          params.timeRange.from + params.timeRange.to < params.eventFrom && params.timeRange.to > params.eventFrom
            ? params.eventFrom
            : undefined;
        getState()
          .loadPlotEvents(plotKey, undefined, undefined, from)
          .catch(() => undefined);
      }
    },
    globalQueryStart() {
      setState((state) => {
        state.globalNumQueries += 1;
      });
      return () => {
        setState((state) => {
          state.globalNumQueries -= 1;
        });
      };
    },
    queryStart(plotKey) {
      setState((state) => {
        state.plotsData[plotKey] ??= getEmptyPlotData();
        state.plotsData[plotKey]!.numQueries++;
      });
      return () => {
        setState((state) => {
          if (state.plotsData[plotKey]) {
            state.plotsData[plotKey]!.numQueries--;
          }
        });
      };
    },
    clearPlotError(plotKey) {
      setState(updateClearPlotError(plotKey));
    },
    setPlotShow(plotKey, idx, show, single) {
      setState((state) => {
        const plotData = state.plotsData[plotKey];
        if (plotData) {
          if (single) {
            const otherShow = plotData.seriesShow.some((_show, indexSeries) => (indexSeries === idx ? false : _show));
            plotData.seriesShow = plotData.seriesShow.map((s, indexSeries) =>
              indexSeries === idx ? true : !otherShow
            );
          } else {
            plotData.seriesShow[idx] = show ?? !plotData.seriesShow[idx];
          }
        }
      });
    },
  };
};
