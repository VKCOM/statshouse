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
import { dequal } from 'dequal/lite';
import { changePlotParamForData } from './changePlotParamForData';
import { useLiveModeStore } from '../liveModeStore';

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
  lastHeals: boolean;
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
  loadPlotData(plotKey: PlotKey, force?: boolean): void;
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
      //todo: optimize load data

      // console.log('updatePlotsData');
      // const {
      //   params: { orderPlot, tabNum },
      //   plotVisibilityList,
      //   plotPreviewList,
      // } = getState();
      //
      // if (+tabNum > -1) {
      //   getState().loadMetricMetaByPlotKey(tabNum).then();
      //   getState().loadPlotData(tabNum);
      // }
      // orderPlot
      //   .filter((plotKey) => plotKey !== tabNum && (plotVisibilityList[plotKey] || plotPreviewList[plotKey]))
      //   .forEach((plotKey) => {
      //     getState().loadPlotData(plotKey);
      //   });

      getState().params.orderPlot.forEach((plotKey) => {
        getState().loadPlotData(plotKey);
      });
    },
    loadPlotData(plotKey, force = false) {
      // console.log('loadPlotData', plotKey);
      const plot = getState().params.plots[plotKey];
      const prevPlotData = getState().plotsData[plotKey];
      const prevPlot = getState().plotsData[plotKey]?.lastPlotParams;
      if (!force) {
        const liveSkip = useLiveModeStore.getState().liveMode.status && !!prevPlotData?.numQueries;
        const visible = getState().plotVisibilityList[plotKey] || getState().plotPreviewList[plotKey];
        if (liveSkip || !visible) {
          // console.log('skip', plotKey, { liveSkip, visible });
          return;
        }
      }

      const plotHeal = getState().isPlotHeal(plotKey);
      if (!plotHeal) {
        // console.log('skip heal');
        return;
      }

      const changeMetricName = plot?.metricName !== prevPlot?.metricName;
      if (!changeMetricName && prevPlotData?.error403) {
        // console.log('exit 1', plotKey);
        return;
      }

      if (getState().params.tabNum === plotKey) {
        getState().loadMetricMetaByPlotKey(plotKey).then();
      }
      const changePlotParam = changePlotParamForData(
        getState().params.plots[plotKey],
        getState().plotsData[plotKey]?.lastPlotParams
      );
      const changeTime =
        getState().params.timeRange.urlTo !== getState().plotsData[plotKey]?.lastTimeRange?.urlTo ||
        getState().params.timeRange.from !== getState().plotsData[plotKey]?.lastTimeRange?.from;
      const changeNowTime = getState().params.timeRange.to !== getState().plotsData[plotKey]?.lastTimeRange?.to;
      const changeTimeShifts = dequal(getState().params.timeShifts, getState().plotsData[plotKey]?.lastTimeShifts);
      // console.log('-====-');
      // console.log({ to: getState().params.timeRange.to, to_last: getState().plotsData[plotKey]?.lastTimeRange?.to });
      // console.log({ plotKey, changePlotParam, changeTime, changeNowTime });
      let update = changePlotParam || changeTime || changeNowTime || changeTimeShifts;
      if (update || force) {
        // console.log('loadPlotData run', plotKey);
        // console.log({ update });
        // setState((state) => {
        //   const scales: UPlotWrapperPropsScales = {};
        //   scales.x = { min: state.params.timeRange.to + state.params.timeRange.from, max: state.params.timeRange.to };
        //   if (state.params.plots[plotKey]?.yLock.min !== 0 || state.params.plots[plotKey]?.yLock.max !== 0) {
        //     scales.y = { ...lastPlotParams.yLock };
        //   }
        //   state.plotsData[index].scales = scales;
        // });
        const queryEnd = getState().queryStart(plotKey);
        loadPlotData(plotKey, getState().params)
          .then((updatePlotData) => {
            if (updatePlotData) {
              setState(updatePlotData);
              getState().setPlotHeal(plotKey, !!getState().plotsData[plotKey]?.lastHeals);
              if (!getState().plotsData[plotKey]?.lastHeals && changePlotParam) {
                setState((state) => {
                  state.plotsData[plotKey] = {
                    ...getEmptyPlotData(),
                    error: state.plotsData[plotKey]?.error || '',
                    error403: state.plotsData[plotKey]?.error403,
                    lastHeals: false,
                  };
                });
              }
            }
          })
          .finally(() => {
            queryEnd();
          });
      }

      // getState().params.plots[plotKey]?.events.forEach((iPlot) => {
      //   if (!getState().plotsData[iPlot]?.numQueries) {
      //     getState().loadPlotData(iPlot, true);
      //   }
      // });
      plot?.events.forEach((iPlot) => {
        if (!getState().plotsData[iPlot]?.numQueries) {
          getState().loadPlotData(iPlot, true);
        }
      });
      if (plot?.type === PLOT_TYPE.Event && plotKey === getState().params.tabNum) {
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
      let start = true;
      return () => {
        if (start) {
          start = false;
          setState((state) => {
            if (state.plotsData[plotKey]) {
              state.plotsData[plotKey]!.numQueries--;
            }
          });
        }
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
