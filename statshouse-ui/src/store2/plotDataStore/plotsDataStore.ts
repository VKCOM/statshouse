// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';
import { type MetricType, PLOT_TYPE, type QueryWhat } from 'api/enum';
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
import { getPlotLoader, queryStart } from '../plotQueryStore';
import { skipTimeout } from '../../common/helpers';
import { usePlotVisibilityStore } from '../plotVisibilityStore';
import { useVariableChangeStatusStore } from '../variableChangeStatusStore';
import { filterVariableByPlot } from '../helpers/filterVariableByPlot';
import { type SelectOptionProps } from 'components/Select';

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
  // scales: UPlotWrapperPropsScales;
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
  loadBadges?: boolean;
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
};

export type PlotsDataStore = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
  togglePromqlExpand(plotKey: PlotKey, status?: boolean): void;
  updatePlotsData(): void;
  loadPlotData(plotKey: PlotKey, force?: boolean): Promise<void>;
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
    // globalNumQueries: 0,
    togglePromqlExpand(plotKey, status) {
      setState((state) => {
        const pd = state.plotsData[plotKey];
        if (pd) {
          pd.promqlExpand = status ?? !pd.promqlExpand;
        }
      });
    },
    updatePlotsData() {
      const {
        params: { tabNum, orderPlot },

        loadPlotData,
      } = getState();
      const { plotVisibilityList, plotPreviewList } = usePlotVisibilityStore.getState();
      const first: PlotKey[] = [];
      const second: PlotKey[] = [];
      const third: PlotKey[] = [];

      orderPlot.forEach((plotKey) => {
        if (tabNum === plotKey) {
          first.push(plotKey);
        } else if (plotVisibilityList[plotKey]) {
          second.push(plotKey);
        } else if (plotPreviewList[plotKey]) {
          third.push(plotKey);
        }
      });

      first.forEach((p) => loadPlotData(p, true));
      second.forEach((p) => loadPlotData(p));
      third.forEach((p) => loadPlotData(p));
    },
    /**
     *
     * @param plotKey
     * @param force - ignore visible
     */
    async loadPlotData(plotKey, force = false) {
      if (useLiveModeStore.getState().status && getPlotLoader(plotKey)) {
        return;
      }
      const prepareEnd = queryStart(plotKey);
      await skipTimeout();
      const timeRange = getState().params.timeRange;
      const plot = getState().params.plots[plotKey];
      const prevPlotData = getState().plotsData[plotKey];
      const prevPlot = getState().plotsData[plotKey]?.lastPlotParams;
      const orderVariables = getState().params.orderVariables;
      const variables = getState().params.variables;
      const isEmbed = getState().isEmbed;
      let priority = 3;
      const changeVariablesKey = useVariableChangeStatusStore.getState().changeVariable;
      const changeVariable = orderVariables.some(
        (vK) => changeVariablesKey[vK] && filterVariableByPlot(plot)(variables[vK])
      );
      if (!force) {
        const visible = usePlotVisibilityStore.getState().plotVisibilityList[plotKey];
        const preview = usePlotVisibilityStore.getState().plotPreviewList[plotKey];
        const deltaTime = Math.floor(-timeRange.from / 5);
        if (!visible && preview) {
          if (
            prevPlotData?.lastTimeRange?.from === timeRange.from &&
            Math.abs(prevPlotData?.lastTimeRange?.to - timeRange.to) < deltaTime &&
            !changeVariable
          ) {
            prepareEnd();
            return;
          }
        }
        if (!visible && !preview) {
          prepareEnd();
          return;
        }
        if (visible) {
          priority = 2;
        }
      }

      const plotHeal = getState().isPlotHeal(plotKey);
      if (!plotHeal) {
        // console.log('skip heal');
        prepareEnd();
        return;
      }
      const changeMetricName = plot?.metricName !== prevPlot?.metricName;
      if (!changeMetricName && prevPlotData?.error403) {
        // console.log('exit 1', plotKey);
        prepareEnd();
        return;
      }

      if (getState().params.tabNum === plotKey) {
        priority = 1;
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
      const changeTimeShifts = !dequal(getState().params.timeShifts, getState().plotsData[plotKey]?.lastTimeShifts);
      const fetchBadges = priority === 1 && !isEmbed;
      const loadBadges = fetchBadges && !getState().plotsData[plotKey]?.loadBadges;
      let update = changePlotParam || changeTime || changeNowTime || changeTimeShifts || changeVariable || loadBadges;
      prepareEnd();
      if (update) {
        const queryEnd = queryStart(plotKey);
        loadPlotData(plotKey, getState().params, fetchBadges, priority)
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

      plot?.events.forEach((iPlot) => {
        if (!getPlotLoader(iPlot)) {
          getState().loadPlotData(iPlot, true);
        }
      });
      if (plot?.type === PLOT_TYPE.Event && plotKey === getState().params.tabNum && update) {
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
