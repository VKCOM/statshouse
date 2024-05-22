import { createStore } from '../createStore';
import { type PlotKey, PlotParams, promQLMetric, TimeRange, urlEncodePlotFilters, useUrlStore } from '../urlStore';
import uPlot from 'uplot';
import { apiQueryFetch, ApiQueryGet } from '../../api/query';
import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION } from '../../api/enum';
import { PlotsStore } from './plotsStore';

export function getEmptyPlotData(): PlotData {
  return {
    data: [],
    dataShow: [],
    bands: [],
    series: [],
    seriesShow: [],
    scales: {},
  };
}

export type PlotData = {
  data: uPlot.AlignedData;
  dataShow: uPlot.AlignedData;
  bands?: uPlot.Band[];
  series: uPlot.Series[];
  // seriesTimeShift: number[];
  seriesShow: boolean[];
  scales: Record<string, { min: number; max: number }>;
};

export type PlotsData = {
  plotsData: Partial<Record<PlotKey, PlotData>>;
};

export const usePlotsDataStore = createStore<PlotsData>(() => ({ plotsData: {} }));

export async function loadPlotData(plotKey: PlotKey, plot: PlotParams, timeRange: TimeRange, priority?: number) {
  const fetchBadges = true;
  // urlEncodePlotFilters('',plot.filterIn,plot.filterNotIn).map(([k,v])=>v)
  const params: ApiQueryGet = {
    // [GET_PARAMS.metricName]: plot.metricName,
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: timeRange.to.toString(),
    [GET_PARAMS.fromTime]: timeRange.from.toString(),
    [GET_PARAMS.width]: '2000',
    [GET_PARAMS.version]: plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.metricFilter]: urlEncodePlotFilters('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    // [GET_PARAMS.metricPromQL]: plot.promQL,
    [GET_PARAMS.metricTimeShifts]: plot.timeShifts.map(toString),
    // [GET_PARAMS.metricMaxHost]: plot.maxHost ? '1' : undefined,
    // [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : undefined,
    // [GET_PARAMS.dataFormat]? : string;
    // [GET_PARAMS.avoidCache]? : string;
    [GET_PARAMS.excessPoints]: '1',
  };
  if (plot.metricName && plot.metricName !== promQLMetric) {
    params[GET_PARAMS.metricName] = plot.metricName;
  }
  if (plot.promQL || plot.metricName === promQLMetric) {
    params[GET_PARAMS.metricPromQL] = plot.promQL;
  }
  if (plot.maxHost) {
    params[GET_PARAMS.metricMaxHost] = '1';
  }
  if (fetchBadges) {
    params[GET_PARAMS.metricVerbose] = '1';
  }
  if (priority) {
    params[GET_PARAMS.priority] = priority.toString();
  }

  const { response, error, status } = await apiQueryFetch(params);

  if (error) {
    if (status === 403) {
    }
  }
  if (response) {
  }
  // console.log(response, error, status);
}

export function plotsDataStoreSubscribe(state: PlotsStore, prevState: PlotsStore) {
  const urlStore = useUrlStore.getState();
  state.viewOrderPlot.forEach((plotKey) => {
    const plotInfo = state.plotsInfo[plotKey];
    const plot = urlStore.params.plots[plotKey];
    if (plot && plotInfo) {
      loadPlotData(plotKey, plot, urlStore.params.timeRange);
    }
  });
}

// usePlotsInfoStore.subscribe(plotsDataStoreSubscribe);
