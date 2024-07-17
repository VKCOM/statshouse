// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type PlotKey, promQLMetric, type QueryParams, urlEncodePlotFilters } from 'url2';
import { apiQueryFetch, type ApiQueryGet } from 'api/query';
import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION } from 'api/enum';
import { normalizePlotData } from './normalizePlotData';
import { type ProduceUpdate } from '../helpers';
import { type StatsHouseStore } from '../statsHouseStore';
import { produce } from 'immer';
import { getEmptyPlotData } from './getEmptyPlotData';
import { autoLowAgg, autoAgg } from '../constants';
import { replaceVariable } from '../helpers/replaceVariable';

export function getLoadPlotUrlParams(
  plotKey: PlotKey,
  params: QueryParams,
  fetchBadges: boolean = false,
  priority?: number
): ApiQueryGet | null {
  let plot = params.plots[plotKey];
  if (!plot) {
    return null;
  }
  plot = replaceVariable(plotKey, plot, params.variables);
  const width = plot.customAgg === -1 ? autoLowAgg : plot.customAgg === 0 ? autoAgg : `${plot.customAgg}s`;
  const urlParams: ApiQueryGet = {
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: params.timeRange.to.toString(),
    [GET_PARAMS.fromTime]: params.timeRange.from.toString(),
    [GET_PARAMS.width]: width,
    [GET_PARAMS.version]: plot.useV2 ? METRIC_VALUE_BACKEND_VERSION.v2 : METRIC_VALUE_BACKEND_VERSION.v1,
    [GET_PARAMS.metricFilter]: urlEncodePlotFilters('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    // [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    [GET_PARAMS.metricTimeShifts]: params.timeShifts.map((t) => t.toString()),
    [GET_PARAMS.excessPoints]: '1',
    [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : '0',
  };
  if (plot.metricName && plot.metricName !== promQLMetric) {
    urlParams[GET_PARAMS.metricName] = plot.metricName;
  }
  if (plot.promQL || plot.metricName === promQLMetric) {
    urlParams[GET_PARAMS.metricPromQL] = plot.promQL;
  }
  if (plot.maxHost) {
    urlParams[GET_PARAMS.metricMaxHost] = '1';
  }
  if (priority) {
    urlParams[GET_PARAMS.priority] = priority.toString();
  }
  // if (allParams) {
  //   urlParams.push(...encodeVariableValues(allParams));
  //   urlParams.push(...encodeVariableConfig(allParams));
  // }

  return urlParams;
}

export async function loadPlotData(
  plotKey: PlotKey,
  params: QueryParams,
  fetchBadges: boolean = false,
  priority?: number
): Promise<ProduceUpdate<StatsHouseStore> | null> {
  const urlParams = getLoadPlotUrlParams(plotKey, params, fetchBadges, priority);
  const plot = params.plots[plotKey];
  if (!urlParams || !plot) {
    return null;
  }
  // todo:
  // loadMetricMeta(plot.metricName).then();
  const { response, error, status } = await apiQueryFetch(urlParams, `loadPlotData_${plotKey}`);

  if (error) {
    if (status === 403) {
    }
  }
  if (response) {
    const data = normalizePlotData(response.data, plot, params);
    return (state) => {
      state.plotsData[plotKey] = produce(state.plotsData[plotKey] ?? getEmptyPlotData(), data);
    };
  }
  return null;
}
