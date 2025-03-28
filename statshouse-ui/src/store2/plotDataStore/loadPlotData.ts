// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { metricFilterEncode, type PlotKey, promQLMetric, type QueryParams, urlEncodeVariables } from '@/url2';
import { apiQuery, type ApiQueryGet } from '@/api/query';
import { GET_PARAMS } from '@/api/enum';
import { normalizePlotData } from './normalizePlotData';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '@/store2';
import { autoAgg, autoLowAgg } from '@/store2';
import { produce } from 'immer';
import { getEmptyPlotData } from './getEmptyPlotData';
import { replaceVariable } from '../helpers/replaceVariable';
import { MetricMeta, tagsArrToObject } from '../metricsMetaStore';
import { ExtendedError } from '@/api/api';

export function getLoadPlotUrlParams(
  plotKey: PlotKey,
  params: QueryParams,
  interval?: number,
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
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: params.timeRange.to.toString(),
    [GET_PARAMS.fromTime]: params.timeRange.from.toString(),
    [GET_PARAMS.width]: width.toString(),
    [GET_PARAMS.version]: plot.backendVersion,
    [GET_PARAMS.metricFilter]: metricFilterEncode('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    [GET_PARAMS.metricTimeShifts]: params.timeShifts.map((t) => t.toString()),
    [GET_PARAMS.excessPoints]: '1',
    [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : '0',
  };
  if (plot.metricName && plot.metricName !== promQLMetric) {
    urlParams[GET_PARAMS.metricName] = plot.metricName;
    urlParams[GET_PARAMS.numResults] = plot.numSeries.toString();
  }
  if (plot.promQL || plot.metricName === promQLMetric) {
    urlParams[GET_PARAMS.metricPromQL] = plot.promQL;
    //add variable params for PromQL
    urlEncodeVariables(params).forEach(([key, value]) => {
      urlParams[key] ??= [];
      if (typeof urlParams[key] === 'string') {
        urlParams[key] = [urlParams[key]];
      }
      urlParams[key].push(value);
    });
  }
  if (plot.maxHost) {
    urlParams[GET_PARAMS.metricMaxHost] = '1';
  }
  if (plot.prometheusCompat) {
    urlParams[GET_PARAMS.prometheusCompat] = '1';
  }
  if (priority) {
    urlParams[GET_PARAMS.priority] = priority.toString();
  }
  if (interval) {
    urlParams[GET_PARAMS.metricLive] = interval?.toString();
  }

  return urlParams;
}

export async function loadPlotData(
  plotKey: PlotKey,
  params: QueryParams,
  interval?: number,
  fetchBadges: boolean = false,
  priority?: number
): Promise<ProduceUpdate<StatsHouseStore> | null> {
  const plot = params.plots[plotKey];
  if (!plot) {
    return null;
  }

  const { response, error, status } = await apiQuery(plot, params, interval, priority);

  if (error) {
    if (status === 403) {
      return (state) => {
        state.plotsData[plotKey] = {
          ...getEmptyPlotData(),
          error403: error.toString(),
          lastHeals: false,
        };
      };
    } else if (error.name !== 'AbortError' && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
      return (state) => {
        if (state.plotsData[plotKey]) {
          state.plotsData[plotKey]!.error = error.toString();
          state.plotsData[plotKey]!.lastHeals = false;
          state.setPlotHeal(plotKey, false);
        }
        //if (resetCache) {
        //                   state.plotsData[index] = {
        //                     ...getEmptyPlotData(),
        //                     error: error.toString(),
        //                   };
        //                 } else {
        //                   state.plotsData[index].error = error.toString();
        //                 }
        //
        //                 addStatus(index.toString(), false);
      };
    }
  }
  if (response) {
    const data = normalizePlotData(response.data, plot, params);
    let metricMeta: MetricMeta;
    if (response.data.metric) {
      metricMeta = {
        ...response.data.metric,
        ...tagsArrToObject(response.data.metric.tags),
      };
    }
    return (state) => {
      state.plotsData[plotKey] = produce(state.plotsData[plotKey] ?? getEmptyPlotData(), data);
      state.plotsData[plotKey]!.lastHeals = true;
      state.plotsData[plotKey]!.loadBadges = fetchBadges;
      if (metricMeta?.name) {
        state.metricMeta[metricMeta.name] = metricMeta;
      }
      if (state.plotHeals[plotKey]?.status) {
        state.plotsData[plotKey]!.error = '';
      }
      // state.setPlotHeal(plotKey, true);
    };
  }
  return null;
}
