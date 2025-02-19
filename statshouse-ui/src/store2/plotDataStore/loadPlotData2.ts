// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  metricFilterEncode,
  PlotParams,
  promQLMetric,
  TimeRange,
  urlEncodeVariable,
  VariableKey,
  VariableParams,
} from '@/url2';
import type { ApiQueryGet } from '@/api/query';
import { GET_PARAMS } from '@/api/enum';
import { autoAgg, autoLowAgg } from '@/store2';
import { produce } from 'immer';
import { isNotNil } from '@/common/helpers';

export function replaceVariable(plot: PlotParams, variables: Partial<Record<string, VariableParams>>): PlotParams {
  return produce(plot, (p) => {
    Object.values(variables)
      .filter(isNotNil)
      .forEach(({ link, values, groupBy, negative }) => {
        const [, tagKey] = link.find(([iPlot]) => iPlot === plot.id) ?? [];
        if (tagKey) {
          const ind = p.groupBy.indexOf(tagKey);
          if (groupBy) {
            if (ind === -1) {
              p.groupBy.push(tagKey);
            }
          } else {
            if (ind > -1) {
              p.groupBy.splice(ind, 1);
            }
          }
          delete p.filterIn[tagKey];
          delete p.filterNotIn[tagKey];
          if (negative) {
            p.filterNotIn[tagKey] = values.slice();
          } else {
            p.filterIn[tagKey] = values.slice();
          }
        }
      });
  });
}

export function urlEncodeVariables(variables: Partial<Record<VariableKey, VariableParams>>): [string, string][] {
  const paramArr: [string, string][] = [];
  Object.values(variables).forEach((p) => {
    if (p) {
      paramArr.push(...urlEncodeVariable(p));
    }
  });
  return paramArr;
}

export function getLoadPlotUrlParamsLight(
  plot: PlotParams,
  timeRange: TimeRange,
  timeShifts: number[],
  variables: Partial<Record<VariableKey, VariableParams>>,
  interval?: number,
  fetchBadges: boolean = false,
  priority?: number
): ApiQueryGet | null {
  plot = replaceVariable(plot, variables);
  const width = plot.customAgg === -1 ? autoLowAgg : plot.customAgg === 0 ? autoAgg : `${plot.customAgg}s`;
  const urlParams: ApiQueryGet = {
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: interval ? '0' : timeRange.to.toString(),
    [GET_PARAMS.fromTime]: timeRange.from.toString(),
    [GET_PARAMS.width]: width.toString(),
    [GET_PARAMS.version]: plot.backendVersion,
    [GET_PARAMS.metricFilter]: metricFilterEncode('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: plot.groupBy,
    [GET_PARAMS.metricTimeShifts]: timeShifts.map((t) => t.toString()),
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
    urlEncodeVariables(variables).forEach(([key, value]) => {
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
