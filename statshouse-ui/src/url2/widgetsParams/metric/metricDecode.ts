// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  freeKeyPrefix,
  getNewMetric,
  metricFilterDecode,
  metricLayoutDecode,
  PlotKey,
  PlotParams,
  promQLMetric,
  removeValueChar,
  sortUniqueKeys,
  toPlotKey,
  TreeParamsObject,
  treeParamsObjectValueSymbol,
} from '@/url2';
import {
  GET_PARAMS,
  isQueryWhat,
  isTagKey,
  metricTypeUrlToMetricType,
  PLOT_TYPE,
  toMetricValueBackendVersion,
  toPlotType,
} from '@/api/enum';
import { isNotNil, toNumber, toNumberM } from '@/common/helpers';

export function metricDecode(
  plotKey: PlotKey,
  searchParams?: TreeParamsObject,
  defaultPlot: PlotParams = getNewMetric()
): PlotParams | undefined {
  if (searchParams?.[treeParamsObjectValueSymbol]?.[0] === removeValueChar) {
    return undefined;
  }
  const type = toPlotType(searchParams?.[GET_PARAMS.metricType]?.[treeParamsObjectValueSymbol]?.[0], defaultPlot.type);
  const rawUseV2 = searchParams?.[GET_PARAMS.version]?.[treeParamsObjectValueSymbol]?.[0];
  const rawMaxHost = searchParams?.[GET_PARAMS.metricMaxHost]?.[treeParamsObjectValueSymbol]?.[0];
  const rawTotalLine = searchParams?.[GET_PARAMS.viewTotalLine]?.[treeParamsObjectValueSymbol]?.[0];
  const rawFilledGraph = searchParams?.[GET_PARAMS.viewFilledGraph]?.[treeParamsObjectValueSymbol]?.[0];
  const rawLogScale = searchParams?.[GET_PARAMS.viewLogScale]?.[treeParamsObjectValueSymbol]?.[0];
  const rawPrometheusCompat = searchParams?.[GET_PARAMS.prometheusCompat]?.[treeParamsObjectValueSymbol]?.[0];
  const metricName = searchParams?.[GET_PARAMS.metricName]?.[treeParamsObjectValueSymbol]?.[0];
  const promQL = searchParams?.[GET_PARAMS.metricPromQL]?.[treeParamsObjectValueSymbol]?.[0];
  const rawLayout = searchParams?.[GET_PARAMS.metricLayout]?.[treeParamsObjectValueSymbol]?.[0];
  const rawGroup = searchParams?.[GET_PARAMS.metricGroupKey]?.[treeParamsObjectValueSymbol]?.[0];

  return {
    id: plotKey,
    type,
    metricName: metricName ?? ((promQL != null && promQLMetric) || defaultPlot.metricName),
    promQL: promQL ?? defaultPlot.promQL,
    customName:
      searchParams?.[GET_PARAMS.metricCustomName]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultPlot.customName,
    customDescription:
      searchParams?.[GET_PARAMS.metricCustomDescription]?.[treeParamsObjectValueSymbol]?.[0] ??
      defaultPlot.customDescription,
    metricUnit:
      metricTypeUrlToMetricType(searchParams?.[GET_PARAMS.metricMetricUnit]?.[treeParamsObjectValueSymbol]?.[0]) ??
      defaultPlot.metricUnit,
    what: searchParams?.[GET_PARAMS.metricWhat]?.[treeParamsObjectValueSymbol]?.filter(isQueryWhat) ?? defaultPlot.what,
    customAgg:
      toNumber(searchParams?.[GET_PARAMS.metricAgg]?.[treeParamsObjectValueSymbol]?.[0]) ?? defaultPlot.customAgg,
    groupBy: sortUniqueKeys(
      searchParams?.[GET_PARAMS.metricGroupBy]?.[treeParamsObjectValueSymbol]?.map(freeKeyPrefix).filter(isTagKey) ??
        defaultPlot.groupBy
    ),
    group: rawGroup == removeValueChar ? undefined : (rawGroup ?? defaultPlot.group),
    ...metricFilterDecode(GET_PARAMS.metricFilter, searchParams, defaultPlot),
    layout:
      rawLayout === removeValueChar
        ? undefined
        : rawLayout == null
          ? defaultPlot.layout
          : metricLayoutDecode(rawLayout),
    numSeries:
      toNumber(searchParams?.[GET_PARAMS.numResults]?.[treeParamsObjectValueSymbol]?.[0]) ??
      (type === PLOT_TYPE.Event ? 0 : defaultPlot.numSeries),
    backendVersion: toMetricValueBackendVersion(rawUseV2) ?? defaultPlot.backendVersion,
    yLock: {
      min:
        toNumber(searchParams?.[GET_PARAMS.metricLockMin]?.[treeParamsObjectValueSymbol]?.[0]) ?? defaultPlot.yLock.min,
      max:
        toNumber(searchParams?.[GET_PARAMS.metricLockMax]?.[treeParamsObjectValueSymbol]?.[0]) ?? defaultPlot.yLock.max,
    },
    maxHost: rawMaxHost != null ? rawMaxHost === '1' : defaultPlot.maxHost,
    events: sortUniqueKeys(
      searchParams?.[GET_PARAMS.metricEvent]?.[treeParamsObjectValueSymbol]
        ?.map((s) => toPlotKey(s))
        .filter(isNotNil) ?? defaultPlot.events
    ),
    eventsBy: sortUniqueKeys(
      searchParams?.[GET_PARAMS.metricEventBy]?.[treeParamsObjectValueSymbol]?.map(freeKeyPrefix).filter(isTagKey) ??
        defaultPlot.eventsBy
    ),
    eventsHide: sortUniqueKeys(
      searchParams?.[GET_PARAMS.metricEventHide]?.[treeParamsObjectValueSymbol]?.map(freeKeyPrefix).filter(isTagKey) ??
        defaultPlot.eventsHide
    ),
    totalLine: rawTotalLine != null ? rawTotalLine === '1' : defaultPlot.totalLine,
    filledGraph: rawFilledGraph != null ? rawFilledGraph !== '0' : defaultPlot.filledGraph,
    logScale: rawLogScale != null ? rawLogScale === '1' : defaultPlot.logScale,
    prometheusCompat: rawPrometheusCompat != null ? rawPrometheusCompat === '1' : defaultPlot.prometheusCompat,
    timeShifts:
      searchParams?.[GET_PARAMS.metricLocalTimeShifts]?.[treeParamsObjectValueSymbol]
        ?.map(toNumberM)
        .filter(isNotNil)
        .sort() ?? defaultPlot.timeShifts,
  };
}
