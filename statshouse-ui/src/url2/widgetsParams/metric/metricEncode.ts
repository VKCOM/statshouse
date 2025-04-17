// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotParams } from '@/url2';
import { getNewMetric, metricFilterEncode, promQLMetric, removeValueChar, toPlotPrefix } from '@/url2';
import { GET_PARAMS, metricTypeToMetricTypeUrl, PLOT_TYPE, type TagKey } from '@/api/enum';
import { dequal } from 'dequal/lite';
import { metricLayoutEncode } from '@/url2/widgetsParams/metric/metricLayoutEncode';

export function metricEncode(plot: PlotParams, defaultPlot: PlotParams = getNewMetric()): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultPlot.id === '' && plot.type === PLOT_TYPE.Event) {
    defaultPlot.numSeries = 0;
  }
  if (defaultPlot === plot) {
    return paramArr;
  }
  const prefix = toPlotPrefix(plot.id);
  if (defaultPlot.metricName !== plot.metricName && plot.metricName !== promQLMetric) {
    paramArr.push([prefix + GET_PARAMS.metricName, plot.metricName]);
  }
  if (defaultPlot.promQL !== plot.promQL && (plot.promQL || defaultPlot.promQL)) {
    paramArr.push([prefix + GET_PARAMS.metricPromQL, plot.promQL]);
  }
  if (defaultPlot.customName !== plot.customName) {
    paramArr.push([prefix + GET_PARAMS.metricCustomName, plot.customName]);
  }
  if (defaultPlot.customDescription !== plot.customDescription) {
    paramArr.push([prefix + GET_PARAMS.metricCustomDescription, plot.customDescription]);
  }
  if (defaultPlot.metricUnit !== plot.metricUnit) {
    paramArr.push([prefix + GET_PARAMS.metricMetricUnit, metricTypeToMetricTypeUrl(plot.metricUnit)]);
  }
  if (!dequal(defaultPlot.what, plot.what)) {
    plot.what.forEach((w) => {
      paramArr.push([prefix + GET_PARAMS.metricWhat, w]);
    });
  }
  if (defaultPlot.customAgg !== plot.customAgg) {
    paramArr.push([prefix + GET_PARAMS.metricAgg, plot.customAgg.toString()]);
  }
  if (!dequal(defaultPlot.groupBy, plot.groupBy)) {
    if (plot.groupBy.length === 0 && defaultPlot.groupBy.length > 0) {
      paramArr.push([prefix + GET_PARAMS.metricGroupBy, removeValueChar]);
    } else {
      plot.groupBy.forEach((g) => {
        paramArr.push([prefix + GET_PARAMS.metricGroupBy, g]);
      });
    }
  }
  if (!dequal(defaultPlot.filterIn, plot.filterIn) || !dequal(defaultPlot.filterNotIn, plot.filterNotIn)) {
    paramArr.push(...metricFilterEncode(prefix, plot.filterIn, plot.filterNotIn));

    //remove filter
    Object.entries(plot.filterIn).forEach(([keyTag, valuesTag]) => {
      if (defaultPlot.filterIn[keyTag as TagKey]?.length && !valuesTag.length) {
        paramArr.push([prefix + GET_PARAMS.metricFilter, removeValueChar]);
      }
    });
    Object.entries(plot.filterNotIn).forEach(([keyTag, valuesTag]) => {
      if (defaultPlot.filterNotIn[keyTag as TagKey]?.length && !valuesTag.length) {
        paramArr.push([prefix + GET_PARAMS.metricFilter, removeValueChar]);
      }
    });
  }

  if (defaultPlot.numSeries !== plot.numSeries) {
    paramArr.push([prefix + GET_PARAMS.numResults, plot.numSeries.toString()]);
  }

  if (defaultPlot.backendVersion !== plot.backendVersion) {
    paramArr.push([prefix + GET_PARAMS.version, plot.backendVersion]);
  }

  if (defaultPlot.yLock.min !== plot.yLock.min) {
    paramArr.push([prefix + GET_PARAMS.metricLockMin, plot.yLock.min.toString()]);
  }

  if (defaultPlot.yLock.max !== plot.yLock.max) {
    paramArr.push([prefix + GET_PARAMS.metricLockMax, plot.yLock.max.toString()]);
  }

  if (defaultPlot.maxHost !== plot.maxHost) {
    paramArr.push([prefix + GET_PARAMS.metricMaxHost, plot.maxHost ? '1' : '0']);
  }

  if (defaultPlot.type !== plot.type) {
    paramArr.push([prefix + GET_PARAMS.metricType, plot.type]);
  }

  if (!dequal(defaultPlot.events, plot.events)) {
    //remove metric event
    if (plot.events.length === 0 && defaultPlot.events.length > 0) {
      paramArr.push([prefix + GET_PARAMS.metricEvent, removeValueChar]);
    } else {
      plot.events.forEach((e) => {
        paramArr.push([prefix + GET_PARAMS.metricEvent, e]);
      });
    }
  }

  if (!dequal(defaultPlot.eventsBy, plot.eventsBy)) {
    //remove event by
    if (plot.eventsBy.length === 0 && defaultPlot.eventsBy.length > 0) {
      paramArr.push([prefix + GET_PARAMS.metricEventBy, removeValueChar]);
    } else {
      plot.eventsBy.forEach((e) => {
        paramArr.push([prefix + GET_PARAMS.metricEventBy, e]);
      });
    }
  }

  if (!dequal(defaultPlot.eventsHide, plot.eventsHide)) {
    //remove event hide
    if (plot.eventsHide.length === 0 && defaultPlot.eventsHide.length > 0) {
      paramArr.push([prefix + GET_PARAMS.metricEventHide, removeValueChar]);
    } else {
      plot.eventsHide.forEach((e) => {
        paramArr.push([prefix + GET_PARAMS.metricEventHide, e]);
      });
    }
  }

  if (defaultPlot.totalLine !== plot.totalLine) {
    paramArr.push([prefix + GET_PARAMS.viewTotalLine, plot.totalLine ? '1' : '0']);
  }

  if (defaultPlot.filledGraph !== plot.filledGraph) {
    paramArr.push([prefix + GET_PARAMS.viewFilledGraph, plot.filledGraph ? '1' : '0']);
  }

  if (defaultPlot.logScale !== plot.logScale) {
    paramArr.push([prefix + GET_PARAMS.viewLogScale, plot.logScale ? '1' : '0']);
  }

  if (defaultPlot.prometheusCompat !== plot.prometheusCompat) {
    paramArr.push([prefix + GET_PARAMS.prometheusCompat, plot.prometheusCompat ? '1' : '0']);
  }

  if (!dequal(defaultPlot.timeShifts, plot.timeShifts)) {
    //remove local time shifts
    if (plot.timeShifts.length === 0 && defaultPlot.timeShifts.length > 0) {
      paramArr.push([prefix + GET_PARAMS.metricLocalTimeShifts, removeValueChar]);
    } else {
      plot.timeShifts.forEach((t) => {
        paramArr.push([prefix + GET_PARAMS.metricLocalTimeShifts, t.toString()]);
      });
    }
  }

  if (defaultPlot.group !== plot.group) {
    if (plot.group == null) {
      paramArr.push([prefix + GET_PARAMS.metricGroupKey, removeValueChar]);
    } else {
      paramArr.push([prefix + GET_PARAMS.metricGroupKey, plot.group.toString()]);
    }
  }

  if (!dequal(defaultPlot.layout, plot.layout)) {
    //remove metric layout
    if (plot.layout == null) {
      paramArr.push([prefix + GET_PARAMS.metricLayout, removeValueChar]);
    } else {
      paramArr.push([prefix + GET_PARAMS.metricLayout, metricLayoutEncode(plot.layout)]);
    }
  }

  if (!paramArr.length && !defaultPlot.id) {
    if (plot.metricName === promQLMetric) {
      paramArr.push([prefix + GET_PARAMS.metricPromQL, plot.promQL]);
    } else {
      paramArr.push([prefix + GET_PARAMS.metricName, plot.metricName]);
    }
  }

  return paramArr;
}
