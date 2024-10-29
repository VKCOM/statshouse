import { type PlotParams } from '../../queryParams';
import { getNewMetric } from './getNewMetric';
import { GET_PARAMS, metricTypeToMetricTypeUrl, PLOT_TYPE, type TagKey } from '../../../api/enum';
import { toPlotPrefix } from '../../urlHelpers';
import { promQLMetric, removeValueChar } from '../../constants';
import { dequal } from 'dequal/lite';
import { metricFilterEncode } from './metricFilterEncode';

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
    plot.events.forEach((e) => {
      paramArr.push([prefix + GET_PARAMS.metricEvent, e]);
    });
  }

  if (!dequal(defaultPlot.eventsBy, plot.eventsBy)) {
    plot.eventsBy.forEach((e) => {
      paramArr.push([prefix + GET_PARAMS.metricEventBy, e]);
    });
  }

  if (!dequal(defaultPlot.eventsHide, plot.eventsHide)) {
    plot.eventsHide.forEach((e) => {
      paramArr.push([prefix + GET_PARAMS.metricEventHide, e]);
    });
  }

  if (defaultPlot.totalLine !== plot.totalLine) {
    paramArr.push([prefix + GET_PARAMS.viewTotalLine, plot.totalLine ? '1' : '0']);
  }

  if (defaultPlot.filledGraph !== plot.filledGraph) {
    paramArr.push([prefix + GET_PARAMS.viewFilledGraph, plot.filledGraph ? '1' : '0']);
  }

  if (defaultPlot.prometheusCompat !== plot.prometheusCompat) {
    paramArr.push([prefix + GET_PARAMS.prometheusCompat, plot.prometheusCompat ? '1' : '0']);
  }

  if (!dequal(defaultPlot.timeShifts, plot.timeShifts)) {
    plot.timeShifts.forEach((t) => {
      paramArr.push([prefix + GET_PARAMS.metricLocalTimeShifts, t.toString()]);
    });
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
