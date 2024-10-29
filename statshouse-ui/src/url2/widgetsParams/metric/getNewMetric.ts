import type { PlotParams } from '../../queryParams';
import { METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE, QUERY_WHAT } from '../../../api/enum';

export function getNewMetric(): PlotParams {
  return {
    id: '',
    metricName: '',
    customName: '',
    customDescription: '',
    promQL: '',
    metricUnit: undefined,
    what: [QUERY_WHAT.countNorm],
    customAgg: 0,
    groupBy: [],
    filterIn: {},
    filterNotIn: {},
    numSeries: 5,
    backendVersion: METRIC_VALUE_BACKEND_VERSION.v2,
    yLock: {
      min: 0,
      max: 0,
    },
    maxHost: false,
    type: PLOT_TYPE.Metric,
    events: [],
    eventsBy: [],
    eventsHide: [],
    totalLine: false,
    filledGraph: true,
    timeShifts: [],
    prometheusCompat: false,
  };
}
