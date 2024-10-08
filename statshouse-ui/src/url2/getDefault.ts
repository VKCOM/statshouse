import type { GroupInfo, PlotParams, QueryParams, VariableParams, VariableParamsSource } from './queryParams';
import { METRIC_VALUE_BACKEND_VERSION, PLOT_TYPE, QUERY_WHAT, TAG_KEY, TIME_RANGE_KEYS_TO } from 'api/enum';
import { deepClone } from 'common/helpers';
import { globalSettings } from 'common/settings';

export function getDefaultParams(): QueryParams {
  return {
    orderPlot: [],
    plots: {},
    dashboardId: undefined,
    eventFrom: 0,
    timeShifts: [],
    tabNum: '0',
    theme: undefined,
    live: false,
    orderVariables: [],
    orderGroup: ['0'],
    groups: { '0': { ...getNewGroup(), id: '0' } },
    timeRange: {
      from: 0,
      urlTo: TIME_RANGE_KEYS_TO.default,
      absolute: false,
      now: 0,
      to: 0,
    },
    variables: {},
    dashboardDescription: '',
    dashboardName: '',
    dashboardVersion: undefined,
  };
}

export function getNewPlot(): PlotParams {
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
  };
}

export function getHomePlot(): PlotParams {
  const plot = getNewPlot();
  plot.metricName = globalSettings.default_metric;
  plot.what = globalSettings.default_metric_what.slice();
  plot.groupBy = globalSettings.default_metric_group_by.slice();
  plot.filterIn = deepClone(globalSettings.default_metric_filter_in);
  plot.filterNotIn = deepClone(globalSettings.default_metric_filter_not_in);
  plot.numSeries = globalSettings.default_num_series;
  return plot;
}

export function getNewGroup(): GroupInfo {
  return {
    id: '',
    name: '',
    description: '',
    count: 0,
    size: '2',
    show: true,
  };
}

export function getNewVariable(): VariableParams {
  return {
    id: '',
    name: '',
    description: '',
    link: [],
    source: {},
    sourceOrder: [],
    values: [],
    negative: false,
    groupBy: false,
  };
}

export function getNewVariableSource(): VariableParamsSource {
  return {
    id: '',
    metric: '',
    tag: TAG_KEY._0,
    filterIn: {},
    filterNotIn: {},
  };
}
