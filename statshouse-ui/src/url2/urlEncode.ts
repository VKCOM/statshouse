import { FilterTag, GroupInfo, PlotParams, QueryParams, VariableParams, VariableParamsSource } from './queryParams';
import { GET_PARAMS, METRIC_VALUE_BACKEND_VERSION, metricTypeToMetricTypeUrl, PLOT_TYPE, TagKey } from 'api/enum';
import { dequal } from 'dequal/lite';

import { getDefaultParams, getNewGroup, getNewPlot, getNewVariable, getNewVariableSource } from './getDefault';
import {
  filterInSep,
  filterNotInSep,
  orderGroupSplitter,
  orderPlotSplitter,
  orderVariableSplitter,
  promQLMetric,
  removeValueChar,
} from './constants';
import { toGroupInfoPrefix, toPlotPrefix, toVariablePrefix, toVariableValuePrefix } from './urlHelpers';

export function urlEncode(params: QueryParams, defaultParams?: QueryParams): [string, string][] {
  return [
    ...urlEncodeGlobalParam(params, defaultParams),
    ...urlEncodeTimeRange(params, defaultParams),
    ...urlEncodePlots(params, defaultParams),
    ...urlEncodeGroups(params, defaultParams),
    ...urlEncodeVariables(params, defaultParams),
  ];
}

export function urlEncodeGlobalParam(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams === params) {
    return paramArr;
  }
  if (defaultParams.live !== params.live) {
    paramArr.push([GET_PARAMS.metricLive, params.live ? '1' : '0']);
  }
  if (defaultParams.theme !== params.theme && params.theme) {
    paramArr.push([GET_PARAMS.theme, params.theme]);
  }
  if (defaultParams.tabNum !== params.tabNum) {
    paramArr.push([GET_PARAMS.metricTabNum, params.tabNum]);
  }
  if (!dequal(defaultParams.timeShifts, params.timeShifts)) {
    params.timeShifts.forEach((shift) => {
      paramArr.push([GET_PARAMS.metricTimeShifts, shift.toString()]);
    });
  }
  if (defaultParams.eventFrom !== params.eventFrom) {
    paramArr.push([GET_PARAMS.metricEventFrom, params.eventFrom.toString()]);
  }
  if (params.dashboardId) {
    paramArr.push([GET_PARAMS.dashboardID, params.dashboardId.toString()]);
  }
  if (defaultParams.dashboardName !== params.dashboardName) {
    paramArr.push([GET_PARAMS.dashboardName, params.dashboardName]);
  }
  if (defaultParams.dashboardDescription !== params.dashboardDescription) {
    paramArr.push([GET_PARAMS.dashboardDescription, params.dashboardDescription]);
  }
  if (defaultParams.dashboardVersion !== params.dashboardVersion && params.dashboardVersion) {
    paramArr.push([GET_PARAMS.dashboardVersion, params.dashboardVersion.toString()]);
  }
  return paramArr;
}

export function urlEncodeTimeRange(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.timeRange === params.timeRange) {
    return paramArr;
  }
  if (defaultParams.timeRange.urlTo !== params.timeRange.urlTo) {
    paramArr.push([GET_PARAMS.toTime, params.timeRange.urlTo.toString()]);
  }
  if (defaultParams.timeRange.from !== params.timeRange.from) {
    paramArr.push([GET_PARAMS.fromTime, params.timeRange.from.toString()]);
  }
  return paramArr;
}

export function urlEncodePlots(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.plots !== params.plots) {
    Object.values(params.plots).forEach((p) => {
      if (p) {
        paramArr.push(...urlEncodePlot(p, defaultParams.plots[p.id]));
      }
    });
    //remove plots
    Object.values(defaultParams.plots).forEach((dPlot) => {
      if (dPlot && !params.plots[dPlot.id]) {
        paramArr.push([GET_PARAMS.plotPrefix + dPlot.id, removeValueChar]);
      }
    });
  }
  if (!dequal(defaultParams.orderPlot, params.orderPlot)) {
    paramArr.push([GET_PARAMS.orderPlot, params.orderPlot.join(orderPlotSplitter)]);
  }
  return paramArr;
}

export function urlEncodePlot(plot: PlotParams, defaultPlot: PlotParams = getNewPlot()): [string, string][] {
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
    plot.groupBy.forEach((g) => {
      paramArr.push([prefix + GET_PARAMS.metricGroupBy, g]);
    });
  }
  if (!dequal(defaultPlot.filterIn, plot.filterIn) || !dequal(defaultPlot.filterNotIn, plot.filterNotIn)) {
    paramArr.push(...urlEncodePlotFilters(prefix, plot.filterIn, plot.filterNotIn));

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

export function urlEncodePlotFilters(prefix: string, filterIn: FilterTag, filterNotIn: FilterTag): [string, string][] {
  const paramArr: [string, string][] = [];
  Object.entries(filterIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) => paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterInSep + valueTag]))
  );
  Object.entries(filterNotIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) =>
      paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterNotInSep + valueTag])
    )
  );
  return paramArr;
}

export function urlEncodeGroups(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.groups !== params.groups) {
    Object.values(params.groups).forEach((p) => {
      if (p) {
        paramArr.push(...urlEncodeGroup(p, defaultParams.groups[p.id]));
      }
    });
  }
  //remove group
  Object.values(defaultParams.groups).forEach((dGroup) => {
    if (dGroup && !params.groups[dGroup.id]) {
      paramArr.push([GET_PARAMS.dashboardGroupInfoPrefix + dGroup.id, removeValueChar]);
    }
  });
  if (!dequal(defaultParams.orderGroup, params.orderGroup)) {
    paramArr.push([GET_PARAMS.orderGroup, params.orderGroup.join(orderGroupSplitter)]);
  }
  return paramArr;
}

export function urlEncodeGroup(group: GroupInfo, defaultGroup: GroupInfo = getNewGroup()): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultGroup === group) {
    return paramArr;
  }
  const prefix = toGroupInfoPrefix(group.id);
  if (defaultGroup.name !== group.name) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoName, group.name]);
  }
  if (defaultGroup.description !== group.description) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoDescription, group.description]);
  }
  if (defaultGroup.count !== group.count) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoCount, group.count.toString()]);
  }
  if (defaultGroup.size !== group.size) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoSize, group.size]);
  }
  if (defaultGroup.show !== group.show) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoShow, group.show ? '1' : '0']);
  }
  if (!paramArr.length && !defaultGroup.id) {
    paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoName, group.name]);
  }
  return paramArr;
}

export function urlEncodeVariables(
  params: QueryParams,
  defaultParams: QueryParams = getDefaultParams()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultParams.variables !== params.variables) {
    Object.values(params.variables).forEach((p) => {
      if (p) {
        paramArr.push(...urlEncodeVariable(p, defaultParams.variables[p.id]));
      }
    });

    //remove variable
    Object.values(defaultParams.variables).forEach((dVariable) => {
      if (dVariable && !params.variables[dVariable.id]) {
        paramArr.push([GET_PARAMS.variablePrefix + dVariable.id, removeValueChar]);
        // paramArr.push([GET_PARAMS.variableValuePrefix + '.' + dVariable.name, removeValueChar]);
      }
    });
  }
  if (!dequal(defaultParams.orderVariables, params.orderVariables)) {
    paramArr.push([GET_PARAMS.orderVariable, params.orderVariables.join(orderVariableSplitter)]);
  }

  return paramArr;
}

export function urlEncodeVariable(
  variable: VariableParams,
  defaultVariable: VariableParams = getNewVariable()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultVariable === variable) {
    return paramArr;
  }

  const prefix = toVariablePrefix(variable.id);
  const prefixValue = toVariableValuePrefix(variable.name);

  if (defaultVariable.name !== variable.name) {
    paramArr.push([prefix + GET_PARAMS.variableName, variable.name]);
  }
  if (defaultVariable.description !== variable.description) {
    paramArr.push([prefix + GET_PARAMS.variableDescription, variable.description]);
  }

  if (!dequal(defaultVariable.link, variable.link)) {
    const link = [...variable.link];
    link.sort((a, b) => +a[0] - +b[0]);
    paramArr.push([prefix + GET_PARAMS.variableLinkPlot, link.map((s) => s.join('.')).join('-')]);
  }

  Object.values(variable.source).forEach((p) => {
    if (p) {
      paramArr.push(
        ...urlEncodeVariableSource(
          prefix + GET_PARAMS.variableSourcePrefix + p.id + '.',
          p,
          defaultVariable.source[p.id]
        )
      );
    }
  });
  //remove source
  Object.values(defaultVariable.source).forEach((dSource) => {
    if (dSource && !variable.source[dSource.id]) {
      paramArr.push([prefix + GET_PARAMS.variableSourcePrefix + dSource.id, removeValueChar]);
    }
  });

  if (!dequal(defaultVariable.values, variable.values)) {
    variable.values.forEach((v) => {
      paramArr.push([prefixValue, v]);
    });
    if (variable.values.length === 0) {
      paramArr.push([prefixValue, removeValueChar]);
    }
  }

  if (defaultVariable.groupBy !== variable.groupBy) {
    paramArr.push([prefixValue + '.' + GET_PARAMS.variableGroupBy, variable.groupBy ? '1' : '0']);
  }

  if (defaultVariable.negative !== variable.negative) {
    paramArr.push([prefixValue + '.' + GET_PARAMS.variableNegative, variable.negative ? '1' : '0']);
  }
  if (!paramArr.length && !defaultVariable.id) {
    paramArr.push([prefix + GET_PARAMS.variableName, variable.name]);
  }
  return paramArr;
}

export function urlEncodeVariableSource(
  prefix: string,
  source: VariableParamsSource,
  defaultSource: VariableParamsSource = getNewVariableSource()
): [string, string][] {
  const paramArr: [string, string][] = [];
  if (defaultSource === source) {
    return paramArr;
  }

  if (defaultSource.metric !== source.metric) {
    paramArr.push([prefix + GET_PARAMS.variableSourceMetricName, source.metric]);
  }

  if (defaultSource.tag !== source.tag) {
    paramArr.push([prefix + GET_PARAMS.variableSourceTag, source.tag]);
  }

  if (
    !dequal(
      { filterIn: defaultSource.filterIn, filterNotIn: defaultSource.filterNotIn },
      { filterIn: source.filterIn, filterNotIn: source.filterNotIn }
    )
  ) {
    paramArr.push(...urlEncodePlotFilters(prefix, source.filterIn, source.filterNotIn));
  }

  if (!paramArr.length && !defaultSource.id) {
    paramArr.push([prefix + GET_PARAMS.variableSourceMetricName, source.metric]);
  }

  return paramArr;
}
