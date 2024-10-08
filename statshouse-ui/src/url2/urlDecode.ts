import {
  FilterTag,
  GroupInfo,
  GroupKey,
  PlotKey,
  PlotParams,
  QueryParams,
  VariableKey,
  VariableParams,
  VariableParamsSource,
  VariableSourceKey,
} from './queryParams';
import { isNotNil, sortEntity, toNumber, toNumberM, uniqueArray } from 'common/helpers';
import {
  GET_PARAMS,
  isQueryWhat,
  isTagKey,
  METRIC_VALUE_BACKEND_VERSION,
  metricTypeUrlToMetricType,
  PLOT_TYPE,
  toMetricValueBackendVersion,
  toPlotType,
  toTagKey,
} from 'api/enum';
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
import {
  freeKeyPrefix,
  isKeyId,
  isNotNilVariableLink,
  isPlotKey,
  sortUniqueKeys,
  toPlotKey,
  TreeParamsObject,
  treeParamsObjectValueSymbol,
} from './urlHelpers';
import { readTimeRange } from './timeRangeHelpers';

export function urlDecode(
  searchParams: TreeParamsObject,
  defaultParams: QueryParams = getDefaultParams()
): QueryParams {
  const plotKeys: string[] = [];
  const groupKeys: string[] = [];
  const variableKeys: string[] = [];
  Object.keys(searchParams).forEach((key) => {
    if (!isNaN(+key[1])) {
      switch (key[0]) {
        case GET_PARAMS.plotPrefix:
          plotKeys.push(String(key.slice(1)));
          break;
        case GET_PARAMS.dashboardGroupInfoPrefix:
          groupKeys.push(String(key.slice(1)));
          break;
        case GET_PARAMS.variablePrefix:
          if (key[1]) {
            variableKeys.push(String(key.slice(1)));
          }
          break;
      }
    } else if (key === GET_PARAMS.metricName || key === GET_PARAMS.metricPromQL) {
      plotKeys.push('0');
    }
  });
  const global = urlDecodeGlobalParam(searchParams, defaultParams);
  const timeRange = urlDecodeTimeRange(searchParams, defaultParams);
  const plots = urlDecodePlots(searchParams, uniqueArray([...plotKeys, ...defaultParams.orderPlot]), defaultParams);
  const groups = urlDecodeGroups(searchParams, uniqueArray([...groupKeys, ...defaultParams.orderGroup]), defaultParams);
  const variables = urlDecodeVariables(
    searchParams,
    uniqueArray([...variableKeys, ...defaultParams.orderVariables]),
    defaultParams
  );
  if (groups.orderGroup.length === 1 && groups.groups[0]?.count !== plots.orderPlot.length) {
    groups.groups[0]!.count = plots.orderPlot.length;
  }
  return {
    ...global,
    ...timeRange,
    ...plots,
    ...groups,
    ...variables,
  };
}

export function urlDecodeGlobalParam(
  searchParams: TreeParamsObject,
  defaultParams: QueryParams
): Pick<
  QueryParams,
  | 'live'
  | 'tabNum'
  | 'theme'
  | 'timeShifts'
  | 'dashboardId'
  | 'eventFrom'
  | 'dashboardName'
  | 'dashboardDescription'
  | 'dashboardVersion'
> {
  const rawLive = searchParams[GET_PARAMS.metricLive]?.[treeParamsObjectValueSymbol]?.[0];
  const rawTheme = searchParams[GET_PARAMS.theme]?.[treeParamsObjectValueSymbol]?.[0];
  const rawTabNum = searchParams[GET_PARAMS.metricTabNum]?.[treeParamsObjectValueSymbol]?.[0];
  const rawTimeShifts = searchParams[GET_PARAMS.metricTimeShifts]?.[treeParamsObjectValueSymbol]
    ?.map(toNumberM)
    .filter(isNotNil);
  const rawEventFrom = toNumber(searchParams[GET_PARAMS.metricEventFrom]?.[treeParamsObjectValueSymbol]?.[0]);
  const rawDashboardId = searchParams[GET_PARAMS.dashboardID]?.[treeParamsObjectValueSymbol]?.[0];
  const rawDashboardName = searchParams[GET_PARAMS.dashboardName]?.[treeParamsObjectValueSymbol]?.[0];
  const rawDashboardDescription = searchParams[GET_PARAMS.dashboardDescription]?.[treeParamsObjectValueSymbol]?.[0];
  const rawDashboardVersion = toNumber(searchParams[GET_PARAMS.dashboardVersion]?.[treeParamsObjectValueSymbol]?.[0]);

  return {
    live: rawLive ? rawLive === '1' : defaultParams.live,
    theme: rawTheme ?? defaultParams.theme,
    tabNum: rawTabNum ?? defaultParams.tabNum,
    timeShifts: rawTimeShifts ?? defaultParams.timeShifts,
    eventFrom: rawEventFrom ?? defaultParams.eventFrom,
    dashboardId: rawDashboardId ?? defaultParams.dashboardId,
    dashboardName: rawDashboardName ?? defaultParams.dashboardName,
    dashboardDescription: rawDashboardDescription ?? defaultParams.dashboardDescription,
    dashboardVersion: rawDashboardVersion ?? defaultParams.dashboardVersion,
  };
}

export function urlDecodeTimeRange(
  searchParams: TreeParamsObject,
  defaultParams: QueryParams
): Pick<QueryParams, 'timeRange'> {
  return {
    timeRange: readTimeRange(
      searchParams[GET_PARAMS.fromTime]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultParams.timeRange.from,
      searchParams[GET_PARAMS.toTime]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultParams.timeRange.urlTo
    ),
  };
}

export function urlDecodePlots(
  searchParams: TreeParamsObject,
  keys: PlotKey[],
  defaultParams: QueryParams
): Pick<QueryParams, 'plots' | 'orderPlot'> {
  const orderPlot = uniqueArray([
    ...(searchParams[GET_PARAMS.orderPlot]?.[treeParamsObjectValueSymbol]?.[0]
      ?.split(orderPlotSplitter)
      .filter((s) => isPlotKey(s)) ?? defaultParams.orderPlot),
    ...keys,
  ]);
  const plots: Partial<Record<PlotKey, PlotParams>> = {};
  keys.forEach((key) => {
    const p = urlDecodePlot(
      key,
      searchParams[GET_PARAMS.plotPrefix + key] ?? (key === '0' ? searchParams : undefined),
      defaultParams.plots[key]
    );
    if (p) {
      plots[key] = p;
    } else {
      const remove = orderPlot.indexOf(key);
      if (remove > -1) {
        orderPlot.splice(remove, 1);
      }
    }
  });
  //fix event link
  orderPlot.forEach((pK) => {
    if (plots[pK]) {
      plots[pK]!.events = plots[pK]!.events.filter((eK) => !!plots[eK]);
    }
  });
  return { plots, orderPlot };
}

export function urlDecodePlot(
  plotKey: PlotKey,
  searchParams?: TreeParamsObject,
  defaultPlot: PlotParams = getNewPlot()
): PlotParams | undefined {
  if (searchParams?.[treeParamsObjectValueSymbol]?.[0] === removeValueChar) {
    return undefined;
  }
  const rawUseV2 = searchParams?.[GET_PARAMS.version]?.[treeParamsObjectValueSymbol]?.[0];
  const rawMaxHost = searchParams?.[GET_PARAMS.metricMaxHost]?.[treeParamsObjectValueSymbol]?.[0];
  const rawTotalLine = searchParams?.[GET_PARAMS.viewTotalLine]?.[treeParamsObjectValueSymbol]?.[0];
  const rawFilledGraph = searchParams?.[GET_PARAMS.viewFilledGraph]?.[treeParamsObjectValueSymbol]?.[0];
  const metricName = searchParams?.[GET_PARAMS.metricName]?.[treeParamsObjectValueSymbol]?.[0];
  const promQL = searchParams?.[GET_PARAMS.metricPromQL]?.[treeParamsObjectValueSymbol]?.[0];
  const type = toPlotType(searchParams?.[GET_PARAMS.metricType]?.[treeParamsObjectValueSymbol]?.[0], defaultPlot.type);
  return {
    id: plotKey,
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
    ...urlDecodePlotFilters(GET_PARAMS.metricFilter, searchParams, defaultPlot),
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
    type,
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
    timeShifts:
      searchParams?.[GET_PARAMS.metricLocalTimeShifts]?.[treeParamsObjectValueSymbol]
        ?.map(toNumberM)
        .filter(isNotNil)
        .sort() ?? defaultPlot.timeShifts,
  };
}

export function urlDecodePlotFilters(
  paramKey: string,
  searchParams?: TreeParamsObject,
  defaultFilter?: { filterIn: FilterTag; filterNotIn: FilterTag }
) {
  const filterIn: FilterTag = {};
  const filterNotIn: FilterTag = {};
  const filters = searchParams?.[paramKey]?.[treeParamsObjectValueSymbol];
  if (!filters) {
    return { filterIn: defaultFilter?.filterIn ?? filterIn, filterNotIn: defaultFilter?.filterNotIn ?? filterNotIn };
  }
  filters.forEach((s) => {
    const pos = s.indexOf(filterInSep);
    const pos2 = s.indexOf(filterNotInSep);
    if (pos2 === -1 || (pos2 > pos && pos > -1)) {
      const tagKey = toTagKey(freeKeyPrefix(String(s.substring(0, pos))));
      const tagValue = String(s.substring(pos + 1));
      if (tagKey && tagValue) {
        filterIn[tagKey] = sortEntity(uniqueArray([...(filterIn[tagKey] ?? []), tagValue]));
      }
    } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
      const tagKey = toTagKey(freeKeyPrefix(String(s.substring(0, pos2))));
      const tagValue = String(s.substring(pos2 + 1));
      if (tagKey && tagValue) {
        filterNotIn[tagKey] = sortEntity(uniqueArray([...(filterNotIn[tagKey] ?? []), tagValue]));
      }
    }
  });
  return { filterIn, filterNotIn };
}

export function urlDecodeGroups(
  searchParams: TreeParamsObject,
  keys: GroupKey[],
  defaultParams: QueryParams
): Pick<QueryParams, 'groups' | 'orderGroup'> {
  const orderGroup = uniqueArray([
    ...(searchParams[GET_PARAMS.orderGroup]?.[treeParamsObjectValueSymbol]?.[0]
      ?.split(orderGroupSplitter)
      .filter(isKeyId) ?? defaultParams.orderGroup),
    ...keys,
  ]);

  const groups: Partial<Record<string, GroupInfo>> = {};
  keys.forEach((groupKey) => {
    const g = urlDecodeGroup(
      groupKey,
      searchParams[GET_PARAMS.dashboardGroupInfoPrefix + groupKey],
      defaultParams.groups[groupKey]
    );
    if (g) {
      groups[groupKey] = g;
    } else {
      const remove = orderGroup.indexOf(groupKey);
      if (remove > -1) {
        orderGroup.splice(remove, 1);
      }
    }
  });

  return { groups, orderGroup };
}

export function urlDecodeGroup(
  groupKey: GroupKey,
  searchParams?: TreeParamsObject,
  defaultGroup: GroupInfo = getNewGroup()
): GroupInfo | undefined {
  if (searchParams?.[treeParamsObjectValueSymbol]?.[0] === removeValueChar) {
    return undefined;
  }
  const show = searchParams?.[GET_PARAMS.dashboardGroupInfoShow]?.[treeParamsObjectValueSymbol]?.[0];
  return {
    id: groupKey,
    name: searchParams?.[GET_PARAMS.dashboardGroupInfoName]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultGroup.name,
    description:
      searchParams?.[GET_PARAMS.dashboardGroupInfoDescription]?.[treeParamsObjectValueSymbol]?.[0] ??
      defaultGroup.description,
    count: toNumber(
      searchParams?.[GET_PARAMS.dashboardGroupInfoCount]?.[treeParamsObjectValueSymbol]?.[0],
      defaultGroup.count
    ),
    size: searchParams?.[GET_PARAMS.dashboardGroupInfoSize]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultGroup.size,
    show: show != null ? show !== '0' : defaultGroup.show,
  };
}

export function urlDecodeVariables(
  searchParams: TreeParamsObject,
  keys: VariableKey[],
  defaultParams: QueryParams
): Pick<QueryParams, 'variables' | 'orderVariables'> {
  const orderVariables = uniqueArray([
    ...(searchParams[GET_PARAMS.orderVariable]?.[treeParamsObjectValueSymbol]?.[0]
      ?.split(orderVariableSplitter)
      .filter(isKeyId) || defaultParams.orderVariables),
    ...keys,
  ]);

  const variables: Partial<Record<string, VariableParams>> = {};
  keys.forEach((variableKey) => {
    const v = urlDecodeVariable(
      variableKey,
      searchParams[GET_PARAMS.variablePrefix + variableKey],
      searchParams[GET_PARAMS.variableValuePrefix],
      defaultParams.variables[variableKey]
    );
    if (v) {
      variables[variableKey] = v;
    } else {
      const remove = orderVariables.indexOf(variableKey);
      if (remove > -1) {
        orderVariables.splice(remove, 1);
      }
    }
  });
  return { variables: variables, orderVariables };
}

export function urlDecodeVariable(
  variableKey: VariableKey,
  searchParamsConfig?: TreeParamsObject,
  searchParamsValues?: TreeParamsObject,
  defaultVariable: VariableParams = getNewVariable()
): VariableParams | undefined {
  if (searchParamsConfig?.[treeParamsObjectValueSymbol]?.[0] === removeValueChar) {
    return undefined;
  }
  const name =
    searchParamsConfig?.[GET_PARAMS.variableName]?.[treeParamsObjectValueSymbol]?.[0] ?? defaultVariable.name;
  const sourceKeyList = uniqueArray([
    ...Object.keys(searchParamsConfig ?? {})
      .filter((key) => key[0] === GET_PARAMS.variableSourcePrefix && !isNaN(+key[1]))
      .map((s) => String(s.slice(1)))
      .filter(isKeyId),
    ...defaultVariable.sourceOrder,
  ]);
  const sourceOrder = [...sourceKeyList];
  const source: Partial<Record<string, VariableParamsSource>> = {};
  sourceKeyList.forEach((sourceKey) => {
    const s = urlDecodeVariableSource(
      sourceKey,
      searchParamsConfig?.[GET_PARAMS.variableSourcePrefix + sourceKey],
      defaultVariable.source[sourceKey]
    );
    if (s) {
      source[sourceKey] = s;
    } else {
      const remove = sourceOrder.indexOf(sourceKey);
      if (remove > -1) {
        sourceOrder.splice(remove, 1);
      }
    }
  });
  const groupBy = searchParamsValues?.[name]?.[GET_PARAMS.variableGroupBy]?.[treeParamsObjectValueSymbol]?.[0];
  const negative = searchParamsValues?.[name]?.[GET_PARAMS.variableNegative]?.[treeParamsObjectValueSymbol]?.[0];
  const values = searchParamsValues?.[name]?.[treeParamsObjectValueSymbol];
  const link =
    searchParamsConfig?.[GET_PARAMS.variableLinkPlot]?.[treeParamsObjectValueSymbol]?.[0]
      ?.split('-')
      .map((s) => {
        const [p, t] = s.split('.', 2);
        return [p, toTagKey(t)];
      })
      .filter(isNotNilVariableLink) ?? defaultVariable.link.map((l) => [...l]);
  link.sort((a, b) => +a[0] - +b[0]);
  return {
    id: variableKey,
    name,
    description:
      searchParamsConfig?.[GET_PARAMS.variableDescription]?.[treeParamsObjectValueSymbol]?.[0] ??
      defaultVariable.description,
    link,
    source,
    sourceOrder,
    values: values?.[0] === removeValueChar ? [] : values ?? defaultVariable.values,
    groupBy: groupBy != null ? groupBy === '1' : defaultVariable.groupBy,
    negative: negative != null ? negative === '1' : defaultVariable.negative,
  };
}

export function urlDecodeVariableSource(
  sourceKey: VariableSourceKey,
  searchParamsConfig?: TreeParamsObject,
  defaultSource: VariableParamsSource = getNewVariableSource()
): VariableParamsSource | undefined {
  if (searchParamsConfig?.[treeParamsObjectValueSymbol]?.[0] === removeValueChar) {
    return undefined;
  }
  return {
    id: sourceKey,
    metric:
      searchParamsConfig?.[GET_PARAMS.variableSourceMetricName]?.[treeParamsObjectValueSymbol]?.[0] ??
      defaultSource.metric,
    tag: toTagKey(
      searchParamsConfig?.[GET_PARAMS.variableSourceTag]?.[treeParamsObjectValueSymbol]?.[0],
      defaultSource.tag
    ),
    ...urlDecodePlotFilters(GET_PARAMS.variableSourceFilter, searchParamsConfig, defaultSource),
  };
}
