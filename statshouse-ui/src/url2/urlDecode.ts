import {
  GroupInfo,
  GroupKey,
  QueryParams,
  VariableKey,
  VariableParams,
  VariableParamsSource,
  VariableSourceKey,
} from './queryParams';
import { isNotNil, toNumber, toNumberM, uniqueArray } from 'common/helpers';
import { GET_PARAMS, toTagKey } from 'api/enum';
import { getDefaultParams, getNewGroup, getNewVariable, getNewVariableSource } from './getDefault';
import { orderGroupSplitter, orderVariableSplitter, removeValueChar } from './constants';
import { isKeyId, isNotNilVariableLink, TreeParamsObject, treeParamsObjectValueSymbol } from './urlHelpers';
import { readTimeRange } from './timeRangeHelpers';
import { metricFilterDecode, widgetsParamsDecode } from './widgetsParams';

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
  const plots = widgetsParamsDecode(
    searchParams,
    uniqueArray([...plotKeys, ...defaultParams.orderPlot]),
    defaultParams
  );
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
    ...metricFilterDecode(GET_PARAMS.variableSourceFilter, searchParamsConfig, defaultSource),
  };
}
