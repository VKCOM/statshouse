// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GroupInfo, QueryParams, VariableParams, VariableParamsSource } from './queryParams';
import { GET_PARAMS } from '@/api/enum';
import { dequal } from 'dequal/lite';

import { getDefaultParams, getNewGroup, getNewVariable, getNewVariableSource } from './getDefault';
import { orderGroupSplitter, orderVariableSplitter, removeValueChar } from './constants';
import { toGroupInfoPrefix, toVariablePrefix, toVariableValuePrefix } from './urlHelpers';
import { metricFilterEncode, widgetsParamsEncode } from './widgetsParams';
import { getFullDashSave } from '@/common/migrate/migrate3to4';

export function urlEncode(params: QueryParams, defaultParams?: QueryParams): [string, string][] {
  return [
    ...urlEncodeGlobalParam(params, defaultParams),
    ...urlEncodeTimeRange(params, defaultParams),
    ...widgetsParamsEncode(params, defaultParams),
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
    if (params.timeShifts.length === 0 && defaultParams.timeShifts.length > 0) {
      paramArr.push([GET_PARAMS.metricTimeShifts, removeValueChar]);
    } else {
      params.timeShifts.forEach((shift) => {
        paramArr.push([GET_PARAMS.metricTimeShifts, shift.toString()]);
      });
    }
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

  if (
    params.dashboardVersion &&
    params.dashboardCurrentVersion &&
    params.dashboardVersion !== params.dashboardCurrentVersion
  ) {
    paramArr.push([GET_PARAMS.dashboardVersion, params.dashboardVersion.toString()]);
  }

  if (getFullDashSave() && params.version) {
    paramArr.push([GET_PARAMS.dashboardSchemeVersion, params.version]);
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
  if (getFullDashSave()) {
    if (group.count != null && defaultGroup.count !== group.count) {
      paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoCount, group.count.toString()]);
    }
    if (group.size != null && defaultGroup.size !== group.size) {
      paramArr.push([prefix + GET_PARAMS.dashboardGroupInfoSize, group.size]);
    }
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
    paramArr.push(...metricFilterEncode(prefix, source.filterIn, source.filterNotIn));
  }

  if (!paramArr.length && !defaultSource.id) {
    paramArr.push([prefix + GET_PARAMS.variableSourceMetricName, source.metric]);
  }

  return paramArr;
}
