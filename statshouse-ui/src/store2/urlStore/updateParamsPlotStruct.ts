// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { GroupKey, QueryParams, VariableKey, VariableParams } from '@/url2';
import { getRandomKey, isNotNil, toNumberM } from '@/common/helpers';
import { selectorOrderPlot } from '@/store2/selectors';
import { GET_PARAMS } from '@/api/enum';

export function getNextVariableKey(params: Pick<QueryParams, 'orderVariables'>): VariableKey {
  return (Math.max(-1, ...params.orderVariables.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextVariableName(
  params: Pick<QueryParams, 'orderVariables' | 'variables'>,
  variableKey: VariableKey
): string {
  let name = `${GET_PARAMS.variableNamePrefix}${variableKey}`;
  let i = 1;
  while (params.orderVariables.some((vKey) => params.variables[vKey]?.name === name)) {
    if (i > 32) {
      name = `${GET_PARAMS.variableNamePrefix}${variableKey}_${getRandomKey()}`;
    } else {
      name = `${GET_PARAMS.variableNamePrefix}${variableKey}_${i}`;
    }
    i++;
  }
  return name;
}

export function getNextGroupKey(params: Pick<QueryParams, 'orderGroup'>): GroupKey {
  return (Math.max(-1, ...params.orderGroup.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextPlotKey(params: QueryParams): GroupKey {
  const orderPlot = selectorOrderPlot({ params });
  return (Math.max(-1, ...orderPlot.map(toNumberM).filter(isNotNil)) + 1).toString();
}

export function getNextVariableSourceKey(params: Pick<VariableParams, 'sourceOrder'>): VariableKey {
  return (Math.max(-1, ...params.sourceOrder.map(toNumberM).filter(isNotNil)) + 1).toString();
}
