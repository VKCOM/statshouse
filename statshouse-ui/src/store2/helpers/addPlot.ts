// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  getNewMetricLayout,
  type GroupKey,
  type PlotParams,
  type QueryParams,
  type VariableKey,
  type VariableParamsLink,
} from '@/url2';
import { produce } from 'immer';
import { getNextPlotKey } from '../urlStore/updateParamsPlotStruct';
import { findGroupPositionLayout } from '@/store2/urlStore';
import { filterVariableByPlot } from '@/store2/helpers/filterVariableByPlot';

export function addPlot(
  plot: PlotParams,
  params: QueryParams,
  groupKey?: GroupKey,
  activeInsert: boolean = true
): QueryParams {
  const nextId = getNextPlotKey(params);
  const plotFilter = filterVariableByPlot(plot);
  const variableLinks: { id: VariableKey; link: VariableParamsLink[] }[] = [];
  params.orderVariables.forEach((vK) => {
    if (plotFilter(params.variables[vK])) {
      variableLinks.push({
        id: params.variables[vK]?.id,
        link: params.variables[vK]?.link
          .filter(([plotKey]) => plotKey === plot.id)
          .map(([_, tagKey]) => [nextId, tagKey]),
      });
    }
  });
  const activeGroupKey = params.plots[params.tabNum]?.group;
  groupKey ??= plot.group ?? activeGroupKey ?? params.orderGroup.slice(-1)[0] ?? '0';
  const size = params.groups[groupKey]?.size ?? '2';
  const nextPlot = {
    ...plot,
    id: nextId,
    group: groupKey,
    layout: findGroupPositionLayout(params, plot.layout ?? getNewMetricLayout(plot.type, size), groupKey),
  };
  return produce<QueryParams>(params, (p) => {
    p.plots[nextId] = nextPlot;
    if (activeInsert) {
      p.tabNum = nextId;
    }
    variableLinks.forEach(({ id, link }) => {
      p.variables[id]?.link.push(...link);
    });
  });
}
