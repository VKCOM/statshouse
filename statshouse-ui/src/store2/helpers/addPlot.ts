// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type GroupKey, type PlotParams, type QueryParams } from 'url2';
import { produce } from 'immer';
import { getNextPlotKey, updateQueryParamsPlotStruct } from '../urlStore/updateParamsPlotStruct';

export function addPlot(
  plot: PlotParams,
  params: QueryParams,
  groupKey?: GroupKey,
  activeInsert: boolean = true
): QueryParams {
  const tabNum = params.plots[params.tabNum] ? params.tabNum : params.orderPlot.slice(-1)[0];
  const nextId = getNextPlotKey(params);
  return produce<QueryParams>(
    { ...params, tabNum: activeInsert ? nextId : params.tabNum },
    updateQueryParamsPlotStruct((plotStruct) => {
      groupKey ??= plotStruct.mapPlotToGroup[tabNum] ?? '0';
      const groupIndex = plotStruct.mapGroupIndex[groupKey]!;
      if (plotStruct.groups[groupIndex]) {
        plotStruct.groups[groupIndex].plots.push({
          plotInfo: { ...plot, id: nextId },
          variableLinks: [],
        });
      }
    })
  );
}
