// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewPlot, type QueryParams, urlEncode } from 'url2';
import { deepClone } from 'common/helpers';
import { getNextPlotKey, updateQueryParamsPlotStruct } from '../urlStore/updateParamsPlotStruct';
import { produce } from 'immer';

export function getAddPlotLink(params: QueryParams, saveParams?: QueryParams): string {
  const tabNum = params.plots[params.tabNum] ? params.tabNum : params.orderPlot.slice(-1)[0];
  const nextId = getNextPlotKey(params);

  const nextParams = produce<QueryParams>(
    { ...params, tabNum: nextId },
    updateQueryParamsPlotStruct((plotStruct) => {
      const groupKey = plotStruct.mapPlotToGroup[tabNum]!;
      const groupIndex = plotStruct.mapGroupIndex[groupKey]!;
      const plotIndex = plotStruct.mapPlotIndex[tabNum]!;
      const { plotInfo, variableLinks } = deepClone(plotStruct.groups[groupIndex]?.plots[plotIndex]) ?? {
        plotInfo: getNewPlot(),
        variableLinks: [],
      };
      if (plotStruct.groups[groupIndex]) {
        plotStruct.groups[groupIndex].plots.push({
          plotInfo: {
            ...plotInfo,
            id: nextId,
          },
          variableLinks,
        });
      }
    })
  );

  return '?' + new URLSearchParams(urlEncode(nextParams, saveParams)).toString();
}
