// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewMetric, type QueryParams, urlEncode } from '@/url2';

import { fixMessageTrouble } from '@/url/fixMessageTrouble';
import { addPlot } from '@/store2/helpers/addPlot';
import { selectorOrderPlot } from '@/store2/selectors';

export function getAddPlotLink(params: QueryParams, saveParams?: QueryParams): string {
  const orderPlot = selectorOrderPlot({ params });
  const tabNum = params.plots[params.tabNum] ? params.tabNum : orderPlot.slice(-1)[0];
  const nextParams = addPlot(params.plots[tabNum] ?? getNewMetric(), params);
  // const nextParams = produce<QueryParams>(
  //   { ...params, tabNum: nextId },
  //   updateQueryParamsPlotStruct((plotStruct) => {
  //     const groupKey = plotStruct.mapPlotToGroup[tabNum]!;
  //     const groupIndex = plotStruct.mapGroupIndex[groupKey]!;
  //     const plotIndex = plotStruct.mapPlotIndex[tabNum]!;
  //     const plotInfo = clonePlot(plotStruct.groups[groupIndex]?.plots[plotIndex].plotInfo) ?? getNewMetric();
  //     const variableLinks = plotStruct.groups[groupIndex]?.plots[plotIndex].variableLinks ?? [];
  //
  //     if (plotStruct.groups[groupIndex]) {
  //       plotStruct.groups[groupIndex].plots.push({
  //         plotInfo: {
  //           ...plotInfo,
  //           id: nextId,
  //         },
  //         variableLinks,
  //       });
  //     }
  //   })
  // );

  return fixMessageTrouble('?' + new URLSearchParams(urlEncode(nextParams, saveParams)).toString());
}
