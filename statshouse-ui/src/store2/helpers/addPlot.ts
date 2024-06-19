// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewPlotIndex, type GroupKey, type PlotParams, type QueryParams } from 'url2';
import { produce } from 'immer';
import { updateGroupInfo } from '../plotsInfoStore/updateGroupInfo';
import { updateGroupsPlot } from './updateGroupsPlot';

export function addPlot(
  plot: PlotParams,
  params: QueryParams,
  group?: GroupKey,
  activeInsert: boolean = true
): QueryParams {
  return produce(params, (p) => {
    const tabNum = p.plots[p.tabNum] ? p.tabNum : p.orderPlot.slice(-1)[0];
    const groupPlotMap = updateGroupInfo(p);
    const activeGroup = group ?? groupPlotMap.plotToGroupMap[tabNum] ?? p.orderGroup.slice(-1)[0];
    const newTabNum = getNewPlotIndex(p);
    p.plots[newTabNum] = { ...plot, id: newTabNum };
    const { groups, orderPlot } = updateGroupsPlot(
      produce(groupPlotMap, (gpm) => {
        gpm.groupPlots[activeGroup]?.push(newTabNum);
      }),
      p
    );
    p.orderPlot = orderPlot;
    p.groups = groups;
    if (activeInsert) {
      p.tabNum = newTabNum;
    }
  });
}
