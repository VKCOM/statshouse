// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotKey, QueryParams } from 'url2';
import { produce } from 'immer';
import { updateGroupsPlot } from './updateGroupsPlot';
import { updateGroupInfo } from '../plotsInfoStore/updateGroupInfo';

export function removePlot(plotKey: PlotKey, params: QueryParams) {
  return produce(params, (p) => {
    p.orderPlot = p.orderPlot.filter((p) => p !== plotKey);
    p.orderVariables.forEach((variableKey) => {
      const variable = p.variables[variableKey];
      if (variable) {
        const link = variable.link.filter(([pKey]) => plotKey !== pKey);
        if (variable.link.length !== link.length) {
          variable.link = link;
        }
      }
    });
    const groupPlotMap = updateGroupInfo(p);
    const plotGroup = groupPlotMap.plotToGroupMap[plotKey];
    if (plotGroup) {
      const { orderPlot, groups } = updateGroupsPlot(
        produce(groupPlotMap, (gpm) => {
          if (gpm.groupPlots[plotGroup]) {
            gpm.groupPlots[plotGroup] = gpm.groupPlots[plotGroup]?.filter((p) => p !== plotKey);
          }
        }),
        p
      );
      p.orderPlot = orderPlot;
      p.groups = groups;
    }
    delete p.plots[plotKey];
  });
}
