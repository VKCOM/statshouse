// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {
  getNewMetricLayout,
  type GroupKey,
  type PlotKey,
  type PlotParams,
  type QueryParams,
  type VariableKey,
  type VariableParamsLink,
} from '@/url2';
import { getNextPlotKey } from '@/store2/urlStore/updateParamsPlotStruct';
import { filterVariableByPlot } from '@/store2/helpers/filterVariableByPlot';
import { findGroupPositionLayout } from '@/store2/urlStore';
import { produce } from 'immer';

export function addPlots(
  plots: PlotParams[],
  params: QueryParams,
  groupKey?: GroupKey,
  activeFirstInsert: boolean = true
) {
  let nextParams = params;
  const mapInsertIndex: Record<PlotKey, PlotKey> = {};
  const needMapEvent: PlotKey[] = [];
  const firstInsertId = getNextPlotKey(nextParams);
  plots.forEach((plot) => {
    nextParams = produce<QueryParams>(nextParams, (p) => {
      const nextId = getNextPlotKey(p);
      const plotFilter = filterVariableByPlot(plot);
      const variableLinks: { id: VariableKey; link: VariableParamsLink[] }[] = [];
      p.orderVariables.forEach((vK) => {
        if (plotFilter(p.variables[vK])) {
          variableLinks.push({
            id: p.variables[vK]?.id,
            link: p.variables[vK]?.link
              .filter(([plotKey]) => plotKey === plot.id)
              .map(([_, tagKey]) => [nextId, tagKey]),
          });
        }
      });
      const activeGroupKey = p.plots[p.tabNum]?.group;
      groupKey ??= plot.group ?? activeGroupKey ?? p.orderGroup.slice(-1)[0] ?? '0';
      const size = p.groups[groupKey]?.size ?? '2';
      const nextPlot: PlotParams = {
        ...plot,
        id: nextId,
        group: groupKey,
        layout: findGroupPositionLayout(p, plot.layout ?? getNewMetricLayout(plot.type, size), groupKey),
      };
      mapInsertIndex[plot.id] = nextId;

      // if insert plot wish events and events plots
      if (
        plot.events.length > 0 &&
        plots.length > 0 &&
        plot.events.every((eventKey) => !!plots.find((p) => p.id === eventKey))
      ) {
        needMapEvent.push(nextId);
      }

      p.plots[nextPlot.id] = nextPlot;
      variableLinks.forEach(({ id, link }) => {
        p.variables[id]?.link.push(...link);
      });
    });
  });

  return produce<QueryParams>(nextParams, (p) => {
    //remap events key
    needMapEvent.forEach((plotKey) => {
      if (p.plots[plotKey]) {
        p.plots[plotKey].events = p.plots[plotKey].events.map((eventKey) => mapInsertIndex[eventKey]);
      }
    });
    if (activeFirstInsert) {
      p.tabNum = firstInsertId;
    }
  });
}
