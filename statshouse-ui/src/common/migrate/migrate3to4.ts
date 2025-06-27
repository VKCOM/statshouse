// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getNewMetricLayout, GroupKey, PlotKey, PlotParams, QueryParams } from '@/url2';
import { ProduceUpdate } from '@/store2/helpers';
import { toNumber } from '@/common/helpers';

export function migrate3to4(): ProduceUpdate<QueryParams> {
  return (params) => {
    if (params.version !== '4') {
      if (params.orderPlot?.length) {
        getMapGroupV3(params).forEach(({ groupKey, plotKeys }) => {
          setGroupAutoPosition(plotKeys, params.groups[groupKey]?.size ?? '2')(params);
          plotKeys.forEach((plotKey) => {
            if (params.plots[plotKey]) {
              params.plots[plotKey].group ??= groupKey;
            }
          });
        });
        params.orderPlot = [];
      }
      params.version = '4';
    }
  };
}

export function getMapGroupV3(params: Pick<QueryParams, 'orderPlot' | 'groups' | 'orderGroup'>) {
  const orderPlots = [...(params.orderPlot ?? [])];
  return params.orderGroup.map((groupKey) => ({
    groupKey,
    plotKeys: orderPlots.splice(0, params.groups[groupKey]?.count ?? 0),
  }));
}

export function setLayoutAutoPosition(
  groupKeys: GroupKey[],
  sizes: string[]
): ProduceUpdate<Pick<QueryParams, 'plots' | 'orderGroup' | 'groups'>> {
  return (params) => {
    const groupPlots = sortGroupPlots(groupKeys, params.plots);
    groupPlots.forEach(({ plotKeys }, groupIndex) => {
      setGroupAutoPosition(plotKeys, sizes[groupIndex])(params);
    });
  };
}
export function setGroupAutoPosition(
  plotKeys: PlotKey[],
  size: string
): ProduceUpdate<Pick<QueryParams, 'plots' | 'orderGroup'>> {
  return (params) => {
    let row = 0;
    let col = 0;
    plotKeys.forEach((plotKey) => {
      if (params.plots[plotKey]) {
        params.plots[plotKey].layout = getNewMetricLayout(params.plots[plotKey].type, size, { x: col, y: row });

        col = params.plots[plotKey].layout.x + params.plots[plotKey].layout.w;
        row = params.plots[plotKey].layout.y;
      }
    });
  };
}

export function getMapGroupPlotKeys(plots: Partial<Record<PlotKey, PlotParams>>) {
  return Object.values(plots)
    .sort((a, b) => {
      if (a && b) {
        return toLayoutSort(a) - toLayoutSort(b);
      }
      return 0;
    })
    .reduce(
      (res, plot) => {
        if (plot && plot.group != null) {
          res[plot.group] ??= {
            groupKey: plot.group,
            plotKeys: [],
          };
          res[plot.group]?.plotKeys.push(plot.id);
        }
        return res;
      },
      {} as Partial<Record<GroupKey, { groupKey: GroupKey; plotKeys: PlotKey[] }>>
    );
}

export function sortGroupPlots(orderGroup: GroupKey[], plots: Partial<Record<PlotKey, PlotParams>>) {
  const mapGroups = getMapGroupPlotKeys(plots);
  return orderGroup.map(
    (groupKey) =>
      mapGroups[groupKey] ?? {
        groupKey,
        plotKeys: [],
      }
  );
}

export function toLayoutSort({ group, layout }: Pick<PlotParams, 'group' | 'layout'>): number {
  if (group && layout) {
    return (toNumber(group, 0) << 24) + (layout.y << 8) + layout.x;
  }
  return 0;
}
export function addLeadSymbols(str: string = '', numberSymbol: number = 0, symbol: string = '0'): string {
  return (symbol.repeat(numberSymbol) + str).substring(-numberSymbol);
}

let fullSave = false;
export function setFullDashSave(toggle?: boolean) {
  fullSave = toggle ?? !fullSave;
}
export function getFullDashSave() {
  return fullSave;
}
