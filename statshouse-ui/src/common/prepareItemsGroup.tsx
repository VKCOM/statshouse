// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DEFAULT_LAYOUT_COORDS } from '@/components2/Dashboard/constants';
import type { GroupKey, PlotKey, QueryParams } from '@/url2';
import type { Layout } from 'react-grid-layout';

export function prepareItemsGroup({
  orderGroup,
  orderPlot,
  groups,
}: Pick<QueryParams, 'orderGroup' | 'orderPlot' | 'groups'>) {
  const orderP = [...orderPlot];
  return orderGroup.map((groupKey) => {
    const plots = orderP.splice(0, groups[groupKey]?.count ?? 0);
    return {
      groupKey,
      plots,
    };
  });
}

type PrepareItemsGroupWithLayoutProps = {
  groups: QueryParams['groups'];
  orderGroup: QueryParams['orderGroup'];
  orderPlot: PlotKey[];
};

type PrepareItemsGroupWithLayoutResult = {
  itemsGroup: { groupKey: GroupKey; plots: PlotKey[] }[];
  layoutsCoords: { groupKey: GroupKey; layout: Layout[] }[];
};

export function prepareItemsGroupWithLayout({
  groups,
  orderGroup,
  orderPlot,
}: PrepareItemsGroupWithLayoutProps): PrepareItemsGroupWithLayoutResult {
  const orderP = [...orderPlot];
  const itemsGroup = orderGroup
    .filter((groupKey) => groups[groupKey]?.show !== false)
    .map((groupKey) => {
      const count = groups[groupKey]?.count ?? 0;
      const groupPlots = orderP.splice(0, count);

      return {
        groupKey,
        plots: groupPlots,
      };
    });

  const layoutsCoords = orderGroup
    .filter((groupKey) => groups[groupKey]?.show !== false)
    .map((groupKey) => ({
      groupKey,
      layout: groups[groupKey]?.layouts ?? [DEFAULT_LAYOUT_COORDS],
    }));

  return { itemsGroup, layoutsCoords };
}
