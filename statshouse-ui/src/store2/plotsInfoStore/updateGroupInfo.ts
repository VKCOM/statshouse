// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { GroupKey, PlotKey, QueryParams } from '@/url2';
import { isNotNil } from '@/common/helpers';

export function updateGroupInfo(params: QueryParams) {
  const orderPlots = [...params.orderPlot];
  const plotToGroupMap: Partial<Record<PlotKey, GroupKey>> = {};
  const groupPlots: Partial<Record<GroupKey, PlotKey[]>> = params.orderGroup.reduce(
    (res, groupKey) => {
      const group = params.groups[groupKey];
      if (group) {
        const plots = orderPlots.splice(0, group.count);
        res[groupKey] = plots;
        Object.assign(plotToGroupMap, Object.fromEntries(plots.map((p) => [p, groupKey])));
      }
      return res;
    },
    {} as Partial<Record<GroupKey, PlotKey[]>>
  );
  const viewOrderPlot = params.orderGroup
    .filter((g) => params.groups[g]?.show ?? true)
    .flatMap((g) => groupPlots[g])
    .filter(isNotNil);
  return {
    plotToGroupMap,
    groupPlots,
    viewOrderPlot,
  };
}
