// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { QueryParams } from 'url2';
import { produce } from 'immer';
import { isNotNil } from 'common/helpers';
import { type PlotsInfoStore } from '../plotsInfoStore';

export function updateGroupsPlot(groupPlotsMap: Pick<PlotsInfoStore, 'groupPlots'>, params: QueryParams): QueryParams {
  return produce<QueryParams>(params, (p) => {
    p.orderPlot = p.orderGroup
      .flatMap((g) => {
        const group = p.groups[g];
        if (group) {
          group.count = groupPlotsMap.groupPlots[g]?.length ?? 0;
        }
        return groupPlotsMap.groupPlots[g];
      })
      .filter(isNotNil);
  });
}
