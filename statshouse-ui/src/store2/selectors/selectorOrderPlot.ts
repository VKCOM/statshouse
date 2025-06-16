// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createSelector } from 'reselect';
import type { StatsHouseStore } from '@/store2';
import { selectorMapGroupPlotKeys } from '@/store2/selectors/selectorMapGroupPlotKeys';

export const selectorOrderPlot = createSelector(
  [(s: Pick<StatsHouseStore, 'params'>) => s.params.orderGroup, selectorMapGroupPlotKeys],
  (orderGroup, mapGroupPlotKeys) => orderGroup.flatMap((groupKey) => mapGroupPlotKeys[groupKey]?.plotKeys ?? [])
);

export const selectorOrderPlotShow = createSelector(
  [
    (s: Pick<StatsHouseStore, 'params'>) => s.params.orderGroup,
    (s: Pick<StatsHouseStore, 'params'>) => s.params.groups,
    selectorMapGroupPlotKeys,
  ],
  (orderGroup, groups, mapGroupPlotKeys) =>
    orderGroup
      .filter((groupKey) => groups[groupKey]?.show)
      .flatMap((groupKey) => mapGroupPlotKeys[groupKey]?.plotKeys ?? [])
);
