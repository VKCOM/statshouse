// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { createSelector } from 'reselect';
import type { StatsHouseStore } from '@/store2';
import { getMapGroupPlotKeys } from '@/common/migrate/migrate3to4';

export const selectorMapGroupPlotKeys = createSelector(
  [(s: Pick<StatsHouseStore, 'params'>) => s.params.plots],
  getMapGroupPlotKeys
);
