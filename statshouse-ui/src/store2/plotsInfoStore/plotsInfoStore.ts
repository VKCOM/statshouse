// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type TagKey, TimeRangeAbbrev } from '@/api/enum';
import type { VariableKey } from '@/url2';
import type { StoreSlice } from '../createStore';
import { defaultBaseRange } from '../constants';
import { StatsHouseStore } from '../statsHouseStore';

export type PlotVariablesLink = Partial<
  Record<
    TagKey,
    {
      variableKey: VariableKey;
      variableName: string;
    }
  >
>;

export type PlotsInfoStore = {
  baseRange: TimeRangeAbbrev;
  setBaseRange(r: TimeRangeAbbrev): void;
};

export const plotsInfoStore: StoreSlice<StatsHouseStore, PlotsInfoStore> = (setState) => ({
  baseRange: defaultBaseRange,
  setBaseRange(r: TimeRangeAbbrev) {
    setState((s) => {
      s.baseRange = r;
    });
  },
});
