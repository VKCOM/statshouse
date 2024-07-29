// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { TimeRangeKeysTo } from 'api/enum';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '../statsHouseStore';
import { readTimeRange } from 'url2';

export function updateTimeRange(from: number, to: number | TimeRangeKeysTo): ProduceUpdate<StatsHouseStore> {
  return (s) => {
    s.params.timeRange = readTimeRange(from, to);
  };
}
