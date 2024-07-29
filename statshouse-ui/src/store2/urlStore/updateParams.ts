// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { ProduceUpdate } from '../helpers';
import type { QueryParams } from 'url2';
import type { StatsHouseStore } from '../statsHouseStore';
import { produce } from 'immer';

export function updateParams(next: ProduceUpdate<QueryParams>): ProduceUpdate<StatsHouseStore> {
  return (s) => {
    s.params = produce(s.params, next);
  };
}
