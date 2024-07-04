// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { PlotParams } from 'url2';
import type { TagKey } from 'api/enum';

export function filterHasTagID(
  params: Pick<PlotParams, 'filterIn' | 'filterNotIn' | 'groupBy'>,
  tagKey: TagKey
): boolean {
  return (
    (params.filterIn[tagKey] !== undefined && params.filterIn[tagKey]?.length !== 0) ||
    (params.filterNotIn[tagKey] !== undefined && params.filterNotIn[tagKey]?.length !== 0) ||
    params.groupBy.indexOf(tagKey) >= 0
  );
}
