// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { FilterTag } from '../../queryParams';
import { GET_PARAMS } from '../../../api/enum';
import { filterInSep, filterNotInSep } from '../../constants';

export function metricFilterEncode(prefix: string, filterIn: FilterTag, filterNotIn: FilterTag): [string, string][] {
  const paramArr: [string, string][] = [];
  Object.entries(filterIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) => paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterInSep + valueTag]))
  );
  Object.entries(filterNotIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) =>
      paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterNotInSep + valueTag])
    )
  );
  return paramArr;
}
