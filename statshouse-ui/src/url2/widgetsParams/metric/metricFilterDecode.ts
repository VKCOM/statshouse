// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { freeKeyPrefix, TreeParamsObject, treeParamsObjectValueSymbol } from '../../urlHelpers';
import { FilterTag } from '../../queryParams';
import { filterInSep, filterNotInSep } from '../../constants';
import { toTagKey } from '../../../api/enum';
import { sortEntity, uniqueArray } from '../../../common/helpers';

export function metricFilterDecode(
  paramKey: string,
  searchParams?: TreeParamsObject,
  defaultFilter?: { filterIn: FilterTag; filterNotIn: FilterTag }
) {
  const filterIn: FilterTag = {};
  const filterNotIn: FilterTag = {};
  const filters = searchParams?.[paramKey]?.[treeParamsObjectValueSymbol];
  if (!filters) {
    return { filterIn: defaultFilter?.filterIn ?? filterIn, filterNotIn: defaultFilter?.filterNotIn ?? filterNotIn };
  }
  filters.forEach((s) => {
    const pos = s.indexOf(filterInSep);
    const pos2 = s.indexOf(filterNotInSep);
    if (pos2 === -1 || (pos2 > pos && pos > -1)) {
      const tagKey = toTagKey(freeKeyPrefix(String(s.substring(0, pos))));
      const tagValue = String(s.substring(pos + 1));
      if (tagKey && tagValue) {
        filterIn[tagKey] = sortEntity(uniqueArray([...(filterIn[tagKey] ?? []), tagValue]));
      }
    } else if (pos === -1 || (pos > pos2 && pos2 > -1)) {
      const tagKey = toTagKey(freeKeyPrefix(String(s.substring(0, pos2))));
      const tagValue = String(s.substring(pos2 + 1));
      if (tagKey && tagValue) {
        filterNotIn[tagKey] = sortEntity(uniqueArray([...(filterNotIn[tagKey] ?? []), tagValue]));
      }
    }
  });
  return { filterIn, filterNotIn };
}
